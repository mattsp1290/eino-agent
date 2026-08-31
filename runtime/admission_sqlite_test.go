package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestAdmissionSQLiteReplaysFrozenClockPairsAfterReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "store.db")
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
	}()
	frozenAt := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	var responseNumber int
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			responseNumber++
			return []*einoschema.Message{einoschema.AssistantMessage(fmt.Sprintf("answer-%d", responseNumber), nil)}, nil
		})}),
		WithIDGenerator(&reverseAdmissionIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithClock(func() time.Time { return frozenAt }),
		WithOwnerID("sqlite-admission-test"),
	)
	if err != nil {
		t.Fatal(err)
	}

	const sessionID session.ID = "frozen-session"
	metadata := map[string]string{"source": "sqlite-integration"}
	prompts := []string{"first prompt", "  héllo 世界\n"}
	var originalSession session.Session
	for index, prompt := range prompts {
		handle, err := orchestrator.Start(ctx, Request{SessionID: sessionID, Message: UserMessage{Content: prompt}, Config: orchestratorConfig(), Metadata: metadata})
		if err != nil {
			t.Fatalf("Start %d: %v", index+1, err)
		}
		result := <-handle.Done()
		if result.Error != nil || result.Status != session.RunCompleted {
			t.Fatalf("run %d result = %+v", index+1, result)
		}
		storedSession, err := store.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			originalSession = storedSession
		} else if !reflect.DeepEqual(storedSession, originalSession) {
			t.Fatalf("session changed across admissions:\nfirst=%#v\nsecond=%#v", originalSession, storedSession)
		}
	}
	beforeDrift, err := store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orchestrator.Start(ctx, Request{
		SessionID: sessionID,
		Message:   UserMessage{Content: "identity drift"},
		Config:    orchestratorConfig(),
		Metadata:  map[string]string{"source": "changed"},
	}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("identity drift error = %v, want ErrConflict", err)
	}
	afterDrift, err := store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 100})
	if err != nil || !reflect.DeepEqual(afterDrift, beforeDrift) {
		t.Fatalf("history after identity drift = %#v, %v; want %#v", afterDrift, err, beforeDrift)
	}
	storedSession, err := store.GetSession(ctx, sessionID)
	if err != nil || !reflect.DeepEqual(storedSession, originalSession) {
		t.Fatalf("session after identity drift = %#v, %v; want %#v", storedSession, err, originalSession)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	providerHistory, err := LoadHistory(ctx, reopened, sessionID, history.Options{IncludeReasoning: false})
	if err != nil {
		t.Fatal(err)
	}
	wantContent := []string{"first prompt", "answer-1", "  héllo 世界\n", "answer-2"}
	if len(providerHistory) != len(wantContent) {
		t.Fatalf("history = %#v, want %d messages", providerHistory, len(wantContent))
	}
	for index, want := range wantContent {
		if providerHistory[index].Content != want {
			t.Fatalf("history[%d] = %q, want %q", index, providerHistory[index].Content, want)
		}
	}

	var rawMessages []session.Message
	cursor := session.ReplayCursor{Limit: 1}
	for {
		page, err := reopened.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			t.Fatal(err)
		}
		rawMessages = append(rawMessages, page.Messages...)
		if page.Next == (session.ReplayCursor{}) {
			break
		}
		cursor = page.Next
	}
	wantIDs := []session.MessageID{"z-user-1", "a-assistant-1", "z-user-2", "a-assistant-2"}
	if len(rawMessages) != len(wantIDs) {
		t.Fatalf("raw messages = %#v", rawMessages)
	}
	for index, want := range wantIDs {
		if rawMessages[index].ID != want {
			t.Fatalf("raw message order = %#v, want %#v", rawMessages, wantIDs)
		}
	}
}

func TestAdmissionSQLiteRollsBackAfterUserPartWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	firstRequest := testRunAdmission()
	first, err := (admitter{Store: store, Clock: func() time.Time { return now }}).admit(ctx, firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Execution(session.RunFence{RunID: first.Run.ID, ClaimToken: first.Run.ClaimToken}).SettleRun(ctx, session.SettleRunRequest{
		Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: now.Add(time.Second)},
		Event:      session.RunSettlementEvent{ID: "first-finished", MessageID: first.AssistantMessage.ID},
	}); err != nil {
		t.Fatal(err)
	}
	beforeSession, err := store.GetSession(ctx, first.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeHistory, err := store.ListMessages(ctx, first.Session.ID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	beforeEvents, err := store.ListEvents(ctx, first.Session.ID, session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	beforeEpochs, err := store.ListContextEpochs(ctx, first.Session.ID)
	if err != nil {
		t.Fatal(err)
	}

	secondRequest := testRunAdmission()
	secondRequest.IDs.RunID = "run-2"
	secondRequest.IDs.UserMessageID = "user-2"
	secondRequest.IDs.UserPartID = "user-part-2"
	secondRequest.IDs.AssistantMessageID = "assistant-2"
	secondRequest.IDs.ContextEpochID = "epoch-2"
	secondRequest.IDs.EventID = "event-2"
	secondRequest.IDs.RunClaimToken = "claim-run-2"
	secondRequest.UserMessage.Content = "second prompt"
	_, err = (admitter{Store: &failingAdmissionStore{Store: store}, Clock: func() time.Time { return now }}).admit(ctx, secondRequest)
	if !errors.Is(err, errInjectedSecondAdmissionMessage) {
		t.Fatalf("Admit error = %v, want injected second-message failure", err)
	}
	afterSession, err := store.GetSession(ctx, first.Session.ID)
	if err != nil || !reflect.DeepEqual(afterSession, beforeSession) {
		t.Fatalf("session after rollback = %#v, %v; want %#v", afterSession, err, beforeSession)
	}
	afterHistory, err := store.ListMessages(ctx, first.Session.ID, session.ReplayCursor{Limit: 100})
	if err != nil || !reflect.DeepEqual(afterHistory, beforeHistory) {
		t.Fatalf("history after rollback = %#v, %v; want %#v", afterHistory, err, beforeHistory)
	}
	afterEvents, err := store.ListEvents(ctx, first.Session.ID, session.EventCursor{Limit: 100})
	if err != nil || !reflect.DeepEqual(afterEvents, beforeEvents) {
		t.Fatalf("events after rollback = %#v, %v; want %#v", afterEvents, err, beforeEvents)
	}
	afterEpochs, err := store.ListContextEpochs(ctx, first.Session.ID)
	if err != nil || !reflect.DeepEqual(afterEpochs, beforeEpochs) {
		t.Fatalf("epochs after rollback = %#v, %v; want %#v", afterEpochs, err, beforeEpochs)
	}
	if _, err := store.GetRun(ctx, secondRequest.IDs.RunID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("failed run persisted: %v", err)
	}
}

type reverseAdmissionIDs struct {
	mu                    sync.Mutex
	runs, messages, parts int
	toolCalls, events     int
	epochs                int
}

func (s *reverseAdmissionIDs) next(counter *int, prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	*counter = *counter + 1
	return fmt.Sprintf("%s-%d", prefix, *counter)
}

func (s *reverseAdmissionIDs) NewRunID() session.RunID {
	return session.RunID(s.next(&s.runs, "run"))
}

func (s *reverseAdmissionIDs) NewMessageID() session.MessageID {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages++
	turn := (s.messages + 1) / 2
	if s.messages%2 == 1 {
		return session.MessageID(fmt.Sprintf("z-user-%d", turn))
	}
	return session.MessageID(fmt.Sprintf("a-assistant-%d", turn))
}

func (s *reverseAdmissionIDs) NewPartID() session.PartID {
	return session.PartID(s.next(&s.parts, "part"))
}

func (s *reverseAdmissionIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next(&s.toolCalls, "tool-call"))
}

func (s *reverseAdmissionIDs) NewEventID() session.EventID {
	return session.EventID(s.next(&s.events, "event"))
}

func (s *reverseAdmissionIDs) NewEpochID() session.EpochID {
	return session.EpochID(s.next(&s.epochs, "epoch"))
}

var errInjectedSecondAdmissionMessage = errors.New("injected second admission message failure")

type failingAdmissionStore struct {
	session.Store
}

func (s *failingAdmissionStore) WithinTx(ctx context.Context, fn func(context.Context, session.Store) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		return fn(ctx, &failingAdmissionTx{Store: tx})
	})
}

type failingAdmissionTx struct {
	session.Store
	appendMessages int
}

func (s *failingAdmissionTx) Execution(fence session.RunFence) session.ExecutionStore {
	return &failingAdmissionExecution{ExecutionStore: s.Store.Execution(fence), tx: s}
}

type failingAdmissionExecution struct {
	session.ExecutionStore
	tx *failingAdmissionTx
}

func (s *failingAdmissionExecution) AppendMessage(ctx context.Context, message session.Message) (session.Message, error) {
	s.tx.appendMessages++
	if s.tx.appendMessages == 2 {
		return session.Message{}, errInjectedSecondAdmissionMessage
	}
	return s.ExecutionStore.AppendMessage(ctx, message)
}

var _ session.Store = (*failingAdmissionStore)(nil)
var _ session.Store = (*failingAdmissionTx)(nil)
var _ session.ExecutionStore = (*failingAdmissionExecution)(nil)
