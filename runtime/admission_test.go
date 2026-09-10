package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestAdmitPersistsDurableRecordsBeforeExecution(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	admitter := admitter{Store: store, Clock: func() time.Time { return now }}
	request := testRunAdmission()
	admitted, err := admitter.admit(context.Background(), request)
	if err != nil {
		t.Fatalf("Admit error = %v", err)
	}
	if admitted.Session.ID != "session-1" || admitted.Run.ID != "run-1" || admitted.UserMessage.ID != "user-1" || admitted.UserPart.ID != "user-part-1" || admitted.AssistantMessage.ID != "assistant-1" {
		t.Fatalf("admitted identity = %+v", admitted)
	}
	if admitted.Run.ParentMsgID != admitted.UserMessage.ID || admitted.AssistantMessage.ParentID != admitted.UserMessage.ID {
		t.Fatalf("admission parentage = run %q assistant %q user %q", admitted.Run.ParentMsgID, admitted.AssistantMessage.ParentID, admitted.UserMessage.ID)
	}
	if admitted.UserMessage.SessionID != admitted.Session.ID || admitted.UserMessage.RunID != admitted.Run.ID || admitted.UserMessage.ParentID != "" || admitted.UserMessage.Agent != "" || admitted.UserMessage.ModelID != "" {
		t.Fatalf("user envelope = %#v", admitted.UserMessage)
	}
	if _, err := store.GetSession(context.Background(), "session-1"); err != nil {
		t.Fatalf("session was not durable: %v", err)
	}
	if _, err := store.GetRun(context.Background(), "run-1"); err != nil {
		t.Fatalf("run was not durable: %v", err)
	}
	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(batch.Messages) != 2 || batch.Messages[0].Role != session.RoleUser || batch.Messages[1].Role != session.RoleAssistant {
		t.Fatalf("messages = %#v", batch.Messages)
	}
	if len(batch.Parts) != 1 || batch.Parts[0].SessionID != admitted.Session.ID || batch.Parts[0].RunID != admitted.Run.ID || batch.Parts[0].MessageID != admitted.UserMessage.ID || batch.Parts[0].Ordinal != 0 || string(batch.Parts[0].Payload) != `{"text":"hello"}` {
		t.Fatalf("parts = %#v", batch.Parts)
	}
	if !admitted.AssistantMessage.CreatedAt.Equal(admitted.UserMessage.CreatedAt.Add(time.Nanosecond)) {
		t.Fatalf("message times = user %s assistant %s", admitted.UserMessage.CreatedAt, admitted.AssistantMessage.CreatedAt)
	}
	events, err := store.ListEvents(context.Background(), "session-1", session.EventCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events.Events) != 1 || events.Events[0].Kind != string(EventRunStarted) {
		t.Fatalf("events = %#v", events.Events)
	}
	epochs, err := store.ListContextEpochs(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("list context epochs: %v", err)
	}
	if len(epochs) != 1 || epochs[0].ID != "epoch-1" || epochs[0].Trigger != "turn" || epochs[0].Reason != "run_admission" {
		t.Fatalf("epochs = %#v", epochs)
	}
	if !reflect.DeepEqual(admitted.Event, events.Events[0]) {
		t.Fatalf("admitted event = %#v, want canonical %#v", admitted.Event, events.Events[0])
	}
	request.Config.Agent.Options["temperature"] = "changed"
	request.UserMessage.Content = "changed"
	if admitted.Snapshot.Config.Agent.Options["temperature"] != "0.2" {
		t.Fatalf("snapshot config mutated: %#v", admitted.Snapshot.Config.Agent.Options)
	}
	if admitted.Snapshot.Messages[0].Content != "hello" {
		t.Fatalf("snapshot messages mutated: %#v", admitted.Snapshot.Messages[0])
	}
	if string(admitted.UserPart.Payload) != `{"text":"hello"}` {
		t.Fatalf("persisted user part mutated: %s", admitted.UserPart.Payload)
	}
}

func TestAdmitRequiresContextEpochID(t *testing.T) {
	t.Parallel()

	request := testRunAdmission()
	request.IDs.ContextEpochID = ""
	_, err := (admitter{Store: newAdmissionStore()}).admit(context.Background(), request)
	if !errors.Is(err, ErrInvalidAdmission) {
		t.Fatalf("Admit error = %v, want ErrInvalidAdmission", err)
	}
}

func TestAdmissionAcceptsAndPreservesStoredTitle(t *testing.T) {
	store := newAdmissionStore()
	request := testRunAdmission()
	frozen, err := freezeAdmission(request)
	if err != nil {
		t.Fatal(err)
	}
	stored := admissionSession(frozen, time.Now().UTC().Add(-time.Hour))
	stored.Title = "custom title"
	store.sessions[stored.ID] = stored
	got, err := getOrCreateAdmissionSession(context.Background(), store, frozen, time.Now().UTC())
	if err != nil || got.Title != stored.Title || !got.UpdatedAt.Equal(stored.UpdatedAt) {
		t.Fatalf("admission session = %#v, error = %v", got, err)
	}
}

func TestAdmitRejectsCollidingGeneratedIDsBeforeStoreUse(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	request := testRunAdmission()
	request.IDs.UserPartID = session.PartID(request.IDs.UserMessageID)
	_, err := (admitter{Store: store}).admit(context.Background(), request)
	if !errors.Is(err, ErrInvalidAdmission) {
		t.Fatalf("Admit error = %v, want ErrInvalidAdmission", err)
	}
	if store.listMessagesCalls.Load() != 0 || len(store.sessions) != 0 || len(store.runs) != 0 {
		t.Fatalf("colliding IDs touched store: list=%d sessions=%d runs=%d", store.listMessagesCalls.Load(), len(store.sessions), len(store.runs))
	}
}

func TestAdmitBuildsProviderInputFromFencedHistoryAndCurrentMessage(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	request := testRunAdmission()
	frozenRequest, err := freezeAdmission(request)
	if err != nil {
		t.Fatal(err)
	}
	priorAt := time.Unix(1, 0).UTC()
	store.sessions[request.IDs.SessionID] = admissionSession(frozenRequest, priorAt)
	store.messages["prior-user"] = session.Message{
		ID: "prior-user", SessionID: request.IDs.SessionID, RunID: "prior-run",
		Role: session.RoleUser, CreatedAt: priorAt, UpdatedAt: priorAt,
	}
	store.parts["prior-part"] = session.Part{
		ID: "prior-part", SessionID: request.IDs.SessionID, RunID: "prior-run", MessageID: "prior-user",
		Kind: session.PartText, Payload: mustJSON(map[string]string{"text": "prior"}), CreatedAt: priorAt, UpdatedAt: priorAt,
	}
	store.listMessagesHook = func(tx *admissionStore, _ session.ID) {
		if _, ok := tx.runs[request.IDs.RunID]; !ok {
			t.Fatal("history loaded before AdmitRun established the fence")
		}
	}

	admitted, err := (admitter{Store: store, Clock: func() time.Time { return priorAt }}).admit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.Snapshot.Messages) != 2 || admitted.Snapshot.Messages[0].Content != "prior" || admitted.Snapshot.Messages[1].Content != "hello" {
		t.Fatalf("provider messages = %#v, want prior then current user", admitted.Snapshot.Messages)
	}
	if !admitted.UserMessage.CreatedAt.Equal(priorAt.Add(time.Nanosecond)) || !admitted.AssistantMessage.CreatedAt.Equal(priorAt.Add(2*time.Nanosecond)) {
		t.Fatalf("admission times = user %s assistant %s, want after prior %s", admitted.UserMessage.CreatedAt, admitted.AssistantMessage.CreatedAt, priorAt)
	}
}

func TestAdmitPersistsResolvedWorkspaceAcrossSymlinkRetarget(t *testing.T) {
	parent := t.TempDir()
	first := filepath.Join(parent, "first")
	second := filepath.Join(parent, "second")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "workspace")
	if err := os.Symlink(first, alias); err != nil {
		t.Fatal(err)
	}
	request := testRunAdmission()
	request.Config.Metadata["workspace_root"] = alias
	admitted, err := (admitter{Store: newAdmissionStore()}).admit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(first)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Run.Config["workspace_root"] != resolved || admitted.Snapshot.Config.Metadata["workspace_root"] != resolved {
		t.Fatalf("workspace identity = run %q snapshot %q, want %q", admitted.Run.Config["workspace_root"], admitted.Snapshot.Config.Metadata["workspace_root"], resolved)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(second, alias); err != nil {
		t.Fatal(err)
	}
	if admitted.Run.Config["workspace_root"] != resolved {
		t.Fatalf("retarget changed persisted root to %q", admitted.Run.Config["workspace_root"])
	}
}

func TestAdmitRejectsNonexistentWorkspace(t *testing.T) {
	request := testRunAdmission()
	request.Config.Metadata["workspace_root"] = filepath.Join(t.TempDir(), "missing")
	if _, err := (admitter{Store: newAdmissionStore()}).admit(context.Background(), request); err == nil {
		t.Fatal("expected nonexistent workspace rejection")
	}
}

func TestAdmitRejectsDuplicateActiveRun(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	admitter := admitter{Store: store, Clock: func() time.Time { return time.Unix(1, 0) }}
	request := testRunAdmission()
	if _, err := admitter.admit(context.Background(), request); err != nil {
		t.Fatalf("first Admit error = %v", err)
	}
	_, err := admitter.admit(context.Background(), request)
	if !errors.Is(err, session.ErrConflict) {
		t.Fatalf("duplicate Admit error = %v, want ErrConflict", err)
	}
}

func TestAdmitHistoryProjectionFailureHasNoNewDurableOrLiveSideEffects(t *testing.T) {
	store := newAdmissionStore()
	request := testRunAdmission()
	frozenRequest, err := freezeAdmission(request)
	if err != nil {
		t.Fatal(err)
	}
	store.sessions[request.IDs.SessionID] = admissionSession(frozenRequest, time.Unix(1, 0))
	store.messages["history-user"] = session.Message{
		ID:        "history-user",
		SessionID: request.IDs.SessionID,
		RunID:     "history-run",
		Role:      session.RoleUser,
		CreatedAt: time.Unix(1, 0),
		UpdatedAt: time.Unix(1, 0),
	}
	store.parts["history-part"] = session.Part{
		ID:        "history-part",
		SessionID: request.IDs.SessionID,
		RunID:     "history-run",
		MessageID: "history-user",
		Kind:      session.PartText,
		Payload:   []byte(`{"text":`),
	}
	_, err = (admitter{Store: store}).admit(context.Background(), request)
	if err == nil {
		t.Fatal("Admit error = nil, want history projection failure")
	}
	if len(store.sessions) != 1 || len(store.runs) != 0 || len(store.messages) != 1 || len(store.parts) != 1 || len(store.events) != 0 || len(store.epochs) != 0 {
		t.Fatalf("projection failure mutated state: sessions=%d runs=%d messages=%d parts=%d events=%d epochs=%d", len(store.sessions), len(store.runs), len(store.messages), len(store.parts), len(store.events), len(store.epochs))
	}
}

func TestAdmitRejectsEveryRepeatedRunID(t *testing.T) {
	tests := map[string]func(*admissionRequest){
		"session":           func(r *admissionRequest) { r.IDs.SessionID = "other-session" },
		"epoch":             func(r *admissionRequest) { r.IDs.ContextEpochID = "other-epoch" },
		"user message id":   func(r *admissionRequest) { r.IDs.UserMessageID = "other-user" },
		"user part id":      func(r *admissionRequest) { r.IDs.UserPartID = "other-part" },
		"assistant message": func(r *admissionRequest) { r.IDs.AssistantMessageID = "other-assistant" },
		"config":            func(r *admissionRequest) { r.Config.Agent.Mode = "other-mode" },
		"model": func(r *admissionRequest) {
			r.Config.Model.ModelID = "other-model"
			r.Model.Model.ID = "other-model"
		},
		"message": func(r *admissionRequest) { r.UserMessage.Content = "other-input" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			store := newAdmissionStore()
			admitter := admitter{Store: store}
			request := testRunAdmission()
			if _, err := admitter.admit(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			mutate(&request)
			if _, err := admitter.admit(context.Background(), request); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("retry error = %v, want ErrConflict", err)
			}
		})
	}
}

func TestAdmitRollsBackDurableRecordsWhenTransactionalAdmissionFails(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	store.appendEventErr = errors.New("append event failed")
	admitter := admitter{Store: store}
	_, err := admitter.admit(context.Background(), testRunAdmission())
	if !errors.Is(err, store.appendEventErr) {
		t.Fatalf("Admit error = %v, want append event failure", err)
	}
	if _, err := store.GetRun(context.Background(), "run-1"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("run leaked after rollback err = %v, want ErrNotFound", err)
	}
	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("ListMessages error = %v", err)
	}
	if len(batch.Messages) != 0 || len(batch.Parts) != 0 || len(store.sessions) != 0 || len(store.epochs) != 0 || len(store.events) != 0 {
		t.Fatalf("admission leaked after rollback: messages=%#v parts=%#v sessions=%d epochs=%d events=%d", batch.Messages, batch.Parts, len(store.sessions), len(store.epochs), len(store.events))
	}
}

func TestAdmitRollsBackUserAndSessionWhenUserPartFails(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	store.appendPartErrAt = 1
	_, err := (admitter{Store: store}).admit(context.Background(), testRunAdmission())
	if err == nil {
		t.Fatal("Admit error = nil, want injected user-part failure")
	}
	if len(store.sessions) != 0 || len(store.runs) != 0 || len(store.messages) != 0 || len(store.parts) != 0 || len(store.events) != 0 || len(store.epochs) != 0 {
		t.Fatalf("failed admission leaked state: sessions=%d runs=%d messages=%d parts=%d events=%d epochs=%d", len(store.sessions), len(store.runs), len(store.messages), len(store.parts), len(store.events), len(store.epochs))
	}
}

func TestAdmitRejectsIncompleteResolvedModelBeforeStoreUse(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	request := testRunAdmission()
	request.Model = model.Resolved{}
	_, err := (admitter{Store: store}).admit(context.Background(), request)
	if !errors.Is(err, ErrInvalidAdmission) {
		t.Fatalf("Admit error = %v, want ErrInvalidAdmission", err)
	}
	if store.getRunCalls.Load() != 0 || len(store.sessions) != 0 || len(store.runs) != 0 || len(store.messages) != 0 || len(store.events) != 0 {
		t.Fatalf("invalid admission touched store: sessions=%d runs=%d messages=%d events=%d", len(store.sessions), len(store.runs), len(store.messages), len(store.events))
	}
}

func testRunAdmission() admissionRequest {
	selection := model.Selection{ProviderID: "openai", ModelID: "gpt-4.1"}
	return admissionRequest{
		IDs: admissionIDs{
			SessionID:          "session-1",
			RunID:              "run-1",
			UserMessageID:      "user-1",
			UserPartID:         "user-part-1",
			AssistantMessageID: "assistant-1",
			ContextEpochID:     "epoch-1",
			EventID:            "event-1",
			RunClaimToken:      "claim-run-1",
		},
		UserMessage: UserMessage{Content: "hello"},
		Config: config.Snapshot{
			Agent: config.Agent{
				Name:         "default",
				Model:        selection,
				SystemPrompt: "system",
				Options:      map[string]string{"temperature": "0.2"},
			},
			Model: selection,
			Metadata: map[string]string{
				"workspace_id":   "workspace-1",
				"workspace_root": os.TempDir(),
			},
		},
		Model: model.Resolved{
			Provider: model.Provider{ID: "openai"},
			Model:    model.Descriptor{ID: "gpt-4.1", ProviderID: "openai"},
			Streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, nil }),
		},
		OwnerID:       "owner-1",
		LeaseDuration: time.Minute,
		Metadata:      map[string]string{"request": "admission"},
		ExtensionPlan: emptyTestPlanDescriptor(),
	}
}

type capturingSink struct {
	mu     sync.Mutex
	events []session.EventRecord
}

func (s *capturingSink) Emit(_ context.Context, event session.EventRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *capturingSink) snapshot() []session.EventRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]session.EventRecord(nil), s.events...)
}

func (s *capturingSink) waitFor(t testing.TB, count int) []session.EventRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		events := s.snapshot()
		if len(events) >= count {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink events = %#v, want at least %d", events, count)
		}
		time.Sleep(time.Millisecond)
	}
}

func (s *capturingSink) waitForKind(t testing.TB, kind string, count int) []session.EventRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		events := s.snapshot()
		if countEvents(events, kind) >= count {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink events = %#v, want at least %d of kind %q", events, count, kind)
		}
		time.Sleep(time.Millisecond)
	}
}
