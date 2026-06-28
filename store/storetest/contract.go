package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// Subject is one store implementation under test.
type Subject struct {
	Store      session.Store
	Transactor session.Transactor
	Cleanup    func()
}

// Factory creates an isolated store subject for one test. Each call must return
// a fresh store namespace so subtests do not share durable state.
type Factory func(testing.TB) Subject

// Run executes the durable store contract suite against a store implementation.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("atomic run ownership", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-ownership")
		const contenders = 8
		start := make(chan struct{})
		type result struct {
			run session.Run
			err error
		}
		results := make(chan result, contenders)
		var wg sync.WaitGroup
		for i := range contenders {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				r, err := subject.Store.AdmitRun(ctx, run(session.RunID(fmt.Sprintf("run-%02d", i)), s.ID, fmt.Sprintf("owner-%02d", i)))
				results <- result{run: r, err: err}
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		var admitted session.Run
		var successes int
		var busy int
		for result := range results {
			switch {
			case result.err == nil:
				successes++
				admitted = result.run
			case errors.Is(result.err, session.ErrSessionBusy):
				busy++
			default:
				t.Fatalf("unexpected concurrent admit err: %v", result.err)
			}
		}
		if successes != 1 || busy != contenders-1 {
			t.Fatalf("successes=%d busy=%d, want 1/%d", successes, busy, contenders-1)
		}
		admitted.Status = session.RunInterrupted
		admitted.FinishedAt = admitted.CreatedAt.Add(time.Minute)
		if err := subject.Store.FinishRun(ctx, admitted); err != nil {
			t.Fatalf("finish first run: %v", err)
		}
		if _, err := subject.Store.AdmitRun(ctx, run("run-3", s.ID, "owner-3")); err != nil {
			t.Fatalf("admit after terminal run: %v", err)
		}
	})

	t.Run("transaction rollback hides writes", func(t *testing.T) {
		subject := setup(t, factory)
		if subject.Transactor == nil {
			t.Skip("store does not expose session.Transactor")
		}
		ctx := context.Background()
		if err := subject.Transactor.WithinTx(ctx, func(ctx context.Context, tx session.Tx) error {
			s, err := tx.CreateSession(ctx, sessionRecord("committed"))
			if err != nil {
				return err
			}
			r, err := tx.AdmitRun(ctx, run("committed-run", s.ID, "owner"))
			if err != nil {
				return err
			}
			_, err = tx.AppendEvent(ctx, eventRecord("committed-event", s.ID, r.ID, 1))
			return err
		}); err != nil {
			t.Fatalf("commit transaction: %v", err)
		}
		if _, err := subject.Store.GetSession(ctx, "committed"); err != nil {
			t.Fatalf("committed session missing: %v", err)
		}
		errRollback := errors.New("rollback")
		err := subject.Transactor.WithinTx(ctx, func(ctx context.Context, tx session.Tx) error {
			_, err := tx.CreateSession(ctx, sessionRecord("rolled-back"))
			if err != nil {
				return err
			}
			return errRollback
		})
		if !errors.Is(err, errRollback) {
			t.Fatalf("transaction err = %v, want rollback sentinel", err)
		}
		if _, err := subject.Store.GetSession(ctx, "rolled-back"); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("rolled back session err = %v, want ErrNotFound", err)
		}
		assertPanicRollback(t, ctx, subject)
	})

	t.Run("replay ordering is stable by message and part order", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-replay")
		r := admitRun(t, ctx, subject.Store, run("run-replay", s.ID, "owner"))
		msg1 := appendMessage(t, ctx, subject.Store, message("msg-z", s.ID, r.ID, session.RoleUser))
		msg2 := appendMessage(t, ctx, subject.Store, message("msg-a", s.ID, r.ID, session.RoleAssistant))
		appendPart(t, ctx, subject.Store, part("prt-z", msg2.ID, s.ID, r.ID, 20))
		appendPart(t, ctx, subject.Store, part("prt-m", msg1.ID, s.ID, r.ID, 10))
		appendPart(t, ctx, subject.Store, part("prt-a", msg2.ID, s.ID, r.ID, 30))
		batch, err := subject.Store.ListMessages(ctx, s.ID, session.ReplayCursor{Limit: 10})
		if err != nil {
			t.Fatalf("list messages: %v", err)
		}
		if got := ids(batch.Messages); got != "msg-z,msg-a" {
			t.Fatalf("message order = %s, want msg-z,msg-a", got)
		}
		if got := partIDs(batch.Parts); got != "prt-m,prt-z,prt-a" {
			t.Fatalf("part order = %s, want prt-m,prt-z,prt-a", got)
		}
		first, err := subject.Store.ListMessages(ctx, s.ID, session.ReplayCursor{Limit: 1})
		if err != nil {
			t.Fatalf("list first replay page: %v", err)
		}
		second, err := subject.Store.ListMessages(ctx, s.ID, first.Next)
		if err != nil {
			t.Fatalf("list second replay page: %v", err)
		}
		if got := ids(append(first.Messages, second.Messages...)); got != "msg-z,msg-a" {
			t.Fatalf("paged message order = %s, want msg-z,msg-a", got)
		}
	})

	t.Run("compatible duplicate writes are idempotent and conflicts are rejected", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := sessionRecord("session-idempotent")
		first, err := subject.Store.CreateSession(ctx, s)
		if err != nil {
			t.Fatalf("create session: %v", err)
		}
		again, err := subject.Store.CreateSession(ctx, s)
		if err != nil {
			t.Fatalf("repeat create session: %v", err)
		}
		if again.ID != first.ID {
			t.Fatalf("repeat create returned %s, want %s", again.ID, first.ID)
		}
		conflict := s
		conflict.Title = "different"
		if _, err := subject.Store.CreateSession(ctx, conflict); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting duplicate session err = %v, want ErrConflict", err)
		}
		r := admitRun(t, ctx, subject.Store, run("run-idempotent", s.ID, "owner"))
		msg := message("msg-idempotent", s.ID, r.ID, session.RoleUser)
		if _, err := subject.Store.AppendMessage(ctx, msg); err != nil {
			t.Fatalf("append message: %v", err)
		}
		if _, err := subject.Store.AppendMessage(ctx, msg); err != nil {
			t.Fatalf("repeat append message: %v", err)
		}
		changed := msg
		changed.Role = session.RoleAssistant
		if _, err := subject.Store.AppendMessage(ctx, changed); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting duplicate message err = %v, want ErrConflict", err)
		}
	})

	t.Run("tool call create claim and settlement are single owner", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-tools")
		r := admitRun(t, ctx, subject.Store, run("run-tools", s.ID, "owner"))
		msg := appendMessage(t, ctx, subject.Store, message("msg-tools", s.ID, r.ID, session.RoleAssistant))
		call := session.ToolCall{
			ID:        "call-1",
			SessionID: s.ID,
			RunID:     r.ID,
			MessageID: msg.ID,
			Name:      "file_read",
			Status:    session.ToolCallPending,
			RetrySafe: true,
		}
		if _, err := subject.Store.CreateToolCall(ctx, call); err != nil {
			t.Fatalf("create tool call: %v", err)
		}
		call.ClaimedBy = "worker-1"
		call.ClaimToken = "claim-1"
		call.LeaseUntil = time.Now().Add(time.Minute)
		claimed, err := subject.Store.ClaimToolCall(ctx, call)
		if err != nil {
			t.Fatalf("claim tool call: %v", err)
		}
		if claimed.Status != session.ToolCallRunning {
			t.Fatalf("claimed status = %q, want %q", claimed.Status, session.ToolCallRunning)
		}
		call.ClaimedBy = "worker-2"
		call.ClaimToken = "claim-2"
		if _, err := subject.Store.ClaimToolCall(ctx, call); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("second claim err = %v, want ErrConflict", err)
		}
		stolen := claimed
		stolen.ClaimedBy = "worker-2"
		stolen.ClaimToken = "claim-2"
		stolen.Status = session.ToolCallCompleted
		stolen.CompletedAt = time.Now()
		if err := subject.Store.FinishToolCall(ctx, stolen); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("finish with wrong claim err = %v, want ErrConflict", err)
		}
		claimed.Status = session.ToolCallCompleted
		claimed.Output = []byte(`{"text":"ok"}`)
		claimed.CompletedAt = time.Now()
		if err := subject.Store.FinishToolCall(ctx, claimed); err != nil {
			t.Fatalf("finish tool call: %v", err)
		}
		if err := subject.Store.FinishToolCall(ctx, claimed); err != nil {
			t.Fatalf("idempotent finish tool call: %v", err)
		}
		conflictingFinish := claimed
		conflictingFinish.Output = []byte(`{"text":"changed"}`)
		if err := subject.Store.FinishToolCall(ctx, conflictingFinish); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting repeat finish err = %v, want ErrConflict", err)
		}
		conflictingFinish = claimed
		conflictingFinish.Status = session.ToolCallFailed
		if err := subject.Store.FinishToolCall(ctx, conflictingFinish); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting status finish err = %v, want ErrConflict", err)
		}
		conflictingFinish = claimed
		conflictingFinish.Error = "different error"
		if err := subject.Store.FinishToolCall(ctx, conflictingFinish); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting error finish err = %v, want ErrConflict", err)
		}
		conflictingFinish = claimed
		conflictingFinish.Metadata = map[string]string{"output_status": "changed"}
		if err := subject.Store.FinishToolCall(ctx, conflictingFinish); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting metadata finish err = %v, want ErrConflict", err)
		}
		conflictingFinish = claimed
		conflictingFinish.CompletedAt = claimed.CompletedAt.Add(time.Second)
		if err := subject.Store.FinishToolCall(ctx, conflictingFinish); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting completion time finish err = %v, want ErrConflict", err)
		}
		if unfinished, err := subject.Store.ListUnfinishedToolCalls(ctx, r.ID); err != nil || len(unfinished) != 0 {
			t.Fatalf("unfinished calls = %d, err = %v; want none", len(unfinished), err)
		}
	})

	t.Run("unfinished runs and events are discoverable for recovery", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-recovery")
		r := admitRun(t, ctx, subject.Store, run("run-recovery", s.ID, "owner"))
		event := session.EventRecord{
			ID:          "evt-1",
			SessionID:   s.ID,
			RunID:       r.ID,
			ProviderID:  "provider",
			ModelID:     "model",
			Kind:        "run_started",
			Correlation: "corr-1",
			Usage: session.Usage{
				InputTokens: 3,
			},
			Redaction: session.RedactionMetadata,
			CreatedAt: time.Now(),
		}
		if _, err := subject.Store.AppendEvent(ctx, event); err != nil {
			t.Fatalf("append event: %v", err)
		}
		if _, err := subject.Store.AppendEvent(ctx, event); err != nil {
			t.Fatalf("repeat append event: %v", err)
		}
		if _, err := subject.Store.AppendEvent(ctx, eventRecord("evt-2", s.ID, r.ID, 2)); err != nil {
			t.Fatalf("append second event: %v", err)
		}
		runs, err := subject.Store.ListUnfinishedRuns(ctx)
		if err != nil {
			t.Fatalf("list unfinished runs: %v", err)
		}
		if !containsRun(runs, r.ID) {
			t.Fatalf("unfinished runs did not include %s", r.ID)
		}
		events, err := subject.Store.ListEvents(ctx, s.ID, session.EventCursor{Limit: 10})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		if got := eventIDs(events.Events); got != "evt-1,evt-2" {
			t.Fatalf("events = %s, want evt-1,evt-2", got)
		}
		if events.Events[0].ProviderID != "provider" || events.Events[0].Usage.InputTokens != 3 || events.Events[0].Redaction != session.RedactionMetadata {
			t.Fatalf("event projection lost first-class fields: %#v", events.Events[0])
		}
		first, err := subject.Store.ListEvents(ctx, s.ID, session.EventCursor{Limit: 1})
		if err != nil {
			t.Fatalf("list first event page: %v", err)
		}
		next, err := subject.Store.ListEvents(ctx, s.ID, first.Next)
		if err != nil {
			t.Fatalf("list next event page: %v", err)
		}
		if len(first.Events) != 1 || first.Events[0].ID != "evt-1" || len(next.Events) != 1 || next.Events[0].ID != "evt-2" {
			t.Fatalf("next events = %#v, want evt-2", next.Events)
		}
		later := time.Now().UTC()
		earlier := later.Add(-time.Minute)
		epoch, err := subject.Store.StartContextEpoch(ctx, session.ContextEpoch{
			ID:               "epoch-2",
			SessionID:        s.ID,
			SummaryMessageID: "summary",
			SummarizedFromID: "msg-1",
			SummarizedToID:   "msg-1",
			TailStartID:      "msg-2",
			ModelID:          "model",
			ProviderID:       "provider",
			Trigger:          "manual",
			Reason:           "contract",
			NextAction:       session.EpochNextStop,
			CreatedAt:        later,
		})
		if err != nil {
			t.Fatalf("start context epoch: %v", err)
		}
		duplicate, err := subject.Store.StartContextEpoch(ctx, epoch)
		if err != nil {
			t.Fatalf("duplicate start context epoch: %v", err)
		}
		if duplicate.ID != epoch.ID {
			t.Fatalf("duplicate epoch = %#v, want %#v", duplicate, epoch)
		}
		if _, err := subject.Store.StartContextEpoch(ctx, session.ContextEpoch{
			ID:        "epoch-1",
			SessionID: s.ID,
			Trigger:   "manual",
			Reason:    "earlier",
			CreatedAt: earlier,
		}); err != nil {
			t.Fatalf("start earlier context epoch: %v", err)
		}
		epoch.ClosedAt = epoch.CreatedAt.Add(time.Minute)
		if err := subject.Store.FinishContextEpoch(ctx, epoch); err != nil {
			t.Fatalf("finish context epoch: %v", err)
		}
		reader, ok := subject.Store.(session.ContextEpochReader)
		if !ok {
			t.Fatal("store does not expose session.ContextEpochReader")
		}
		epochs, err := reader.ListContextEpochs(ctx, s.ID)
		if err != nil {
			t.Fatalf("list context epochs: %v", err)
		}
		if len(epochs) != 2 || epochs[0].ID != "epoch-1" || epochs[1].ID != "epoch-2" || epochs[1].SummaryMessageID != "summary" || epochs[1].ClosedAt.IsZero() {
			t.Fatalf("epochs = %#v, want chronological epoch-1 then finished epoch-2", epochs)
		}
	})
}

// RunTransactional executes the transaction-specific portion of the contract.
// Transactional backends should call both Run and RunTransactional.
func RunTransactional(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("transactions", func(t *testing.T) {
		subject := setup(t, factory)
		if subject.Transactor == nil {
			t.Fatal("factory returned nil Transactor")
		}
		ctx := context.Background()
		if err := subject.Transactor.WithinTx(ctx, func(ctx context.Context, tx session.Tx) error {
			s, err := tx.CreateSession(ctx, sessionRecord("tx-committed"))
			if err != nil {
				return err
			}
			r, err := tx.AdmitRun(ctx, run("tx-run", s.ID, "owner"))
			if err != nil {
				return err
			}
			_, err = tx.AppendEvent(ctx, eventRecord("tx-event", s.ID, r.ID, 1))
			return err
		}); err != nil {
			t.Fatalf("commit transaction: %v", err)
		}
		if _, err := subject.Store.GetSession(ctx, "tx-committed"); err != nil {
			t.Fatalf("committed session missing: %v", err)
		}
		assertPanicRollback(t, ctx, subject)
	})
}

func setup(t testing.TB, factory Factory) Subject {
	t.Helper()
	subject := factory(t)
	if subject.Store == nil {
		t.Fatal("factory returned nil Store")
	}
	t.Cleanup(func() {
		if subject.Cleanup != nil {
			subject.Cleanup()
		}
	})
	return subject
}

func assertPanicRollback(t testing.TB, ctx context.Context, subject Subject) {
	t.Helper()
	if subject.Transactor == nil {
		return
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("transaction panic was not rethrown")
			}
		}()
		_ = subject.Transactor.WithinTx(ctx, func(ctx context.Context, tx session.Tx) error {
			_, err := tx.CreateSession(ctx, sessionRecord("panic-rolled-back"))
			if err != nil {
				return err
			}
			panic("rollback")
		})
	}()
	if _, err := subject.Store.GetSession(ctx, "panic-rolled-back"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("panic rolled back session err = %v, want ErrNotFound", err)
	}
}

func createSession(t testing.TB, ctx context.Context, st session.Store, id session.ID) session.Session {
	t.Helper()
	s, err := st.CreateSession(ctx, sessionRecord(id))
	if err != nil {
		t.Fatalf("create session %s: %v", id, err)
	}
	return s
}

func admitRun(t testing.TB, ctx context.Context, st session.Store, r session.Run) session.Run {
	t.Helper()
	admitted, err := st.AdmitRun(ctx, r)
	if err != nil {
		t.Fatalf("admit run %s: %v", r.ID, err)
	}
	return admitted
}

func appendMessage(t testing.TB, ctx context.Context, st session.Store, msg session.Message) session.Message {
	t.Helper()
	got, err := st.AppendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("append message %s: %v", msg.ID, err)
	}
	return got
}

func appendPart(t testing.TB, ctx context.Context, st session.Store, p session.Part) session.Part {
	t.Helper()
	got, err := st.AppendPart(ctx, p)
	if err != nil {
		t.Fatalf("append part %s: %v", p.ID, err)
	}
	return got
}

func sessionRecord(id session.ID) session.Session {
	now := time.Now().UTC()
	return session.Session{
		ID:        id,
		Directory: "/workspace",
		Title:     string(id),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func run(id session.RunID, sessionID session.ID, owner string) session.Run {
	now := time.Now().UTC()
	return session.Run{
		ID:         id,
		SessionID:  sessionID,
		OwnerID:    owner,
		LeaseUntil: now.Add(time.Minute),
		Agent:      "default",
		ProviderID: "provider",
		ModelID:    "model",
		Status:     session.RunPending,
		CreatedAt:  now,
	}
}

func message(id session.MessageID, sessionID session.ID, runID session.RunID, role session.Role) session.Message {
	now := time.Now().UTC()
	return session.Message{
		ID:        id,
		SessionID: sessionID,
		RunID:     runID,
		Role:      role,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func part(id session.PartID, messageID session.MessageID, sessionID session.ID, runID session.RunID, ordinal int64) session.Part {
	now := time.Now().UTC()
	return session.Part{
		ID:        id,
		MessageID: messageID,
		SessionID: sessionID,
		RunID:     runID,
		Kind:      session.PartText,
		Ordinal:   ordinal,
		Payload:   []byte(`{"text":"part"}`),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func ids(messages []session.Message) string {
	values := make([]string, 0, len(messages))
	for _, msg := range messages {
		values = append(values, string(msg.ID))
	}
	return strings.Join(values, ",")
}

func partIDs(parts []session.Part) string {
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, string(part.ID))
	}
	return strings.Join(values, ",")
}

func eventIDs(events []session.EventRecord) string {
	values := make([]string, 0, len(events))
	for _, event := range events {
		values = append(values, string(event.ID))
	}
	return strings.Join(values, ",")
}

func containsRun(runs []session.Run, id session.RunID) bool {
	for _, run := range runs {
		if run.ID == id {
			return true
		}
	}
	return false
}

func eventRecord(id session.EventID, sessionID session.ID, runID session.RunID, offset int) session.EventRecord {
	return session.EventRecord{
		ID:        id,
		SessionID: sessionID,
		RunID:     runID,
		Kind:      "event",
		Payload:   json.RawMessage(`{"ok":true}`),
		CreatedAt: time.Now().UTC().Add(time.Duration(offset) * time.Second),
	}
}
