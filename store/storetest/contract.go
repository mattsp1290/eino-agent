package storetest

import (
	"context"
	"errors"
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

// Factory creates an isolated store subject for one test.
type Factory func(testing.TB) Subject

// Run executes the durable store contract suite against a store implementation.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("atomic run ownership", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-ownership")
		first := run("run-1", s.ID, "owner-1")
		admitted, err := subject.Store.AdmitRun(ctx, first)
		if err != nil {
			t.Fatalf("admit first run: %v", err)
		}
		if admitted.Status != session.RunPending {
			t.Fatalf("admitted status = %q, want %q", admitted.Status, session.RunPending)
		}
		_, err = subject.Store.AdmitRun(ctx, run("run-2", s.ID, "owner-2"))
		if !errors.Is(err, session.ErrSessionBusy) {
			t.Fatalf("admit second active run err = %v, want ErrSessionBusy", err)
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
	})

	t.Run("replay ordering is stable by message and part order", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-replay")
		r := admitRun(t, ctx, subject.Store, run("run-replay", s.ID, "owner"))
		msg1 := appendMessage(t, ctx, subject.Store, message("msg-1", s.ID, r.ID, session.RoleUser))
		msg2 := appendMessage(t, ctx, subject.Store, message("msg-2", s.ID, r.ID, session.RoleAssistant))
		appendPart(t, ctx, subject.Store, part("prt-2", msg2.ID, s.ID, r.ID, 2))
		appendPart(t, ctx, subject.Store, part("prt-1", msg1.ID, s.ID, r.ID, 1))
		appendPart(t, ctx, subject.Store, part("prt-3", msg2.ID, s.ID, r.ID, 3))
		batch, err := subject.Store.ListMessages(ctx, s.ID, session.ReplayCursor{Limit: 10})
		if err != nil {
			t.Fatalf("list messages: %v", err)
		}
		if got := ids(batch.Messages); got != "msg-1,msg-2" {
			t.Fatalf("message order = %s, want msg-1,msg-2", got)
		}
		if got := partIDs(batch.Parts); got != "prt-1,prt-2,prt-3" {
			t.Fatalf("part order = %s, want prt-1,prt-2,prt-3", got)
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
		claimed.Status = session.ToolCallCompleted
		claimed.Output = []byte(`{"text":"ok"}`)
		claimed.CompletedAt = time.Now()
		if err := subject.Store.FinishToolCall(ctx, claimed); err != nil {
			t.Fatalf("finish tool call: %v", err)
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
			ID:        "evt-1",
			SessionID: s.ID,
			RunID:     r.ID,
			Kind:      "run_started",
			CreatedAt: time.Now(),
		}
		if _, err := subject.Store.AppendEvent(ctx, event); err != nil {
			t.Fatalf("append event: %v", err)
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
		if len(events.Events) != 1 || events.Events[0].ID != event.ID {
			t.Fatalf("events = %#v, want evt-1", events.Events)
		}
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
	switch len(messages) {
	case 0:
		return ""
	case 1:
		return string(messages[0].ID)
	}
	out := string(messages[0].ID)
	for _, msg := range messages[1:] {
		out += "," + string(msg.ID)
	}
	return out
}

func partIDs(parts []session.Part) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return string(parts[0].ID)
	}
	out := string(parts[0].ID)
	for _, part := range parts[1:] {
		out += "," + string(part.ID)
	}
	return out
}

func containsRun(runs []session.Run, id session.RunID) bool {
	for _, run := range runs {
		if run.ID == id {
			return true
		}
	}
	return false
}
