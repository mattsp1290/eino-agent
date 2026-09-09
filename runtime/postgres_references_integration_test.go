//go:build postgres_integration

package runtime

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

func testPostgresRuntimeOptionalReferences(t *testing.T, server *testpostgres.Server) {
	f := newPostgresRuntimeFixture(t, server)
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	wantSession := session.Session{ID: "optional-session", ParentID: "missing-session", CreatedAt: now, UpdatedAt: now}
	if _, err := f.store.CreateSession(f.ctx, wantSession); err != nil {
		t.Fatal(err)
	}
	run, err := f.store.AdmitRun(f.ctx, session.Run{
		ID: "optional-run", SessionID: wantSession.ID, ParentRunID: "missing-run",
		ParentMsgID: "future-user", ContextEpoch: "future-epoch", OwnerID: "owner", ClaimToken: "claim",
		Status: session.RunPending, CreatedAt: now,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := session.Message{ID: "message", SessionID: run.SessionID, RunID: run.ID, ParentID: "missing-message", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}
	if _, err := execution.AppendMessage(f.ctx, message); err != nil {
		t.Fatal(err)
	}
	epoch := session.ContextEpoch{
		ID: "epoch", SessionID: run.SessionID, ParentEpochID: "missing-epoch",
		SummaryMessageID: "missing-summary", SummarizedFromID: "missing-from", SummarizedToID: "missing-to", TailStartID: "missing-tail", CreatedAt: now,
	}
	if _, err := execution.StartContextEpoch(f.ctx, epoch); err != nil {
		t.Fatal(err)
	}
	request := session.ModelRequestRecord{
		ID: "request", SessionID: run.SessionID, RunID: run.ID, AssistantMessageID: "future-assistant",
		State: session.ModelRequestPrepared, Messages: json.RawMessage(`[]`), Tools: json.RawMessage(`[]`),
		SafeCallConfig: json.RawMessage(`{}`), CreatedAt: now, UpdatedAt: now,
	}
	if _, err := execution.CreateModelRequest(f.ctx, request); err != nil {
		t.Fatal(err)
	}
	event := session.EventRecord{
		ID: "event", SessionID: run.SessionID, RunID: run.ID, MessageID: "missing-event-message",
		PartID: "missing-event-part", EpochID: "missing-event-epoch", Kind: "reference-proof", Payload: json.RawMessage(`{}`), CreatedAt: now,
	}
	if _, err := execution.AppendEvent(f.ctx, event); err != nil {
		t.Fatal(err)
	}

	f.reopen(t)
	if got, err := f.store.GetSession(f.ctx, wantSession.ID); err != nil || !reflect.DeepEqual(got, wantSession) {
		t.Fatalf("session optional parent changed: %v", err)
	}
	if got, err := f.store.GetRun(f.ctx, run.ID); err != nil || !reflect.DeepEqual(got, run) {
		t.Fatalf("run optional references changed: %v", err)
	}
	if got, err := f.store.GetMessage(f.ctx, message.ID); err != nil || !reflect.DeepEqual(got, message) {
		t.Fatalf("message optional parent changed: %v", err)
	}
	if got, err := f.store.ListContextEpochs(f.ctx, run.SessionID); err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], epoch) {
		t.Fatalf("epoch optional references changed: %v", err)
	}
	if got, err := f.store.GetModelRequest(f.ctx, request.ID); err != nil || !reflect.DeepEqual(got, request) {
		t.Fatalf("unmaterialized assistant correlation changed: %v", err)
	}
	if got, err := f.store.ListEvents(f.ctx, run.SessionID, session.EventCursor{Limit: 10}); err != nil || len(got.Events) != 1 || !reflect.DeepEqual(got.Events[0], event) {
		t.Fatalf("event optional references changed: %v", err)
	}
	if _, err := f.store.GetMessage(f.ctx, request.AssistantMessageID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("model correlation fabricated an assistant: %v", err)
	}
	// Counts also reject fabricated placeholder rows for every missing parent.
	for table, want := range map[string]int{"sessions": 1, "runs": 1, "messages": 1, "parts": 0, "context_epochs": 1, "model_requests": 1, "events": 1, "tool_calls": 0} {
		var count int
		if err := f.db.QueryRowContext(f.ctx, "SELECT count(*) FROM public."+table).Scan(&count); err != nil || count != want {
			t.Fatalf("%s rows = %d, want %d: %v", table, count, want, err)
		}
	}
}
