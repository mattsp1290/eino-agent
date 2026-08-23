package tools_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/sqlite"
	"github.com/mattsp1290/eino-agent/tools"
)

func TestBuildToolSettlementIsAcceptedByAtomicStore(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	now := time.Now().UTC()
	if _, err := store.CreateSession(ctx, session.Session{ID: "session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdmitRun(ctx, session.Run{ID: "run", SessionID: "session", OwnerID: "worker", Status: session.RunPending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendMessage(ctx, session.Message{ID: "assistant", SessionID: "session", RunID: "run", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	durable, err := store.CreateToolCall(ctx, session.ToolCall{
		ID: "call", SessionID: "session", RunID: "run", MessageID: "assistant",
		ResultMessageID: "result-message", ResultPartID: "result-part", Name: "echo", Status: session.ToolCallPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	durable.ClaimedBy = "worker"
	durable.ClaimToken = "claim"
	durable, err = store.ClaimToolCall(ctx, durable)
	if err != nil {
		t.Fatal(err)
	}
	tool := runtime.Tool{Retention: runtime.RetentionPolicy{MaxInlineBytes: 100}}
	call := runtime.ToolCall{
		ID: durable.ID, SessionID: durable.SessionID, RunID: durable.RunID, MessageID: durable.MessageID,
		ResultMessageID: durable.ResultMessageID, ResultPartID: durable.ResultPartID, Name: durable.Name,
	}
	settlement, part, err := tools.BuildToolSettlement(runtime.ToolSettlementInput{
		Tool: tool, Call: call, Claimed: durable, Disposition: runtime.ToolExecuted,
		Result: runtime.ToolResult{Output: "ok"}, CompletedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("build settlement: %v", err)
	}
	if err := store.SettleToolCall(ctx, settlement); err != nil {
		t.Fatalf("settle tool call: %v", err)
	}
	settled, err := store.GetToolCall(ctx, durable.ID)
	if err != nil || settled.Status != session.ToolCallCompleted {
		t.Fatalf("settled call = %+v, %v", settled, err)
	}
	if _, err := store.GetMessage(ctx, durable.ResultMessageID); err != nil {
		t.Fatalf("result message: %v", err)
	}
	batch, err := store.ListMessages(ctx, durable.SessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, storedPart := range batch.Parts {
		if storedPart.ID == part.ID && storedPart.MessageID == durable.ResultMessageID {
			found = true
		}
	}
	if !found {
		t.Fatalf("reserved result part %q was not persisted: %#v", part.ID, batch.Parts)
	}
}
