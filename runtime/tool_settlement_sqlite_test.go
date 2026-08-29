package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/sqlite"
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
	run, err := store.AdmitRun(ctx, session.Run{ID: "run", SessionID: "session", OwnerID: "worker", ClaimToken: "claim-run", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant", SessionID: "session", RunID: "run", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	created, err := execution.CreateToolCall(ctx, testCreateToolRequest(session.ToolCall{ID: "call", SessionID: "session", RunID: "run", MessageID: "assistant", ResultMessageID: "result-message", ResultPartID: "result-part", Name: "echo", Status: session.ToolCallPending}, "event-create", now))
	if err != nil {
		t.Fatal(err)
	}
	durable := created.Call
	durable.ClaimedBy = "worker"
	durable.ClaimToken = "claim"
	claimResult, err := execution.ClaimToolCall(ctx, testClaimToolRequest(durable, "event-claim", time.Minute, now))
	if err != nil {
		t.Fatal(err)
	}
	durable = claimResult.Call
	call := ToolCall{ID: durable.ID, SessionID: durable.SessionID, RunID: durable.RunID, MessageID: durable.MessageID, ResultMessageID: durable.ResultMessageID, ResultPartID: durable.ResultPartID, Name: durable.Name}
	settlement, _, err := BuildToolSettlement(ToolSettlementInput{Tool: Tool{Retention: RetentionPolicy{MaxInlineBytes: 100}}, Call: call, Claimed: durable, Disposition: ToolExecuted, Result: ToolResult{Output: "ok"}, CompletedAt: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.SettleToolCall(ctx, testSettleToolRequest(settlement, "event-settle")); err != nil {
		t.Fatalf("settle tool call: %v", err)
	}
	settled, err := store.GetToolCall(ctx, durable.ID)
	if err != nil || settled.Status != session.ToolCallCompleted {
		t.Fatalf("settled call = %+v, %v", settled, err)
	}
	batch, err := store.ListMessages(ctx, durable.SessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var foundMessage, foundPart bool
	for _, message := range batch.Messages {
		foundMessage = foundMessage || message.ID == durable.ResultMessageID
	}
	for _, part := range batch.Parts {
		foundPart = foundPart || part.ID == durable.ResultPartID && part.MessageID == durable.ResultMessageID
	}
	if !foundMessage || !foundPart {
		t.Fatalf("reserved result envelope missing: messages=%#v parts=%#v", batch.Messages, batch.Parts)
	}
}
