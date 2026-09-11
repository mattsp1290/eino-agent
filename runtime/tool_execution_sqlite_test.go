package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// SQL stores decode an unsettled call's absent output as JSON null. Settling a
// running call as interrupted must produce a real bounded output, not "null".
func TestSettleInterruptedToolTreatsSQLNullOutputAsAbsent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	host, err := NewStreamingOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) { return nil, nil })}),
		WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithClock(func() time.Time { return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := host.acquireRunPlan(ctx, RunPlanRequest{SessionID: "null-output", Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	resolved, _ := host.model.Resolve(ctx, orchestratorConfig().Model, model.Runtime{})
	ids := admissionIDs{SessionID: "null-output", RunID: "run-null", UserMessageID: "user", UserPartIDs: []session.PartID{"user-part"}, AssistantMessageID: "assistant", ContextEpochID: "epoch", EventID: "event", RunClaimToken: "claim", TurnID: "turn-1", TurnStartedEventID: "turn-started-1"}
	assignedBlocks, err := assignContentBlockIDs(TextUserMessage("hello").Blocks, host.ids)
	if err != nil {
		t.Fatal(err)
	}
	userMessage := UserMessage{Blocks: assignedBlocks}
	admitted, err := host.admitter().admit(ctx, admissionRequest{IDs: ids, UserMessage: userMessage, Config: orchestratorConfig(), Model: resolved, OwnerID: host.ownerID(), LeaseDuration: time.Minute, ExtensionPlan: plan.Descriptor(), ContentLimits: host.contentLimits})
	if err != nil {
		t.Fatal(err)
	}
	execution := newRunExecution(host, plan, admitted.Run)
	defer execution.release()
	if _, err := execution.store.StartRun(ctx, host.now()); err != nil {
		t.Fatal(err)
	}
	snapshot := admitted.Snapshot
	tool := Tool{Name: "echo", Info: &einoschema.ToolInfo{Name: "echo"}, Retention: RetentionPolicy{MaxInlineBytes: 1 << 16}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })}
	snapshot.Tools = []Tool{tool}
	msg := agenticAssistantToolCalls(agenticToolCall("call-1", "echo", `{}`))
	prepared, err := host.prepareToolCalls(ctx, execution, snapshot, admitted.AssistantMessage.ID, functionToolCalls(msg))
	if err != nil {
		t.Fatal(err)
	}
	blockIDs := make([]string, len(msg.ContentBlocks))
	for index := range blockIDs {
		blockIDs[index] = string(host.ids.NewPartID())
	}
	if _, err := host.persistAssistantTurn(ctx, execution, snapshot, admitted.AssistantMessage.ID, msg, blockIDs, nil, prepared); err != nil {
		t.Fatal(err)
	}
	startedAt := host.now()
	if _, err := execution.persistToolClaim(ctx, session.ClaimToolCallRequest{ID: "call-1", ClaimedBy: host.ownerID(), ClaimToken: "tool-claim", StartedAt: startedAt, LeaseDuration: time.Minute, Event: toolTransitionEnvelope(host, snapshot, startedAt)}); err != nil {
		t.Fatal(err)
	}
	running, err := store.GetToolCall(ctx, "call-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(running.Output) != "null" {
		t.Fatalf("precondition: SQL decodes absent output as %q", running.Output)
	}
	settlement, err := execution.settleInterruptedRunningTool(ctx, admitted.Run, tool, running)
	if err != nil {
		t.Fatal(err)
	}
	var output ToolOutput
	if err := json.Unmarshal(settlement.Output, &output); err != nil || output.Status != "interrupted" || output.Content != "tool execution interrupted" {
		t.Fatalf("settlement output = %s (%v)", settlement.Output, err)
	}
	stored, err := store.GetToolCall(ctx, "call-1")
	if err != nil || stored.Status != session.ToolCallInterrupted || string(stored.Output) != string(settlement.Output) {
		t.Fatalf("stored = %+v (%v)", stored, err)
	}
}
