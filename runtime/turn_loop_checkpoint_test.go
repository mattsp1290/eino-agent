package runtime

import (
	"context"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// pausingInterruptPolicy pauses a call exactly once via a durable ADK
// checkpoint (see runtime.ToolInterruptPolicy / adkTool.InvokableRun),
// letting resume through unconditionally once targeted.
type pausingInterruptPolicy struct{}

func (pausingInterruptPolicy) RequiresInterrupt(context.Context, ToolCall) (string, bool) {
	return "needs host decision", true
}

// TestTurnLoopChecksPointsAndResumesInterruptedTool proves the promoted W5
// checkpoint/pause/resume path end to end on the production adk_checkpoint.go
// / turn_loop.go engine: a tool that pauses via ToolInterruptPolicy produces
// a durable RunPaused result with a promoted checkpoint; ResumeRun claims the
// paused run, rebuilds the engine from the checkpoint, and targets the
// exact interrupted leaf by its current-generation InterruptCtx.ID, letting
// the turn complete without re-executing the tool from scratch.
func TestTurnLoopChecksPointsAndResumesInterruptedTool(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	var executions int
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executions++
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1", Message: TextUserMessage("hello"), Config: orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	if executions != 0 {
		t.Fatalf("tool executed before resume: executions=%d", executions)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil || run.Status != session.RunPaused {
		t.Fatalf("run = %+v, err=%v", run, err)
	}

	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
	if executions != 1 {
		t.Fatalf("tool executions after resume = %d, want 1", executions)
	}
	finalRun, err := store.GetRun(context.Background(), result.RunID)
	if err != nil || finalRun.Status != session.RunCompleted {
		t.Fatalf("final run = %+v, err=%v", finalRun, err)
	}
}

// TestCompleteTurnLeavesPromotedCheckpointIntactInsideLiveRun proves round-
// six reconciliation item 2 (round-seven fix-pass-6 item 3/RT-2's own
// completeTurn coverage gap): turnLoopCoordinator.completeTurn -- driven for
// real by a genuine ADK tool-interrupt pause, ResumeRun, and a second queued
// turn under the SAME live loop -- must leave its own now-stale promoted
// checkpoint in place rather than retiring it immediately (the shipped
// round-five behavior this branch removed). This is the exact inverse of
// the deleted TestCompleteTurnRetiresItsOwnPromotedCheckpointImmediately: it
// fails the moment anyone re-adds completeTurn's own retirement, because the
// checkpoint would already be gone by the time turn2 is mid-dispatch, still
// inside the same live Run().
func TestCompleteTurnLeavesPromotedCheckpointIntactInsideLiveRun(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	var executions int
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executions++
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	turn2Dispatched := make(chan struct{})
	release := make(chan struct{})
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticAssistantText("turn1 answered")}, nil
		default:
			close(turn2Dispatched)
			<-release
			return []*einoschema.AgenticMessage{agenticAssistantText("turn2 answered")}, nil
		}
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	ctx := context.Background()
	handle, err := orch.Start(ctx, Request{SessionID: "session-1", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	if _, found, err := store.ReadPromotedCheckpoint(ctx, result.RunID); err != nil || !found {
		t.Fatalf("promoted checkpoint before resume: found=%v err=%v", found, err)
	}
	// Durably queue the second turn's content BEFORE resuming: ResumeRun's
	// own drainQueuedInbox picks this up and drives it as turn2 under the
	// SAME fence, once turn1's resumed dispatch completes.
	if _, err := orch.Enqueue(ctx, "session-1", EnqueueRequest{
		RunID: result.RunID, IdempotencyKey: "turn2-key", Message: TextUserMessage("second"),
	}); err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}

	resumeHandle, err := orch.ResumeRun(ctx, result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	select {
	case <-turn2Dispatched:
	case <-time.After(3 * time.Second):
		t.Fatal("turn2 never dispatched")
	}
	// Turn1 has now completed for real, through turnLoopCoordinator.
	// completeTurn, inside this still-live Run() (turn2 is mid-dispatch,
	// blocked on release below): its promoted checkpoint (revision 1,
	// TurnID=turn1) must still be there -- stale by fact, not retired.
	if _, found, err := store.ReadPromotedCheckpoint(ctx, result.RunID); err != nil || !found {
		t.Fatalf("promoted checkpoint after turn1 completes, mid-run: found=%v err=%v, want still present", found, err)
	}
	if executions != 1 {
		t.Fatalf("tool executions = %d, want exactly 1", executions)
	}
	close(release)

	final := <-resumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("final result = %+v, want completed", final)
	}
	// The run has now settled terminally: settleCleanRunCompletion retires
	// the checkpoint at that point, not before.
	if _, found, err := store.ReadPromotedCheckpoint(ctx, result.RunID); err != nil || found {
		t.Fatalf("promoted checkpoint after run completion: found=%v err=%v, want retired", found, err)
	}
}

// TestTurnLoopEnqueueRunsSecondTurnUnderSameFence proves "two normal turns
// under one loop/run/fence with distinct turn/message/invocation identities"
// (see docs/architecture/eino-feature-support.md's W5 acceptance): Enqueue
// pushes a second durable inbox item into the still-live loop before it
// idles out, and the run settles completed exactly once with two distinct
// turns and ledger rows.
func TestTurnLoopEnqueueRunsSecondTurnUnderSameFence(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		return []*einoschema.AgenticMessage{agenticAssistantText("turn ok")}, nil
	}))

	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1", Message: TextUserMessage("first"), Config: orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	if _, err := orch.Enqueue(context.Background(), "session-1", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "second-message", Message: TextUserMessage("second"),
	}); err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	turns, err := store.ListTurns(context.Background(), result.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[0].Ordinal != 1 || turns[1].Ordinal != 2 {
		t.Fatalf("turns = %+v", turns)
	}
	if turns[0].AssistantMessageID == turns[1].AssistantMessageID {
		t.Fatalf("turns share an assistant message id: %+v", turns)
	}
	requests, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(requests.Records) != 2 || requests.Records[0].InvocationID == requests.Records[1].InvocationID {
		t.Fatalf("model requests = %+v", requests.Records)
	}
	if calls != 2 {
		t.Fatalf("provider calls = %d, want 2", calls)
	}
}
