//go:build postgres_integration

package runtime

import (
	"context"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// testPostgresRuntimeTurnLoopPauseResume proves the production TurnLoop
// pause/promote/resume protocol (runtime/turn_loop.go,
// runtime/adk_checkpoint.go) against real PostgreSQL, not only SQLite
// (see runtime/turn_loop_sqlite_test.go's
// TestTurnLoopPauseAndResumeAgainstSQLite, which this mirrors): a tool
// that pauses via ToolInterruptPolicy produces a durable RunPaused result
// with a promoted checkpoint, and ResumeRun claims the paused run,
// rebuilds the engine from the checkpoint, and completes the turn without
// re-executing the already-decided tool call. This is reconciliation.md
// item 8 (round-2 reviewers' nice-to-have: "add a PostgreSQL-tagged
// runtime test covering TurnLoop pause -> ResumeRun -> completion ... and
// register it as a required suite").
func testPostgresRuntimeTurnLoopPauseResume(t *testing.T, server *testpostgres.Server) {
	f := newPostgresRuntimeFixture(t, server)
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
	orch, err := NewStreamingOrchestrator(
		WithStore(f.store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			calls++
			if calls == 1 {
				return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
			}
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		})}),
		WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("postgres-turnloop-owner"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatal(err)
	}
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(f.ctx, Request{SessionID: "postgres-turnloop-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitPostgresRuntime(t, f.ctx, handle)
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
	run, err := f.store.GetRun(f.ctx, result.RunID)
	if err != nil || run.Status != session.RunPaused {
		t.Fatalf("run = %+v, err=%v, want status=paused", run, err)
	}

	resumeHandle, err := orch.ResumeRun(f.ctx, result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resumed := awaitPostgresRuntime(t, f.ctx, resumeHandle)
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
	if executions != 1 {
		t.Fatalf("tool executions after resume = %d, want 1", executions)
	}
	finalRun, err := f.store.GetRun(f.ctx, result.RunID)
	if err != nil || finalRun.Status != session.RunCompleted {
		t.Fatalf("final run = %+v, err=%v", finalRun, err)
	}
}
