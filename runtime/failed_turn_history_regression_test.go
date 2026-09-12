package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestFailedTurnDoesNotPoisonLaterTurnsAgainstSQLite is the eino-agent-978
// regression test, run against the real SQLite store: a turn that fails
// through a model error must never poison every later turn admitted on the
// same session.
//
// Root cause: AdmitTurn always durably creates a turn's assistant
// placeholder row at admission, before that turn's own model dispatch ever
// runs (see admission.go's admitDurable and turn_loop.go's admitTurn); a
// turn that then fails before ever finalizing that placeholder (a plain
// model error, before any commit) leaves it behind forever, still carrying
// zero content blocks -- session.ApplyFailTurn only transitions the turn's
// state, it never touches the placeholder row. adkEngine.buildDurableBaseline
// always drops such a placeholder (dropUnfinalizedAssistantPlaceholders)
// before comparing a fresh reload's length against e.baseMessageCount. But
// admission.go's admitDurable -- the path that admits a fresh run's first
// turn -- computed its own admitted-base message count from a raw
// loadProviderHistory reload that never dropped the SAME kind of leftover
// placeholder from an earlier failed run on this session, over-counting by
// exactly the number of prior failed turns. The next turn's very first
// buildDurableBaseline reload then correctly drops it, comes up short
// against that inflated base, and trips "durable history shrank below this
// turn's admitted base" -- a permanent, self-perpetuating failure for every
// later turn on the session (each of which leaves behind its own leftover
// placeholder in turn). turnLoopCoordinator.admitTurn and resumeEngine
// (turn_loop.go) already called dropUnfinalizedAssistantPlaceholders right
// after their own loadProviderHistory reloads; admitDurable was the one
// admission-time reload site missing it.
func TestFailedTurnDoesNotPoisonLaterTurnsAgainstSQLite(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "failed-turn.db")
	store, pool, err := openTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	defer func() { _ = pool.Close() }()

	var calls atomic.Int32
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("injected model error for turn 1")
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("turn 2 reply")}, nil
	})
	orch, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}), WithOwnerID("failed-turn-owner"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}
	configureTestTools(orch, staticToolRegistry{tools: nil})

	sessionID := session.ID("failed-turn-session")

	// Turn 1 fails through a genuine model error.
	handle1, err := orch.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("first message"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start (turn 1) error = %v", err)
	}
	result1 := <-handle1.Done()
	if result1.Status != session.RunFailed || result1.Error == nil {
		t.Fatalf("turn 1 result = %+v, want failed with a non-nil error", result1)
	}

	// Turn 2 -- a fresh run admitted on the SAME session -- must run
	// normally despite turn 1's leftover, never-finalized assistant
	// placeholder still sitting in durable history.
	handle2, err := orch.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("second message"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start (turn 2) error = %v", err)
	}
	result2 := <-handle2.Done()
	if result2.Status != session.RunCompleted || result2.Error != nil {
		t.Fatalf("turn 2 result = %+v, want completed with no error", result2)
	}

	wantText := []string{"first message", "second message", "turn 2 reply"}
	if got := reconstructedUserAndAssistantText(t, ctx, store, sessionID); !stringSlicesEqual(got, wantText) {
		t.Fatalf("reconstructed history = %#v, want %#v", got, wantText)
	}
}

// TestFailedTurnDoesNotPoisonResumedTurnAgainstSQLite proves the same fix
// holds across a genuine pause/resume: turn 1 fails through a model error,
// then turn 2 (a fresh run on the same session) reaches a tool interrupt and
// pauses, then resumes and completes normally. Turn 2's own ADMISSION (which
// goes through admission.go's admitDurable, the exact site this bug lived
// in) must not trip on turn 1's leftover placeholder before the pause ever
// happens; and its resume (turn_loop.go's resumeEngine, which already
// dropped unfinalized placeholders correctly) must not regress either.
func TestFailedTurnDoesNotPoisonResumedTurnAgainstSQLite(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "failed-turn-resume.db")
	store, pool, err := openTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	defer func() { _ = pool.Close() }()

	var executions int
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executions++
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	var calls atomic.Int32
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		switch calls.Add(1) {
		case 1:
			return nil, errors.New("injected model error for turn 1")
		case 2:
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	})
	orch, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}), WithOwnerID("failed-turn-resume-owner"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	sessionID := session.ID("failed-turn-resume-session")

	handle1, err := orch.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("first message"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start (turn 1) error = %v", err)
	}
	result1 := <-handle1.Done()
	if result1.Status != session.RunFailed || result1.Error == nil {
		t.Fatalf("turn 1 result = %+v, want failed with a non-nil error", result1)
	}

	handle2, err := orch.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("second message"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start (turn 2) error = %v", err)
	}
	result2 := <-handle2.Done()
	if result2.Status != session.RunPaused || !result2.Interrupted {
		t.Fatalf("turn 2 result = %+v, want paused (tool interrupt)", result2)
	}
	pause, ok := <-handle2.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}

	resumeHandle, err := orch.ResumeRun(ctx, result2.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed turn 2 result = %+v, want completed with no error", resumed)
	}
	if executions != 1 {
		t.Fatalf("tool executions = %d, want exactly 1", executions)
	}

	wantText := []string{"first message", "second message", "done"}
	if got := reconstructedUserAndAssistantText(t, ctx, store, sessionID); !stringSlicesEqual(got, wantText) {
		t.Fatalf("reconstructed history = %#v, want %#v", got, wantText)
	}
}

// TestFailedTurnDoesNotPoisonTurnAfterProcessRestartAgainstSQLite proves the
// fix holds across a genuine process restart: turn 1 fails through a model
// error under one orchestrator instance/pool, that pool is closed, a second
// pool is opened over the SAME durable SQLite file (reopenTestSQLite, no
// re-migration -- mirrors TestProcessRestartRecoversMultipleQueuedInputsAfterFirstCommittedTurn),
// and a brand-new orchestrator instance starts turn 2 on the same session.
func TestFailedTurnDoesNotPoisonTurnAfterProcessRestartAgainstSQLite(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "failed-turn-restart.db")
	storeA, poolA, err := openTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	poolAClosed := false
	defer func() {
		if !poolAClosed {
			_ = poolA.Close()
		}
	}()

	orchA, err := NewStreamingOrchestrator(
		WithStore(storeA), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return nil, errors.New("injected model error for turn 1")
		})}),
		WithIDGenerator(&namespacedSequenceIDs{namespace: "restart-a"}), WithOwnerID("failed-turn-restart-owner-a"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator (A): %v", err)
	}
	configureTestTools(orchA, staticToolRegistry{tools: nil})

	sessionID := session.ID("failed-turn-restart-session")
	handle1, err := orchA.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("first message"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start (turn 1) error = %v", err)
	}
	result1 := <-handle1.Done()
	if result1.Status != session.RunFailed || result1.Error == nil {
		t.Fatalf("turn 1 result = %+v, want failed with a non-nil error", result1)
	}

	// Simulate a process restart: close the first pool, reopen a second one
	// over the SAME file, and drive a brand-new orchestrator instance.
	if err := poolA.Close(); err != nil {
		t.Fatalf("close pool A: %v", err)
	}
	poolAClosed = true
	storeB, poolB, err := reopenTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopenTestSQLite: %v", err)
	}
	defer func() { _ = poolB.Close() }()

	orchB, err := NewStreamingOrchestrator(
		WithStore(storeB), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{agenticAssistantText("turn 2 reply")}, nil
		})}),
		WithIDGenerator(&namespacedSequenceIDs{namespace: "restart-b"}), WithOwnerID("failed-turn-restart-owner-b"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator (B): %v", err)
	}
	configureTestTools(orchB, staticToolRegistry{tools: nil})

	handle2, err := orchB.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("second message"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start (turn 2) error = %v", err)
	}
	result2 := <-handle2.Done()
	if result2.Status != session.RunCompleted || result2.Error != nil {
		t.Fatalf("turn 2 result = %+v, want completed with no error", result2)
	}

	wantText := []string{"first message", "second message", "turn 2 reply"}
	if got := reconstructedUserAndAssistantText(t, ctx, storeB, sessionID); !stringSlicesEqual(got, wantText) {
		t.Fatalf("reconstructed history = %#v, want %#v", got, wantText)
	}
}
