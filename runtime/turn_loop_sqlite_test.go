package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// newSQLiteTestOrchestrator builds a StreamingOrchestrator against a real,
// on-disk SQLite store (not the in-memory admissionStore fixture used
// elsewhere in this package): reconciliation.md's Criticals 1-5 must be
// proven against the production adapters, not just the fixture whose own
// divergence from the SQL stores (see fakeExecutionStore.PromotePause) once
// hid Critical 1/TL-1 from CI. The caller must call the returned cleanup.
func newSQLiteTestOrchestrator(t *testing.T, streamer model.Streamer, extra ...Option) (*StreamingOrchestrator, func()) {
	t.Helper()
	ctx := context.Background()
	store, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	options := []Option{
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("sqlite-owner-1"),
		WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	}
	orch, err := NewStreamingOrchestrator(append(options, extra...)...)
	if err != nil {
		_ = pool.Close()
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}
	return orch, func() { _ = pool.Close() }
}

// TestTurnLoopPauseAndResumeAgainstSQLite ports
// TestTurnLoopChecksPointsAndResumesInterruptedTool (turn_loop_checkpoint_test.go)
// onto the real SQLite store: it is a first-turn pause, which is exactly
// the case Critical TL-1 broke (the first-turn sentinel leaking into
// PromotePause.InboxIDs, which only the real store's interruptInboxItems --
// not the old in-memory fixture -- rejected with ErrConflict).
func TestTurnLoopPauseAndResumeAgainstSQLite(t *testing.T) {
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
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-pause-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
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
	run, err := orch.store.GetRun(context.Background(), result.RunID)
	if err != nil || run.Status != session.RunPaused {
		t.Fatalf("run = %+v, err=%v, want status=paused", run, err)
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
	finalRun, err := orch.store.GetRun(context.Background(), result.RunID)
	if err != nil || finalRun.Status != session.RunCompleted {
		t.Fatalf("final run = %+v, err=%v", finalRun, err)
	}
}

// TestTurnLoopSecondEnqueuedItemSurvivesPauseAndResumeAgainstSQLite proves
// reconciliation.md item 5 (ED-3): a second item Enqueued while the loop is
// still live -- so it becomes state.UnhandledItems, buffered but never
// consumed, when the first turn pauses -- must stay `queued` (not get
// marked `interrupted` by PromotePause) so a resumed GenInput can still
// admit it as this run's second turn.
func TestTurnLoopSecondEnqueuedItemSurvivesPauseAndResumeAgainstSQLite(t *testing.T) {
	toolCalled := make(chan struct{})
	release := make(chan struct{})
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	var calls int
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			// Signal the test goroutine once the model has dispatched the
			// tool call that will pause -- Enqueue must land while the loop
			// is still live (before the pause), never after.
			close(toolCalled)
			<-release
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticAssistantText("first turn done")}, nil
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("second turn done")}, nil
		}
	}))
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-second-item-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case <-toolCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("model never dispatched the first call")
	}
	item, err := orch.Enqueue(context.Background(), "sqlite-second-item-session", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "second-item-key", Message: TextUserMessage("second message"),
	})
	if err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}
	close(release)

	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	// The buffered-but-never-consumed item must stay `queued` through the
	// pause -- PromotePause must never have marked it `interrupted`.
	items, err := orch.store.ListInbox(context.Background(), "sqlite-second-item-session", nil)
	if err != nil {
		t.Fatalf("ListInbox error = %v", err)
	}
	for _, it := range items {
		if it.ID == item.ID && it.State != session.InboxQueued {
			t.Fatalf("buffered item state after pause = %q, want queued", it.State)
		}
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
	turns, err := orch.store.ListTurns(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("ListTurns error = %v", err)
	}
	if len(turns) != 2 {
		t.Fatalf("turns = %d, want 2 (first paused-then-resumed, second carrying the buffered item)", len(turns))
	}
	for _, turn := range turns {
		if turn.State != session.TurnCompleted {
			t.Fatalf("turn %+v not completed", turn)
		}
	}
	items, err = orch.store.ListInbox(context.Background(), "sqlite-second-item-session", nil)
	if err != nil {
		t.Fatalf("ListInbox error = %v", err)
	}
	var found bool
	for _, it := range items {
		if it.ID != item.ID {
			continue
		}
		found = true
		if it.State != session.InboxCompleted {
			t.Fatalf("second item state = %q, want completed", it.State)
		}
	}
	if !found {
		t.Fatalf("second item %q missing after resume", item.ID)
	}
}

// TestToolBatchInterruptPolicyPauseDoesNotBlockSiblingAgainstSQLite proves
// reconciliation.md item 2 (ED-2/TL-2): a two-call assistant message whose
// first-declared call raises a durable ToolInterruptPolicy pause must not
// block its sibling in awaitToolTurn forever -- both settle and the run
// reaches a durable pause promptly.
func TestToolBatchInterruptPolicyPauseDoesNotBlockSiblingAgainstSQLite(t *testing.T) {
	var siblingExecuted bool
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "gate-ok"}, nil
		}),
	}
	other := Tool{
		Name: "other", Info: &einoschema.ToolInfo{Name: "other"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			siblingExecuted = true
			return ToolResult{Output: "other-ok"}, nil
		}),
	}
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(
			agenticToolCall("call-1", "gate", `{}`),
			agenticToolCall("call-2", "other", `{}`),
		)}, nil
	}))
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate, other}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-batch-pause-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case result := <-handle.Done():
		if result.Status != session.RunPaused || !result.Interrupted {
			t.Fatalf("result = %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run never reached Done(): a sibling tool call deadlocked in awaitToolTurn")
	}
	// The sibling declared after the paused call still runs to its own
	// completion (ADK's tools node waits for every task; see
	// adkEngine.registerToolBatch's doc comment) -- it is not itself
	// blocked, only ordered after the paused call settles.
	if !siblingExecuted {
		t.Fatal("sibling tool never executed; the batch gate likely never released it")
	}
}

// TestToolBatchToolSearchDeclaredFirstDoesNotBlockSiblingAgainstSQLite
// proves the other half of reconciliation.md item 2: tool_search must
// itself participate in the sibling-batch settlement protocol. Before the
// fix, adkToolSearch.InvokableRun never called settleToolTurn at all, so a
// tool declared after tool_search in the same message blocked forever.
func TestToolBatchToolSearchDeclaredFirstDoesNotBlockSiblingAgainstSQLite(t *testing.T) {
	var siblingExecuted bool
	other := Tool{
		Name: "other", Info: &einoschema.ToolInfo{Name: "other"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			siblingExecuted = true
			return ToolResult{Output: "other-ok"}, nil
		}),
	}
	var turn int
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		turn++
		if turn == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(
				agenticToolCall("call-1", "tool_search", `{"query":"other"}`),
				agenticToolCall("call-2", "other", `{}`),
			)}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	defer cleanup()
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{other}, &ToolSearchConfig{Name: "tool_search"})}

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-batch-search-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case result := <-handle.Done():
		if result.Status != session.RunCompleted {
			t.Fatalf("result = %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run never reached Done(): the sibling after tool_search likely deadlocked in awaitToolTurn")
	}
	if !siblingExecuted {
		t.Fatal("sibling tool declared after tool_search never executed")
	}
}

// TestResumeRunAfterStopBeforeFirstDispatchAgainstSQLite proves
// reconciliation.md item 3 (TL-1/TL-4's "between-turn pause and resume ...
// against the real SQLite store" requirement): a Stop that lands before
// the loop's very first GenInput ever consumes the first-turn sentinel
// checkpoints the sentinel into ADK's own UnhandledItems. A resume must
// still complete the run (genInput's fallback branch filters the sentinel
// before it ever reaches loadInboxItems) rather than fail with "inbox item
//   first-turn not found".
func TestResumeRunAfterStopBeforeFirstDispatchAgainstSQLite(t *testing.T) {
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("answered")}, nil
	}))
	defer cleanup()

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-stop-before-dispatch-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	// Racing Stop immediately after Start returns, with no synchronization,
	// is deliberate: it is the only way to land before the loop's first
	// GenInput call without instrumenting ADK's internals directly (the
	// same approach the turnloop-lifecycle-reviewer's probe used to
	// reproduce this).
	if err := orch.Stop(context.Background(), handle.RunID(), StopPolicy{Graceful: true, Cause: "stop before dispatch"}); err != nil {
		t.Fatalf("Stop error = %v", err)
	}
	first := <-handle.Done()
	t.Logf("first result = %+v", first)
	if first.Status != session.RunPaused {
		t.Skip("Stop did not land before the first dispatch this run; race not reproduced this run")
	}
	run, err := orch.store.GetRun(context.Background(), first.RunID)
	if err != nil || run.Status != session.RunPaused {
		t.Fatalf("run after stop = %+v, err=%v, want paused", run, err)
	}

	resumeHandle, err := orch.ResumeRun(context.Background(), first.RunID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
}
