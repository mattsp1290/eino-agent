package runtime

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// deleteGateStore delays the SECOND call to ReadPromotedCheckpoint until
// the test closes release. For a fresh, single-turn Start with no checkpoint
// ever staged, the first call is adkCheckpointStore.Get inside upstream's
// tryLoadCheckpoint (Run's very first step); the second is
// adkCheckpointStore.Delete's lookup, made from inside upstream's cleanup
// AFTER atomic.StoreInt32(&l.stopped, 1) but BEFORE Wait returns and
// runTurnLoop unregisters the loop -- exactly the window an Enqueue must
// land in to reach TurnLoop's late-item buffer (pushWithConfig routes any
// Push once l.stopped != 0 to appendLate) rather than the normal buffer or
// a sealed late buffer (TakeLateItems, called only later in
// finishTurnLoop, well after Wait returns).
type deleteGateStore struct {
	session.Store
	calls   int
	mu      sync.Mutex
	reached chan struct{}
	release chan struct{}
}

func (s *deleteGateStore) ReadPromotedCheckpoint(ctx context.Context, runID session.RunID) (session.Checkpoint, bool, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n == 2 {
		close(s.reached)
		<-s.release
	}
	return s.Store.ReadPromotedCheckpoint(ctx, runID)
}

// TestEnqueueDuringLateItemWindowSurvivesAsQueuedContinuation proves
// round-three reconciliation item 8 (RD-I4): the genuine TakeLateItems
// window -- an Enqueue reaching Push while the loop is still registered but
// already stopped, so upstream routes it to appendLate -- is reachable from
// outside this package and, once TakeLateItems drains it in
// finishTurnLoop, must divert to a between-turn queued-continuation pause
// with the item durably `queued`, never a panic and never a RunCompleted
// settlement that strands it.
func TestEnqueueDuringLateItemWindowSurvivesAsQueuedContinuation(t *testing.T) {
	ctx := context.Background()
	sqliteStore, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	defer func() { _ = pool.Close() }()

	gate := &deleteGateStore{Store: sqliteStore, reached: make(chan struct{}), release: make(chan struct{})}
	orch, err := NewStreamingOrchestrator(
		WithStore(gate), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{agenticAssistantText("first turn done")}, nil
		})}),
		WithIDGenerator(&sequenceIDs{}), WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("sqlite-owner-late-item"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}
	configureTestTools(orch, staticToolRegistry{tools: nil})

	handle, err := orch.Start(ctx, Request{SessionID: "sqlite-late-item-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}

	var item session.InboxItem
	var enqueueErr error
	select {
	case <-gate.reached:
		item, enqueueErr = orch.Enqueue(ctx, "sqlite-late-item-session", EnqueueRequest{
			RunID: handle.RunID(), IdempotencyKey: "late-item-key", Message: TextUserMessage("late message"),
		})
		close(gate.release)
	case <-time.After(5 * time.Second):
		t.Fatal("delete gate never reached")
	}
	if enqueueErr != nil {
		t.Fatalf("Enqueue error = %v", enqueueErr)
	}

	result := <-handle.Done()
	if result.Status != session.RunPaused {
		t.Fatalf("result = %+v, want paused (the late item must divert to a queued-continuation pause)", result)
	}
	items, err := orch.store.ListInbox(ctx, "sqlite-late-item-session", nil)
	if err != nil {
		t.Fatalf("ListInbox error = %v", err)
	}
	var found bool
	for _, it := range items {
		if it.ID == item.ID {
			found = true
			if it.State != session.InboxQueued {
				t.Fatalf("late item state = %q, want queued", it.State)
			}
		}
	}
	if !found {
		t.Fatalf("late item %s not found in inbox: %#v", item.ID, items)
	}

	resumeHandle, err := orch.ResumeRun(ctx, result.RunID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
}

// upstreamTurnLoopCheckpointShape is a field-identical mirror of eino's
// private adk.turnLoopCheckpoint[session.InboxID] (adk/turn_loop.go): gob
// matches by field name and type, not by concrete struct identity, so
// decoding marshalEmptyLoopCheckpoint's bytes into this type proves what
// eino's own unmarshalTurnLoopCheckpoint would see, without importing an
// unexported upstream type. Round-three reconciliation item 5 (SR-5): the
// turnLoopCheckpointShape doc comment in adk_checkpoint.go claims this
// round trip is verified by a test; this is that test.
type upstreamTurnLoopCheckpointShape struct {
	RunnerCheckpoint []byte
	HasRunnerState   bool
	UnhandledItems   []session.InboxID
	CanceledItems    []session.InboxID
}

func TestEmptyLoopCheckpointMatchesUpstreamGobShape(t *testing.T) {
	raw, err := marshalEmptyLoopCheckpoint()
	if err != nil {
		t.Fatalf("marshalEmptyLoopCheckpoint: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("empty payload would fail decodeCheckpointEnvelope's len(Payload) != 0 guard")
	}
	var out upstreamTurnLoopCheckpointShape
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&out); err != nil {
		t.Fatalf("eino's unmarshalTurnLoopCheckpoint would fail decoding this payload: %v", err)
	}
	if out.HasRunnerState || len(out.RunnerCheckpoint) != 0 || len(out.UnhandledItems) != 0 || len(out.CanceledItems) != 0 {
		t.Fatalf("decoded = %+v, want the zero between-turns shape", out)
	}
}

// TestDuplicateDeliveryOfAlreadyAdmittedItemDoesNotDispatch proves the
// round-three reconciliation item 2 "belt and braces" guard in genInput
// (runtime/turn_loop.go): a batch that claimItems empties entirely because
// every non-sentinel id in it was already admitted earlier in this
// coordinator's lifetime must NOT mint a fresh, content-free turn that
// dispatches the model again -- it must divert to a between-turn
// queued-continuation pause instead.
//
// The natural race this guards against (a duplicate delivery splitting
// across two GenInput calls) is closed for the reachable, in-process case
// by item 2's primary fix (pushing pushIDs before loop.Run in runTurnLoop),
// so this test forces the residual condition deterministically instead of
// racing for it: it re-pushes an inbox id into the still-live loop from
// INSIDE the very model dispatch that admits it, at which point claimItems
// has already (synchronously, before PrepareAgent/dispatch) recorded that
// id in admittedItems -- so the re-push is guaranteed to be treated as a
// genuine duplicate on the loop's next cycle, not a fresh item.
func TestDuplicateDeliveryOfAlreadyAdmittedItemDoesNotDispatch(t *testing.T) {
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
	var orch *StreamingOrchestrator
	var dupRunID session.RunID
	var dupItemID session.InboxID

	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			// Signal the test goroutine once the model has dispatched the
			// tool call that will pause -- Enqueue must land while the
			// loop is still live, never after.
			close(toolCalled)
			<-release
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticAssistantText("first turn done")}, nil
		case 3:
			// This dispatch is admitting the enqueued item as the run's
			// second turn: genInput's claimItems has already recorded
			// dupItemID in admittedItems by now (it runs synchronously
			// inside genInput, strictly before PrepareAgent/dispatch).
			// Re-pushing it here, mid-dispatch, simulates a genuine
			// duplicate re-delivery landing while this coordinator's loop
			// is still live -- the loop cannot have gone idle yet, so this
			// is deterministic, not a race.
			if entry := orch.liveLoopFor(dupRunID); entry != nil {
				entry.loop.Push(dupItemID)
			}
			return []*einoschema.AgenticMessage{agenticAssistantText("second turn done")}, nil
		default:
			t.Errorf("unexpected dispatch #%d: genInput's duplicate-delivery guard should have prevented a third turn from ever dispatching", calls)
			return []*einoschema.AgenticMessage{agenticAssistantText("should not happen")}, nil
		}
	})
	var cleanup func()
	orch, cleanup = newSQLiteTestOrchestrator(t, streamer)
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-dup-delivery-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case <-toolCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("model never dispatched the first call")
	}
	item, err := orch.Enqueue(context.Background(), "sqlite-dup-delivery-session", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "dup-delivery-key", Message: TextUserMessage("second message"),
	})
	if err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}
	dupItemID = item.ID
	close(release)

	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	dupRunID = result.RunID

	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	// Round-four reconciliation item 2 (CR-I1): the re-pushed duplicate's
	// id is, by the time the loop settles, already durably InboxCompleted
	// (turn 2's own CompleteTurn consumed it as real content) -- not merely
	// `queued` elsewhere. finishTurnLoop's durable-state filter recognizes
	// there is nothing genuinely left to do and settles the run normally,
	// instead of the pre-fix behavior of promoting a pointless degenerate
	// pause-carrier turn that a later resume would still have to redeliver
	// the stale id into (the actual dispatch-#4 hazard this item guards
	// against end-to-end).
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v, want completed with no error (no spurious pause, no spurious dispatch)", resumed)
	}
	if calls != 3 {
		t.Fatalf("model dispatch count = %d, want exactly 3 (no spurious third-turn dispatch)", calls)
	}

	turns, err := orch.store.ListTurns(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("ListTurns error = %v", err)
	}
	// Exactly two turns: turn 1 (tool call, paused/resumed) and turn 2 (the
	// enqueued item, dispatched). No degenerate third turn is minted: the
	// re-pushed duplicate never needed one.
	if len(turns) != 2 {
		t.Fatalf("turns = %d, want 2 (turn 1, and turn 2 with the enqueued item)", len(turns))
	}
	for _, turn := range turns {
		if turn.Ordinal == 2 && len(turn.UserMessageIDs) != 1 {
			t.Fatalf("second turn UserMessageIDs = %d, want exactly 1", len(turn.UserMessageIDs))
		}
		if len(turn.UserMessageIDs) == 0 && turn.State == session.TurnCompleted {
			t.Fatalf("turn %s completed with zero UserMessageIDs (a content-free spurious dispatch): %#v", turn.ID, turn)
		}
	}

	finalRun, err := orch.store.GetRun(context.Background(), result.RunID)
	if err != nil || finalRun.Status != session.RunCompleted {
		t.Fatalf("final run = %+v, err=%v, want status=completed", finalRun, err)
	}
}

// queuedInputAlwaysStore is a minimal session.ExecutionStore whose SettleRun
// always reports session.ErrRunHasQueuedInput, counting calls.
type queuedInputAlwaysStore struct {
	session.ExecutionStore
	calls int
}

func (s *queuedInputAlwaysStore) SettleRun(context.Context, session.SettleRunRequest) (session.RunSettlementResult, error) {
	s.calls++
	return session.RunSettlementResult{}, session.ErrRunHasQueuedInput
}

// TestSettleRunRetryingSkipsRetryOnQueuedInput proves round-three
// reconciliation item 9's settleRunRetrying half (SR-S1/RD-S4):
// ErrRunHasQueuedInput is a structural conflict a bounded in-process retry
// loop can never clear (only a future run's drain does), so it must be
// returned on the FIRST attempt, not burn the full 40x5ms retry budget the
// way an ordinary session.ErrConflict (an in-flight tool settlement that
// really can clear) still does.
func TestSettleRunRetryingSkipsRetryOnQueuedInput(t *testing.T) {
	store := &queuedInputAlwaysStore{}
	start := time.Now()
	_, err := settleRunRetrying(context.Background(), store, session.SettleRunRequest{
		Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: time.Now()},
		Event:      session.RunSettlementEvent{ID: "settle-retry-test-event"},
	})
	elapsed := time.Since(start)
	if !errors.Is(err, session.ErrRunHasQueuedInput) {
		t.Fatalf("err = %v, want ErrRunHasQueuedInput", err)
	}
	if !errors.Is(err, session.ErrConflict) {
		t.Fatalf("err = %v, want it to still satisfy errors.Is(err, session.ErrConflict)", err)
	}
	if store.calls != 1 {
		t.Fatalf("SettleRun calls = %d, want exactly 1 (no retry)", store.calls)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("settleRunRetrying took %v, want an immediate return with no retry sleep", elapsed)
	}
}
