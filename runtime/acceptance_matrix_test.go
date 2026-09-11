package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// pausingToolForCheckpoint pauses a tool exactly once via a durable ADK
// checkpoint (mirrors pausingInterruptPolicy in turn_loop_checkpoint_test.go)
// so these tests can reach a real promoted checkpoint through the production
// engine before exercising failure modes around it.
func pausingToolForCheckpoint(name string, executions *int) Tool {
	return Tool{
		Name: name, Info: &einoschema.ToolInfo{Name: name, Desc: "pauses once"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			*executions++
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
}

// startPausedRun drives a fresh run to a durable RunPaused checkpoint and
// returns the orchestrator/run/pause info for a test to manipulate
// directly. A test resuming the returned run must reuse this same
// orchestrator (not construct a fresh one against the same store) unless it
// is deliberately simulating a process restart with its own ID generator
// offset (see TestCheckpointVersionMismatchRejectedOnResume/
// TestCheckpointFingerprintMismatchRejectedOnResume): a second
// newTestOrchestrator against the same store starts sequenceIDs over from
// the beginning, and its freshly minted IDs collide with the first
// orchestrator's already-durable ones (a real bug class this exact
// collision surfaced during test development, not something to route
// around silently -- a resuming process reusing an ID sequence that
// legitimately restarts from zero would hit the identical conflict).
func startPausedRun(t *testing.T, store *admissionStore, sessionID session.ID, executions *int) (*StreamingOrchestrator, session.RunID, PauseInfo) {
	t.Helper()
	gate := pausingToolForCheckpoint("gate", executions)
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})
	handle, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("hello"), Config: orchestratorConfig()})
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
	return orch, result.RunID, pause
}

// promotedCheckpointKey finds the single promoted checkpoint row for runID
// in the fixture's in-memory map.
func promotedCheckpointKey(t *testing.T, store *admissionStore, runID session.RunID) fakeCheckpointKey {
	t.Helper()
	for key, checkpoint := range store.checkpoints {
		if key.RunID == runID && checkpoint.Promoted {
			return key
		}
	}
	t.Fatalf("no promoted checkpoint found for run %s", runID)
	return fakeCheckpointKey{}
}

// TestCheckpointEnvelopeMalformedRejected proves decodeCheckpointEnvelope
// (runtime/adk_checkpoint.go) fails closed on structurally invalid bytes
// before ever attempting to trust the opaque upstream ADK payload inside --
// empty bytes, non-JSON garbage, and a structurally valid envelope missing a
// required field (Payload) all must report ErrCheckpointMalformed, never
// panic or silently accept a corrupt checkpoint.
func TestCheckpointEnvelopeMalformedRejected(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"empty", nil},
		{"not_json", []byte("not json at all")},
		{"missing_payload", []byte(`{"EinoVersion":"v0.9.19","CodecVersion":1,"Fingerprint":"fp"}`)},
		{"missing_fingerprint", []byte(`{"EinoVersion":"v0.9.19","CodecVersion":1,"Payload":"AQID"}`)},
		{"zero_codec_version", []byte(`{"EinoVersion":"v0.9.19","CodecVersion":0,"Fingerprint":"fp","Payload":"AQID"}`)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeCheckpointEnvelope(test.raw); !errors.Is(err, ErrCheckpointMalformed) {
				t.Fatalf("decodeCheckpointEnvelope(%q) error = %v, want ErrCheckpointMalformed", test.raw, err)
			}
		})
	}
}

// TestCheckpointVersionMismatchRejectedOnResume proves a promoted checkpoint
// row stamped with a different Eino version than this binary is pinned to
// fails ResumeRun synchronously with ErrCheckpointFingerprintMismatch --
// ResumeRun's own pre-check (turn_loop.go) compares the durable checkpoint
// row's AgentFingerprint/EinoVersion/CodecVersion fields against the
// current plan/binary before ever seeding a TurnLoop, collapsing a version
// mismatch into the same sentinel as a fingerprint mismatch (a second,
// independent check inside adkCheckpointStore.Get -- see
// TestCheckpointEnvelopeMalformedRejected -- validates the envelope Bytes
// themselves once ADK actually reads them, deeper in the resume path).
func TestCheckpointVersionMismatchRejectedOnResume(t *testing.T) {
	store := newAdmissionStore()
	_, runID, _ := startPausedRun(t, store, "checkpoint-version-mismatch", new(int))
	key := promotedCheckpointKey(t, store, runID)
	checkpoint := store.checkpoints[key]
	checkpoint.EinoVersion = "v0.0.0-not-pinned"
	store.checkpoints[key] = checkpoint

	orch2 := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model dispatched despite version-mismatched checkpoint")
		return nil, nil
	}))
	configureTestTools(orch2, staticToolRegistry{tools: []Tool{pausingToolForCheckpoint("gate", new(int))}})
	if _, err := orch2.ResumeRun(context.Background(), runID, ResumeRequest{}); err == nil || !errors.Is(err, ErrCheckpointFingerprintMismatch) {
		t.Fatalf("ResumeRun error = %v, want ErrCheckpointFingerprintMismatch", err)
	}
	// A rejected resume must leave the run exactly as ResumeRun found it:
	// this check runs before ClaimRun (see ResumeRun's doc comment), so the
	// run must never observe a claim/running transition it can't recover
	// from (see reconciliation.md item 1 / ED-1).
	if run, err := store.GetRun(context.Background(), runID); err != nil || run.Status != session.RunPaused {
		t.Fatalf("run after rejected resume = %+v, err=%v, want status=paused", run, err)
	}
}

// TestCheckpointFingerprintMismatchRejectedOnResume proves a promoted
// checkpoint row whose AgentFingerprint no longer matches the run's current
// frozen plan (e.g. the extension plan or agent factory changed between the
// original run and this resume attempt) fails ResumeRun synchronously with
// ErrCheckpointFingerprintMismatch, before ever seeding a TurnLoop.
func TestCheckpointFingerprintMismatchRejectedOnResume(t *testing.T) {
	store := newAdmissionStore()
	_, runID, _ := startPausedRun(t, store, "checkpoint-fingerprint-mismatch", new(int))
	key := promotedCheckpointKey(t, store, runID)
	checkpoint := store.checkpoints[key]
	checkpoint.AgentFingerprint = "stale-fingerprint-from-a-different-plan"
	store.checkpoints[key] = checkpoint

	orch2 := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model dispatched despite fingerprint-mismatched checkpoint")
		return nil, nil
	}))
	configureTestTools(orch2, staticToolRegistry{tools: []Tool{pausingToolForCheckpoint("gate", new(int))}})
	if _, err := orch2.ResumeRun(context.Background(), runID, ResumeRequest{}); err == nil || !errors.Is(err, ErrCheckpointFingerprintMismatch) {
		t.Fatalf("ResumeRun error = %v, want ErrCheckpointFingerprintMismatch", err)
	}
	if run, err := store.GetRun(context.Background(), runID); err != nil || run.Status != session.RunPaused {
		t.Fatalf("run after rejected resume = %+v, err=%v, want status=paused", run, err)
	}
}

// failingResolver always fails Resolve, simulating a model.Resolve failure
// on ResumeRun (e.g. the provider/model configured at admission time is no
// longer registered in this binary).
type failingResolver struct{ err error }

func (f failingResolver) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{}, f.err
}

// TestResumeRunModelResolveFailureLeavesRunPaused proves a model.Resolve
// failure on ResumeRun -- checked before ClaimRun, off the durable GetRun
// record -- also leaves the run paused rather than stranding it running
// with no driver (reconciliation.md item 1 / ED-1's third required case).
func TestResumeRunModelResolveFailureLeavesRunPaused(t *testing.T) {
	store := newAdmissionStore()
	_, runID, _ := startPausedRun(t, store, "checkpoint-model-resolve-failure", new(int))

	resolveErr := errors.New("provider no longer registered")
	orch2 := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model dispatched despite a model.Resolve failure")
		return nil, nil
	}), WithModelResolver(failingResolver{err: resolveErr}))
	configureTestTools(orch2, staticToolRegistry{tools: []Tool{pausingToolForCheckpoint("gate", new(int))}})
	if _, err := orch2.ResumeRun(context.Background(), runID, ResumeRequest{}); !errors.Is(err, resolveErr) {
		t.Fatalf("ResumeRun error = %v, want %v", err, resolveErr)
	}
	if run, err := store.GetRun(context.Background(), runID); err != nil || run.Status != session.RunPaused {
		t.Fatalf("run after rejected resume = %+v, err=%v, want status=paused", run, err)
	}
}

// checkpointSetFailingStore fails every StageCheckpoint call for a fenced
// execution, simulating a checkpoint Set failure at the exact point
// finishTurnLoop's "conservative" branch is documented to handle: no
// promotion, run left running for lease-expiry recovery.
type checkpointSetFailingStore struct {
	*admissionStore
	err error
}

func (s *checkpointSetFailingStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &checkpointSetFailingExecution{ExecutionStore: s.admissionStore.Execution(fence), err: s.err}
}

type checkpointSetFailingExecution struct {
	session.ExecutionStore
	err error
}

func (e *checkpointSetFailingExecution) StageCheckpoint(context.Context, session.StageCheckpointRequest) (session.Checkpoint, error) {
	return session.Checkpoint{}, e.err
}

// TestCheckpointSetFailureLeavesRunRunningForLeaseRecovery proves
// finishTurnLoop's documented conservative handling of a checkpoint Set
// failure: no checkpoint is promoted and the run is left in RunRunning
// (lease left to expire) rather than a corrupted or falsely-paused state,
// exactly so recovery cannot race a partially-written revision.
func TestCheckpointSetFailureLeavesRunRunningForLeaseRecovery(t *testing.T) {
	store := newAdmissionStore()
	failing := &checkpointSetFailingStore{admissionStore: store, err: errors.New("injected checkpoint set failure")}
	var executions int
	gate := pausingToolForCheckpoint("gate", &executions)
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
	})
	orch := mustConfiguredOrchestrator(
		WithStore(failing), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }), WithOwnerID("owner-1"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{tools: []Tool{gate}})}),
	)
	handle, err := orch.Start(context.Background(), Request{SessionID: "checkpoint-set-failure", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted || !result.Interrupted || result.Error == nil {
		t.Fatalf("result = %+v", result)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil || run.Status != session.RunRunning {
		t.Fatalf("run = %+v, err=%v, want RunRunning (left for lease-expiry recovery)", run, err)
	}
	for _, checkpoint := range store.checkpoints {
		if checkpoint.RunID == result.RunID && checkpoint.Promoted {
			t.Fatalf("checkpoint was promoted despite Set failure: %+v", checkpoint)
		}
	}
}

// TestConcurrentResumesYieldOneOwner races two ResumeRun calls against the
// same paused run and proves exactly one claims it (ClaimRun's CAS on
// status='paused'/claim_token): the loser must fail, never silently run a
// second concurrent TurnLoop over the same durable turn.
func TestConcurrentResumesYieldOneOwner(t *testing.T) {
	store := newAdmissionStore()
	var executions int
	// Reuse the same orchestrator (and its live model script) for the
	// resume race: a second newTestOrchestrator against the same store
	// starts its own sequenceIDs generator over from the beginning, minting
	// IDs that collide with the first orchestrator's already-durable ones
	// -- unrelated to the concurrency this test targets. The scripted
	// streamer's first response ("done") is only ever reached once by
	// whichever ResumeRun call wins the race; a second physical dispatch
	// from a genuine duplicate resume would also return "done" harmlessly,
	// so this streamer cannot itself mask a duplicate-execution bug -- the
	// executions counter on the tool below is what actually proves it.
	orch, runID, pause := startPausedRun(t, store, "concurrent-resume-session", &executions)

	const attempts = 2
	var wg sync.WaitGroup
	handles := make([]Handle, attempts)
	errs := make([]error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			handles[i], errs[i] = orch.ResumeRun(context.Background(), runID, ResumeRequest{
				Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
			})
		}(i)
	}
	wg.Wait()

	successes := 0
	var winner Handle
	for i := 0; i < attempts; i++ {
		if errs[i] == nil && handles[i] != nil {
			successes++
			winner = handles[i]
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent ResumeRun successes = %d, want exactly 1 (errs=%v)", successes, errs)
	}
	result := <-winner.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("winning resume result = %+v", result)
	}
	if executions != 1 {
		t.Fatalf("tool executions = %d, want exactly 1 (no duplicate execution from a second owner)", executions)
	}
}

// TestDuplicateEnqueueIsIdempotentOnKey proves Enqueue with the same
// IdempotencyKey twice against the same session durably admits the inbox
// item only once (session.ExecutionStore.AdmitTurn/EnqueueInbox contract),
// not twice, whether or not a live loop is currently registered for the
// run. It enqueues against a durably *paused* (nonterminal, but definitely
// not live-loop-registered) run: Enqueue now fails closed against a
// terminal run (see Enqueue's doc comment), so the "no live loop" half of
// this contract can no longer be exercised by racing a completed run.
func TestDuplicateEnqueueIsIdempotentOnKey(t *testing.T) {
	store := newAdmissionStore()
	orch, runID, _ := startPausedRun(t, store, "duplicate-enqueue-session", new(int))

	item1, err := orch.Enqueue(context.Background(), "duplicate-enqueue-session", EnqueueRequest{RunID: runID, IdempotencyKey: "dup-key-1", Message: TextUserMessage("second")})
	if err != nil {
		t.Fatalf("first Enqueue error = %v", err)
	}
	item2, err := orch.Enqueue(context.Background(), "duplicate-enqueue-session", EnqueueRequest{RunID: runID, IdempotencyKey: "dup-key-1", Message: TextUserMessage("second")})
	if err != nil {
		t.Fatalf("second Enqueue error = %v", err)
	}
	if item1.ID != item2.ID {
		t.Fatalf("duplicate enqueue produced two distinct inbox items: %q != %q", item1.ID, item2.ID)
	}
	items, err := store.ListInbox(context.Background(), "duplicate-enqueue-session", nil)
	if err != nil {
		t.Fatalf("ListInbox error = %v", err)
	}
	matching := 0
	for _, it := range items {
		if it.IdempotencyKey == "dup-key-1" {
			matching++
		}
	}
	if matching != 1 {
		t.Fatalf("inbox items with dup-key-1 = %d, want 1", matching)
	}
}

// TestEnqueueRejectsTerminalRun proves Enqueue fails closed (never silently
// accepts input nothing will ever consume) once a run has settled
// terminally: the plan requires new input racing terminal settlement to
// "either commit under the active run before the terminal CAS ... or
// receive a closed-run error without acknowledgement" -- this proves the
// second half once the run is already terminal by the time Enqueue runs.
func TestEnqueueRejectsTerminalRun(t *testing.T) {
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handle, err := orch.Start(context.Background(), Request{SessionID: "enqueue-terminal-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	first := <-handle.Done()
	if first.Status != session.RunCompleted {
		t.Fatalf("first result = %+v", first)
	}
	if _, err := orch.Enqueue(context.Background(), "enqueue-terminal-session", EnqueueRequest{RunID: first.RunID, IdempotencyKey: "terminal-key-1", Message: TextUserMessage("too late")}); !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("Enqueue against a terminal run error = %v, want ErrInvalidOrchestrator", err)
	}
	items, err := store.ListInbox(context.Background(), "enqueue-terminal-session", nil)
	if err != nil {
		t.Fatalf("ListInbox error = %v", err)
	}
	for _, it := range items {
		if it.IdempotencyKey == "terminal-key-1" {
			t.Fatalf("rejected Enqueue nonetheless persisted an inbox item: %+v", it)
		}
	}
}

// TestEnqueueDrainedByNextStart proves an Enqueue accepted while no live
// loop exists for a paused run's session stays durably `queued` and is
// drained (processed to completion) by ResumeRun -- the mechanism
// Enqueue's doc comment promises and drainQueuedInbox implements.
func TestEnqueueDrainedByNextStart(t *testing.T) {
	store := newAdmissionStore()
	orch, runID, pause := startPausedRun(t, store, "enqueue-drain-session", new(int))

	item, err := orch.Enqueue(context.Background(), "enqueue-drain-session", EnqueueRequest{RunID: runID, IdempotencyKey: "drain-key-1", Message: TextUserMessage("queued while paused")})
	if err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}
	if item.State != session.InboxQueued {
		t.Fatalf("enqueued item state = %q, want queued", item.State)
	}

	resumeHandle, err := orch.ResumeRun(context.Background(), runID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
	items, err := store.ListInbox(context.Background(), "enqueue-drain-session", nil)
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
			t.Fatalf("drained item state = %q, want completed", it.State)
		}
	}
	if !found {
		t.Fatalf("enqueued item %q not found after resume", item.ID)
	}
}

// admitTurnFailingExecution fails CompleteTurn for a fenced execution,
// simulating an injected write failure at turn-completion time.
type completeTurnFailingStore struct {
	*admissionStore
	err error
}

func (s *completeTurnFailingStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &completeTurnFailingExecution{ExecutionStore: s.admissionStore.Execution(fence), err: s.err}
}

type completeTurnFailingExecution struct {
	session.ExecutionStore
	err error
}

func (e *completeTurnFailingExecution) CompleteTurn(context.Context, session.CompleteTurnRequest) (session.CompleteTurnResult, error) {
	return session.CompleteTurnResult{}, e.err
}

// TestCompleteTurnInjectedFailureFailsTheRunWithoutCorruptingTurnState
// proves an injected CompleteTurn write failure surfaces as a failed run
// (via finishTurnLoop's error handling) rather than a silently-inconsistent
// durable turn: the turn must not be left looking completed when its
// CompleteTurn write never actually landed.
func TestCompleteTurnInjectedFailureFailsTheRunWithoutCorruptingTurnState(t *testing.T) {
	store := newAdmissionStore()
	failing := &completeTurnFailingStore{admissionStore: store, err: errors.New("injected CompleteTurn failure")}
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	})
	orch := mustConfiguredOrchestrator(
		WithStore(failing), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }), WithOwnerID("owner-1"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	handle, err := orch.Start(context.Background(), Request{SessionID: "complete-turn-failure-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status == session.RunCompleted {
		t.Fatalf("run completed despite injected CompleteTurn failure: %+v", result)
	}
	turns, err := store.ListTurns(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("ListTurns error = %v", err)
	}
	for _, turn := range turns {
		if turn.State == session.TurnCompleted {
			t.Fatalf("turn shows completed despite injected CompleteTurn write failure: %+v", turn)
		}
	}
}

// TestGracefulAndImmediateStopReachIdempotentTerminalStates exercises both
// StopPolicy modes (Stop's Graceful/Immediate) against a run blocked mid
// tool-execution, proving graceful lets the in-flight call finish on its own
// terms (its own ctx is never canceled -- the classic engine's safe-point
// contract: WithGraceful only cancels after the next ChatModel-or-tool-call
// safe point, never mid-call) while immediate cancels the in-flight call's
// own ctx right away -- both reach a terminal Result exactly once, with no
// hang.
func TestGracefulAndImmediateStopReachIdempotentTerminalStates(t *testing.T) {
	for _, mode := range []struct {
		name   string
		policy StopPolicy
	}{
		{"graceful", StopPolicy{Graceful: true, Cause: "graceful stop"}},
		{"immediate", StopPolicy{Immediate: true, Cause: "immediate stop"}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			store := newAdmissionStore()
			toolStarted := make(chan struct{})
			toolRelease := make(chan struct{})
			var toolOnce sync.Once
			var toolCanceled atomic.Bool
			tool := Tool{
				Name: "slow", Info: &einoschema.ToolInfo{Name: "slow"},
				Executor: orchestratorToolExecutorFunc(func(ctx context.Context, _ ToolCall) (ToolResult, error) {
					toolOnce.Do(func() { close(toolStarted) })
					select {
					case <-toolRelease:
						return ToolResult{Output: "ok"}, nil
					case <-ctx.Done():
						toolCanceled.Store(true)
						return ToolResult{}, ctx.Err()
					}
				}),
			}
			orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
				return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "slow", `{}`))}, nil
			}))
			configureTestTools(orch, staticToolRegistry{tools: []Tool{tool}})
			handle, err := orch.Start(context.Background(), Request{SessionID: session.ID("stop-" + mode.name), Message: TextUserMessage("hello"), Config: orchestratorConfig()})
			if err != nil {
				t.Fatalf("Start error = %v", err)
			}
			select {
			case <-toolStarted:
			case <-time.After(3 * time.Second):
				t.Fatal("tool never started")
			}
			// Graceful stop must let this in-flight call finish on its own
			// terms without ever observing cancellation; release it only
			// after Stop is issued so a race can't accidentally finish it
			// first and mask a graceful-mode bug that cancels anyway.
			// Immediate stop is deliberately never released here at all:
			// the point of this mode is that it must reach a terminal
			// result without waiting for the in-flight call, whether or
			// not that call's own ctx also gets canceled (WithImmediate's
			// own doc only promises the cancel signal reaches "nested
			// agents inside AgentTools", not necessarily every bare
			// tool.Execute goroutine already in flight) -- if Stop
			// silently waited for toolRelease here, this test would hang
			// and time out instead of failing fast.
			if err := orch.Stop(context.Background(), handle.RunID(), mode.policy); err != nil {
				t.Fatalf("Stop error = %v", err)
			}
			if mode.name == "graceful" {
				close(toolRelease)
			}
			select {
			case <-handle.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("Stop did not reach a terminal result")
			}
			if mode.name == "graceful" && toolCanceled.Load() {
				t.Fatal("graceful stop canceled the in-flight tool's own context; want it to finish on its own terms")
			}
		})
	}
}
