package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// This file proves round-six reconciliation's four Critical/Important tests
// (reviews/.../fix-pass-5/reconciliation.md item 4) and item 8's checkpoint-
// TurnID-seeding guard. All four SQLite tests drive the real
// Resume -> reconcileCrashedRun -> ResumeRun path with no hand-written
// checkpoint envelopes: direct store calls only ever construct the
// PRE-crash durable state (exactly like every earlier round's reconciliation
// tests -- w5_round3_reconciliation_test.go's dangling-turn tests, and the
// checkpoint-precedence-reviewer's own C1 probe, already establish this as
// the accepted way to simulate "what a crashed process left behind" without
// literally crashing a process); the crash itself is simulated by claiming
// the run with a near-instantly-expiring lease, then letting real wall-clock
// time pass, exactly as w5_round3_reconciliation_test.go already does.

// TestResumeRunAfterGracefulStopWithQueuedInputSurvivesCrashAgainstSQLite
// proves round-six reconciliation item 4(a) (checkpoint-precedence-
// reviewer's probe D / action item 1, Critical C1): a graceful Stop with
// queued input, followed by a crash before the resuming process does
// anything, must still recover: Resume repauses (never settles terminal
// with the queued item stranded) and a subsequent ResumeRun completes the
// run with the queued item consumed exactly once. Before round six's fix,
// crash reconciliation staged its fresh checkpoint for a DIFFERENT turn
// than ResumeRun's own selector picked (a lower-ordinal dangling turn vs. a
// higher-ordinal already-interrupted carrier), and ResumeRun refused
// permanently; round six's unified currentTurn selector (turn_loop.go)
// removes that disagreement by construction.
func TestResumeRunAfterGracefulStopWithQueuedInputSurvivesCrashAgainstSQLite(t *testing.T) {
	ctx := context.Background()
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("answered")}, nil
	}))
	defer cleanup()

	sessionID := session.ID("graceful-stop-queued-session")
	handle, err := orch.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	item, err := orch.Enqueue(ctx, sessionID, EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "graceful-stop-queued-key", Message: TextUserMessage("queued message"),
	})
	if err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}
	// Racing Stop immediately after Enqueue, with no synchronization, is
	// deliberate: it is the same technique
	// TestResumeRunAfterStopBeforeFirstDispatchAgainstSQLite uses, and
	// either race outcome (Stop landing before the first dispatch, leaving
	// the first turn's content stuck TurnAdmitted behind a content-free
	// carrier turn -- checkpoint-precedence-reviewer's exact probe D shape
	// -- or landing after it completes) must recover correctly under the
	// unified selector.
	if err := orch.Stop(ctx, handle.RunID(), StopPolicy{Graceful: true, Cause: "graceful stop with queued input"}); err != nil {
		t.Fatalf("Stop error = %v", err)
	}
	first := <-handle.Done()
	if first.Status != session.RunPaused {
		t.Skipf("Stop settled %+v instead of pausing; race not reproduced this run", first)
	}
	run, err := orch.store.GetRun(ctx, first.RunID)
	if err != nil || run.Status != session.RunPaused {
		t.Fatalf("run after stop = %+v, err=%v, want paused", run, err)
	}

	// Simulate a crashed resume process: claim the run directly with a
	// near-instantly-expiring lease, exactly like a process that claimed it
	// and died before doing anything else.
	if _, err := orch.store.ClaimRun(ctx, session.RunClaim{
		RunID: first.RunID, OwnerID: "crashed-owner", ClaimToken: "crashed-claim", LeaseDuration: time.Nanosecond,
	}); err != nil {
		t.Fatalf("simulate crashed claim: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	reconcileHandle, err := orch.Resume(ctx, first.RunID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	reconciled := <-reconcileHandle.Done()
	if reconciled.Status != session.RunPaused {
		t.Fatalf("reconcile result = %+v, want paused", reconciled)
	}

	resumeHandle, err := orch.ResumeRun(ctx, first.RunID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	final := <-resumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("final result = %+v, want completed", final)
	}
	completed, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxCompleted})
	if err != nil {
		t.Fatalf("ListInbox error = %v", err)
	}
	var count int
	for _, it := range completed {
		if it.ID == item.ID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("completed count for queued item = %d, want exactly 1", count)
	}
}

// TestResumeRunRepausesAndRedrivesOnlyInFlightTurnAfterCrashAgainstSQLite
// proves round-six reconciliation item 4(b) (checkpoint-precedence-
// reviewer's probes A/B, Critical C2): a genuine tool-interrupt pause,
// resumed and completed, followed by a second turn admitted and then a
// crash before it settles, must repause (never settle terminal) and a
// subsequent ResumeRun must redrive ONLY the in-flight second turn -- the
// first turn's completion, and its now-stale promoted checkpoint, are left
// untouched. Before round six's fix, completeTurn deleted its own promoted
// checkpoint immediately on completion, so reconcileCrashedRun's
// hasCheckpoint gate read false and settled the run terminally interrupted
// with the second turn's content stranded; round six (item 2) stops
// deleting on completion, so hasCheckpoint correctly stays true (the
// revision is merely stale, not absent).
func TestResumeRunRepausesAndRedrivesOnlyInFlightTurnAfterCrashAgainstSQLite(t *testing.T) {
	ctx := context.Background()
	var dispatches int
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		dispatches++
		return []*einoschema.AgenticMessage{agenticAssistantText("turn2 answered")}, nil
	}))
	defer cleanup()

	sessionID := session.ID("inflight-turn2-crash-session")
	newTestSession(t, ctx, orch, sessionID)
	plan := newTestToolPlan(staticToolRegistry{})
	fingerprint := planFingerprint(plan)

	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "inflight-turn2-crash-run", SessionID: sessionID, OwnerID: "owner-0", ClaimToken: "claim-0",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: plan.Descriptor(),
	}, time.Minute)
	if err != nil {
		t.Fatalf("AdmitRun: %v", err)
	}
	runID := admittedRun.ID
	execution0 := orch.store.Execution(session.RunFence{RunID: runID, ClaimToken: admittedRun.ClaimToken})
	if _, err := execution0.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// Turn 1: real content, promoted with a genuine tool-interrupt-shaped
	// checkpoint (HasRunnerState=true, Kind=Runner) recorded for turn1 --
	// exactly what a real ADK-level pause leaves behind.
	userParts1 := crashUserParts(t, "t2c-user-1", "t2c-user-1", sessionID, runID, "hello", orch.now())
	assistant1 := session.MessageID("t2c-assistant-1")
	admitted1, err := execution0.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "t2c-turn-1", RunID: runID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"t2c-user-1"}, AssistantMessageID: assistant1, CreatedAt: orch.now(),
		},
		UserMessages:         []session.Message{{ID: "t2c-user-1", SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:            userParts1,
		AssistantPlaceholder: session.Message{ID: assistant1, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "t2c-turn-1-started", SessionID: sessionID, RunID: runID, TurnID: "t2c-turn-1", Kind: session.TurnStartedEventKind, CreatedAt: orch.now()},
	})
	if err != nil {
		t.Fatalf("AdmitTurn (turn1): %v", err)
	}
	payload1 := gobEncodeTurnLoopCheckpointShape(t, turnLoopCheckpointShape{RunnerCheckpoint: []byte("runner-state-1"), HasRunnerState: true})
	envelope1 := adkCheckpointEnvelope{EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, Fingerprint: fingerprint, TurnID: admitted1.Turn.ID, Payload: payload1}
	raw1, err := encodeCheckpointEnvelope(envelope1)
	if err != nil {
		t.Fatalf("encodeCheckpointEnvelope: %v", err)
	}
	if _, err := execution0.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: session.Checkpoint{
		RunID: runID, Revision: 1, Kind: session.CheckpointKindRunner, AgentFingerprint: fingerprint,
		EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, CheckpointID: string(runID),
		Bytes: raw1, CreatedAt: orch.now(),
	}}); err != nil {
		t.Fatalf("StageCheckpoint (turn1): %v", err)
	}
	if _, err := execution0.PromotePause(ctx, session.PromotePauseRequest{
		Revision: 1, TurnID: admitted1.Turn.ID,
		Event: session.EventRecord{ID: "t2c-turn-1-paused", SessionID: sessionID, RunID: runID, Kind: session.RunPausedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("PromotePause (turn1): %v", err)
	}

	// A second process resumes turn1 for real (ResumeInterruptedTurn),
	// completes it, admits a SECOND turn with new content, then crashes
	// before that turn ever settles -- leaving turn1's now-stale checkpoint
	// (revision 1, TurnID=turn1) still promoted (round-six reconciliation
	// item 2: nothing retires it on completion anymore).
	claimed, err := orch.store.ClaimRun(ctx, session.RunClaim{RunID: runID, OwnerID: "owner-1", ClaimToken: "claim-1", LeaseDuration: time.Nanosecond})
	if err != nil {
		t.Fatalf("ClaimRun (2nd process): %v", err)
	}
	execution1 := orch.store.Execution(session.RunFence{RunID: runID, ClaimToken: claimed.ClaimToken})
	if _, err := execution1.ResumeInterruptedTurn(ctx, session.ResumeInterruptedTurnRequest{TurnID: admitted1.Turn.ID, ResumedAt: orch.now()}); err != nil {
		t.Fatalf("ResumeInterruptedTurn: %v", err)
	}
	if _, err := execution1.CompleteTurn(ctx, session.CompleteTurnRequest{
		TurnID: admitted1.Turn.ID, ResponseMessageIDs: []session.MessageID{assistant1},
		Event: session.EventRecord{ID: "t2c-turn-1-completed", SessionID: sessionID, RunID: runID, MessageID: assistant1, TurnID: admitted1.Turn.ID, Kind: session.TurnCompletedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("CompleteTurn (turn1): %v", err)
	}
	userParts2 := crashUserParts(t, "t2c-user-2", "t2c-user-2", sessionID, runID, "second message", orch.now())
	assistant2 := session.MessageID("t2c-assistant-2")
	admitted2, err := execution1.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "t2c-turn-2", RunID: runID, SessionID: sessionID, Ordinal: 2, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"t2c-user-2"}, AssistantMessageID: assistant2, CreatedAt: orch.now(),
		},
		UserMessages:         []session.Message{{ID: "t2c-user-2", SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:            userParts2,
		AssistantPlaceholder: session.Message{ID: assistant2, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "t2c-turn-2-started", SessionID: sessionID, RunID: runID, TurnID: "t2c-turn-2", Kind: session.TurnStartedEventKind, CreatedAt: orch.now()},
	})
	if err != nil {
		t.Fatalf("AdmitTurn (turn2): %v", err)
	}
	// No settlement, no repause: this reclaim simply stops here, exactly
	// like a crashed process. Its nanosecond lease is already expired by
	// the time the real Resume below runs.
	time.Sleep(2 * time.Millisecond)

	reconcileHandle, err := orch.Resume(ctx, runID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	reconciled := <-reconcileHandle.Done()
	if reconciled.Status != session.RunPaused {
		t.Fatalf("reconcile result = %+v, want paused (not terminal)", reconciled)
	}
	turn1AfterReconcile, err := orch.store.GetTurn(ctx, admitted1.Turn.ID)
	if err != nil || turn1AfterReconcile.State != session.TurnCompleted {
		t.Fatalf("turn1 after reconcile = %#v, err=%v, want still completed", turn1AfterReconcile, err)
	}
	turn2AfterReconcile, err := orch.store.GetTurn(ctx, admitted2.Turn.ID)
	if err != nil || turn2AfterReconcile.State != session.TurnInterrupted {
		t.Fatalf("turn2 after reconcile = %#v, err=%v, want interrupted", turn2AfterReconcile, err)
	}

	resumeHandle, err := orch.ResumeRun(ctx, runID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	final := <-resumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("final result = %+v, want completed", final)
	}
	if dispatches != 1 {
		t.Fatalf("model dispatches = %d, want exactly 1 (only turn2's redrive)", dispatches)
	}
	finalTurn1, err := orch.store.GetTurn(ctx, admitted1.Turn.ID)
	if err != nil || finalTurn1.State != session.TurnCompleted {
		t.Fatalf("turn1 after ResumeRun = %#v, err=%v, want completed", finalTurn1, err)
	}
	finalTurn2, err := orch.store.GetTurn(ctx, admitted2.Turn.ID)
	if err != nil || finalTurn2.State != session.TurnCompleted {
		t.Fatalf("turn2 after ResumeRun = %#v, err=%v, want completed", finalTurn2, err)
	}
	turns, err := orch.store.ListTurns(ctx, runID)
	if err != nil {
		t.Fatalf("ListTurns: %v", err)
	}
	for _, turn := range turns {
		if turn.State == session.TurnInterrupted {
			t.Fatalf("turn %+v left interrupted under a terminal run", turn)
		}
	}
}

// TestResumeRunAcceptsAfterCrashRightAfterCompleteTurnWithStaleCheckpointAgainstSQLite
// proves round-six reconciliation item 4(c) (branch-approval-reviewer's C1
// probe): a crash landing right after CompleteTurn, with nothing else
// pending, must not permanently wedge the run. Before round six's fix, a
// crash there left a promoted checkpoint recorded for a now-TurnCompleted
// turn with no dangling turn to reconcile; reconciliation repaused over it
// unchanged, and ResumeRun's own TurnID-consistency check then refused it
// forever, with Resume routing straight back into the same refusal --
// bricking the whole session (Start also failed, since the run stayed
// nonterminal). Round six mints a fresh, content-free carrier turn whenever
// hasCheckpoint is true but nothing is currently in flight, so ResumeRun
// always finds a turn its checkpoint validly names, and -- finding nothing
// left to redrive or drain -- completes the run on its own.
func TestResumeRunAcceptsAfterCrashRightAfterCompleteTurnWithStaleCheckpointAgainstSQLite(t *testing.T) {
	ctx := context.Background()
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model must never be dispatched: nothing is left pending to redrive")
		return nil, nil
	}))
	defer cleanup()

	sessionID := session.ID("stale-after-complete-session")
	newTestSession(t, ctx, orch, sessionID)
	plan := newTestToolPlan(staticToolRegistry{})
	fingerprint := planFingerprint(plan)

	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "stale-after-complete-run", SessionID: sessionID, OwnerID: "owner-0", ClaimToken: "claim-0",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: plan.Descriptor(),
	}, time.Minute)
	if err != nil {
		t.Fatalf("AdmitRun: %v", err)
	}
	runID := admittedRun.ID
	execution0 := orch.store.Execution(session.RunFence{RunID: runID, ClaimToken: admittedRun.ClaimToken})
	if _, err := execution0.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	userParts := crashUserParts(t, "bac1-user-1", "bac1-user-1", sessionID, runID, "hello", orch.now())
	assistantID := session.MessageID("bac1-assistant-1")
	admitted, err := execution0.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "bac1-turn-1", RunID: runID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"bac1-user-1"}, AssistantMessageID: assistantID, CreatedAt: orch.now(),
		},
		UserMessages:         []session.Message{{ID: "bac1-user-1", SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:            userParts,
		AssistantPlaceholder: session.Message{ID: assistantID, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "bac1-turn-1-started", SessionID: sessionID, RunID: runID, TurnID: "bac1-turn-1", Kind: session.TurnStartedEventKind, CreatedAt: orch.now()},
	})
	if err != nil {
		t.Fatalf("AdmitTurn: %v", err)
	}
	payload := gobEncodeTurnLoopCheckpointShape(t, turnLoopCheckpointShape{RunnerCheckpoint: []byte("runner-state"), HasRunnerState: true})
	envelope := adkCheckpointEnvelope{EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, Fingerprint: fingerprint, TurnID: admitted.Turn.ID, Payload: payload}
	raw, err := encodeCheckpointEnvelope(envelope)
	if err != nil {
		t.Fatalf("encodeCheckpointEnvelope: %v", err)
	}
	if _, err := execution0.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: session.Checkpoint{
		RunID: runID, Revision: 1, Kind: session.CheckpointKindRunner, AgentFingerprint: fingerprint,
		EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, CheckpointID: string(runID),
		Bytes: raw, CreatedAt: orch.now(),
	}}); err != nil {
		t.Fatalf("StageCheckpoint: %v", err)
	}
	if _, err := execution0.PromotePause(ctx, session.PromotePauseRequest{
		Revision: 1, TurnID: admitted.Turn.ID,
		Event: session.EventRecord{ID: "bac1-turn-1-paused", SessionID: sessionID, RunID: runID, Kind: session.RunPausedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("PromotePause: %v", err)
	}

	// Second process resumes turn1 for real and completes it, then crashes
	// immediately -- no retirement, no repause, no settlement. Nothing is
	// left admitted or interrupted: the promoted checkpoint (revision 1,
	// TurnID=turn1) is now stale by fact (round-six reconciliation item 2).
	claimed, err := orch.store.ClaimRun(ctx, session.RunClaim{RunID: runID, OwnerID: "owner-1", ClaimToken: "claim-1", LeaseDuration: time.Nanosecond})
	if err != nil {
		t.Fatalf("ClaimRun (2nd process): %v", err)
	}
	execution1 := orch.store.Execution(session.RunFence{RunID: runID, ClaimToken: claimed.ClaimToken})
	if _, err := execution1.ResumeInterruptedTurn(ctx, session.ResumeInterruptedTurnRequest{TurnID: admitted.Turn.ID, ResumedAt: orch.now()}); err != nil {
		t.Fatalf("ResumeInterruptedTurn: %v", err)
	}
	if _, err := execution1.CompleteTurn(ctx, session.CompleteTurnRequest{
		TurnID: admitted.Turn.ID, ResponseMessageIDs: []session.MessageID{assistantID},
		Event: session.EventRecord{ID: "bac1-turn-1-completed", SessionID: sessionID, RunID: runID, MessageID: assistantID, TurnID: admitted.Turn.ID, Kind: session.TurnCompletedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("CompleteTurn: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	reconcileHandle, err := orch.Resume(ctx, runID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	reconciled := <-reconcileHandle.Done()
	if reconciled.Status != session.RunPaused {
		t.Fatalf("reconcile result = %+v, want paused (a fresh carrier turn, resumable)", reconciled)
	}
	run, err := orch.store.GetRun(ctx, runID)
	if err != nil || run.Status != session.RunPaused {
		t.Fatalf("run after reconcile = %+v, err=%v, want paused", run, err)
	}

	resumeHandle, err := orch.ResumeRun(ctx, runID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	final := <-resumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("final result = %+v, want completed with nothing left to redrive", final)
	}
}

// TestStopAbandonSettlesPausedRunAndFreesSessionForNewStart proves round-six
// reconciliation item 4(d): the "Stop-with-abandon" operator escape settles
// a durably paused run terminally interrupted, retires its checkpoints, and
// frees the session for a fresh Start -- the documented way forward for any
// paused run a host does not intend to resume (see StopPolicy.Abandon).
func TestStopAbandonSettlesPausedRunAndFreesSessionForNewStart(t *testing.T) {
	ctx := context.Background()
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
	}))
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	sessionID := session.ID("abandon-session")
	handle, err := orch.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	if _, ok := <-handle.AwaitPause(); !ok {
		t.Fatal("AwaitPause closed with no value")
	}
	if _, found, err := orch.store.ReadPromotedCheckpoint(ctx, result.RunID); err != nil || !found {
		t.Fatalf("promoted checkpoint before abandon: found=%v err=%v", found, err)
	}

	if err := orch.Stop(ctx, result.RunID, StopPolicy{Abandon: true, Cause: "operator abandon"}); err != nil {
		t.Fatalf("Stop(Abandon) error = %v", err)
	}
	run, err := orch.store.GetRun(ctx, result.RunID)
	if err != nil || run.Status != session.RunInterrupted || !run.Terminal() {
		t.Fatalf("run after abandon = %+v, err=%v, want terminal interrupted", run, err)
	}
	if _, found, err := orch.store.ReadPromotedCheckpoint(ctx, result.RunID); err != nil || found {
		t.Fatalf("promoted checkpoint after abandon: found=%v err=%v, want retired", found, err)
	}

	// A fresh Start on the same session must succeed now: the session is no
	// longer busy.
	second, err := orch.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("hello again"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start after abandon error = %v", err)
	}
	<-second.Done()

	// A second Stop(Abandon) against the now-terminal (not paused) original
	// run reports the standard ErrInvalidOrchestrator rather than silently
	// succeeding again.
	if err := orch.Stop(ctx, result.RunID, StopPolicy{Abandon: true}); !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("Stop(Abandon) on an already-terminal run = %v, want ErrInvalidOrchestrator", err)
	}
}

// TestAdkCheckpointStoreStageSucceedsOnceSeededWithNoPriorGenInput proves
// round-six reconciliation item 8 (branch-approval-reviewer S3 / action item
// 6): stage() must never observe an empty currentTurnID on the exact
// construction Start (orchestrator.go) and ResumeRun (turn_loop.go) now
// perform -- setCurrentTurnID called synchronously, before their TurnLoop is
// even constructed, with no coordinator, GenInput, or GenResume call
// involved at all. This is deliberately a fully deterministic, single-
// threaded reproduction rather than a live race against a real TurnLoop
// (the window this closes -- an upstream Set landing before the first
// GenInput/GenResume call -- was not reproducible live even after 120
// iterations in the branch-approval-reviewer's own probe E; racing it here
// would be equally flaky and prove nothing a revert of the seeding call
// would reliably fail).
func TestAdkCheckpointStoreStageSucceedsOnceSeededWithNoPriorGenInput(t *testing.T) {
	ctx := context.Background()
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model must never be dispatched: this test drives the checkpoint store directly")
		return nil, nil
	}))
	defer cleanup()

	sessionID := session.ID("stage-seed-session")
	newTestSession(t, ctx, orch, sessionID)
	plan := newTestToolPlan(staticToolRegistry{})

	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "stage-seed-run", SessionID: sessionID, OwnerID: "owner-0", ClaimToken: "claim-0",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: plan.Descriptor(),
	}, time.Minute)
	if err != nil {
		t.Fatalf("AdmitRun: %v", err)
	}
	runID := admittedRun.ID
	// newRunExecution is this package's own internal execution wrapper --
	// the exact type newAdkCheckpointStore requires, and what production
	// Start/ResumeRun/reconcileCrashedRun all build around a claimed run.
	execution := newRunExecution(orch, plan, admittedRun)
	if _, err := execution.store.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	userParts := crashUserParts(t, "stage-seed-user-1", "stage-seed-user-1", sessionID, runID, "hello", orch.now())
	assistantID := session.MessageID("stage-seed-assistant-1")
	admitted, err := execution.store.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "stage-seed-turn-1", RunID: runID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"stage-seed-user-1"}, AssistantMessageID: assistantID, CreatedAt: orch.now(),
		},
		UserMessages:         []session.Message{{ID: "stage-seed-user-1", SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:            userParts,
		AssistantPlaceholder: session.Message{ID: assistantID, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "stage-seed-turn-1-started", SessionID: sessionID, RunID: runID, TurnID: "stage-seed-turn-1", Kind: session.TurnStartedEventKind, CreatedAt: orch.now()},
	})
	if err != nil {
		t.Fatalf("AdmitTurn: %v", err)
	}

	// A fresh checkpoint store, exactly as Start/ResumeRun construct one,
	// with NO coordinator and NO GenInput/GenResume call ever having run --
	// simulating an upstream Set landing in the window between construction
	// and the first GenInput/GenResume call.
	unseeded := newAdkCheckpointStore(orch, execution, plan, runID)
	if err := unseeded.stageLoopCheckpoint(ctx); err == nil {
		t.Fatal("stageLoopCheckpoint with no currentTurnID set = nil error, want the documented guard to fire")
	}

	seeded := newAdkCheckpointStore(orch, execution, plan, runID)
	// The exact call Start (orchestrator.go) and ResumeRun (turn_loop.go)
	// now make synchronously, before their TurnLoop is even constructed --
	// see both call sites' doc comments.
	seeded.setCurrentTurnID(admitted.Turn.ID)
	if err := seeded.stageLoopCheckpoint(ctx); err != nil {
		t.Fatalf("stageLoopCheckpoint after seeding currentTurnID = %v, want success with no prior GenInput/setEngine call", err)
	}
}
