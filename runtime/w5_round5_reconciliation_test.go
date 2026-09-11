package runtime

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// gobEncodeTurnLoopCheckpointShape gob-encodes shape exactly as
// adkCheckpointStore.stage/stageLoopCheckpoint do in production (see
// marshalEmptyLoopCheckpoint), letting a test construct a promoted
// checkpoint envelope with a caller-chosen UnhandledItems set -- production
// code only ever writes the empty shape itself (a real non-empty
// UnhandledItems payload is normally upstream ADK's own Set call, driven by
// its shouldSaveCheckpoint computation, which a unit test cannot trigger
// deterministically without racing a live TurnLoop's dispatch).
func gobEncodeTurnLoopCheckpointShape(t *testing.T, shape turnLoopCheckpointShape) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(shape); err != nil {
		t.Fatalf("gob encode turnLoopCheckpointShape: %v", err)
	}
	return buf.Bytes()
}

// TestResumeRunRedrivesReconciledTurnPastStaleCheckpointUnhandledItems
// proves round-five reconciliation item 1 (TR-C1): the reconciled-turn
// sentinel genInput pushes for ResumeRun must be recognised anywhere in the
// GenInput batch, not only at items[0]. Upstream's own tryLoadCheckpoint
// builds that batch as cp.UnhandledItems ++ newItems
// (eino@v0.9.19/adk/turn_loop.go), so a promoted checkpoint whose
// UnhandledItems is non-empty -- the shape a between-turn stop with queued
// input leaves behind (promoteQueuedContinuation) -- pushes the sentinel
// off index 0 whenever ResumeRun also needs to redrive a crash-reconciled
// turn in the same call.
//
// This test builds that exact durable shape directly against the real
// SQLite store (a degenerate "between turns" checkpoint carrier turn
// promotes a checkpoint whose UnhandledItems still names an item a LATER
// turn has since durably consumed and a crash left dangling; that later
// turn is then reconciled -- ReconcileInterruptedTurn + RepauseRun,
// exactly as reconcileCrashedRun leaves it, and exactly reproducing round-
// five reviewer probe 3) rather than racing a live TurnLoop's dispatch,
// so the promoted checkpoint's UnhandledItems is deterministic. Before the
// round-five fix this made the follow-up ResumeRun fail the run outright
// (`invalid orchestrator: TurnLoop GenInput delivered only already-
// admitted items with no prior turn to fall back to`) instead of redriving
// the reconciled turn -- reverting genInput's sentinel scan back to
// items[0] reproduces that failure and fails this test.
func TestResumeRunRedrivesReconciledTurnPastStaleCheckpointUnhandledItems(t *testing.T) {
	ctx := context.Background()
	var dispatches int
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		dispatches++
		return []*einoschema.AgenticMessage{agenticAssistantText(fmt.Sprintf("reply-%d", dispatches))}, nil
	}))
	defer cleanup()

	sessionID := session.ID("stale-checkpoint-session")
	newTestSession(t, ctx, orch, sessionID)
	plan := newTestToolPlan(staticToolRegistry{})
	fingerprint := planFingerprint(plan)

	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "stale-checkpoint-run", SessionID: sessionID, OwnerID: "owner-0", ClaimToken: "claim-0",
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

	// The item a between-turn stop leaves buffered but undispatched: it
	// stays durably `queued` at the moment the checkpoint below is staged
	// (promoteQueuedContinuation never touches it), matching production.
	item, err := orch.store.EnqueueInbox(ctx, crashInboxItem("stale-inbox-1", sessionID, "stale-key-1", "queued message", orch.now()), session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EnqueueInbox: %v", err)
	}

	// A degenerate, content-free carrier turn purely to hold PromotePause's
	// required turn identity -- exactly promoteQueuedContinuation's own
	// shape -- distinct from the real turn admitted below.
	carrierAssistantID := session.MessageID("stale-carrier-assistant")
	carrierResult, err := execution0.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "stale-carrier-turn", RunID: runID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted,
			AssistantMessageID: carrierAssistantID, CreatedAt: orch.now(),
		},
		AssistantPlaceholder: session.Message{ID: carrierAssistantID, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "stale-carrier-started", SessionID: sessionID, RunID: runID, TurnID: "stale-carrier-turn", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
	})
	if err != nil {
		t.Fatalf("AdmitTurn (carrier): %v", err)
	}

	// Stage + promote a Kind=loop checkpoint whose UnhandledItems still
	// names the queued item -- upstream's real shape for a between-turn
	// stop with input still buffered but undispatched.
	payload := gobEncodeTurnLoopCheckpointShape(t, turnLoopCheckpointShape{UnhandledItems: []session.InboxID{item.ID}})
	envelope := adkCheckpointEnvelope{EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, Fingerprint: fingerprint, Payload: payload}
	raw, err := encodeCheckpointEnvelope(envelope)
	if err != nil {
		t.Fatalf("encodeCheckpointEnvelope: %v", err)
	}
	if _, err := execution0.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: session.Checkpoint{
		RunID: runID, Revision: 1, Kind: session.CheckpointKindLoop, AgentFingerprint: fingerprint,
		EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, CheckpointID: string(runID),
		Bytes: raw, CreatedAt: orch.now(),
	}}); err != nil {
		t.Fatalf("StageCheckpoint: %v", err)
	}
	if _, err := execution0.PromotePause(ctx, session.PromotePauseRequest{
		Revision: 1, TurnID: carrierResult.Turn.ID,
		Event: session.EventRecord{ID: "stale-carrier-paused", SessionID: sessionID, RunID: runID, Kind: session.RunPausedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("PromotePause: %v", err)
	}

	// A second process resumes, admits a real turn consuming the still-
	// queued item, and crashes before it ever settles: reconciliation
	// leaves it TurnInterrupted with the item InboxInterrupted, and
	// (RepauseRun's own documented contract) the checkpoint above stays
	// promoted unchanged.
	claimed, err := orch.store.ClaimRun(ctx, session.RunClaim{RunID: runID, OwnerID: "owner-1", ClaimToken: "claim-1", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	execution1 := orch.store.Execution(session.RunFence{RunID: runID, ClaimToken: claimed.ClaimToken})
	if _, err := execution1.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("StartRun (2nd process): %v", err)
	}
	userParts := crashUserParts(t, "stale-user-1", "stale-user-1", sessionID, runID, "queued message", orch.now())
	reconciledAssistantID := session.MessageID("stale-reconciled-assistant")
	admitResult, err := execution1.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "stale-reconciled-turn", RunID: runID, SessionID: sessionID, Ordinal: 2, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"stale-user-1"}, AssistantMessageID: reconciledAssistantID, CreatedAt: orch.now(),
		},
		UserMessages:         []session.Message{{ID: "stale-user-1", SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:            userParts,
		AssistantPlaceholder: session.Message{ID: reconciledAssistantID, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "stale-reconciled-started", SessionID: sessionID, RunID: runID, TurnID: "stale-reconciled-turn", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
		InboxIDs:             []session.InboxID{item.ID},
	})
	if err != nil {
		t.Fatalf("AdmitTurn (reconciled): %v", err)
	}
	if _, err := execution1.ReconcileInterruptedTurn(ctx, session.ReconcileInterruptedTurnRequest{
		TurnID: admitResult.Turn.ID,
		Event:  session.EventRecord{ID: "stale-reconciled-crash", SessionID: sessionID, RunID: runID, TurnID: admitResult.Turn.ID, Kind: session.RunPausedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("ReconcileInterruptedTurn: %v", err)
	}
	if _, err := execution1.RepauseRun(ctx, session.RepauseRunRequest{
		Event: session.EventRecord{ID: "stale-run-repaused", SessionID: sessionID, RunID: runID, Kind: session.RunPausedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("RepauseRun: %v", err)
	}

	// Sanity on the state ResumeRun is about to read: the promoted
	// checkpoint's UnhandledItems is [item.ID], but item.ID is no longer
	// InboxQueued (it is InboxInterrupted, consumed by the reconciled
	// turn) -- exactly the stale-UnhandledItems shape TR-C1 describes.
	checkpoint, ok, err := orch.store.ReadPromotedCheckpoint(ctx, runID)
	if err != nil || !ok {
		t.Fatalf("ReadPromotedCheckpoint: ok=%v err=%v", ok, err)
	}
	if checkpoint.Revision != 1 {
		t.Fatalf("promoted checkpoint revision = %d, want 1 (untouched by RepauseRun)", checkpoint.Revision)
	}
	interruptedBefore, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxInterrupted})
	if err != nil || len(interruptedBefore) != 1 || interruptedBefore[0].ID != item.ID {
		t.Fatalf("interrupted inbox before ResumeRun = %#v, %v, want exactly item.ID", interruptedBefore, err)
	}

	// The actual system under test: ResumeRun must redrive the reconciled
	// turn under its own TurnID despite the stale UnhandledItems ahead of
	// the sentinel in upstream's batch.
	resumeHandle, err := orch.ResumeRun(ctx, runID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	final := <-resumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("ResumeRun result = %+v, want completed with no error (a reverted items[0]-only sentinel scan fails this)", final)
	}
	if dispatches != 1 {
		t.Fatalf("model dispatches = %d, want exactly 1 (only the reconciled turn's redrive)", dispatches)
	}

	finalTurn, err := orch.store.GetTurn(ctx, admitResult.Turn.ID)
	if err != nil || finalTurn.State != session.TurnCompleted {
		t.Fatalf("reconciled turn after ResumeRun = %#v, err=%v, want completed", finalTurn, err)
	}
	carrierTurn, err := orch.store.GetTurn(ctx, carrierResult.Turn.ID)
	if err != nil || carrierTurn.State != session.TurnInterrupted {
		t.Fatalf("carrier turn after ResumeRun = %#v, err=%v, want still interrupted (never redriven)", carrierTurn, err)
	}
	completed, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxCompleted})
	if err != nil || len(completed) != 1 || completed[0].ID != item.ID {
		t.Fatalf("completed inbox = %#v, %v, want exactly item.ID completed once", completed, err)
	}
}

// TestResumeRunAllStaleCheckpointBatchCompletesRatherThanFailing proves the
// second half of round-five reconciliation item 1 (TR-C1): a fresh
// ResumeRun coordinator whose very first GenInput batch is entirely stale
// ids (every id in the promoted checkpoint's UnhandledItems already durably
// settled by the time this resume runs, and no reconciled turn to push a
// sentinel for) must not fail the run outright -- there is no prior engine
// for the old code's "no prior turn to fall back to" fallback to use, and
// erroring there turned a harmless duplicate/stale delivery into a hard
// RunFailed with no answer. It must settle the run cleanly instead.
func TestResumeRunAllStaleCheckpointBatchCompletesRatherThanFailing(t *testing.T) {
	ctx := context.Background()
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model must never be dispatched: every id in this batch is already settled")
		return nil, nil
	}))
	defer cleanup()

	sessionID := session.ID("all-stale-session")
	newTestSession(t, ctx, orch, sessionID)
	plan := newTestToolPlan(staticToolRegistry{})
	fingerprint := planFingerprint(plan)

	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "all-stale-run", SessionID: sessionID, OwnerID: "owner-0", ClaimToken: "claim-0",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: plan.Descriptor(),
	}, time.Minute)
	if err != nil {
		t.Fatalf("AdmitRun: %v", err)
	}
	runID := admittedRun.ID
	execution := orch.store.Execution(session.RunFence{RunID: runID, ClaimToken: admittedRun.ClaimToken})
	if _, err := execution.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// An item that reaches InboxCompleted before the checkpoint below is
	// ever read by ResumeRun -- admitted and completed by an ordinary turn,
	// exactly as a normal earlier delivery would leave it.
	item, err := orch.store.EnqueueInbox(ctx, crashInboxItem("all-stale-inbox-1", sessionID, "all-stale-key-1", "already answered", orch.now()), session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EnqueueInbox: %v", err)
	}
	userParts := crashUserParts(t, "all-stale-user-1", "all-stale-user-1", sessionID, runID, "already answered", orch.now())
	settledAssistantID := session.MessageID("all-stale-assistant-1")
	settledTurn, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "all-stale-settled-turn", RunID: runID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"all-stale-user-1"}, AssistantMessageID: settledAssistantID, CreatedAt: orch.now(),
		},
		UserMessages:         []session.Message{{ID: "all-stale-user-1", SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:            userParts,
		AssistantPlaceholder: session.Message{ID: settledAssistantID, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "all-stale-settled-started", SessionID: sessionID, RunID: runID, TurnID: "all-stale-settled-turn", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
		InboxIDs:             []session.InboxID{item.ID},
	})
	if err != nil {
		t.Fatalf("AdmitTurn (settled): %v", err)
	}
	if _, err := execution.CompleteTurn(ctx, session.CompleteTurnRequest{
		TurnID: settledTurn.Turn.ID, ResponseMessageIDs: []session.MessageID{settledAssistantID},
		Event: session.EventRecord{ID: "all-stale-settled-completed", SessionID: sessionID, RunID: runID, MessageID: settledAssistantID, TurnID: settledTurn.Turn.ID, Kind: session.TurnCompletedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("CompleteTurn: %v", err)
	}

	// A degenerate carrier turn holds the checkpoint's required identity;
	// its promoted checkpoint's UnhandledItems still names the now-
	// completed item -- stale by the time this test's ResumeRun runs, with
	// no interrupted turn anywhere for a reconciled-turn sentinel to carry.
	carrierAssistantID := session.MessageID("all-stale-carrier-assistant")
	carrierResult, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "all-stale-carrier-turn", RunID: runID, SessionID: sessionID, Ordinal: 2, State: session.TurnAdmitted,
			AssistantMessageID: carrierAssistantID, CreatedAt: orch.now(),
		},
		AssistantPlaceholder: session.Message{ID: carrierAssistantID, SessionID: sessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "all-stale-carrier-started", SessionID: sessionID, RunID: runID, TurnID: "all-stale-carrier-turn", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
	})
	if err != nil {
		t.Fatalf("AdmitTurn (carrier): %v", err)
	}
	payload := gobEncodeTurnLoopCheckpointShape(t, turnLoopCheckpointShape{UnhandledItems: []session.InboxID{item.ID}})
	envelope := adkCheckpointEnvelope{EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, Fingerprint: fingerprint, Payload: payload}
	raw, err := encodeCheckpointEnvelope(envelope)
	if err != nil {
		t.Fatalf("encodeCheckpointEnvelope: %v", err)
	}
	if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: session.Checkpoint{
		RunID: runID, Revision: 1, Kind: session.CheckpointKindLoop, AgentFingerprint: fingerprint,
		EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, CheckpointID: string(runID),
		Bytes: raw, CreatedAt: orch.now(),
	}}); err != nil {
		t.Fatalf("StageCheckpoint: %v", err)
	}
	if _, err := execution.PromotePause(ctx, session.PromotePauseRequest{
		Revision: 1, TurnID: carrierResult.Turn.ID,
		Event: session.EventRecord{ID: "all-stale-carrier-paused", SessionID: sessionID, RunID: runID, Kind: session.RunPausedEventKind, CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("PromotePause: %v", err)
	}

	resumeHandle, err := orch.ResumeRun(ctx, runID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	final := <-resumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("ResumeRun result = %+v, want completed with no error (an all-stale batch must not fail the run)", final)
	}
}
