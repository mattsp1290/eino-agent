package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// This file proves round-three reconciliation item 4 (SR-4 / RD-S5) against
// the real SQLite store: (a) a post-claim StartRun failure on ResumeRun
// compensates with a re-pause instead of stranding the run `running` with
// no driver; (b) a turn a crashed process left admitted/running is
// conservatively reconciled -- interrupted, its consumed inbox items
// carried forward as `interrupted`, never requeued to `queued` -- on the
// next Resume; (c) a `running` run whose lease expired and which has a
// promoted checkpoint is recoverable (reclaim -> reconcile -> paused), not
// stranded behind ErrSessionBusy.

// startRunFailOnceStore fails exactly the first ExecutionStore.StartRun
// call any fence it hands out makes, then behaves normally.
type startRunFailOnceStore struct {
	session.Store
	failed bool
}

func (s *startRunFailOnceStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &startRunFailOnceExecution{ExecutionStore: s.Store.Execution(fence), gate: s}
}

type startRunFailOnceExecution struct {
	session.ExecutionStore
	gate *startRunFailOnceStore
}

func (e *startRunFailOnceExecution) StartRun(ctx context.Context, startedAt time.Time) (session.Run, error) {
	if !e.gate.failed {
		e.gate.failed = true
		return session.Run{}, errors.New("injected StartRun failure")
	}
	return e.ExecutionStore.StartRun(ctx, startedAt)
}

// TestResumeRunStartFailureRepauses proves item 4a (SR-I4): a post-claim
// StartRun failure on ResumeRun's path must not strand the run `running`
// with no driver -- it must compensate back to paused so a later ResumeRun
// can simply try again.
func TestResumeRunStartFailureRepauses(t *testing.T) {
	ctx := context.Background()
	sqliteStore, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	defer func() { _ = pool.Close() }()

	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	var calls int
	orch, err := NewStreamingOrchestrator(
		WithStore(sqliteStore), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			calls++
			if calls == 1 {
				return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
			}
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		})}),
		WithIDGenerator(&sequenceIDs{}), WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("sqlite-owner-repause"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(ctx, Request{SessionID: "sqlite-repause-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}

	failingOrch, err := NewStreamingOrchestrator(
		WithStore(&startRunFailOnceStore{Store: sqliteStore}), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		})}),
		WithIDGenerator(&sequenceIDs{}), WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 1, 0, time.UTC) }),
		WithOwnerID("sqlite-owner-repause"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator (failing): %v", err)
	}
	configureTestTools(failingOrch, staticToolRegistry{tools: []Tool{gate}})

	resumeHandle, err := failingOrch.ResumeRun(ctx, result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunPaused {
		t.Fatalf("resumed result after injected StartRun failure = %+v, want paused (compensating repause)", resumed)
	}

	// The run must not be stranded: GetRun shows paused with no live
	// lease, and a further ResumeRun (against the real, non-failing store)
	// succeeds and completes normally.
	stranded, err := orch.store.GetRun(ctx, result.RunID)
	if err != nil || stranded.Status != session.RunPaused {
		t.Fatalf("run after injected failure = %+v, err=%v, want status=paused", stranded, err)
	}
	if stranded.LeaseUntil.After(time.Now().UTC()) {
		t.Fatalf("run after injected failure retains a live lease: %v", stranded.LeaseUntil)
	}
	secondResumeHandle, err := orch.ResumeRun(ctx, result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("second ResumeRun error = %v", err)
	}
	final := <-secondResumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("final resumed result = %+v", final)
	}
}

func newTestSession(t *testing.T, ctx context.Context, orch *StreamingOrchestrator, id session.ID) session.Session {
	t.Helper()
	s, err := orch.store.CreateSession(ctx, session.Session{ID: id, Directory: "/workspace", Title: string(id), CreatedAt: orch.now(), UpdatedAt: orch.now()})
	if err != nil {
		t.Fatalf("create session %s: %v", id, err)
	}
	return s
}

func crashInboxItem(id session.InboxID, sessionID session.ID, key, text string, at time.Time) session.InboxItem {
	return session.InboxItem{
		ID: id, SessionID: sessionID, IdempotencyKey: key,
		Blocks: []session.ContentBlock{{ID: "block-1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: text}}},
		State:  session.InboxQueued, CreatedAt: at, UpdatedAt: at,
	}
}

// crashUserParts encodes real session.Part rows for a user message carrying
// text, exactly as the real admitTurn (runtime/turn_loop.go) does via
// session.EncodeContentParts -- used so the round-four reconciliation item 1
// (CR-C1) tests below reproduce the actual shape a crashed process leaves
// behind (a bare session.Message with no Part rows cannot see the
// duplicated-history defect: decoding it yields no text to compare).
func crashUserParts(t *testing.T, partIDPrefix string, msgID session.MessageID, sessionID session.ID, runID session.RunID, text string, at time.Time) []session.Part {
	t.Helper()
	content := session.Content{Role: session.RoleUser, Blocks: []session.ContentBlock{{ID: "block-1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: text}}}}
	n := 0
	parts, err := session.EncodeContentParts(content, func() session.PartID {
		n++
		return session.PartID(fmt.Sprintf("%s-part-%d", partIDPrefix, n))
	}, msgID, sessionID, runID, at, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	return parts
}

// countUserMessageText lists sessionID's full committed history and counts
// how many user messages decode to content containing exactly one
// user_input_text block whose text equals want.
func countUserMessageText(t *testing.T, ctx context.Context, store session.Store, sessionID session.ID, want string) int {
	t.Helper()
	batch, err := store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 1000})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	partsByMessage := make(map[session.MessageID][]session.Part, len(batch.Messages))
	for i, part := range batch.Parts {
		owner := part.MessageID
		if len(batch.PartOwnerMessageIDs) == len(batch.Parts) {
			owner = batch.PartOwnerMessageIDs[i]
		}
		partsByMessage[owner] = append(partsByMessage[owner], part)
	}
	count := 0
	for _, msg := range batch.Messages {
		if msg.Role != session.RoleUser {
			continue
		}
		content, err := session.DecodeContentParts(session.RoleUser, partsByMessage[msg.ID], session.DefaultContentLimits())
		if err != nil {
			continue
		}
		for _, block := range content.Blocks {
			if block.Kind == session.BlockKindUserInputText && block.Text != nil && block.Text.Text == want {
				count++
			}
		}
	}
	return count
}

// TestResumeReconcilesDanglingAdmittedTurnAfterCrash proves item 4b (SR-4/
// RD-S5): a turn admitted directly against the store (simulating a process
// that admitted it, then crashed before ever calling StartRun, driving the
// TurnLoop, or staging a checkpoint) is conservatively reconciled on the
// next Resume: interrupted, its consumed inbox item carried forward as
// `interrupted` -- never requeued to `queued`. With no checkpoint ever
// promoted for this run, there is nothing to resume, so it settles
// interrupted -- the reconciled turn's content is answered only via its
// own already-committed history, on some later run over this session.
func TestResumeReconcilesDanglingAdmittedTurnAfterCrash(t *testing.T) {
	ctx := context.Background()
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("unused")}, nil
	}))
	defer cleanup()

	sessionID := session.ID("crash-session-1")
	newTestSession(t, ctx, orch, sessionID)
	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "crash-run-1", SessionID: sessionID, OwnerID: "owner-1", ClaimToken: "claim-1",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: newTestToolPlan(staticToolRegistry{}).Descriptor(),
	}, time.Nanosecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := orch.store.Execution(session.RunFence{RunID: admittedRun.ID, ClaimToken: admittedRun.ClaimToken})
	item, err := orch.store.EnqueueInbox(ctx, crashInboxItem("crash-inbox-1", sessionID, "crash-key-1", "hello", orch.now()), session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("enqueue inbox: %v", err)
	}
	// Real UserParts, exactly as the real admitTurn would encode them (round-
	// four reconciliation item 1/CR-C1): the shipped round-three test built a
	// bare, content-free session.Message, which cannot see the duplicated-
	// history defect this test now guards against.
	userParts := crashUserParts(t, "crash-user-msg-1", "crash-user-msg-1", sessionID, admittedRun.ID, "hello", orch.now())
	if _, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn:         session.Turn{ID: "crash-turn-1", RunID: admittedRun.ID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted, UserMessageIDs: []session.MessageID{"crash-user-msg-1"}, CreatedAt: orch.now()},
		UserMessages: []session.Message{{ID: "crash-user-msg-1", SessionID: sessionID, RunID: admittedRun.ID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:    userParts,
		Event:        session.EventRecord{ID: "crash-turn-1-started", SessionID: sessionID, RunID: admittedRun.ID, TurnID: "crash-turn-1", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
		InboxIDs:     []session.InboxID{item.ID},
	}); err != nil {
		t.Fatalf("admit turn: %v", err)
	}
	if _, err := execution.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("start run: %v", err)
	}
	// The lease (1ns) is already expired by real wall-clock time; no
	// checkpoint was ever staged and nothing was ever settled -- exactly
	// what a crashed process leaves behind.
	time.Sleep(2 * time.Millisecond)

	if got := countUserMessageText(t, ctx, orch.store, sessionID, "hello"); got != 1 {
		t.Fatalf("committed 'hello' user messages before reconciliation = %d, want 1", got)
	}

	handle, err := orch.Resume(ctx, admittedRun.ID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted || !result.Interrupted {
		t.Fatalf("reconciled result = %+v, want interrupted (no checkpoint to resume from)", result)
	}
	turn, err := orch.store.GetTurn(ctx, "crash-turn-1")
	if err != nil || turn.State != session.TurnInterrupted {
		t.Fatalf("dangling turn after reconciliation = %#v, err=%v, want interrupted", turn, err)
	}
	// The item must NOT be requeued (that would let a fresh AdmitTurn mint
	// a second, duplicate user message from the same content on a later
	// run): it settles InboxInterrupted, exactly like InterruptTurn, still
	// linked to the turn that already durably consumed it.
	interruptedItems, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxInterrupted})
	if err != nil || len(interruptedItems) != 1 || interruptedItems[0].ID != item.ID || interruptedItems[0].TurnID != "crash-turn-1" {
		t.Fatalf("interrupted inbox = %#v, %v, want exactly the reconciled item, still linked to crash-turn-1", interruptedItems, err)
	}
	if requeued, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxQueued}); err != nil || len(requeued) != 0 {
		t.Fatalf("queued inbox after reconciliation = %#v, %v, want none (never requeued)", requeued, err)
	}
	finalRun, err := orch.store.GetRun(ctx, admittedRun.ID)
	if err != nil || finalRun.Status != session.RunInterrupted {
		t.Fatalf("final run = %+v, err=%v, want interrupted", finalRun, err)
	}
	// The user's text is still in committed history exactly once -- it was
	// never removed, and (with the run now terminal) never re-admitted
	// either.
	if got := countUserMessageText(t, ctx, orch.store, sessionID, "hello"); got != 1 {
		t.Fatalf("committed 'hello' user messages after reconciliation = %d, want exactly 1 (no duplicate)", got)
	}
}

// TestResumeReclaimsLeaseExpiredRunningRunWithPromotedCheckpoint proves
// item 4c (SR-4): a `running` run whose lease expired -- crashed a SECOND
// time, mid its resumed turn, after it had already been durably paused
// once before -- is recoverable via Resume: reclaimed (the same generic
// ClaimRun lease-expiry path the legacy resume already used), the dangling
// turn reconciled, and repaused (since a promoted checkpoint exists from
// the earlier pause) rather than stranded behind ErrSessionBusy or having
// its checkpoint discarded. A further explicit ResumeRun then actually
// completes it, proving the repause is genuinely resumable.
func TestResumeReclaimsLeaseExpiredRunningRunWithPromotedCheckpoint(t *testing.T) {
	ctx := context.Background()
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("unused")}, nil
	}))
	defer cleanup()

	sessionID := session.ID("crash-session-2")
	newTestSession(t, ctx, orch, sessionID)
	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "crash-run-2", SessionID: sessionID, OwnerID: "owner-1", ClaimToken: "claim-1",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: newTestToolPlan(staticToolRegistry{}).Descriptor(),
	}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	firstExecution := orch.store.Execution(session.RunFence{RunID: admittedRun.ID, ClaimToken: admittedRun.ClaimToken})
	admittedTurn, err := firstExecution.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn:                 session.Turn{ID: "crash-turn-2-1", RunID: admittedRun.ID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted, UserMessageIDs: []session.MessageID{"crash-user-msg-2-1"}, CreatedAt: orch.now()},
		UserMessages:         []session.Message{{ID: "crash-user-msg-2-1", SessionID: sessionID, RunID: admittedRun.ID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		AssistantPlaceholder: session.Message{ID: "crash-assistant-2-1", SessionID: sessionID, RunID: admittedRun.ID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "crash-turn-2-1-started", SessionID: sessionID, RunID: admittedRun.ID, TurnID: "crash-turn-2-1", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
	})
	if err != nil {
		t.Fatalf("admit first turn: %v", err)
	}
	// A real, resumable between-turns checkpoint envelope: the same shape
	// stageLoopCheckpoint produces, with a fingerprint ResumeRun's
	// pre-claim check will actually accept -- not just any staged bytes.
	fingerprint := planFingerprint(newTestToolPlan(staticToolRegistry{}))
	payload, err := marshalEmptyLoopCheckpoint()
	if err != nil {
		t.Fatalf("marshalEmptyLoopCheckpoint: %v", err)
	}
	envelopeBytes, err := encodeCheckpointEnvelope(adkCheckpointEnvelope{
		EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, Fingerprint: fingerprint, TurnID: admittedTurn.Turn.ID, Payload: payload,
	})
	if err != nil {
		t.Fatalf("encodeCheckpointEnvelope: %v", err)
	}
	if _, err := firstExecution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: session.Checkpoint{
		RunID: admittedRun.ID, Revision: 1, Kind: session.CheckpointKindLoop, AgentFingerprint: fingerprint,
		EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, CheckpointID: string(admittedRun.ID), Bytes: envelopeBytes, CreatedAt: orch.now(),
	}}); err != nil {
		t.Fatalf("stage checkpoint: %v", err)
	}
	if _, err := firstExecution.PromotePause(ctx, session.PromotePauseRequest{
		Revision: 1, TurnID: admittedTurn.Turn.ID, Event: session.EventRecord{
			ID: "crash-run-2-paused-1", SessionID: sessionID, RunID: admittedRun.ID, Kind: session.RunPausedEventKind, CreatedAt: orch.now(),
		},
	}); err != nil {
		t.Fatalf("promote pause: %v", err)
	}

	// Simulate a second process resuming (ClaimRun), admitting a second
	// turn over a freshly enqueued item, and then crashing again -- never
	// staging a new checkpoint, never settling anything, and leaving the
	// lease to expire.
	claimed, err := orch.store.ClaimRun(ctx, session.RunClaim{RunID: admittedRun.ID, OwnerID: "owner-2", ClaimToken: "claim-2", LeaseDuration: time.Nanosecond})
	if err != nil {
		t.Fatalf("claim run: %v", err)
	}
	secondExecution := orch.store.Execution(session.RunFence{RunID: claimed.ID, ClaimToken: claimed.ClaimToken})
	item, err := orch.store.EnqueueInbox(ctx, crashInboxItem("crash-inbox-2", sessionID, "crash-key-2", "second message", orch.now()), session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("enqueue inbox: %v", err)
	}
	// Real UserParts, exactly as the real admitTurn would encode them (round-
	// four reconciliation item 1/CR-C1): the shipped round-three test built a
	// bare, content-free session.Message, which cannot see the duplicated-
	// history defect this test now guards against.
	userParts := crashUserParts(t, "crash-user-msg-2-2", "crash-user-msg-2-2", sessionID, claimed.ID, "second message", orch.now())
	if _, err := secondExecution.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn:         session.Turn{ID: "crash-turn-2-2", RunID: claimed.ID, SessionID: sessionID, Ordinal: 2, State: session.TurnAdmitted, UserMessageIDs: []session.MessageID{"crash-user-msg-2-2"}, CreatedAt: orch.now()},
		UserMessages: []session.Message{{ID: "crash-user-msg-2-2", SessionID: sessionID, RunID: claimed.ID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		UserParts:    userParts,
		AssistantPlaceholder: session.Message{
			ID: "crash-assistant-2-2", SessionID: sessionID, RunID: claimed.ID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now(),
		},
		Event:    session.EventRecord{ID: "crash-turn-2-2-started", SessionID: sessionID, RunID: claimed.ID, TurnID: "crash-turn-2-2", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
		InboxIDs: []session.InboxID{item.ID},
	}); err != nil {
		t.Fatalf("admit second turn: %v", err)
	}
	if _, err := secondExecution.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("start run: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	if got := countUserMessageText(t, ctx, orch.store, sessionID, "second message"); got != 1 {
		t.Fatalf("committed 'second message' user messages before reconciliation = %d, want 1", got)
	}

	handle, err := orch.Resume(ctx, admittedRun.ID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused {
		t.Fatalf("reconciled result = %+v, want paused (a promoted checkpoint exists to resume from)", result)
	}
	// Round-four reconciliation item 5 (CR-I4): a RunPaused result from
	// Resume() must come from a handle whose AwaitPause actually delivers
	// (Handle's doc comment: "AwaitPause ... is closed without a value
	// otherwise" -- implying it DOES deliver for a pause), never a nil
	// channel that blocks forever.
	pause, ok := <-handle.AwaitPause()
	if !ok || pause.RunID != admittedRun.ID || pause.StopCause == "" {
		t.Fatalf("AwaitPause after reconciled repause = %+v, ok=%v", pause, ok)
	}
	turn, err := orch.store.GetTurn(ctx, "crash-turn-2-2")
	if err != nil || turn.State != session.TurnInterrupted {
		t.Fatalf("dangling second turn after reconciliation = %#v, err=%v, want interrupted", turn, err)
	}
	// The item must NOT be requeued (see the no-checkpoint sibling test):
	// it settles InboxInterrupted, still linked to crash-turn-2-2, so the
	// SAME turn -- never a fresh one -- redrives it on the next resume.
	interruptedItems, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxInterrupted})
	if err != nil || len(interruptedItems) != 1 || interruptedItems[0].ID != item.ID || interruptedItems[0].TurnID != "crash-turn-2-2" {
		t.Fatalf("interrupted inbox = %#v, %v, want exactly the reconciled item, still linked to crash-turn-2-2", interruptedItems, err)
	}
	if requeued, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxQueued}); err != nil || len(requeued) != 0 {
		t.Fatalf("queued inbox after reconciliation = %#v, %v, want none (never requeued)", requeued, err)
	}
	finalRun, err := orch.store.GetRun(ctx, admittedRun.ID)
	if err != nil || finalRun.Status != session.RunPaused {
		t.Fatalf("final run = %+v, err=%v, want paused", finalRun, err)
	}

	// Prove the repause is genuinely resumable, and that it resumes the
	// SAME reconciled turn rather than admitting a fresh one: an explicit
	// ResumeRun redrives crash-turn-2-2 (interrupted -> running -> completed,
	// same TurnID, same committed user message) and completes normally.
	resumeHandle, err := orch.ResumeRun(ctx, admittedRun.ID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun after reconciliation error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
	completedTurn, err := orch.store.GetTurn(ctx, "crash-turn-2-2")
	if err != nil || completedTurn.State != session.TurnCompleted {
		t.Fatalf("crash-turn-2-2 after resume = %#v, err=%v, want completed (the SAME turn, redriven)", completedTurn, err)
	}
	completedItems, err := orch.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxCompleted})
	if err != nil || len(completedItems) != 1 || completedItems[0].ID != item.ID {
		t.Fatalf("completed inbox after resume = %#v, %v, want exactly the reconciled item, completed once", completedItems, err)
	}
	// The critical assertion (CR-C1): the user's text appears in committed
	// history EXACTLY ONCE, never duplicated by a second admission.
	if got := countUserMessageText(t, ctx, orch.store, sessionID, "second message"); got != 1 {
		t.Fatalf("committed 'second message' user messages after resume = %d, want exactly 1 (no duplicate)", got)
	}
}

// panicOnReconcileStore wraps a session.Store so ReconcileInterruptedTurn
// panics -- used by TestReconcileCrashedRunSurvivesPanic to prove round-four
// reconciliation item 3 (CR-I2): a panic anywhere inside reconcileCrashedRun
// (the store, extension dispatch, or terminalizeUnfinishedTools) must settle
// the run RunFailed, never take down the host process.
type panicOnReconcileStore struct {
	session.Store
}

func (s *panicOnReconcileStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &panicOnReconcileExecution{ExecutionStore: s.Store.Execution(fence)}
}

type panicOnReconcileExecution struct {
	session.ExecutionStore
}

func (e *panicOnReconcileExecution) ReconcileInterruptedTurn(context.Context, session.ReconcileInterruptedTurnRequest) (session.ReconcileInterruptedTurnResult, error) {
	panic("injected reconcile panic")
}

func TestReconcileCrashedRunSurvivesPanic(t *testing.T) {
	ctx := context.Background()
	sqliteStore, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	defer func() { _ = pool.Close() }()
	wrapped := &panicOnReconcileStore{Store: sqliteStore}
	orch, err := NewStreamingOrchestrator(
		WithStore(wrapped), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{agenticAssistantText("unused")}, nil
		})}),
		WithIDGenerator(&sequenceIDs{}), WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("sqlite-owner-panic"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}
	configureTestTools(orch, staticToolRegistry{tools: nil})

	sessionID := session.ID("panic-session-1")
	newTestSession(t, ctx, orch, sessionID)
	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "panic-run-1", SessionID: sessionID, OwnerID: "owner-1", ClaimToken: "claim-1",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: newTestToolPlan(staticToolRegistry{}).Descriptor(),
	}, time.Nanosecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := orch.store.Execution(session.RunFence{RunID: admittedRun.ID, ClaimToken: admittedRun.ClaimToken})
	if _, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "panic-turn-1", RunID: admittedRun.ID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"panic-user-msg-1"}, CreatedAt: orch.now(),
		},
		UserMessages: []session.Message{{ID: "panic-user-msg-1", SessionID: sessionID, RunID: admittedRun.ID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		Event:        session.EventRecord{ID: "panic-turn-1-started", SessionID: sessionID, RunID: admittedRun.ID, TurnID: "panic-turn-1", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("admit turn: %v", err)
	}
	if _, err := execution.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("start run: %v", err)
	}
	// The lease (1ns) is already expired; nothing was ever settled -- a
	// dangling turn a crashed process left behind.
	time.Sleep(2 * time.Millisecond)

	handle, err := orch.Resume(ctx, admittedRun.ID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result after injected reconciliation panic = %+v, want failed with an error (the process must survive)", result)
	}
}
