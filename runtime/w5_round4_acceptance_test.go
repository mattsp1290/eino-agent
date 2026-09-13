package runtime

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// namespacedSequenceIDs mints IDs like sequenceIDs but under a distinct
// namespace prefix. A "process restart" test drives TWO independent
// orchestrator instances against the SAME durable store, each with its own
// fresh IDGenerator starting its counter at 1 -- two plain sequenceIDs
// instances would both mint "message-1", "message-2", ... and the second
// orchestrator's IDs would collide with rows the first already durably
// wrote (observed directly: AdmitTurn failing ErrConflict on an id
// collision with an already-committed row).
type namespacedSequenceIDs struct {
	mu        sync.Mutex
	namespace string
	n         int
}

func (s *namespacedSequenceIDs) next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return s.namespace + "-" + prefix + "-" + strconv.Itoa(s.n)
}

func (s *namespacedSequenceIDs) NewRunID() session.RunID { return session.RunID(s.next("run")) }
func (s *namespacedSequenceIDs) NewMessageID() session.MessageID {
	return session.MessageID(s.next("message"))
}
func (s *namespacedSequenceIDs) NewPartID() session.PartID { return session.PartID(s.next("part")) }
func (s *namespacedSequenceIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next("tool-call"))
}
func (s *namespacedSequenceIDs) NewEventID() session.EventID { return session.EventID(s.next("event")) }
func (s *namespacedSequenceIDs) NewEpochID() session.EpochID { return session.EpochID(s.next("epoch")) }
func (s *namespacedSequenceIDs) NewTurnID() session.TurnID   { return session.TurnID(s.next("turn")) }
func (s *namespacedSequenceIDs) NewInboxID() session.InboxID { return session.InboxID(s.next("inbox")) }
func (s *namespacedSequenceIDs) NewInvocationID() string     { return s.next("invocation") }

// listEvents pages through sessionID's full durable event log for run.
func listEvents(t *testing.T, ctx context.Context, orch *StreamingOrchestrator, sessionID session.ID) []session.EventRecord {
	t.Helper()
	var out []session.EventRecord
	cursor := session.EventCursor{Limit: 1000}
	for {
		batch, err := orch.store.ListEvents(ctx, sessionID, cursor)
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		out = append(out, batch.Events...)
		if batch.Next == (session.EventCursor{}) {
			return out
		}
		cursor = batch.Next
	}
}

func countKind(events []session.EventRecord, kind string) int {
	n := 0
	for _, e := range events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// reconstructedUserAndAssistantText returns, in durable order, the text of
// every user_input_text and assistant_gen_text block across sessionID's
// full committed history -- used to assert the plan's "correct reconstructed
// history" acceptance bullet without depending on message/turn boundaries.
func reconstructedUserAndAssistantText(t *testing.T, ctx context.Context, store session.Store, sessionID session.ID) []string {
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
	var out []string
	for _, msg := range batch.Messages {
		if msg.Role != session.RoleUser && msg.Role != session.RoleAssistant {
			continue
		}
		content, err := session.DecodeContentParts(msg.Role, partsByMessage[msg.ID], session.DefaultContentLimits())
		if err != nil {
			continue
		}
		for _, block := range content.Blocks {
			switch {
			case block.Kind == session.BlockKindUserInputText && block.Text != nil:
				out = append(out, block.Text.Text)
			case block.Kind == session.BlockKindAssistantGenText && block.Text != nil:
				out = append(out, block.Text.Text)
			}
		}
	}
	return out
}

// agenticMessagesText extracts the user_input_text/assistant_gen_text
// content of msgs, in order -- the same shape reconstructedUserAndAssistantText
// extracts from durable rows, but read directly off the provider-visible
// []*einoschema.AgenticMessage a model.Request actually carries (round-five
// reconciliation item 5/FR-S2).
func agenticMessagesText(msgs []*einoschema.AgenticMessage) []string {
	var out []string
	for _, msg := range msgs {
		for _, block := range msg.ContentBlocks {
			switch {
			case block.Type == einoschema.ContentBlockTypeUserInputText && block.UserInputText != nil:
				out = append(out, block.UserInputText.Text)
			case block.Type == einoschema.ContentBlockTypeAssistantGenText && block.AssistantGenText != nil:
				out = append(out, block.AssistantGenText.Text)
			}
		}
	}
	return out
}

// TestProcessRestartRecoversMultipleQueuedInputsAfterFirstCommittedTurn
// proves the plan's acceptance bullet exactly (round-four reconciliation
// item 7/MR-2/MR-3, plan 05-adk-runtime-and-recovery.md "Acceptance"):
// "Repeat with multiple queued inputs and process restart after the first
// committed turn; assert correct reconstructed history, exactly-once inbox
// completion, no duplicate run start, and exactly one final enclosing
// terminal event."
//
// Unlike the round-three/round-four crash-reconciliation tests (which
// hand-build a dangling turn directly at the store layer), this test drives
// turn 1 to a REAL completion through a real Start call, enqueues TWO more
// items while the run is between turns, and lets it durably pause with both
// still queued. It then simulates a process restart exactly: the first
// orchestrator's SQLite pool is closed, a second pool is opened over the
// SAME file (reopenTestSQLite, no re-migration), and a brand-new
// orchestrator instance resumes the run through it.
func TestProcessRestartRecoversMultipleQueuedInputsAfterFirstCommittedTurn(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "restart.db")
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

	gate := &deleteGateStore{Store: storeA, reached: make(chan struct{}), release: make(chan struct{})}
	orchA, err := NewStreamingOrchestrator(
		WithStore(gate), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{agenticAssistantText("reply-1")}, nil
		})}),
		WithIDGenerator(&namespacedSequenceIDs{namespace: "a"}), WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("restart-owner-a"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator (A): %v", err)
	}
	configureTestTools(orchA, staticToolRegistry{tools: nil})

	sessionID := session.ID("restart-session")
	handle, err := orchA.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("first message"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}

	var secondItem, thirdItem session.InboxItem
	select {
	case <-gate.reached:
		secondItem, err = orchA.Enqueue(ctx, sessionID, EnqueueRequest{RunID: handle.RunID(), IdempotencyKey: "restart-key-2", Message: TextUserMessage("second message")})
		if err != nil {
			t.Fatalf("enqueue second message: %v", err)
		}
		thirdItem, err = orchA.Enqueue(ctx, sessionID, EnqueueRequest{RunID: handle.RunID(), IdempotencyKey: "restart-key-3", Message: TextUserMessage("third message")})
		if err != nil {
			t.Fatalf("enqueue third message: %v", err)
		}
		close(gate.release)
	case <-time.After(5 * time.Second):
		t.Fatal("delete gate never reached")
	}

	result := <-handle.Done()
	if result.Status != session.RunPaused {
		t.Fatalf("result = %+v, want paused (queued continuation with two still-queued items)", result)
	}
	runID := result.RunID

	// Before the restart: turn 1 is genuinely committed, and both new items
	// are still durably queued (never consumed).
	preTurns, err := orchA.store.ListTurns(ctx, runID)
	if err != nil {
		t.Fatalf("ListTurns before restart: %v", err)
	}
	var firstTurnCompleted bool
	for _, turn := range preTurns {
		if turn.Ordinal == 1 && turn.State == session.TurnCompleted {
			firstTurnCompleted = true
		}
	}
	if !firstTurnCompleted {
		t.Fatalf("turns before restart = %#v, want turn 1 completed", preTurns)
	}
	queuedBefore, err := orchA.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxQueued})
	if err != nil || len(queuedBefore) != 2 {
		t.Fatalf("queued inbox before restart = %#v, %v, want exactly the two enqueued items", queuedBefore, err)
	}

	// Simulate a process restart: close the first pool, reopen a second one
	// over the SAME file (no re-migration), and drive a brand-new
	// orchestrator instance.
	if err := poolA.Close(); err != nil {
		t.Fatalf("close pool A: %v", err)
	}
	poolAClosed = true
	storeB, poolB, err := reopenTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopenTestSQLite: %v", err)
	}
	defer func() { _ = poolB.Close() }()

	var resumedRequests []model.Request
	var reqMu sync.Mutex
	orchB, err := NewStreamingOrchestrator(
		WithStore(storeB), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(_ context.Context, req model.Request) ([]*einoschema.AgenticMessage, error) {
			reqMu.Lock()
			resumedRequests = append(resumedRequests, req)
			reqMu.Unlock()
			return []*einoschema.AgenticMessage{agenticAssistantText("reply-2")}, nil
		})}),
		WithIDGenerator(&namespacedSequenceIDs{namespace: "b"}), WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 1, 0, 0, time.UTC) }),
		WithOwnerID("restart-owner-b"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator (B): %v", err)
	}
	configureTestTools(orchB, staticToolRegistry{tools: nil})

	resumeHandle, err := orchB.ResumeRun(ctx, runID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun (B) error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v, want completed with no error", resumed)
	}

	// Correct reconstructed history: both queued messages are admitted
	// (batched into one turn, preserving submission order) and answered,
	// with no duplication of turn 1's content.
	wantText := []string{"first message", "reply-1", "second message", "third message", "reply-2"}
	if got := reconstructedUserAndAssistantText(t, ctx, orchB.store, sessionID); !stringSlicesEqual(got, wantText) {
		t.Fatalf("reconstructed history = %#v, want %#v", got, wantText)
	}
	// The durable rows above prove what got COMMITTED, not what the
	// post-restart turn's provider INPUT actually carried (round-five
	// reconciliation item 5/FR-S2): admitTurn's model input must be
	// reconstructed from committed prior-turn history plus the newly
	// admitted turn's own messages, never just the newly admitted ones.
	// Exactly one dispatch (the second turn's), carrying at least turn 1's
	// user message, its assistant reply, and both new submissions.
	reqMu.Lock()
	gotRequests := append([]model.Request(nil), resumedRequests...)
	reqMu.Unlock()
	if len(gotRequests) != 1 {
		t.Fatalf("post-restart model dispatches = %d, want exactly 1", len(gotRequests))
	}
	if got := len(gotRequests[0].Messages); got < 4 {
		t.Fatalf("post-restart model input = %d messages, want turn 1's committed history (user + assistant) plus both new submissions (>= 4)", got)
	}
	gotInputText := agenticMessagesText(gotRequests[0].Messages)
	if !stringSlicesEqual(gotInputText, wantText[:len(wantText)-1]) {
		t.Fatalf("post-restart model input text = %#v, want %#v (turn 1's committed history reconstructed, plus both new submissions -- reply-2 is this dispatch's own output, not its input)", gotInputText, wantText[:len(wantText)-1])
	}

	// Exactly-once inbox completion.
	completed, err := orchB.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxCompleted})
	if err != nil || len(completed) != 2 {
		t.Fatalf("completed inbox = %#v, %v, want exactly the two items, completed once each", completed, err)
	}
	completedIDs := map[session.InboxID]bool{}
	for _, item := range completed {
		completedIDs[item.ID] = true
	}
	if !completedIDs[secondItem.ID] || !completedIDs[thirdItem.ID] {
		t.Fatalf("completed inbox ids = %#v, want %s and %s", completed, secondItem.ID, thirdItem.ID)
	}

	// No duplicate run start, and exactly one final enclosing terminal
	// event, across the whole lifecycle (admission on orchA, restart and
	// completion on orchB).
	events := listEvents(t, ctx, orchB, sessionID)
	if got := countKind(events, EventRunStarted); got != 1 {
		t.Fatalf("run_started events = %d, want exactly 1 across the restart", got)
	}
	if got := countKind(events, string(EventRunFinished)); got != 1 {
		t.Fatalf("terminal (run_finished) events = %d, want exactly 1", got)
	}

	finalRun, err := orchB.store.GetRun(ctx, runID)
	if err != nil || finalRun.Status != session.RunCompleted {
		t.Fatalf("final run = %+v, err=%v, want completed", finalRun, err)
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTargetedMultiLeafResumeLeavesUntargetedLeafPaused proves the plan's
// "targeted multi-leaf resume" acceptance item (round-four reconciliation
// item 7/MR-2/MR-3) against the real SQLite store (round-five reconciliation
// item 5/FR-S4: the plan's matrix bullet reads "Real ADK runner/TurnLoop
// with SQLite and PostgreSQL"; the pause/promote/resume checkpoint
// machinery is already covered by the single-leaf SQLite tests, but the
// multi-leaf TARGETING logic itself was previously proven only against the
// in-memory fixture): with two simultaneously-pending tool interrupts,
// targeting only one by its InterruptCtx address resumes that leaf and
// completes its tool call while the OTHER leaf stays paused untouched; a
// second, separately-targeted ResumeRun then resumes it too.
func TestTargetedMultiLeafResumeLeavesUntargetedLeafPaused(t *testing.T) {
	executed := map[string]int{}
	var mu sync.Mutex
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		// executed is keyed by the tool call's ProviderCallID (the scripted
		// model's own "call-leaf-N" literal), not call.ID: prepareToolCalls
		// always mints a fresh, store-unique ID now, so the durable ID no
		// longer equals the scripted literal this test wants to key on.
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			mu.Lock()
			executed[call.ProviderCallID]++
			mu.Unlock()
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	var calls int
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			// Two simultaneously-pending tool calls, both requiring a host
			// decision -- the multi-leaf shape.
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(
				agenticToolCall("call-leaf-1", "gate", `{}`),
				agenticToolCall("call-leaf-2", "gate", `{}`),
			)}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{
		SessionID: "multi-leaf-session", Message: TextUserMessage("hello"), Config: orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 2 {
		t.Fatalf("pause = %+v, ok=%v, want exactly 2 simultaneously-pending leaves", pause, ok)
	}

	// Target only the FIRST leaf.
	firstTarget := pause.InterruptContexts[0].ID
	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{firstTarget: "approve"},
	})
	if err != nil {
		t.Fatalf("first targeted ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	// The untargeted leaf keeps the run paused; the model must not be
	// dispatched again for it.
	if resumed.Status != session.RunPaused || !resumed.Interrupted || calls != 1 {
		t.Fatalf("resumed result = %+v calls=%d, want still paused, no new dispatch", resumed, calls)
	}
	mu.Lock()
	firstExecutions, secondExecutions := executed["call-leaf-1"], executed["call-leaf-2"]
	mu.Unlock()
	if firstExecutions != 1 || secondExecutions != 0 {
		t.Fatalf("executions after first targeted resume: leaf-1=%d leaf-2=%d, want leaf-1=1 leaf-2=0", firstExecutions, secondExecutions)
	}
	repause, ok := <-resumeHandle.AwaitPause()
	if !ok || len(repause.InterruptContexts) != 1 {
		t.Fatalf("repause = %+v, ok=%v, want exactly the one still-untargeted leaf", repause, ok)
	}

	// Now target the SECOND (originally untargeted) leaf, by its
	// current-generation address from the repause above -- an id from the
	// first pause generation is stale by then (see PauseInfo's doc
	// comment); use the fresh one.
	secondResumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{repause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("second targeted ResumeRun error = %v", err)
	}
	final := <-secondResumeHandle.Done()
	if final.Status != session.RunCompleted || final.Error != nil {
		t.Fatalf("final result = %+v, want completed", final)
	}
	mu.Lock()
	firstExecutions, secondExecutions = executed["call-leaf-1"], executed["call-leaf-2"]
	mu.Unlock()
	if firstExecutions != 1 || secondExecutions != 1 {
		t.Fatalf("executions after second targeted resume: leaf-1=%d leaf-2=%d, want exactly 1 each (never re-executed)", firstExecutions, secondExecutions)
	}
}

// TestReconcileCrashedRunTerminalizesUnfinishedToolCall proves crash
// injection at a tool-settlement boundary (round-four reconciliation item
// 7/MR-2/MR-3, plan acceptance: "Inject crashes after each tool settlement
// and checkpoint boundary"): a process that claimed a tool call (durably
// ToolCallRunning) but crashed before it ever settled is recovered through
// reconcileCrashedRun's terminalizeUnfinishedTools call, exactly like the
// legacy resume path does -- the call is terminalized interrupted, never
// re-executed, in the SAME reconciliation that also settles the dangling
// turn that requested it.
func TestReconcileCrashedRunTerminalizesUnfinishedToolCall(t *testing.T) {
	ctx := context.Background()
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("unused")}, nil
	}))
	defer cleanup()

	sessionID := session.ID("tool-crash-session")
	newTestSession(t, ctx, orch, sessionID)
	admittedRun, err := orch.store.AdmitRun(ctx, session.Run{
		ID: "tool-crash-run", SessionID: sessionID, OwnerID: "owner-1", ClaimToken: "claim-1",
		Agent: "default", ProviderID: "test", ModelID: "test", Status: session.RunPending, CreatedAt: orch.now(),
		ExtensionPlan: newTestToolPlan(staticToolRegistry{}).Descriptor(),
	}, time.Nanosecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := orch.store.Execution(session.RunFence{RunID: admittedRun.ID, ClaimToken: admittedRun.ClaimToken})
	if _, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: session.Turn{
			ID: "tool-crash-turn", RunID: admittedRun.ID, SessionID: sessionID, Ordinal: 1, State: session.TurnAdmitted,
			UserMessageIDs: []session.MessageID{"tool-crash-user-msg"}, AssistantMessageID: "tool-crash-assistant", CreatedAt: orch.now(),
		},
		UserMessages:         []session.Message{{ID: "tool-crash-user-msg", SessionID: sessionID, RunID: admittedRun.ID, Role: session.RoleUser, CreatedAt: orch.now(), UpdatedAt: orch.now()}},
		AssistantPlaceholder: session.Message{ID: "tool-crash-assistant", SessionID: sessionID, RunID: admittedRun.ID, Role: session.RoleAssistant, CreatedAt: orch.now(), UpdatedAt: orch.now()},
		Event:                session.EventRecord{ID: "tool-crash-turn-started", SessionID: sessionID, RunID: admittedRun.ID, TurnID: "tool-crash-turn", Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: orch.now()},
	}); err != nil {
		t.Fatalf("admit turn: %v", err)
	}

	call := session.ToolCall{
		ID: "tool-crash-call", SessionID: sessionID, RunID: admittedRun.ID, MessageID: "tool-crash-assistant",
		ResultMessageID: "tool-crash-result-msg", ResultPartID: "tool-crash-result-part",
		Name: "echo", Pattern: "echo", Input: []byte(`{"text":"hi"}`), Status: session.ToolCallPending,
	}
	created, err := execution.CreateToolCall(ctx, testCreateToolRequest(call, "tool-crash-create-event", orch.now()))
	if err != nil {
		t.Fatalf("create tool call: %v", err)
	}
	claimedCall := created.Call
	claimedCall.ClaimedBy = "owner-1"
	claimedCall.ClaimToken = "tool-crash-claim"
	claimedCall.StartedAt = orch.now()
	if _, err := execution.ClaimToolCall(ctx, testClaimToolRequest(claimedCall, "tool-crash-claim-event", time.Millisecond, orch.now())); err != nil {
		t.Fatalf("claim tool call: %v", err)
	}
	if _, err := execution.StartRun(ctx, orch.now()); err != nil {
		t.Fatalf("start run: %v", err)
	}
	// The lease (1ns) and the tool call's own claim lease (1ms) are both
	// already expired by real wall-clock time; nothing was ever settled --
	// exactly what a process that crashed mid tool-call leaves behind.
	time.Sleep(5 * time.Millisecond)

	handle, err := orch.Resume(ctx, admittedRun.ID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted || !result.Interrupted {
		t.Fatalf("reconciled result = %+v, want interrupted (no checkpoint to resume from)", result)
	}
	finalCall, err := orch.store.GetToolCall(ctx, created.Call.ID)
	if err != nil || finalCall.Status != session.ToolCallInterrupted {
		t.Fatalf("tool call after reconciliation = %#v, err=%v, want interrupted (terminalized, never re-executed)", finalCall, err)
	}
	// The run above is already terminal (RunInterrupted): round-six
	// reconciliation item 2 forces any turn ReconcileInterruptedTurn left
	// TurnInterrupted to TurnFailed in that same terminal SettleRun (see
	// session.ApplyFailTurn).
	turn, err := orch.store.GetTurn(ctx, "tool-crash-turn")
	if err != nil || turn.State != session.TurnFailed {
		t.Fatalf("dangling turn after reconciliation = %#v, err=%v, want failed (its run is terminal)", turn, err)
	}
}
