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

	orchB, err := NewStreamingOrchestrator(
		WithStore(storeB), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
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
