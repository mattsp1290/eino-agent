package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// This file proves the round-2 reconciliation fixes
// (reviews/w5-engine-2026-09-11-013808-482da09897eb/fix-pass/reconciliation.md,
// items 1-6) against the real SQLite store, using store decorators to widen
// or force the exact races the round-2 reviewers reproduced -- not the
// in-memory admissionStore fixture, whose own past divergence from the SQL
// stores is what let round-one Critical 1 ship green in the first place.

// settleRunGateStore delays the FIRST call to any fenced ExecutionStore's
// SettleRun this store hands out, until the test closes release. Used to
// force an Enqueue call to land in the exact window item 1 (IG-1) is about:
// after the loop has already been unregistered (so Enqueue never reaches a
// sealed loop's Push) but before the terminal settlement's own CAS commits.
type settleRunGateStore struct {
	session.Store
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *settleRunGateStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &settleRunGateExecution{ExecutionStore: s.Store.Execution(fence), gate: s}
}

type settleRunGateExecution struct {
	session.ExecutionStore
	gate *settleRunGateStore
}

func (e *settleRunGateExecution) SettleRun(ctx context.Context, request session.SettleRunRequest) (session.RunSettlementResult, error) {
	e.gate.once.Do(func() {
		close(e.gate.reached)
		<-e.gate.release
	})
	return e.ExecutionStore.SettleRun(ctx, request)
}

// TestEnqueueRacingIdleTerminalSettlementNeverStrandsItem proves
// reconciliation.md item 1 (IG-1) end to end: an Enqueue that lands after
// the loop has gone idle and cleanly finished, but before that run's
// terminal settlement actually commits, must never panic the caller and
// must never leave the run RunCompleted with the acknowledged item
// stranded `queued` forever. It also exercises item 2 (IG-2): since this is
// a clean, no-tool-call completion, upstream's own checkpoint machinery
// never stages a runner checkpoint (isIdle==true), so the queued-
// continuation pause this forces must stage its own Kind=loop checkpoint
// (promoteQueuedContinuation) rather than promote revision 0 and strand the
// run `running` with no driver.
func TestEnqueueRacingIdleTerminalSettlementNeverStrandsItem(t *testing.T) {
	ctx := context.Background()
	sqliteStore, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	defer func() { _ = pool.Close() }()
	gate := &settleRunGateStore{Store: sqliteStore, reached: make(chan struct{}), release: make(chan struct{})}
	orch, err := NewStreamingOrchestrator(
		WithStore(gate),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		})}),
		WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("sqlite-owner-race"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}

	handle, err := orch.Start(ctx, Request{SessionID: "race-terminal-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}

	var item session.InboxItem
	select {
	case <-gate.reached:
		item, err = orch.Enqueue(ctx, "race-terminal-session", EnqueueRequest{
			RunID: handle.RunID(), IdempotencyKey: "race-key", Message: TextUserMessage("racing message"),
		})
		if err != nil {
			close(gate.release)
			t.Fatalf("Enqueue error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SettleRun gate never reached")
	}
	close(gate.release)

	// The caller (this goroutine) must return normally -- no panic -- and
	// the result must never be RunFailed/RunInterrupted for this scenario.
	result := <-handle.Done()
	if result.Status != session.RunPaused && result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want RunPaused or RunCompleted", result)
	}
	run, err := orch.store.GetRun(ctx, result.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != session.RunPaused && run.Status != session.RunCompleted {
		t.Fatalf("durable run status = %q, want paused or completed -- never running", run.Status)
	}
	items, err := orch.store.ListInbox(ctx, "race-terminal-session", nil)
	if err != nil {
		t.Fatalf("ListInbox: %v", err)
	}
	for _, it := range items {
		if it.ID != item.ID {
			continue
		}
		if result.Status == session.RunCompleted && it.State == session.InboxQueued {
			t.Fatalf("run completed with the racing item stranded queued: %+v", it)
		}
		if result.Status == session.RunPaused && it.State != session.InboxQueued {
			t.Fatalf("run paused but the racing item is not queued: %+v", it)
		}
	}
	if result.Status != session.RunPaused {
		return
	}
	// item 2's "done when": ResumeRun of a queued continuation must work.
	resumeHandle, err := orch.ResumeRun(ctx, result.RunID, ResumeRequest{})
	if err != nil {
		t.Fatalf("ResumeRun of queued continuation error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
	finalItems, err := orch.store.ListInbox(ctx, "race-terminal-session", nil)
	if err != nil {
		t.Fatalf("ListInbox after resume: %v", err)
	}
	for _, it := range finalItems {
		if it.ID == item.ID && it.State != session.InboxCompleted {
			t.Fatalf("racing item state after resume = %q, want completed", it.State)
		}
	}
}

// checkpointGateStore delays the SECOND call to ReadPromotedCheckpoint
// (the first is ResumeRun's own pre-claim fingerprint check; the second is
// adkCheckpointStore.Get, read from inside ADK's tryLoadCheckpoint on the
// resumed TurnLoop's internal goroutine) until the test closes release --
// forcing ResumeRun's own drainQueuedInbox push to land in TurnLoop's
// buffer before tryLoadCheckpoint takes it, so the checkpoint's own
// UnhandledItems and the drained push are deterministically merged into one
// GenInput batch (reconciliation.md item 3 / PR-C1/IG-3/IG-6).
type checkpointGateStore struct {
	session.Store
	calls   int
	mu      sync.Mutex
	reached chan struct{}
	release chan struct{}
}

func (s *checkpointGateStore) ReadPromotedCheckpoint(ctx context.Context, runID session.RunID) (session.Checkpoint, bool, error) {
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

// TestResumeRunDedupesCheckpointRestoredAndDrainedItem proves
// reconciliation.md item 3 (PR-C1/IG-3/IG-6): a buffered-but-unconsumed
// inbox item that survives a pause (item 5/ED-3's fix keeps it durably
// `queued`) is restored by ADK's own checkpoint as UnhandledItems AND
// pushed again by ResumeRun's drainQueuedInbox. Deterministically forcing
// those two deliveries into the SAME GenInput batch (via checkpointGateStore
// above) must still admit the item exactly once -- not mint two durable
// user messages for it.
func TestResumeRunDedupesCheckpointRestoredAndDrainedItem(t *testing.T) {
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
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			close(toolCalled)
			<-release
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	})

	ctx := context.Background()
	sqliteStore, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("openTestSQLite: %v", err)
	}
	defer func() { _ = pool.Close() }()
	orch, err := NewStreamingOrchestrator(
		WithStore(sqliteStore), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }), WithOwnerID("sqlite-owner-dedup"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator: %v", err)
	}
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(ctx, Request{SessionID: "sqlite-dedup-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case <-toolCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("model never dispatched the first call")
	}
	item, err := orch.Enqueue(ctx, "sqlite-dedup-session", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "dedup-item-key", Message: TextUserMessage("second message"),
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

	// Now resume through the gated store: ReadPromotedCheckpoint call #2 is
	// adkCheckpointStore.Get inside tryLoadCheckpoint, blocked until this
	// goroutine has pushed the drained duplicate into the buffer.
	cgs := &checkpointGateStore{Store: sqliteStore, reached: make(chan struct{}), release: make(chan struct{})}
	gatedOrch, err := NewStreamingOrchestrator(
		WithStore(cgs), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 1, 0, time.UTC) }), WithOwnerID("sqlite-owner-dedup"), WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator (gated): %v", err)
	}
	configureTestTools(gatedOrch, staticToolRegistry{tools: []Tool{gate}})

	resumeDone := make(chan Handle, 1)
	resumeErr := make(chan error, 1)
	go func() {
		h, err := gatedOrch.ResumeRun(ctx, result.RunID, ResumeRequest{
			Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
		})
		if err != nil {
			resumeErr <- err
			return
		}
		resumeDone <- h
	}()
	select {
	case <-cgs.reached:
		// The push loop in runTurnLoop runs synchronously right after
		// loop.Run(ctx) returns, with no I/O in between; give it ample
		// scheduling headroom to have already landed in the buffer before
		// unblocking tryLoadCheckpoint's TakeAll.
		time.Sleep(50 * time.Millisecond)
		close(cgs.release)
	case err := <-resumeErr:
		t.Fatalf("ResumeRun error = %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint gate never reached")
	}

	var resumeHandle Handle
	select {
	case resumeHandle = <-resumeDone:
	case err := <-resumeErr:
		t.Fatalf("ResumeRun error = %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ResumeRun never returned a handle")
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}

	turns, err := gatedOrch.store.ListTurns(ctx, result.RunID)
	if err != nil {
		t.Fatalf("ListTurns: %v", err)
	}
	if len(turns) != 2 {
		t.Fatalf("turns = %d, want 2", len(turns))
	}
	var secondTurnUserMessages int
	for _, turn := range turns {
		if turn.Ordinal == 2 {
			secondTurnUserMessages = len(turn.UserMessageIDs)
		}
	}
	if secondTurnUserMessages != 1 {
		t.Fatalf("second turn UserMessageIDs = %d, want exactly 1 (deduped)", secondTurnUserMessages)
	}
	items, err := gatedOrch.store.ListInbox(ctx, "sqlite-dedup-session", nil)
	if err != nil {
		t.Fatalf("ListInbox: %v", err)
	}
	for _, it := range items {
		if it.ID == item.ID && it.State != session.InboxCompleted {
			t.Fatalf("enqueued item state = %q, want completed", it.State)
		}
	}
}

// TestEnqueueRetryAgainstLiveLoopIsNotDoubleDelivered restores the "live
// loop" half of I3's contract (nice-to-have #11 aside, this proves the fix
// directly): an idempotent Enqueue retry (same IdempotencyKey) against a
// run whose loop is still live must push the durable ID at most once --
// EnqueueInboxForRun's created flag, not the mere fact a live loop exists,
// gates the Push.
func TestEnqueueRetryAgainstLiveLoopIsNotDoubleDelivered(t *testing.T) {
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
			close(toolCalled)
			<-release
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-retry-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case <-toolCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("model never dispatched the first call")
	}
	first, err := orch.Enqueue(context.Background(), "sqlite-retry-session", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "retry-key", Message: TextUserMessage("retried message"),
	})
	if err != nil {
		t.Fatalf("first Enqueue error = %v", err)
	}
	// A genuine client retry against the SAME (still live) loop.
	second, err := orch.Enqueue(context.Background(), "sqlite-retry-session", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "retry-key", Message: TextUserMessage("retried message"),
	})
	if err != nil {
		t.Fatalf("second (retry) Enqueue error = %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("retry produced a distinct inbox item: %q != %q", first.ID, second.ID)
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
		t.Fatalf("ListTurns: %v", err)
	}
	var secondTurnUserMessages int
	for _, turn := range turns {
		if turn.Ordinal == 2 {
			secondTurnUserMessages = len(turn.UserMessageIDs)
		}
	}
	if secondTurnUserMessages != 1 {
		t.Fatalf("second turn UserMessageIDs = %d, want exactly 1 (retry not double-delivered)", secondTurnUserMessages)
	}
}

// rogueModel implements einomodel.AgenticModel directly (never routing
// through adkModel), simulating an AgentFactory that substitutes its own
// model instead of build.Model -- the case durableGuard.BeforeAgent cannot
// see, since it only inspects the agent's tool list, never its model.
type rogueModel struct {
	calls *int
}

func (m *rogueModel) Generate(context.Context, []*einoschema.AgenticMessage, ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	*m.calls++
	return agenticAssistantText("rogue response"), nil
}

func (m *rogueModel) Stream(context.Context, []*einoschema.AgenticMessage, ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	*m.calls++
	return einoschema.StreamReaderFromArray([]*einoschema.AgenticMessage{agenticAssistantText("rogue response")}), nil
}

// rogueModelWithGuardFactory installs build.Guard (as a compliant factory
// must) but substitutes rogueModel for build.Model -- proving
// reconciliation.md item 4 (IG-4/IG-5): the guard alone does not catch
// this, only the dispatch-count check added alongside it does.
type rogueModelWithGuardFactory struct {
	calls *int
}

func (f rogueModelWithGuardFactory) BuildAgent(ctx context.Context, build AgentBuildContext) (adk.TypedAgent[*einoschema.AgenticMessage], error) {
	cfg := &adk.TypedChatModelAgentConfig[*einoschema.AgenticMessage]{
		Name: "agent", Model: &rogueModel{calls: f.calls}, MaxIterations: build.MaxIterations,
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: build.Tools, ToolAliases: build.ToolAliases}},
	}
	if build.Guard != nil {
		cfg.Handlers = append(cfg.Handlers, build.Guard)
	}
	return adk.NewTypedChatModelAgent[*einoschema.AgenticMessage](ctx, cfg)
}

// TestRogueModelWithGuardInstalledFailsTurn proves reconciliation.md item 4
// (IG-4/IG-5): a factory that installs the mandatory durable guard but
// substitutes its own model must still fail the turn -- the guard passes
// (it only inspects tools), but zero durable model dispatches were
// recorded, so the engine fails at that point instead of settling
// RunCompleted with a content-free assistant placeholder.
func TestRogueModelWithGuardInstalledFailsTurn(t *testing.T) {
	store := newAdmissionStore()
	var rogueCalls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("the durable adkModel adapter must never be dispatched when the factory substitutes its own model")
		return nil, nil
	}))
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Agent: rogueModelWithGuardFactory{calls: &rogueCalls}})}

	handle, err := orch.Start(context.Background(), Request{SessionID: "rogue-model-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v, want failed (no durable dispatch)", result)
	}
	if !errors.Is(result.Error, ErrInvalidOrchestrator) {
		t.Fatalf("result.Error = %v, want ErrInvalidOrchestrator", result.Error)
	}
	if rogueCalls == 0 {
		t.Fatal("rogue model was never called -- test setup problem, not what this test proves")
	}
	msgs, err := store.ListMessages(context.Background(), "rogue-model-session", session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	for _, m := range msgs.Messages {
		if m.Role != session.RoleAssistant {
			continue
		}
		for _, p := range msgs.Parts {
			if p.MessageID == m.ID {
				t.Fatalf("assistant message %s unexpectedly has durable content from the rogue model: %+v", m.ID, p)
			}
		}
	}
}

// TestApprovalDecisionAcrossResumeAgainstSQLite proves reconciliation.md
// item 5 (IG-7): the approval decision's WithinTx-wrapped commit must not
// deadlock/hang against a real transactional store. Before the fix,
// commitResponse's first nextDurableMessageTime call on the resume path
// read through the non-transactional host store from inside the fenced
// write transaction; ResumeRun now seeds the durable message floor the same
// way Start does, so that read never happens. The in-memory admissionStore
// fixture's WithinTx clones the whole store and cannot reproduce this
// hazard at all -- this must run against SQLite.
func TestApprovalDecisionAcrossResumeAgainstSQLite(t *testing.T) {
	var calls int
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{{
				Role: einoschema.AgenticRoleTypeAssistant,
				ContentBlocks: []*einoschema.ContentBlock{
					{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "apr-1", Name: "remote_write", ServerLabel: "srv", Arguments: `{}`}},
				},
			}}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("continued after approve")}, nil
	}))
	defer cleanup()

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-approval-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
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

	resumeDone := make(chan Result, 1)
	go func() {
		resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
			Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
		})
		if err != nil {
			t.Errorf("ResumeRun error = %v", err)
			return
		}
		resumeDone <- <-resumeHandle.Done()
	}()
	select {
	case resumed := <-resumeDone:
		if resumed.Status != session.RunCompleted || resumed.Error != nil {
			t.Fatalf("resumed result = %+v", resumed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ResumeRun's approval decision never completed -- possible deadlock/hang against the transactional store")
	}
}

// TestResumeRunRestoresSystemPromptAndAgentOptions proves reconciliation
// item 6 (PR-I1, the unmet "done when" for TL-5): a resumed dispatch's
// model.Request.System equals the fresh-run system prompt, and
// run.Config's durable system_prompt/agent_options survive resume -- not
// just that the code happens to carry them (verified by the reviewer
// directly against SQLite), but that a regression is actually caught.
func TestResumeRunRestoresSystemPromptAndAgentOptions(t *testing.T) {
	var systems []string
	var optionsSeen []map[string]string
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	var calls int
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(_ context.Context, req model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		systems = append(systems, req.System)
		optionsSeen = append(optionsSeen, cloneStringMap(req.Options))
		if calls == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	defer cleanup()

	// Round-four reconciliation item 6 (MR-1): rather than asserting against
	// a TurnSnapshot the test builds itself (which only proves
	// boundedTurnMetadata/NewToolScopeContext project the fields they are
	// handed, never in doubt), record the REAL BoundedTurnMetadata and
	// ToolScopeContext ResumeRun's own rebuilt cfg produces for a prepared
	// turn: a TurnPreparePoint hook observes every prepared turn's metadata,
	// and the "gate" tool's own Resolve records the ToolScopeContext
	// sealedPlanTools.ResolveTools calls it with -- both fire once per
	// prepared turn (pre- and post-resume), through the exact same
	// staticRunPlanProvider-held *RunPlan Start and ResumeRun share.
	registry := newTestExtensionRegistry(nil)
	var metaMu sync.Mutex
	var metadataSeen []BoundedTurnMetadata
	observeComponent := extension.Component{InstanceID: "audited-turn-prepare", Artifact: extension.Artifact{Name: "audited-turn-prepare", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	if _, err := registry.Mount(context.Background(), observeComponent, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnHook(registrar, TurnPreparePoint, extension.Registration{ID: "observe", Scope: extension.GlobalScope()}, func(_ context.Context, metadata BoundedTurnMetadata) error {
			metaMu.Lock()
			metadataSeen = append(metadataSeen, cloneBoundedTurnMetadata(metadata))
			metaMu.Unlock()
			return nil
		})
	})); err != nil {
		t.Fatalf("mount turn-prepare observer: %v", err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatalf("dispatch snapshot: %v", err)
	}
	var scopeMu sync.Mutex
	var scopesSeen []ToolScopeContext
	gateCapability := testPlanTool("gate")
	gateCapability.Resolve = func(_ context.Context, scope ToolScopeContext) (Tool, error) {
		scopeMu.Lock()
		scopesSeen = append(scopesSeen, scope.Clone())
		scopeMu.Unlock()
		return cloneToolChecked(gate)
	}
	spec := testDispatchPlanSpec(dispatch)
	spec.Components = append(spec.Components, PlanComponent{Component: testPlanComponent("test-tools"), Tools: []PlanTool{gateCapability}})
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(spec)}

	cfg := orchestratorConfig()
	cfg.Agent.SystemPrompt = "AUDITED-SYSTEM-PROMPT"
	cfg.Agent.Options = map[string]string{"temperature": "0", "audited_option": "present"}
	cfg.Agent.Mode = "audited-mode"
	cfg.Tools.Enabled = []string{"gate", "audited-enabled-tool"}
	cfg.Tools.Disabled = []string{"audited-disabled-tool"}

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-system-prompt-session", Message: TextUserMessage("hello"), Config: cfg})
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
	run, err := orch.store.GetRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Config[systemPromptConfigKey] != cfg.Agent.SystemPrompt {
		t.Fatalf("run.Config[system_prompt] = %q, want %q", run.Config[systemPromptConfigKey], cfg.Agent.SystemPrompt)
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
	if len(systems) < 2 {
		t.Fatalf("dispatch count = %d, want at least 2 (pre- and post-resume)", len(systems))
	}
	if systems[len(systems)-1] != systems[0] || systems[len(systems)-1] != cfg.Agent.SystemPrompt {
		t.Fatalf("post-resume System = %q, pre-resume = %q, want both to equal %q", systems[len(systems)-1], systems[0], cfg.Agent.SystemPrompt)
	}
	if optionsSeen[len(optionsSeen)-1]["audited_option"] != "present" {
		t.Fatalf("post-resume agent options = %#v, want audited_option=present", optionsSeen[len(optionsSeen)-1])
	}

	// Round-three reconciliation item 6 (RD-2): Agent.Mode and
	// Tools.Enabled/Disabled must round-trip through the same durable
	// resumeRunConfig pair the system prompt/options above already do, and
	// must reach the same BoundedTurnMetadata/ToolScopeContext a fresh
	// run's turns see.
	finalRun, err := orch.store.GetRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetRun after resume: %v", err)
	}
	durable := decodeResumeRunConfig(finalRun.Config)
	if durable.AgentMode != cfg.Agent.Mode {
		t.Fatalf("decoded AgentMode = %q, want %q", durable.AgentMode, cfg.Agent.Mode)
	}
	if !reflect.DeepEqual(durable.ToolsEnabled, cfg.Tools.Enabled) || !reflect.DeepEqual(durable.ToolsDisabled, cfg.Tools.Disabled) {
		t.Fatalf("decoded tool scope = enabled=%#v disabled=%#v, want enabled=%#v disabled=%#v",
			durable.ToolsEnabled, durable.ToolsDisabled, cfg.Tools.Enabled, cfg.Tools.Disabled)
	}
	// The assertions above prove admissionConfig -> decodeResumeRunConfig
	// round-trips, which was never in doubt; they cannot observe ResumeRun's
	// OWN rebuilt cfg. Assert against what a prepared turn REALLY got
	// instead (round-four reconciliation item 6/MR-1): ResolveTools runs
	// once per prepared turn off the snapshot ResumeRun itself rebuilt, and
	// TurnPreparePoint fires with that same snapshot's BoundedTurnMetadata,
	// so the LAST recorded value of each is the post-resume turn's real one
	// -- compared whole-struct against the pre-resume (first) value and
	// against cfg. Reverting Mode/Tools restoration in ResumeRun's rebuilt
	// cfg (runtime/turn_loop.go) makes this fail.
	metaMu.Lock()
	if len(metadataSeen) < 2 {
		metaMu.Unlock()
		t.Fatalf("TurnPreparePoint firings = %d, want at least 2 (pre- and post-resume)", len(metadataSeen))
	}
	firstMeta, lastMeta := metadataSeen[0], metadataSeen[len(metadataSeen)-1]
	metaMu.Unlock()
	if lastMeta.AgentMode != firstMeta.AgentMode || lastMeta.AgentMode != cfg.Agent.Mode {
		t.Fatalf("post-resume prepared AgentMode = %q, pre-resume = %q, want both to equal %q", lastMeta.AgentMode, firstMeta.AgentMode, cfg.Agent.Mode)
	}

	scopeMu.Lock()
	if len(scopesSeen) < 2 {
		scopeMu.Unlock()
		t.Fatalf("gate tool ResolveTools firings = %d, want at least 2 (pre- and post-resume)", len(scopesSeen))
	}
	firstScope, lastScope := scopesSeen[0], scopesSeen[len(scopesSeen)-1]
	scopeMu.Unlock()
	if !reflect.DeepEqual(lastScope, firstScope) {
		t.Fatalf("post-resume ToolScopeContext = %#v, pre-resume = %#v; must be identical", lastScope, firstScope)
	}
	if !reflect.DeepEqual(lastScope.EnabledTools, cfg.Tools.Enabled) || !reflect.DeepEqual(lastScope.DisabledTools, cfg.Tools.Disabled) {
		t.Fatalf("post-resume ToolScopeContext = enabled=%#v disabled=%#v, want enabled=%#v disabled=%#v",
			lastScope.EnabledTools, lastScope.DisabledTools, cfg.Tools.Enabled, cfg.Tools.Disabled)
	}
}
