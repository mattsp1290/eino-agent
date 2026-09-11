package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// noGuardAgentFactory builds a plain adk.TypedChatModelAgent from
// build.Model/build.Tools -- so it dispatches through this engine's real
// adapters, unlike a fully noncompliant factory that substitutes its own
// model -- but deliberately never installs build.Guard as a handler, the
// way a buggy or malicious AgentFactory implementation might. It proves
// reconciliation.md item 10 (ED-5): the engine must detect and fail a
// factory that ignores the mandatory durable guard, not merely document
// that the factory "should" install it.
type noGuardAgentFactory struct{}

func (noGuardAgentFactory) BuildAgent(ctx context.Context, build AgentBuildContext) (adk.TypedAgent[*einoschema.AgenticMessage], error) {
	cfg := &adk.TypedChatModelAgentConfig[*einoschema.AgenticMessage]{
		Name: "agent", Model: build.Model, MaxIterations: build.MaxIterations,
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: build.Tools, ToolAliases: build.ToolAliases}},
	}
	// Deliberately never appends build.Guard to cfg.Handlers.
	return adk.NewTypedChatModelAgent[*einoschema.AgenticMessage](ctx, cfg)
}

// TestNonCompliantFactoryWithoutGuardFailsAsConstructionError proves the
// engine-side enforcement added for ED-5: a factory that never installs the
// mandatory durable guard fails the turn as a construction error instead of
// silently letting an agent execute unverified.
func TestNonCompliantFactoryWithoutGuardFailsAsConstructionError(t *testing.T) {
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("should never settle durably")}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Agent: noGuardAgentFactory{}})}

	handle, err := orch.Start(context.Background(), Request{SessionID: "no-guard-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v, want failed (construction error)", result)
	}
	if !errors.Is(result.Error, ErrInvalidOrchestrator) {
		t.Fatalf("result.Error = %v, want ErrInvalidOrchestrator", result.Error)
	}
}

// TestApprovalDecisionSurvivesTransientDispatchFailure proves
// reconciliation.md item 11 (ED-6): a transient dispatch failure
// immediately after a successful approval decision must not wedge the run.
// ADK's own retry wrapper (WithAttempts(2)) re-invokes adkModel.Generate/
// Stream, which re-enters adkApprovalBinding.prepare with the SAME already-
// committed decision -- prepare's idempotent branch must let that retry
// continue rather than hard-failing "already decided" a second time.
func TestApprovalDecisionSurvivesTransientDispatchFailure(t *testing.T) {
	store := newAdmissionStore()
	dispatchErr := errors.New("transient provider error")
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			return []*einoschema.AgenticMessage{{
				Role:          einoschema.AgenticRoleTypeAssistant,
				ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "apr-retry", Name: "remote_write", ServerLabel: "srv"}}},
			}}, nil
		case 2:
			// The decision CAS + response commit (prepare) has already run
			// and succeeded by the time this dispatch is attempted; this
			// error simulates a transient failure of the physical call
			// immediately afterward.
			return nil, dispatchErr
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("finished after retry")}, nil
		}
	}), WithAttempts(2))

	handle, err := orch.Start(context.Background(), Request{SessionID: "approval-retry-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
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
	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v, want completed with no error despite the transient dispatch failure", resumed)
	}
	if calls != 3 {
		t.Fatalf("dispatch attempts = %d, want 3 (approval request, failed attempt, successful retry)", calls)
	}
	batch, err := store.ListMessages(context.Background(), "approval-retry-session", session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatalf("ListMessages error = %v", err)
	}
	roleByMessage := make(map[session.MessageID]session.Role, len(batch.Messages))
	for _, message := range batch.Messages {
		roleByMessage[message.ID] = message.Role
	}
	var responseCount int
	for _, part := range batch.Parts {
		content, err := session.DecodeContentParts(roleByMessage[part.MessageID], []session.Part{part}, session.DefaultContentLimits())
		if err != nil {
			continue
		}
		for _, block := range content.Blocks {
			if block.Kind == session.BlockKindMCPToolApprovalResponse && block.MCPApprovalResponse != nil && block.MCPApprovalResponse.ApprovalRequestID == "apr-retry" {
				responseCount++
			}
		}
	}
	if responseCount != 1 {
		t.Fatalf("committed MCPToolApprovalResponse blocks for apr-retry = %d, want exactly 1", responseCount)
	}
}

func usageMessage(text string, prompt, completion int) *einoschema.AgenticMessage {
	msg := agenticAssistantText(text)
	msg.ResponseMeta = &einoschema.AgenticResponseMeta{TokenUsage: &einoschema.TokenUsage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion}}
	return msg
}

// TestRunFinishedUsageSumsBothTurns proves reconciliation.md item 12 (ED-7):
// a two-turn run's run_finished usage is the sum of both turns' usage, and
// each turn durably records its own usage on its own Turn row -- not just
// the last turn's, and not double-counted.
func TestRunFinishedUsageSumsBothTurns(t *testing.T) {
	store := newAdmissionStore()
	firstDispatched := make(chan struct{})
	release := make(chan struct{})
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			close(firstDispatched)
			<-release
			return []*einoschema.AgenticMessage{usageMessage("first", 10, 5)}, nil
		default:
			return []*einoschema.AgenticMessage{usageMessage("second", 20, 7)}, nil
		}
	}))

	handle, err := orch.Start(context.Background(), Request{SessionID: "usage-two-turn-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case <-firstDispatched:
	case <-time.After(3 * time.Second):
		t.Fatal("first turn never dispatched")
	}
	// Enqueue while the loop is still live (mid-dispatch) so the second
	// item is processed as this run's second turn before the loop's own
	// idle-stop timer fires -- no pause/resume involved.
	if _, err := orch.Enqueue(context.Background(), "usage-two-turn-session", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "usage-second-key", Message: TextUserMessage("second message"),
	}); err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}
	close(release)

	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	if result.Usage.InputTokens != 30 || result.Usage.OutputTokens != 12 {
		t.Fatalf("run_finished usage = %+v, want InputTokens=30 OutputTokens=12 (10+20, 5+7)", result.Usage)
	}
	turns, err := store.ListTurns(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("ListTurns error = %v", err)
	}
	if len(turns) != 2 {
		t.Fatalf("turns = %d, want 2", len(turns))
	}
	for _, turn := range turns {
		switch turn.Ordinal {
		case 1:
			if turn.Usage.InputTokens != 10 || turn.Usage.OutputTokens != 5 {
				t.Fatalf("turn 1 usage = %+v, want InputTokens=10 OutputTokens=5", turn.Usage)
			}
		case 2:
			if turn.Usage.InputTokens != 20 || turn.Usage.OutputTokens != 7 {
				t.Fatalf("turn 2 usage = %+v, want InputTokens=20 OutputTokens=7", turn.Usage)
			}
		default:
			t.Fatalf("unexpected turn ordinal %d", turn.Ordinal)
		}
	}
}

// TestResumeRunEmitsRunResumedEvent proves reconciliation.md item 13
// (ED-11): ResumeRun durably emits a run_resumed event.
func TestResumeRunEmitsRunResumedEvent(t *testing.T) {
	store := newAdmissionStore()
	orch, runID, pause := startPausedRun(t, store, "run-resumed-event-session", new(int))

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
	batch, err := store.ListEvents(context.Background(), "run-resumed-event-session", session.EventCursor{Limit: 1000})
	if err != nil {
		t.Fatalf("ListEvents error = %v", err)
	}
	var found bool
	for _, event := range batch.Events {
		if event.RunID == runID && event.Kind == session.RunResumedEventKind {
			found = true
		}
	}
	if !found {
		t.Fatalf("no run_resumed event found for run %s", runID)
	}
}

// TestResumedHandleInterruptCancelsRun proves reconciliation.md item 9
// (ED-10/TL-8): a resumed run's Handle.Interrupt can actually cancel it --
// before the fix, ResumeRun's turnLoopHandle had a nil cancel func, so
// Interrupt degraded to a best-effort loop.Stop that could not win the
// startup race, and the resumed run context was derived from the caller's
// own ctx rather than being independently cancellable.
func TestResumedHandleInterruptCancelsRun(t *testing.T) {
	store := newAdmissionStore()
	toolStarted := make(chan struct{})
	var toolCanceled bool
	blocker := Tool{
		Name: "blocker", Info: &einoschema.ToolInfo{Name: "blocker"},
		Executor: orchestratorToolExecutorFunc(func(ctx context.Context, _ ToolCall) (ToolResult, error) {
			close(toolStarted)
			<-ctx.Done()
			toolCanceled = true
			return ToolResult{}, ctx.Err()
		}),
	}
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "ok"}, nil
		}),
	}
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-2", "blocker", `{}`))}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate, blocker}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "resumed-interrupt-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
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
	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	select {
	case <-toolStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("blocker tool never started after resume")
	}
	if err := resumeHandle.Interrupt(context.Background(), "test interrupt"); err != nil {
		t.Fatalf("resumed Handle.Interrupt error = %v", err)
	}
	select {
	case resumed := <-resumeHandle.Done():
		if !resumed.Interrupted {
			t.Fatalf("resumed result = %+v, want Interrupted=true", resumed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed run never reached Done() after Interrupt")
	}
	if !toolCanceled {
		t.Fatal("resumed Handle.Interrupt did not cancel the in-flight tool's context")
	}
}
