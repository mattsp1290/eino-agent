package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestDiscoverHandlerToolsFailsClosedForReduction is round-three W6
// authority-regression review S1: reduction became tool-bearing (it always
// registers its own sealed reduction_read_offload tool via
// toolAppendingMiddleware -- see NewReductionHandlerFactoryWithTokenCounter)
// once Group B's scratch-offload work landed, but knownToolBearingHandlerKinds
// was never updated to include it. Before this fix, a failing reduction
// probe silently sealed zero tools (the "degrades gracefully" path meant for
// recipes like summarization that legitimately have none) instead of failing
// plan compilation closed, so every real turn would only discover the
// missing tool much later, at durableGuard, instead of at compile time.
func TestDiscoverHandlerToolsFailsClosedForReduction(t *testing.T) {
	boom := errors.New("boom: misconfigured reduction recipe")
	factory := HandlerFactory(func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		return nil, boom
	})
	_, err := discoverHandlerTools("reduction-1", HandlerKindReduction, factory)
	if err == nil {
		t.Fatal("reduction's construction error was silently absorbed -- want plan compilation to fail closed")
	}
	if !errors.Is(err, errHandlerDiscoveryFailed) {
		t.Fatalf("err = %v, want errHandlerDiscoveryFailed", err)
	}
}

// TestSummarizationCorrelatesAfterAnEarlierHandlerClonesMessagePointers is
// round-three W6 summarization-correctness review Important #1: a handler
// registered BEFORE summarization in plan Order that deep-clones
// state.Messages (via cloneProtectedMessages, a perfectly reasonable
// defensive pattern this package uses internally too, and one nothing
// stops a host-authored handler from using as well) replaces every message
// pointer durableBaselineHandler's own cycleMessageSourceByPointer map was
// keyed on. Before this fix, that silently broke summarization's pointer
// lookup for every message, so Finalize's old "nothing durable this cycle"
// degenerate case fired: it billed a real summary-generation model call,
// wrote no ContextEpoch, and raised no error. summarizationFinalize's
// content-based fallback correlation (matching by exact content against
// this cycle's own durable baseline, in order) must recover correlation
// despite the clone, so the turn still triggers and durably commits an
// epoch, exactly as if the cloning handler were never mounted.
func TestSummarizationCorrelatesAfterAnEarlierHandlerClonesMessagePointers(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("clone-ahead-of-summarization-session")

	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if requestIsSummaryGenerationE2E(request) {
			return []*einoschema.AgenticMessage{agenticAssistantText("a compact summary of everything so far")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("ok")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("clone-ahead-component"),
		AgentHandlers: []PlanAgentHandler{
			{
				// Order 0: strictly before summarization (Order 1) --
				// deep-clones every message this cycle, exactly like
				// durableBaselineHandler's own defensive clone, so every
				// pointer summarization's map was keyed on is replaced by
				// the time summarization's own hook runs.
				ID: "cloner", Order: 0, Scope: extension.GlobalScope(),
				Kind: "test.cloner", Version: HandlerVersion1, ConfigHash: "test-hash-cloner",
				Factory: cloneAheadHandlerFactory,
			},
			{
				ID: "summarization", Order: 1, Scope: extension.GlobalScope(),
				Kind: HandlerKindSummarization, Version: HandlerVersion1, ConfigHash: "test-hash-summarization",
				Factory: NewSummarizationHandlerFactory(SummarizationConfig{TriggerContextMessages: 1, RetainTailCount: 0}),
			},
		},
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}

	handle, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("hello there"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("turn 1 result = %+v, want completed", result)
	}

	handle2, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("continue"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start (turn 2) error = %v", err)
	}
	result2 := <-handle2.Done()
	if result2.Status != session.RunCompleted {
		t.Fatalf("turn 2 result = %+v, want completed -- correlation must recover via content-based fallback despite the earlier handler's clone", result2)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" && epoch.SummaryMessageID != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no summarization ContextEpoch row found among %+v -- the cloning handler silently disabled correlation", epochs)
	}
}

// cloneAheadHandler deep-clones state.Messages via cloneProtectedMessages on
// every BeforeModelRewriteState call, replacing every message pointer
// durableBaselineHandler's own map was keyed on -- see
// TestSummarizationCorrelatesAfterAnEarlierHandlerClonesMessagePointers.
type cloneAheadHandler struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
}

func (h *cloneAheadHandler) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], _ *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	cloned, err := cloneProtectedMessages(state.Messages)
	if err != nil {
		return ctx, nil, err
	}
	next := *state
	next.Messages = cloned
	return ctx, &next, nil
}

func cloneAheadHandlerFactory(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
	return &cloneAheadHandler{}, nil
}

// TestSummarizationFiresAtMostOncePerTurnAcrossFiveCycles is round-three W6
// summarization-correctness review Important #2: upstream summarization
// re-evaluates its own trigger condition on every ReAct cycle, with no
// per-call override, so once a turn's message count crosses the configured
// threshold, EVERY later cycle of that SAME turn would otherwise re-trigger
// -- billing a real summary-generation model call, and (before this fix)
// attempting to commit a new ContextEpoch, on every one of them.
// summarizeAtMostOnceMiddleware must bound this to firing exactly once per
// turn: a turn whose own tool loop runs five cycles, all past the
// threshold, must still produce exactly one summary generation call and
// exactly one ContextEpoch.
func TestSummarizationFiresAtMostOncePerTurnAcrossFiveCycles(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("five-cycle-session")
	echo := Tool{
		Name: "ping", Info: &einoschema.ToolInfo{Name: "ping", Desc: "returns a fixed short reply"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "pong"}, nil
		}),
		Retention: RetentionPolicy{MaxInlineBytes: -1},
	}

	var summaryGenerationCalls int
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if requestIsSummaryGenerationE2E(request) {
			summaryGenerationCalls++
			return []*einoschema.AgenticMessage{agenticAssistantText("a compact summary of everything so far")}, nil
		}
		calls++
		// Four tool-call rounds, then a final plain-text reply: five
		// dispatch cycles total in ONE turn, every one of them past the
		// configured message-count threshold once the first round's own
		// exchange lands.
		if calls <= 4 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-"+string(rune('0'+calls)), "ping", `{}`)}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("five-cycle-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "summarization", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindSummarization, Version: HandlerVersion1, ConfigHash: "test-hash",
			// A message-count threshold of 1 is already exceeded by this
			// turn's own first cycle (system + user = 2 messages present),
			// so EVERY one of the five cycles would independently trigger
			// under the pre-fix behavior.
			Factory: NewSummarizationHandlerFactory(SummarizationConfig{TriggerContextMessages: 1, RetainTailCount: 0}),
		}},
		Tools: testPlanTools(staticToolRegistry{tools: []Tool{echo}}),
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	root := t.TempDir()
	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := awaitResultTimeout(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if summaryGenerationCalls != 1 {
		t.Fatalf("summary generation calls = %d, want exactly 1 across all five cycles", summaryGenerationCalls)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var summarizationEpochs int
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" {
			summarizationEpochs++
		}
	}
	if summarizationEpochs != 1 {
		t.Fatalf("summarization ContextEpoch count = %d, want exactly 1 among %+v", summarizationEpochs, epochs)
	}
}

func awaitResultTimeout(t *testing.T, handle Handle, timeout time.Duration) Result {
	t.Helper()
	select {
	case result := <-handle.Done():
		return result
	case <-time.After(timeout):
		t.Fatal("run did not complete in time")
		return Result{}
	}
}

// TestSummarizationDuringAMidTurnToolLoopStillCorrelates is round-three W6
// summarization-correctness review's acceptance gap for item 8: the
// LITERAL scenario the round-two Critical named (agentsmd mounted
// alongside summarization, triggering mid a real tool-call loop -- not
// merely on a turn's very first, tool-free cycle) had no dedicated repo
// test. agentsmd injects a message into state.Messages EVERY cycle
// (typedInsertBeforeFirstUser, no durable id at all), so a turn that
// reaches its trigger threshold only AFTER a couple of real tool-call
// rounds must still correlate correctly and commit an epoch.
func TestSummarizationDuringAMidTurnToolLoopStillCorrelates(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("mid-turn-loop-session")
	echo := Tool{
		Name: "ping", Info: &einoschema.ToolInfo{Name: "ping", Desc: "returns a fixed short reply"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "pong"}, nil
		}),
		Retention: RetentionPolicy{MaxInlineBytes: -1},
	}
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if requestIsSummaryGenerationE2E(request) {
			return []*einoschema.AgenticMessage{agenticAssistantText("a compact summary of everything so far")}, nil
		}
		calls++
		switch calls {
		case 1, 2:
			// Two real tool-call rounds BEFORE the message-count threshold
			// (configured at 5) is crossed, so the trigger fires mid-loop,
			// not on the turn's first, tool-free cycle.
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-"+string(rune('0'+calls)), "ping", `{}`)}, nil
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("mid-turn-loop-component"),
		AgentHandlers: []PlanAgentHandler{
			{
				ID: "agentsmd", Order: 0, Scope: extension.GlobalScope(),
				Kind: HandlerKindAgentsMD, Version: HandlerVersion1, ConfigHash: "test-hash-agentsmd",
				Factory: NewAgentsMDHandlerFactory(AgentsMDConfig{AgentsMDFiles: []string{"AGENTS.md"}}),
			},
			{
				ID: "summarization", Order: 1, Scope: extension.GlobalScope(),
				Kind: HandlerKindSummarization, Version: HandlerVersion1, ConfigHash: "test-hash-summarization",
				Factory: NewSummarizationHandlerFactory(SummarizationConfig{TriggerContextMessages: 5, RetainTailCount: 0}),
			},
		},
		Tools: testPlanTools(staticToolRegistry{tools: []Tool{echo}}),
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "AGENTS.md"), []byte("Always answer politely."), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := awaitResultTimeout(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed -- summarization must correlate correctly even mid a real tool-call loop with agentsmd's own per-cycle injection present", result)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" && epoch.SummaryMessageID != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no summarization ContextEpoch row found among %+v -- mid-loop trigger with agentsmd mounted did not correlate", epochs)
	}
}
