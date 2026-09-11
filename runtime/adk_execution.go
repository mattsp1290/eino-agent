package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cloudwego/eino/adk"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

func init() {
	einoschema.RegisterName[*adkToolInterruptInfo]("eino_agent_adk_tool_interrupt_info")
	einoschema.RegisterName[*adkToolInterruptState]("eino_agent_adk_tool_interrupt_state")
}

// adkEngine is the per-turn coordinator shared by every adapter (model,
// tools, approval binding) ADK invokes while executing one admitted turn.
// TurnLoop's PrepareAgent (runtime/turn_loop.go) constructs a fresh adkEngine
// -- and, through AgentFactory, a fresh ADK agent -- for every admitted turn
// from the run's frozen plan, the currently active epoch, and committed
// history. One adkEngine never outlives its turn.
type adkEngine struct {
	host      *StreamingOrchestrator
	execution *runExecution
	plan      *RunPlan
	snapshot  TurnSnapshot
	turn      session.Turn
	// agentPath is the joined adk.RunPath of the (sub)agent this engine's
	// adapters report against. Empty for the root agent.
	agentPath string
	// assistantMessageID is the turn's already-admitted assistant
	// placeholder; the model adapter claims it for the first physical
	// dispatch of the turn.
	assistantMessageID session.MessageID
	// historyOptions is used to reload the durable model-input projection
	// fresh before every physical dispatch (see adkModel.durableProjection).
	historyOptions history.Options
	// baseMessageCount is len(allMessages) at turn-admission time, BEFORE
	// contextAssemblePoint's extension transforms ran (prepareSnapshot's
	// Base, not its output snapshot.Messages): the boundary
	// adkModel.durableProjection uses to tell "this turn's own progress
	// since admission" (everything a fresh durable reload finds past this
	// index) apart from the already-transformed, frozen prefix
	// (snapshot.Messages itself, which may carry ephemeral extension
	// content a durable reload can never see).
	baseMessageCount int

	stepMu          sync.Mutex
	step            int
	placeholderUsed bool

	usageMu sync.Mutex
	usage   model.Usage

	responseMu         sync.Mutex
	responseMessageIDs []session.MessageID

	// prepareMu guards prepareErrors, the in-memory record of
	// preparedToolCall.middlewareErr for calls this turn's model adapter
	// prepared but has not yet handed to ADK's tools node for dispatch (see
	// prepareToolCalls's undiscovered-deferred-tool denial): the durable
	// session.ToolCall row itself carries no such field, so adkTool must
	// consult this map (by call ID) to reproduce the same denial
	// executePreparedTools used to apply, since ADK -- not this package --
	// now owns when each pending call actually dispatches.
	prepareMu     sync.Mutex
	prepareErrors map[session.ToolCallID]error
}

func (e *adkEngine) recordPrepareError(id session.ToolCallID, err error) {
	if err == nil {
		return
	}
	e.prepareMu.Lock()
	defer e.prepareMu.Unlock()
	if e.prepareErrors == nil {
		e.prepareErrors = make(map[session.ToolCallID]error)
	}
	e.prepareErrors[id] = err
}

func (e *adkEngine) takePrepareError(id session.ToolCallID) error {
	e.prepareMu.Lock()
	defer e.prepareMu.Unlock()
	return e.prepareErrors[id]
}

func (e *adkEngine) recordResponseMessage(id session.MessageID) {
	e.responseMu.Lock()
	defer e.responseMu.Unlock()
	e.responseMessageIDs = append(e.responseMessageIDs, id)
}

func (e *adkEngine) responseMessageIDsSnapshot() []session.MessageID {
	e.responseMu.Lock()
	defer e.responseMu.Unlock()
	return append([]session.MessageID(nil), e.responseMessageIDs...)
}

func (e *adkEngine) claimPlaceholder() (session.MessageID, bool) {
	e.stepMu.Lock()
	defer e.stepMu.Unlock()
	if e.assistantMessageID == "" || e.placeholderUsed {
		return "", false
	}
	e.placeholderUsed = true
	return e.assistantMessageID, true
}

func (e *adkEngine) nextStep() int {
	e.stepMu.Lock()
	defer e.stepMu.Unlock()
	e.step++
	return e.step
}

func (e *adkEngine) addUsage(u model.Usage) {
	e.usageMu.Lock()
	defer e.usageMu.Unlock()
	e.usage = addUsage(e.usage, u)
}

func (e *adkEngine) usageSnapshot() model.Usage {
	e.usageMu.Lock()
	defer e.usageMu.Unlock()
	return e.usage
}

// buildAgent constructs this turn's mandatory adapters and hands them, along
// with the resolved tool alias configuration, to the run plan's AgentFactory.
// The factory MUST build its agent from exactly the provided Model/Tools:
// buildAgent installs a final BeforeAgent guard (durableGuard) that rejects
// any tool in the resulting agent's tool list that is not one of the
// adapters it handed the factory, closing the gap where a factory (or a
// middleware it configures) could otherwise substitute an undurable tool
// that bypasses claim/settlement.
func (e *adkEngine) buildAgent(ctx context.Context, approval *adkApprovalBinding) (adk.TypedAgent[*einoschema.AgenticMessage], error) {
	inner := &adkModel{host: e.host, execution: e.execution, engine: e, approval: approval}
	tools := make([]tool.BaseTool, 0, len(e.snapshot.Tools))
	durable := make(map[string]bool, len(e.snapshot.Tools))
	aliases := make(map[string]compose.ToolAliasConfig, len(e.snapshot.Tools))
	for _, t := range e.snapshot.Tools {
		wrapped := &adkTool{engine: e, tool: t}
		tools = append(tools, wrapped)
		durable[t.Name] = true
		if len(t.Aliases) != 0 || len(t.ArgumentAliases) != 0 {
			aliases[t.Name] = compose.ToolAliasConfig{NameAliases: cloneSlice(t.Aliases), ArgumentsAliases: cloneStringSliceMap(t.ArgumentAliases)}
		}
	}
	if e.snapshot.ToolSearch != nil {
		search := &adkToolSearch{engine: e, name: e.snapshot.ToolSearch.Name}
		tools = append(tools, search)
		durable[e.snapshot.ToolSearch.Name] = true
	}
	build := AgentBuildContext{
		Model: inner, Tools: tools, ToolAliases: aliases,
		Instruction:   "",
		MaxIterations: e.host.toolTurns(),
		Guard:         newDurableGuard(durable),
		Retry:         defaultRetryConfig(e.host.attempts()),
	}
	factory := e.plan.AgentFactory()
	return factory.BuildAgent(ctx, build)
}

func cloneStringSliceMap(src map[string][]string) map[string][]string {
	if src == nil {
		return nil
	}
	dst := make(map[string][]string, len(src))
	for k, v := range src {
		dst[k] = cloneSlice(v)
	}
	return dst
}

// AgentBuildContext is the bounded construction context an AgentFactory
// receives. Model and Tools are the mandatory durable adapters for this
// turn: an AgentFactory MUST build its agent using exactly these values (not
// substitutes), and every child agent/tool it registers must come from the
// same adapters. ToolAliases mirrors the plan's W4 alias configuration in
// upstream compose.ToolAliasConfig shape, ready to pass through to
// compose.ToolsNodeConfig.ToolAliases so ADK's own dispatch layer routes a
// model-requested alias to the correct adapter.
type AgentBuildContext struct {
	Model         einomodel.AgenticModel
	Tools         []tool.BaseTool
	ToolAliases   map[string]compose.ToolAliasConfig
	Instruction   string
	MaxIterations int
	Retry         *adk.TypedModelRetryConfig[*einoschema.AgenticMessage]
	Failover      *adk.ModelFailoverConfig[*einoschema.AgenticMessage]
	// Guard is the mandatory final BeforeAgent handler an AgentFactory must
	// install last in its Handlers list (see durableGuard): it rejects any
	// tool in the built agent's effective tool list that is not one of the
	// adapters this AgentBuildContext provided.
	Guard adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
}

// AgentFactory builds the typed ADK agent for one admitted turn from a
// bounded AgentBuildContext. It is frozen in RunPlanSpec.Agent (see
// runtime/extension_plan.go) so a resumed run rebuilds an equivalent agent
// graph deterministically.
type AgentFactory interface {
	BuildAgent(ctx context.Context, build AgentBuildContext) (adk.TypedAgent[*einoschema.AgenticMessage], error)
}

// AgentFactoryFunc adapts a function into an AgentFactory.
type AgentFactoryFunc func(context.Context, AgentBuildContext) (adk.TypedAgent[*einoschema.AgenticMessage], error)

func (f AgentFactoryFunc) BuildAgent(ctx context.Context, build AgentBuildContext) (adk.TypedAgent[*einoschema.AgenticMessage], error) {
	return f(ctx, build)
}

// DefaultChatModelAgentFactory builds a plain adk.TypedChatModelAgent from
// the bounded build context: config.Agent's instruction (carried on
// build.Instruction by the caller) and the runtime's configured tool-turn
// limit as MaxIterations.
type DefaultChatModelAgentFactory struct{}

func (DefaultChatModelAgentFactory) BuildAgent(ctx context.Context, build AgentBuildContext) (adk.TypedAgent[*einoschema.AgenticMessage], error) {
	cfg := &adk.TypedChatModelAgentConfig[*einoschema.AgenticMessage]{
		Name:          "agent",
		Instruction:   build.Instruction,
		Model:         build.Model,
		MaxIterations: build.MaxIterations,
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{
			Tools: build.Tools, ToolAliases: build.ToolAliases,
		}},
		ModelRetryConfig:    build.Retry,
		ModelFailoverConfig: build.Failover,
	}
	if build.Guard != nil {
		cfg.Handlers = append(cfg.Handlers, build.Guard)
	}
	return adk.NewTypedChatModelAgent[*einoschema.AgenticMessage](ctx, cfg)
}

// durableTool is implemented by every adapter this package hands to an
// AgentFactory as a tool.BaseTool, and checked by durableGuard.
type durableTool interface {
	durableToolName() string
}

// durableGuard is the last-registered BeforeAgent handler installed around
// every agent an AgentFactory returns. It rejects any tool in the agent's
// effective tool list that is not one of the mandatory durable adapters this
// engine handed the factory -- closing the gap where a factory (or a
// middleware it configures) could otherwise inject a tool that bypasses
// claim/settlement.
type durableGuard struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
	allowed map[string]bool
}

func newDurableGuard(allowed map[string]bool) *durableGuard {
	return &durableGuard{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}, allowed: allowed}
}

func (g *durableGuard) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	for _, t := range runCtx.Tools {
		dt, ok := t.(durableTool)
		if !ok {
			info, _ := t.Info(ctx)
			name := "<unknown>"
			if info != nil {
				name = info.Name
			}
			return ctx, nil, fmt.Errorf("%w: tool %q is not a durable runtime adapter", errADKUnsupportedBlock, name)
		}
		if !g.allowed[dt.durableToolName()] {
			return ctx, nil, fmt.Errorf("%w: tool %q is outside the frozen registry", errADKUnsupportedBlock, dt.durableToolName())
		}
	}
	return ctx, runCtx, nil
}

// adkTool is the mandatory durable tool adapter promoted from the W1 proof's
// adkDurableTool (runtime/adk_proof_test.go). ADK's tools node schedules the
// call, but the leaf executor only ever runs after the persisted call is
// claimed under the current run fence via the existing
// normalize -> permission -> claim -> execute -> transform -> bounded-encode
// -> settle pipeline (tool_preparation.go/tool_execution.go/tool_settlement.go),
// and the model only ever sees the settled output.
type adkTool struct {
	engine *adkEngine
	tool   Tool
}

var _ tool.InvokableTool = (*adkTool)(nil)
var _ durableTool = (*adkTool)(nil)

func (t *adkTool) durableToolName() string { return t.tool.Name }

// Info never returns a nil *schema.ToolInfo: ADK's tools node dereferences
// it unconditionally to build its dispatch table for every registered tool
// (compose.NewAgenticToolsNode), regardless of the classic engine's
// "unadvertised" convention for a Tool with no Info (see
// TurnSnapshot.ProviderRequest, which simply omits such a tool from the
// model-visible tool list but still expects it callable). A Tool with a nil
// Info therefore synthesizes a minimal, name-only schema here so it can
// still be claimed/executed durably; it remains unadvertised to the model
// via ProviderRequest.
func (t *adkTool) Info(context.Context) (*einoschema.ToolInfo, error) {
	if t.tool.Info != nil {
		return t.tool.Info, nil
	}
	return &einoschema.ToolInfo{Name: t.tool.Name}, nil
}

// adkToolInterruptInfo/adkToolInterruptState are the durable-only info/state
// carried by a tool-authored ADK pause (see ToolInterruptPolicy). Both are
// gob-registered because the tools node persists leaf interrupt info inside
// its own composite rerun state.
type adkToolInterruptInfo struct {
	ToolCallID string
	Reason     string
}

type adkToolInterruptState struct {
	ToolCallID string
}

func (t *adkTool) InvokableRun(ctx context.Context, arguments string, _ ...tool.Option) (string, error) {
	e := t.engine
	callID := session.ToolCallID(compose.GetToolCallID(ctx))
	if callID == "" {
		return "", errors.New("tool call id missing from ADK context")
	}
	record, err := e.host.store.GetToolCall(ctx, callID)
	if err != nil {
		return "", fmt.Errorf("persisted tool call %s: %w", callID, err)
	}
	if record.RunID != e.snapshot.RunID || record.SessionID != e.snapshot.SessionID {
		return "", fmt.Errorf("persisted tool call %s belongs to run %s, not %s", callID, record.RunID, e.snapshot.RunID)
	}
	run := session.Run{ID: e.snapshot.RunID, SessionID: e.snapshot.SessionID, ModelID: string(e.snapshot.Model.Model.ID)}
	switch {
	case session.TerminalToolCall(record.Status):
		return string(record.Output), nil
	case record.Status == session.ToolCallRunning:
		settled, err := e.execution.settleInterruptedRunningTool(ctx, run, t.tool, record)
		if err != nil {
			return "", err
		}
		return string(settled.Output), nil
	case record.Status != session.ToolCallPending:
		return "", fmt.Errorf("unexpected tool call status %q for %s", record.Status, callID)
	}
	call := ToolCall{
		ID: record.ID, SessionID: record.SessionID, RunID: record.RunID, MessageID: record.MessageID,
		ResultMessageID: record.ResultMessageID, ResultPartID: record.ResultPartID, Name: record.Name, RequestedName: record.RequestedName,
		Scope: t.tool.Scope, Pattern: record.Pattern, Input: cloneJSON(record.Input), Context: toolContext(e.snapshot, e.snapshot.Tools),
	}
	if t.tool.InterruptPolicy != nil {
		wasInterrupted, hasState, _ := compose.GetInterruptState[*adkToolInterruptState](ctx)
		if wasInterrupted && hasState {
			isTarget, hasData, decision := compose.GetResumeContext[string](ctx)
			if !isTarget {
				return "", compose.StatefulInterrupt(ctx, &adkToolInterruptInfo{ToolCallID: string(callID)}, &adkToolInterruptState{ToolCallID: string(callID)})
			}
			if !hasData {
				return "", fmt.Errorf("host decision for %s missing", callID)
			}
			call.ResumeDecision = decision
		} else if reason, required := t.tool.InterruptPolicy.RequiresInterrupt(ctx, call); required {
			return "", compose.StatefulInterrupt(ctx, &adkToolInterruptInfo{ToolCallID: string(callID), Reason: reason}, &adkToolInterruptState{ToolCallID: string(callID)})
		}
	}
	if canonical, err := canonicalToolObject(record.Input); err != nil || string(canonical) != strings.TrimSpace(arguments) {
		return "", fmt.Errorf("ADK arguments %q diverge from persisted canonical input for %s", arguments, callID)
	}
	startedAt := e.host.now()
	claimed, err := e.execution.persistToolClaim(ctx, session.ClaimToolCallRequest{
		ID: record.ID, ClaimedBy: e.host.ownerID(), ClaimToken: string(e.host.ids.NewEventID()), StartedAt: startedAt,
		LeaseDuration: e.host.lease(), Event: toolTransitionEnvelope(e.host, e.snapshot, startedAt),
	})
	if err != nil {
		return "", err
	}
	call.ResultMessageID = claimed.Call.ResultMessageID
	call.ResultPartID = claimed.Call.ResultPartID
	prepareErr := e.takePrepareError(record.ID)
	settled, err := e.execution.executeAndSettleClaimedTool(ctx, e.snapshot, t.tool, call, claimed.Call, prepareErr)
	if err != nil {
		return "", err
	}
	return string(settled.Settlement.Output), nil
}

// adkToolSearch is the mandatory adapter for this turn's configured
// runtime-implemented tool-search tool (TurnSnapshot.ToolSearch). ADK's
// tools node needs it registered as an ordinary tool.BaseTool to route a
// model call by name at all, but its execution/settlement is entirely
// runtime.tool_search.go's own (executeToolSearchCall), not the normal
// claim/execute/settle pipeline: it carries no execution authority of its
// own since every tool it can surface is still validated against the
// frozen registry when actually called.
type adkToolSearch struct {
	engine *adkEngine
	name   string
}

var _ tool.InvokableTool = (*adkToolSearch)(nil)
var _ durableTool = (*adkToolSearch)(nil)

func (t *adkToolSearch) durableToolName() string { return t.name }

func (t *adkToolSearch) Info(context.Context) (*einoschema.ToolInfo, error) {
	return toolSearchToolInfo(t.engine.snapshot.ToolSearch), nil
}

func (t *adkToolSearch) InvokableRun(ctx context.Context, arguments string, _ ...tool.Option) (string, error) {
	e := t.engine
	callID := session.ToolCallID(compose.GetToolCallID(ctx))
	if callID == "" {
		return "", errors.New("tool call id missing from ADK context")
	}
	record, err := e.host.store.GetToolCall(ctx, callID)
	if err != nil {
		return "", fmt.Errorf("persisted tool call %s: %w", callID, err)
	}
	if record.RunID != e.snapshot.RunID || record.SessionID != e.snapshot.SessionID {
		return "", fmt.Errorf("persisted tool call %s belongs to run %s, not %s", callID, record.RunID, e.snapshot.RunID)
	}
	switch {
	case session.TerminalToolCall(record.Status):
		return string(record.Output), nil
	case record.Status == session.ToolCallRunning:
		run := session.Run{ID: e.snapshot.RunID, SessionID: e.snapshot.SessionID, ModelID: string(e.snapshot.Model.Model.ID)}
		settled, err := e.execution.settleInterruptedRunningTool(ctx, run, Tool{Name: t.name}, record)
		if err != nil {
			return "", err
		}
		return string(settled.Output), nil
	case record.Status != session.ToolCallPending:
		return "", fmt.Errorf("unexpected tool call status %q for %s", record.Status, callID)
	}
	call := ToolCall{
		ID: record.ID, SessionID: record.SessionID, RunID: record.RunID, MessageID: record.MessageID,
		Name: record.Name, RequestedName: record.RequestedName, Input: cloneJSON(record.Input), Context: toolContext(e.snapshot, e.snapshot.Tools),
	}
	if _, err := e.execution.executeToolSearchCall(ctx, e.snapshot, call, record); err != nil {
		return "", err
	}
	settled, err := e.host.store.GetToolCall(ctx, callID)
	if err != nil {
		return "", err
	}
	return string(settled.Output), nil
}

// ToolInterruptPolicy lets a tool pause via a durable ADK checkpoint before
// its first execution attempt, instead of running immediately -- for example
// a tool that needs a host decision delivered out of band through the
// TurnLoop resume path rather than synchronously. It is consulted once per
// pending call, before claiming.
type ToolInterruptPolicy interface {
	// RequiresInterrupt reports whether call must pause before its first
	// execution attempt, and a bounded, durable reason string to record on
	// the checkpoint if so.
	RequiresInterrupt(ctx context.Context, call ToolCall) (reason string, required bool)
}
