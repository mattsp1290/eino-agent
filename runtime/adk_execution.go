package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cloudwego/eino/adk"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
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

	// attemptMu guards lastFailedInvocation/lastFailedAttemptErr: the
	// InvocationID and error of the most recently failed physical dispatch
	// not yet superseded by a retry or failover attempt. adkModel.begin
	// consumes them (emitting attempt_replaced and an observability Retry
	// event) when the next attempt starts.
	attemptMu            sync.Mutex
	lastFailedInvocation string
	lastFailedAttemptErr error

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

	// toolBatchMu guards toolBatchOrder/toolBatchDone/toolBatchAborted: the
	// coordination state that serializes sibling tool calls declared in the
	// same assistant message. ADK's tools node dispatches every call in a
	// batch concurrently (compose.parallelRunToolCall), with no cancellation
	// on a sibling's failure -- left alone, a later call whose executor
	// happens to run fast would complete before an earlier, panicking
	// sibling is even recovered, even though the resume path's equivalent
	// (terminalizeUnfinishedTools/interruptPendingTool) never lets a call
	// after a fatal one execute. registerToolBatch/awaitToolTurn/
	// settleToolTurn (used by adkTool.InvokableRun) restore that invariant
	// for a live turn: calls in one batch run in their declared order, and
	// once one fatally panics every later, not-yet-started sibling is
	// interrupted instead of executed.
	toolBatchMu      sync.Mutex
	toolBatchOrder   map[session.MessageID][]session.ToolCallID
	toolBatchDone    map[session.ToolCallID]chan struct{}
	toolBatchAborted map[session.MessageID]bool

	// guard is the durableGuard instance buildAgent handed this turn's
	// AgentFactory (as AgentBuildContext.Guard). onAgentEvents checks
	// guard.hasRun() after the agent finishes: a compliant agent's
	// BeforeAgent hook always fires before any model dispatch, so this
	// detects a factory that never wires AgentBuildContext.Guard into its
	// agent's handler chain at all (see guardRan's doc comment). It does
	// NOT detect a factory that installs the guard but substitutes its own
	// model instead of build.Model: BeforeAgent only inspects the agent's
	// tool list, never its model -- see dispatches below for that case.
	guard *durableGuard
	// dispatches counts this turn's durable model dispatches, incremented
	// by adkModel.begin once a physical call's ledger row is durably
	// committed. onAgentEvents checks it alongside guardRan after the
	// events iterator drains: a factory that installs the guard but
	// substitutes its own model passes guardRan (the guard only inspects
	// tools) yet never routes a single dispatch through adkModel, so
	// dispatches stays zero even though the provider was actually called --
	// this is the engine-side proof that some model output was durably
	// ledgered this turn (see onAgentEvents in runtime/turn_loop.go). Like
	// guardRan, this check is necessarily post-hoc: it runs after the
	// events iterator has fully drained, so a noncompliant factory's own
	// (unledgered) provider call has already happened by the time the turn
	// is failed.
	dispatches atomic.Int64
}

// guardRan reports whether this turn's durable guard actually fired. A
// factory that never wires AgentBuildContext.Guard into its agent's handler
// chain at all produces an agent whose execution never touches this
// engine's adapters, so this is the only point that can still detect that
// specific noncompliance (see onAgentEvents in runtime/turn_loop.go). See
// adkEngine.dispatches for the complementary check that catches a factory
// which installs the guard but substitutes its own model.
func (e *adkEngine) guardRan() bool {
	if e == nil || e.guard == nil {
		return false
	}
	return e.guard.hasRun()
}

// dispatchCount reports how many of this turn's model dispatches were
// durably ledgered via adkModel.begin -- see adkEngine.dispatches's doc
// comment.
func (e *adkEngine) dispatchCount() int64 {
	if e == nil {
		return 0
	}
	return e.dispatches.Load()
}

// registerToolBatch records the declared call order for a freshly committed
// assistant message's tool calls (see adkModel.commit). A single-call batch
// needs no coordination and is not registered; awaitToolTurn treats an
// unregistered call as ungated.
func (e *adkEngine) registerToolBatch(messageID session.MessageID, callIDs []session.ToolCallID) {
	if len(callIDs) < 2 {
		return
	}
	e.toolBatchMu.Lock()
	defer e.toolBatchMu.Unlock()
	if e.toolBatchOrder == nil {
		e.toolBatchOrder = make(map[session.MessageID][]session.ToolCallID)
		e.toolBatchDone = make(map[session.ToolCallID]chan struct{})
	}
	e.toolBatchOrder[messageID] = append([]session.ToolCallID(nil), callIDs...)
	for _, id := range callIDs {
		e.toolBatchDone[id] = make(chan struct{})
	}
}

// awaitToolTurn blocks callID until every earlier-declared sibling in its
// batch (if any) has settled via settleToolTurn, then reports whether the
// batch was aborted by an earlier fatal sibling. An unregistered call (no
// batch, or the first call in one) returns immediately.
func (e *adkEngine) awaitToolTurn(ctx context.Context, messageID session.MessageID, callID session.ToolCallID) (aborted bool, err error) {
	e.toolBatchMu.Lock()
	order := e.toolBatchOrder[messageID]
	e.toolBatchMu.Unlock()
	position := -1
	for i, id := range order {
		if id == callID {
			position = i
			break
		}
	}
	if position > 0 {
		e.toolBatchMu.Lock()
		waitOn := e.toolBatchDone[order[position-1]]
		e.toolBatchMu.Unlock()
		if waitOn != nil {
			select {
			case <-waitOn:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
	}
	e.toolBatchMu.Lock()
	aborted = e.toolBatchAborted[messageID]
	e.toolBatchMu.Unlock()
	return aborted, nil
}

// settleToolTurn signals callID's completion, unblocking the next sibling in
// its batch (if any), and marks the whole batch aborted when fatal is true
// (a panicking executor) so every later, not-yet-started sibling skips
// execution. It is idempotent: every exit path of adkTool/adkToolSearch's
// InvokableRun now calls it exactly once via a defer (see those methods), so
// a second call for the same callID (there should never be one, but the
// contract must hold regardless) is a safe no-op rather than a double-close
// panic.
func (e *adkEngine) settleToolTurn(messageID session.MessageID, callID session.ToolCallID, fatal bool) {
	e.toolBatchMu.Lock()
	if fatal {
		if e.toolBatchAborted == nil {
			e.toolBatchAborted = make(map[session.MessageID]bool)
		}
		e.toolBatchAborted[messageID] = true
	}
	done, ok := e.toolBatchDone[callID]
	if ok {
		delete(e.toolBatchDone, callID)
	}
	e.toolBatchMu.Unlock()
	if ok && done != nil {
		close(done)
	}
}

// messageIDForToolCall best-effort looks up the owning assistant message for
// a registered batch call ID, for the rare exit paths of InvokableRun that
// return before the tool call's own durable record (and its MessageID) has
// been loaded. A miss (callID not in any registered batch, e.g. a
// single-call "batch" that was never registered at all) returns "" and is
// harmless: settleToolTurn treats an unregistered/unknown callID as a no-op.
func (e *adkEngine) messageIDForToolCall(callID session.ToolCallID) session.MessageID {
	e.toolBatchMu.Lock()
	defer e.toolBatchMu.Unlock()
	for messageID, order := range e.toolBatchOrder {
		for _, id := range order {
			if id == callID {
				return messageID
			}
		}
	}
	return ""
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

// recordFailedAttempt marks invocationID as replaceable: the next physical
// dispatch this turn starts (a retry or failover attempt) consumes it via
// takeFailedAttempt to emit attempt_replaced and an observability Retry
// event carrying err's classification.
func (e *adkEngine) recordFailedAttempt(invocationID string, err error) {
	e.attemptMu.Lock()
	defer e.attemptMu.Unlock()
	e.lastFailedInvocation = invocationID
	e.lastFailedAttemptErr = err
}

// takeFailedAttempt returns and clears the most recently failed
// not-yet-superseded invocation (id == "" if none is pending) and its error.
func (e *adkEngine) takeFailedAttempt() (id string, err error) {
	e.attemptMu.Lock()
	defer e.attemptMu.Unlock()
	id, err = e.lastFailedInvocation, e.lastFailedAttemptErr
	e.lastFailedInvocation, e.lastFailedAttemptErr = "", nil
	return id, err
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
	guard := newDurableGuard(durable)
	build := AgentBuildContext{
		Model: inner, Tools: tools, ToolAliases: aliases,
		Instruction:   "",
		MaxIterations: e.host.toolTurns(),
		Guard:         guard,
		Retry:         defaultRetryConfig(e.host.attempts()),
		Failover:      buildFailoverConfig(e, approval, e.plan.FailoverPolicy()),
	}
	e.guard = guard
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

	mu  sync.Mutex
	ran bool
}

func newDurableGuard(allowed map[string]bool) *durableGuard {
	return &durableGuard{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}, allowed: allowed}
}

// hasRun reports whether BeforeAgent has fired at least once for this guard
// instance -- see adkEngine.guardRan's doc comment.
func (g *durableGuard) hasRun() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ran
}

func (g *durableGuard) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	g.mu.Lock()
	g.ran = true
	g.mu.Unlock()
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
	// Every exit path from here down must release the next sibling in this
	// call's declared batch (see adkEngine.registerToolBatch's doc
	// comment): compose.parallelRunToolCall never cancels a sibling on this
	// one's failure, so a return without settling leaves the next call
	// parked in awaitToolTurn until the run context dies. The defer is
	// installed before the callID=="" check below (not after) so that
	// invariant holds with no exception: an empty call ID can never be a
	// registered batch member in the first place (registerToolBatch only
	// ever registers IDs ADK itself declared on a committed assistant
	// message), so messageIDForToolCall("") finds nothing and
	// settleToolTurn is a harmless no-op for it, exactly like every other
	// unregistered/unknown callID (see settleToolTurn's doc comment).
	// messageID is refined below once the durable record loads.
	messageID := e.messageIDForToolCall(callID)
	fatal := false
	defer func() { e.settleToolTurn(messageID, callID, fatal) }()

	if callID == "" {
		return "", errors.New("tool call id missing from ADK context")
	}
	record, err := e.host.store.GetToolCall(ctx, callID)
	if err != nil {
		fatal = true
		return "", fmt.Errorf("persisted tool call %s: %w", callID, err)
	}
	messageID = record.MessageID
	if record.RunID != e.snapshot.RunID || record.SessionID != e.snapshot.SessionID {
		fatal = true
		return "", fmt.Errorf("persisted tool call %s belongs to run %s, not %s", callID, record.RunID, e.snapshot.RunID)
	}
	run := session.Run{ID: e.snapshot.RunID, SessionID: e.snapshot.SessionID, ModelID: string(e.snapshot.Model.Model.ID)}
	switch {
	case session.TerminalToolCall(record.Status):
		return string(record.Output), nil
	case record.Status == session.ToolCallRunning:
		settled, err := e.execution.settleInterruptedRunningTool(ctx, run, t.tool, record)
		if err != nil {
			fatal = true
			return "", err
		}
		return string(settled.Output), nil
	case record.Status != session.ToolCallPending:
		fatal = true
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
				// A durable pause is a first-class, normal outcome (not a
				// batch failure): settling here (via the deferred call
				// above) lets a sibling declared after this one still run
				// to its own completion/pause, matching ADK's own
				// wg.Wait()-every-task tool-node semantics.
				return "", compose.StatefulInterrupt(ctx, &adkToolInterruptInfo{ToolCallID: string(callID)}, &adkToolInterruptState{ToolCallID: string(callID)})
			}
			if !hasData {
				fatal = true
				return "", fmt.Errorf("host decision for %s missing", callID)
			}
			call.ResumeDecision = decision
		} else if reason, required := t.tool.InterruptPolicy.RequiresInterrupt(ctx, call); required {
			return "", compose.StatefulInterrupt(ctx, &adkToolInterruptInfo{ToolCallID: string(callID), Reason: reason}, &adkToolInterruptState{ToolCallID: string(callID)})
		}
	}
	if canonical, err := canonicalToolObject(record.Input); err != nil || string(canonical) != strings.TrimSpace(arguments) {
		fatal = true
		return "", fmt.Errorf("ADK arguments %q diverge from persisted canonical input for %s", arguments, callID)
	}
	// ADK's tools node dispatches every call declared in the same assistant
	// message concurrently with no cancellation on a sibling's failure (see
	// adkEngine.registerToolBatch's doc comment); wait for this call's turn
	// in its declared batch order and, if an earlier sibling already fatally
	// panicked, skip execution entirely and settle this call interrupted --
	// exactly the resume path's terminalizeUnfinishedTools/
	// interruptPendingTool treatment of an unstarted call after a fatal one.
	aborted, err := e.awaitToolTurn(ctx, record.MessageID, callID)
	if err != nil {
		fatal = true
		return "", err
	}
	if aborted {
		settlement, err := e.execution.interruptPendingTool(ctx, e.snapshot, record)
		if err != nil {
			fatal = true
			return "", err
		}
		return string(settlement.Output), nil
	}
	startedAt := e.host.now()
	claimed, err := e.execution.persistToolClaim(ctx, session.ClaimToolCallRequest{
		ID: record.ID, ClaimedBy: e.host.ownerID(), ClaimToken: string(e.host.ids.NewEventID()), StartedAt: startedAt,
		LeaseDuration: e.host.lease(), Event: toolTransitionEnvelope(e.host, e.snapshot, startedAt),
	})
	if err != nil {
		fatal = true
		return "", err
	}
	extension.Notify(e.execution.dispatch(), ctx, ToolStartedPoint, ToolStartedNotice{
		SessionID: e.snapshot.SessionID, RunID: e.snapshot.RunID, ToolCallID: claimed.Call.ID, ToolName: claimed.Call.Name, Time: claimed.Call.StartedAt,
	})
	call.ResultMessageID = claimed.Call.ResultMessageID
	call.ResultPartID = claimed.Call.ResultPartID
	prepareErr := e.takePrepareError(record.ID)
	settled, err := e.execution.executeAndSettleClaimedTool(ctx, e.snapshot, t.tool, call, claimed.Call, prepareErr)
	if err != nil {
		return "", err
	}
	// A panicking executor, or one whose own context was canceled, settles
	// as an ordinary (non-fatal-looking) tool outcome -- see
	// executeClaimedToolPipeline's recover and dispositionForError's
	// context.Canceled/DeadlineExceeded mapping to ToolInterrupted -- so it
	// is durably recorded like any other terminal tool outcome. But feeding
	// either back to the model as an ordinary function_tool_result and
	// letting the ReAct loop dispatch again would call the model against a
	// turn this runtime no longer trusts (a panic's executor state is
	// unknown; a canceled run should stop, not keep iterating -- left
	// unchecked, a model that unconditionally re-issues the same tool call
	// loops until MaxIterations). Fail this node instead, matching the
	// resume path's equivalent checks (see interrupt.go's
	// errToolExecutionPanic/context.Canceled handling), and mark this
	// call's batch aborted (via the deferred settle above) so every later,
	// not-yet-started sibling is interrupted instead of executed.
	fatal = errors.Is(settled.Outcome.RawError, errToolExecutionPanic) || errors.Is(settled.Outcome.RawError, context.Canceled)
	if fatal {
		return "", settled.Outcome.RawError
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
	// tool_search participates in the same sibling-batch settlement protocol
	// as adkTool.InvokableRun (see that method's doc comment, including why
	// the defer below is installed before the callID=="" check): registered
	// but never settled, it left the next sibling in a mixed batch parked
	// in awaitToolTurn forever.
	messageID := e.messageIDForToolCall(callID)
	fatal := false
	defer func() { e.settleToolTurn(messageID, callID, fatal) }()

	if callID == "" {
		return "", errors.New("tool call id missing from ADK context")
	}
	record, err := e.host.store.GetToolCall(ctx, callID)
	if err != nil {
		fatal = true
		return "", fmt.Errorf("persisted tool call %s: %w", callID, err)
	}
	messageID = record.MessageID
	if record.RunID != e.snapshot.RunID || record.SessionID != e.snapshot.SessionID {
		fatal = true
		return "", fmt.Errorf("persisted tool call %s belongs to run %s, not %s", callID, record.RunID, e.snapshot.RunID)
	}
	switch {
	case session.TerminalToolCall(record.Status):
		return string(record.Output), nil
	case record.Status == session.ToolCallRunning:
		run := session.Run{ID: e.snapshot.RunID, SessionID: e.snapshot.SessionID, ModelID: string(e.snapshot.Model.Model.ID)}
		settled, err := e.execution.settleInterruptedRunningTool(ctx, run, Tool{Name: t.name}, record)
		if err != nil {
			fatal = true
			return "", err
		}
		return string(settled.Output), nil
	case record.Status != session.ToolCallPending:
		fatal = true
		return "", fmt.Errorf("unexpected tool call status %q for %s", record.Status, callID)
	}
	aborted, err := e.awaitToolTurn(ctx, record.MessageID, callID)
	if err != nil {
		fatal = true
		return "", err
	}
	if aborted {
		settlement, err := e.execution.interruptPendingTool(ctx, e.snapshot, record)
		if err != nil {
			fatal = true
			return "", err
		}
		return string(settlement.Output), nil
	}
	call := ToolCall{
		ID: record.ID, SessionID: record.SessionID, RunID: record.RunID, MessageID: record.MessageID,
		Name: record.Name, RequestedName: record.RequestedName, Input: cloneJSON(record.Input), Context: toolContext(e.snapshot, e.snapshot.Tools),
	}
	if _, err := e.execution.executeToolSearchCall(ctx, e.snapshot, call, record); err != nil {
		fatal = true
		return "", err
	}
	settled, err := e.host.store.GetToolCall(ctx, callID)
	if err != nil {
		fatal = true
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
