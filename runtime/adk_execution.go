package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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

	// baselineMessages is this cycle's stable, baseline-indexed durable
	// projection, set by durableBaselineHandler (via
	// adkEngine.buildDurableBaseline) and consumed by
	// adkModel.prepareDispatchInput and settlementSeal -- see both doc
	// comments (runtime/adk_middleware.go, runtime/adk_model.go).
	baselineMessages   []*einoschema.AgenticMessage
	authorizedRewrites *authorizedRewriteSet

	// handlerTools holds this turn's live tool.BaseTool instances
	// contributed by host agent-handler middleware (filesystem's
	// read_file, plantask's task tools, skill's "skill" tool, ...), keyed
	// by name and collected once when each handler is built (see
	// adkEngine.buildAgentHandlers). handlerToolExecutor looks a call's
	// tool up here at dispatch time; a sealed handler tool whose live
	// instance is missing (a handler that stopped declaring a tool it
	// previously sealed) fails that call, not the whole turn.
	handlerTools map[string]tool.BaseTool
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

	authorized := newAuthorizedRewriteSet()
	e.authorizedRewrites = authorized

	// Build host handler middleware instances first (collecting their live
	// tool instances into e.handlerTools) so sealHandlerTools can splice
	// their sealed tool identities into e.snapshot.Tools before the main
	// tool-wrapping loop below runs -- durableGuard's allow-set and
	// prepareToolCalls/resolveToolCall's frozen registry both derive from
	// that same e.snapshot.Tools, so a handler tool participates in both
	// exactly like any composition-registered tool.
	handlers, err := e.buildAgentHandlers(ctx, inner, authorized)
	if err != nil {
		return nil, err
	}
	e.sealHandlerTools()

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
		Instruction:     "",
		MaxIterations:   e.host.toolTurns(),
		Guard:           guard,
		SettlementSeal:  newSettlementSeal(e, authorized),
		DurableBaseline: newDurableBaselineHandler(e),
		Handlers:        handlers,
		Retry:           defaultRetryConfig(e.host.attempts()),
		Failover:        buildFailoverConfig(e, approval, e.plan.FailoverPolicy()),
	}
	e.guard = guard
	factory := e.plan.AgentFactory()
	return factory.BuildAgent(ctx, build)
}

// sealHandlerTools appends a durable runtime.Tool entry to e.snapshot.Tools
// for every tool sealed into this run's plan for a host agent-handler (see
// HandlerToolSpec/discoverHandlerTools), so it is resolved by
// prepareToolCalls/resolveToolCall and goes through the full durable
// claim/permission/execute/settle pipeline via adkTool, exactly like any
// composition-registered tool. Its Executor (handlerToolExecutor) dispatches
// to the live tool instance e.buildAgentHandlers collected into
// e.handlerTools. A write-like tool (see isWriteLikeToolName) is sealed
// with a Permissions tag so the existing permission policy gates it --
// disabled unless a permission explicitly grants it, exactly like any other
// state-changing tool.
func (e *adkEngine) sealHandlerTools() {
	root := e.snapshot.Config.Metadata["workspace_root"]
	workspaceID := e.snapshot.Config.Metadata["workspace_id"]
	for _, entry := range e.plan.AgentHandlers() {
		for _, toolSpec := range entry.Tools {
			scope := ToolScope{WorkspaceID: workspaceID, Root: root}
			if toolSpec.WriteLike {
				scope.Permissions = []string{handlerToolPermission(entry.ID, toolSpec.Name)}
			}
			e.snapshot.Tools = append(e.snapshot.Tools, Tool{
				Name: toolSpec.Name, Info: toolSpec.Info, Scope: scope,
				Executor: handlerToolExecutor{engine: e, name: toolSpec.Name},
				// RetentionPolicy{}'s zero value is MaxInlineBytes: 0 (retain
				// nothing inline -- effectiveToolRetentionPolicy only clamps
				// a negative/over-budget value, never raises a zero one), so
				// an explicit -1 (unbounded, subject to the same
				// content-block budget clamp every other tool's output
				// gets) is required here, unlike a composition-registered
				// tool whose host is expected to set tools.Definition.Retention
				// explicitly.
				Retention: RetentionPolicy{MaxInlineBytes: -1},
			})
		}
	}
}

// buildAgentHandlers resolves this run's plan-ordered, host-registered
// typed-ADK middleware factories (composition.Registrar.Handler) into
// concrete instances for this turn. Each factory receives a
// HandlerBuildContext bounded to this turn: model, a durable tool wrapper
// (durableToolName is derived from the underlying tool's own advertised
// name -- see adkGenericDurableTool), and a read-only workspace view rooted
// at the admitted canonical workspace (nil when none is configured, so a
// recipe requiring one fails construction closed rather than operating
// unscoped -- see workspaceFilesystemBackend/workspaceSkillBackend).
func (e *adkEngine) buildAgentHandlers(ctx context.Context, inner einomodel.AgenticModel, authorized *authorizedRewriteSet) ([]adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
	plan := e.plan.AgentHandlers()
	if len(plan) == 0 {
		return nil, nil
	}
	build := HandlerBuildContext{
		SessionID:        e.snapshot.SessionID,
		RunID:            e.snapshot.RunID,
		WorkspaceRoot:    e.snapshot.Config.Metadata["workspace_root"],
		Model:            inner,
		authorizeRewrite: authorized.record,
		epochs: contextEpochCapability{
			sessionID: e.snapshot.SessionID, runID: e.snapshot.RunID,
			store: e.host.store, execution: e.execution.store, ids: e.host.ids, now: e.host.now,
		},
	}
	// summaryModel is the bounded internal-dispatch adapter handed to ONLY
	// the summarization recipe below (never the turn's own inner adapter):
	// every physical call it makes is still durably ledgered (its own
	// ModelRequestRecord row, agent path "summarizer", usage charged,
	// retried/failed over through the same audited path) but it can never
	// claim the turn's assistant placeholder, persist assistant content, or
	// create a tool call -- see adkModel.internalDispatch and commitInternal.
	summaryModel := &adkModel{host: e.host, execution: e.execution, engine: e, internalDispatch: "summarizer"}
	for _, t := range e.snapshot.Tools {
		if !t.Deferred {
			continue
		}
		build.DeferredTools = append(build.DeferredTools, &adkTool{engine: e, tool: t})
	}
	if build.WorkspaceRoot != "" {
		fsBackend, skillBackend, err := newWorkspaceBackends(build.WorkspaceRoot)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err)
		}
		build.FilesystemBackend = fsBackend
		build.SkillBackend = skillBackend
		planTaskBackend, err := newWritableWorkspaceBackend(build.WorkspaceRoot, filepath.Join(".eino-agent", "plantask"))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err)
		}
		build.PlanTaskBackend = planTaskBackend
		reductionBackend, err := newWritableWorkspaceBackend(build.WorkspaceRoot, filepath.Join(".eino-agent", "reduction"))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err)
		}
		build.ReductionBackend = reductionBackend
	}
	handlers := make([]adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], 0, len(plan))
	liveTools := make(map[string]tool.BaseTool)
	for _, entry := range plan {
		entryBuild := build
		entryBuild.HandlerID = entry.ID
		if entry.Kind == HandlerKindSummarization {
			// summarization's own internal summary-generation call must
			// never route through the turn's real adapter (inner): that
			// would let it claim the turn's assistant placeholder and
			// persist the summary as if the assistant said it to the user
			// -- see adkModel.internalDispatch's doc comment.
			entryBuild.Model = summaryModel
		}
		handler, err := entry.Factory(ctx, entryBuild)
		if err != nil {
			return nil, fmt.Errorf("build agent handler %q (%s): %w", entry.ID, entry.Kind, err)
		}
		if handler == nil {
			return nil, fmt.Errorf("agent handler %q (%s) factory returned nil", entry.ID, entry.Kind)
		}
		handlers = append(handlers, handler)
		// Collect this handler's live tool instances (real backends, real
		// session/workspace identity) for sealHandlerTools' executors to
		// dispatch to -- see handlerToolExecutor. This BeforeAgent call is
		// purely observational (its returned context/runCtx are discarded);
		// the SAME middleware instance's real BeforeAgent runs again later
		// as part of actual agent execution (registered in Handlers below),
		// which is idempotent for every one of this package's own recipes
		// (each just appends a static tool list).
		_, runCtx, err := handler.BeforeAgent(ctx, &adk.ChatModelAgentContext{})
		if err != nil {
			return nil, fmt.Errorf("agent handler %q (%s) tool discovery: %w", entry.ID, entry.Kind, err)
		}
		if runCtx == nil {
			continue
		}
		for _, t := range runCtx.Tools {
			if t == nil {
				continue
			}
			info, infoErr := t.Info(ctx)
			if infoErr != nil || info == nil || info.Name == "" {
				continue
			}
			liveTools[info.Name] = t
		}
	}
	e.handlerTools = liveTools
	return handlers, nil
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
	// SettlementSeal is the mandatory final tool/model-content protection
	// handler (see settlementSeal's doc comment); an AgentFactory must
	// install it last, after Guard, in its Handlers list.
	SettlementSeal adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
	// DurableBaseline is the mandatory FIRST (outermost) handler
	// (durableBaselineHandler): its BeforeModelRewriteState rewrites
	// state.Messages to the durable projection of committed history for
	// this cycle, which every host handler then transforms on top of --
	// see its doc comment (runtime/adk_middleware.go) and
	// adkEngine.buildDurableBaseline (runtime/adk_model.go). An
	// AgentFactory must install it before Handlers.
	DurableBaseline adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
	// Handlers is the ordered, per-execution snapshot of every
	// composition.Registrar.Handler factory output for this run's frozen
	// plan (see RunPlan.AgentHandlers), built fresh for this turn by
	// adkEngine.buildAgent. An AgentFactory that installs its own Handlers
	// must place these after DurableBaseline and before Guard/
	// SettlementSeal, so host handlers are the outermost *wrapper* layer
	// (ADK: first registered is outermost, for WrapModel/WrapToolCall) and
	// this runtime's own tail handlers (durableGuard, settlementSeal) stay
	// innermost, directly around the mandatory Model/Tools adapters --
	// while, for hook methods (BeforeAgent/BeforeModelRewriteState/...),
	// which run in the SAME registration order but as an ordered chain
	// rather than a wrapper nest, DurableBaseline's placement first means
	// its BeforeModelRewriteState establishes the baseline every host
	// handler's own BeforeModelRewriteState then transforms.
	// DefaultChatModelAgentFactory does this automatically.
	Handlers []adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
}

// AgentFactory builds the typed ADK agent for one admitted turn from a
// bounded AgentBuildContext. It is frozen in RunPlanSpec.Agent (see
// runtime/extension_plan.go) so a resumed run rebuilds an equivalent agent
// graph deterministically.
//
// Every compliant agent must route at least one physical model call through
// build.Model per turn: onAgentEvents (runtime/turn_loop.go) fails a
// normally-completed turn whose adkEngine recorded zero durable model
// dispatches, on the theory that a turn producing no ledgered provider call
// means the agent's real execution routed its call through some model the
// host never handed out (the "rogue model" case durableGuard cannot see,
// since it only inspects the tool list). This is a real constraint, not
// just a defensive check: a factory that legitimately never dispatches on
// some turns (e.g. answering purely from tools or a cached/transferred
// result, with no physical model call at all) will fail those turns under
// this check today (see the W5 section of docs/architecture/eino-feature-support.md
// for the exact scope).
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
	// Handler order: first registered is outermost for wrappers (ADK:
	// [A,B,C] wraps as A(B(C(model/tool)))), and first registered is first
	// called for hooks (BeforeAgent/BeforeModelRewriteState/...) -- see
	// AgentBuildContext.Handlers's doc comment. DurableBaseline goes first
	// so its BeforeModelRewriteState establishes the baseline every host
	// handler transforms; host handlers (build.Handlers) go next so they
	// are the outermost wrapper layer; this runtime's own mandatory tail
	// handlers go last so they stay innermost, directly around the
	// mandatory Model/Tools adapters this AgentBuildContext provided -- see
	// durableGuard/settlementSeal's doc comments.
	if build.DurableBaseline != nil {
		cfg.Handlers = append(cfg.Handlers, build.DurableBaseline)
	}
	cfg.Handlers = append(cfg.Handlers, build.Handlers...)
	if build.Guard != nil {
		cfg.Handlers = append(cfg.Handlers, build.Guard)
	}
	if build.SettlementSeal != nil {
		cfg.Handlers = append(cfg.Handlers, build.SettlementSeal)
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

// BeforeAgent enforces the frozen tool universe and, as a side effect,
// deduplicates a host handler's own redundant raw tool copy: a sealed
// handler tool (see adkEngine.sealHandlerTools) is pre-seeded into
// runCtx.Tools as a durable adkTool-wrapped entry (from
// AgentBuildContext.Tools/ToolsConfig.Tools) before any handler's
// BeforeAgent ever runs; when that SAME handler's own real BeforeAgent runs
// later (registered as an ordinary Handler, for its other behaviors) it
// appends its own fresh, non-durable tool object with the same name --
// which this guard drops rather than rejects, since the sealed durable
// entry already present is what actually dispatches. Any other non-durable
// tool, or a durable tool naming an identity outside g.allowed, fails the
// turn as a construction error: the frozen tool universe never expands at
// runtime.
func (g *durableGuard) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	g.mu.Lock()
	g.ran = true
	g.mu.Unlock()
	filtered := make([]tool.BaseTool, 0, len(runCtx.Tools))
	seen := make(map[string]bool, len(runCtx.Tools))
	for _, t := range runCtx.Tools {
		if dt, ok := t.(durableTool); ok {
			name := dt.durableToolName()
			if !g.allowed[name] {
				return ctx, nil, fmt.Errorf("%w: tool %q is outside the frozen registry", errADKUnsupportedBlock, name)
			}
			if seen[name] {
				continue
			}
			seen[name] = true
			filtered = append(filtered, t)
			continue
		}
		info, _ := t.Info(ctx)
		name := "<unknown>"
		if info != nil {
			name = info.Name
		}
		if g.allowed[name] {
			// The redundant raw copy of an already-sealed handler tool:
			// the durable entry with this name is already in filtered (or
			// appears elsewhere in runCtx.Tools and will be added when
			// this loop reaches it) -- drop this one.
			continue
		}
		return ctx, nil, fmt.Errorf("%w: tool %q is not a durable runtime adapter", errADKUnsupportedBlock, name)
	}
	runCtx.Tools = filtered
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
