package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/eino/adk"
	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/skill"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// HandlerBuildContext is the bounded construction context a HandlerFactory
// (a composition.Registrar.Handler registration) receives when this
// package's engine snapshots it for one turn's agent build. Every value here
// is scoped to the current run: Model is the same mandatory ledger-audited
// adapter AgentBuildContext.Model carries (so any model call a handler's
// middleware makes -- e.g. summarization's own summary-generation call -- is
// still durably ledgered through the same audit boundary), and
// FilesystemBackend/SkillBackend are read-only views rooted at the admitted
// canonical workspace (nil when no workspace root is configured for this
// run, so a recipe that requires one fails construction closed rather than
// silently operating unscoped). A tool the middleware built here contributes
// (e.g. filesystem's ls/read_file/write_file tools) is never dispatched
// directly: it is discovered once at plan-compile time
// (discoverHandlerTools) and sealed into the frozen tool universe, and a
// durable runtime.Tool built from that sealed identity is what actually
// dispatches to it (see adkEngine.sealHandlerTools/handlerToolExecutor).
type HandlerBuildContext struct {
	SessionID     session.ID
	RunID         session.RunID
	WorkspaceRoot string
	Model         einomodel.AgenticModel
	// FilesystemBackend and SkillBackend are read-only views rooted at the
	// admitted canonical workspace.
	FilesystemBackend adkfilesystem.Backend
	SkillBackend      skill.Backend
	// PlanTaskBackend and ReductionBackend are writable, workspace-private
	// scratch backends (not part of the read-only content view above): each
	// is rooted at its own subdirectory under the admitted workspace,
	// scoped to that one recipe's own state, never the workspace's real
	// content.
	PlanTaskBackend  *writableWorkspaceBackend
	ReductionBackend *writableWorkspaceBackend
	// DeferredTools are this turn's frozen, already-durably-wrapped deferred
	// tools (session composition tools registered with Deferred: true),
	// ready to hand to dynamictool/toolsearch.Config.DynamicTools.
	DeferredTools []tool.BaseTool
	// Store, RunID, IDs and Now let a handler factory perform its own
	// bounded durable reads/writes (e.g. summarization's Finalize mapping a
	// completed summary into a session.ContextEpoch via
	// compaction.AppendBoundary). Store is the same durable store this run's
	// engine uses; IDs mints canonical identities the same way the engine
	// does.
	Store session.Store
	// Execution is the current run's fenced ExecutionStore -- the same
	// mutation capability this run's own engine writes through -- for a
	// handler factory that needs to durably write (e.g. summarization's
	// Finalize appending a compaction boundary).
	Execution session.ExecutionStore
	IDs       IDGenerator
	Now       func() time.Time

	// authorizeRewrite records a function_tool_result content change as
	// sanctioned content management (see authorizedRewriteSet), so
	// settlementSeal treats it as a deliberate rewrite instead of an
	// unauthorized one. It is unexported: only this package's own
	// content-management recipes (patchtoolcalls, reduction) are wrapped
	// with it (see wrapAuthorizedContentRewrites); an arbitrary
	// host-registered HandlerFactory is never given this authority.
	authorizeRewrite func(callID string)
}

// HandlerFactory builds one typed ADK agent middleware instance for one
// turn's agent from a bounded HandlerBuildContext. Registered via
// composition.Registrar.Handler(HandlerRegistration{Factory: ...}); the
// closure is never serialized or part of the sealed plan fingerprint (only
// its HandlerDescriptor{Kind,Version,Config} identity, and its discovered
// HandlerToolSpec set, are -- see session.AgentHandlerPlanIdentity).
// AgentFactory.BuildAgent's caller (adkEngine.buildAgent) invokes every
// plan-ordered factory fresh for each admitted turn and installs the
// results into AgentBuildContext.Handlers, ahead of the runtime's mandatory
// tail handlers (durableGuard, settlementSeal).
type HandlerFactory func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error)

// durableBaselineHandler is this runtime's mandatory FIRST (outermost)
// handler: its BeforeModelRewriteState rewrites state.Messages to the fresh
// durable projection of this cycle's committed history
// (adkEngine.buildDurableBaseline), which every host handler registered
// after it then transforms on top of. Host content-injecting/content-
// management handlers (agentsmd, skill, reduction, summarization,
// patchtoolcalls) therefore operate on real durable history, not a value
// the ledger adapter discards and replaces later -- see
// adkModel.prepareDispatchInput, which dispatches exactly what the handler
// chain leaves ADK's state.Messages as, without re-projecting.
type durableBaselineHandler struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
	engine *adkEngine
}

func newDurableBaselineHandler(engine *adkEngine) *durableBaselineHandler {
	return &durableBaselineHandler{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}, engine: engine}
}

func (h *durableBaselineHandler) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	baseline, err := h.engine.buildDurableBaseline(ctx)
	if err != nil {
		return ctx, nil, err
	}
	// state.Messages must be a deep clone of the engine's own baseline, not
	// the same message/block pointers: a host handler mutating content in
	// place (rather than replacing it with a new value) would otherwise
	// silently corrupt h.engine.baselineMessages too, since Go slices and
	// the pointers they hold are shared, not copied -- which would corrupt
	// settlementSeal's own comparison basis (it reads
	// h.engine.baselineMessages expecting it to still be exactly what this
	// cycle's durable reload produced, before any handler transformed it).
	cloned, err := cloneProtectedMessages(baseline)
	if err != nil {
		return ctx, nil, err
	}
	next := adk.TypedChatModelAgentState[*einoschema.AgenticMessage]{Messages: cloned}
	if state != nil {
		next.ToolInfos = state.ToolInfos
		next.DeferredToolInfos = state.DeferredToolInfos
	}
	return ctx, &next, nil
}

// authorizedRewriteSet is the per-turn record of function_tool_result call
// IDs a sanctioned content-management recipe (patchtoolcalls, reduction --
// see wrapAuthorizedContentRewrites) deliberately rewrote in
// BeforeModelRewriteState. settlementSeal consults it to distinguish an
// authorized content-management rewrite from an unauthorized one.
type authorizedRewriteSet struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newAuthorizedRewriteSet() *authorizedRewriteSet {
	return &authorizedRewriteSet{seen: make(map[string]bool)}
}

func (s *authorizedRewriteSet) add(callID string) {
	if s == nil || callID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[callID] = true
}

func (s *authorizedRewriteSet) contains(callID string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[callID]
}

// wrapAuthorizedContentRewrites decorates a sanctioned content-management
// middleware (this package's own patchtoolcalls/reduction recipes only --
// never an arbitrary host-registered handler) so that every
// function_tool_result content change it makes in its own
// BeforeModelRewriteState is recorded into authorize, which settlementSeal
// consults. It diffs state.Messages' function_tool_result content before
// and after delegating to inner, by call ID, so it works regardless of
// inner's own internal mechanism (patchtoolcalls' PatchedToolResultGenerator
// callback, reduction's Trunc/ClearHandler-driven rewriting, or any other
// upstream implementation detail).
func wrapAuthorizedContentRewrites(inner adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], authorize func(callID string)) adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage] {
	if inner == nil || authorize == nil {
		return inner
	}
	return &authorizedRewriteMiddleware{TypedChatModelAgentMiddleware: inner, authorize: authorize}
}

type authorizedRewriteMiddleware struct {
	adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
	authorize func(callID string)
}

func (m *authorizedRewriteMiddleware) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	before := snapshotToolResultContent(state)
	ctx, next, err := m.TypedChatModelAgentMiddleware.BeforeModelRewriteState(ctx, state, mc)
	if err != nil || next == nil {
		return ctx, next, err
	}
	after := snapshotToolResultContent(next)
	for callID, content := range after {
		if before[callID] != content {
			m.authorize(callID)
		}
	}
	return ctx, next, nil
}

func snapshotToolResultContent(state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage]) map[string]string {
	if state == nil {
		return map[string]string{}
	}
	return toolResultContentByCallID(state.Messages)
}

func toolResultContentByCallID(messages []*einoschema.AgenticMessage) map[string]string {
	result := make(map[string]string)
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			callID := block.FunctionToolResult.CallID
			if callID == "" {
				continue
			}
			result[callID] = canonicalFunctionToolResultContent(block.FunctionToolResult.Content)
		}
	}
	return result
}

func canonicalFunctionToolResultContent(content []*einoschema.FunctionToolResultContentBlock) string {
	var b []byte
	for _, part := range content {
		if part == nil {
			continue
		}
		b = append(b, []byte(part.Type)...)
		b = append(b, 0)
		if part.Text != nil {
			b = append(b, []byte(part.Text.Text)...)
		}
		b = append(b, 0)
	}
	return string(b)
}

// errSettledToolResultDiverged reports that a function_tool_result content
// block about to be sent to the model no longer matches the durably settled
// ToolCall output for that call ID -- i.e. some host handler mutated a
// settled, model-visible tool result after this runtime's own durable
// settlement already committed it, without that rewrite being authorized.
// See settlementSeal's doc comment.
var errSettledToolResultDiverged = errors.New("host handler diverged a settled tool result before it reached the model")

// errUnauthorizedFabricatedToolResult reports a function_tool_result block
// for a call ID with no durable ToolCall row that is also not recorded as
// an authorized content-management rewrite (see authorizedRewriteSet): a
// handler fabricated a tool result for a call this run never settled,
// without going through a sanctioned recipe that records the patch.
var errUnauthorizedFabricatedToolResult = errors.New("host handler fabricated an unauthorized tool result for an unsettled call")

// settlementSeal is one of this runtime's mandatory tail handlers (the
// other being durableGuard): it is appended to AgentBuildContext.Handlers/
// TypedChatModelAgentConfig.Handlers after every host-registered handler, so
// its BeforeModelRewriteState hook -- hooks run in registration order, first
// registered first called -- observes state.Messages only after every host
// handler's own BeforeModelRewriteState has already run, i.e. exactly what
// this turn is about to send to the model. It enforces exactly two
// invariants, regardless of what a content-management handler otherwise
// does to the baseline (see durableBaselineHandler):
//
//  1. every function_tool_result content block whose call ID matches a
//     durably *settled* session.ToolCall row must byte-for-byte match that
//     row's settled Output, UNLESS the rewrite was recorded as authorized
//     (reduction's own legitimate clearing/truncation of a settled result --
//     see wrapAuthorizedContentRewrites);
//  2. a function_tool_result content block whose call ID has NO durable
//     settlement is rejected as a fabrication, UNLESS it was recorded as an
//     authorized patch (patchtoolcalls' own legitimate patching of a
//     dangling call).
type settlementSeal struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
	engine     *adkEngine
	authorized *authorizedRewriteSet
}

func newSettlementSeal(engine *adkEngine, authorized *authorizedRewriteSet) *settlementSeal {
	return &settlementSeal{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}, engine: engine, authorized: authorized}
}

// BeforeModelRewriteState compares against durableBaselineHandler's own
// reconstruction (e.engine.baselineMessages, computed this same cycle,
// before any host handler ran) rather than re-deriving expected content
// from the durable ToolCall row directly: the baseline is exactly what
// replay would show for this call (a single text block for an ordinary
// adkTool result, multiple parts for an enhanced/multimodal one -- e.g. a
// sealed handler tool's multimodal read_file, or a classic W4 enhanced
// tool result), so comparing against it handles every settled shape
// uniformly without hardcoding an assumption about how many content blocks
// a settled result has.
func (s *settlementSeal) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	if state == nil {
		return ctx, state, nil
	}
	baseline := toolResultContentByCallID(s.engine.baselineMessages)
	current := toolResultContentByCallID(state.Messages)
	for callID, content := range current {
		if s.authorized.contains(callID) {
			continue
		}
		baselineContent, settled := baseline[callID]
		if !settled {
			// No durable settlement for this call ID in the fresh
			// baseline: a fabricated result is only acceptable if a
			// sanctioned recipe recorded it as an authorized patch.
			return ctx, nil, fmt.Errorf("%w: call %s", errUnauthorizedFabricatedToolResult, callID)
		}
		if content != baselineContent {
			return ctx, nil, fmt.Errorf("%w: call %s", errSettledToolResultDiverged, callID)
		}
	}
	return ctx, state, nil
}
