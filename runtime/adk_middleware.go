package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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
// is scoped to the current run: Model is a durably ledgered adapter -- for
// every recipe except this package's own summarization it is the exact same
// adapter AgentBuildContext.Model carries; summarization instead receives a
// bounded internal-dispatch adapter (see adkEngine.buildAgentHandlers and
// adkModel.internalDispatch) so its own summary-generation call is still
// fully audited but can never claim the turn's assistant placeholder or
// persist as conversational content. FilesystemBackend/SkillBackend are
// read-only views rooted at the admitted canonical workspace (nil when no
// workspace root is configured for this run, so a recipe that requires one
// fails construction closed rather than silently operating unscoped). A tool
// the middleware built here contributes (e.g. filesystem's
// ls/read_file/write_file tools) is never dispatched directly: it is
// discovered once at plan-compile time (discoverHandlerTools) and sealed
// into the frozen tool universe, and a durable runtime.Tool built from that
// sealed identity is what actually dispatches to it (see
// adkEngine.sealHandlerTools/handlerToolExecutor).
//
// HandlerBuildContext deliberately exposes NO durable store authority:
// there is no Store, ExecutionStore, ID minting, or clock field on this
// type. Every registered HandlerFactory -- a third-party host's own
// included -- receives the identical value, so any such field would let an
// arbitrary host handler fabricate a durable ToolCall settlement or read
// checkpoint/provider-private state (see the W6 round-1 review's C1
// finding). The one recipe in this package that legitimately needs a
// bounded durable write (summarization, mapping a completed summary into a
// session.ContextEpoch) is given a narrow, unexported capability instead
// (see contextEpochCapability and the unexported epochs field below), the
// same isolation mechanism authorizeRewrite already uses.
type HandlerBuildContext struct {
	SessionID     session.ID
	RunID         session.RunID
	WorkspaceRoot string
	Model         einomodel.AgenticModel
	// FilesystemBackend and SkillBackend are read-only views rooted at the
	// admitted canonical workspace.
	FilesystemBackend adkfilesystem.Backend
	SkillBackend      skill.Backend
	// PlanTaskBackend and ReductionBackend are writable, per-session scratch
	// backends (not part of the read-only content view above): a real turn
	// gets one rooted at its own subdirectory under the admitted workspace,
	// scoped to that one session's recipe state, never the workspace's real
	// content (writableWorkspaceBackend); compile-time tool discovery
	// (discoverHandlerTools) gets an in-memory stand-in instead, so probing
	// never touches the filesystem -- see writableTaskBackend.
	PlanTaskBackend  writableTaskBackend
	ReductionBackend writableTaskBackend
	// DeferredTools are this turn's frozen, already-durably-wrapped deferred
	// tools (session composition tools registered with Deferred: true),
	// ready to hand to dynamictool/toolsearch.Config.DynamicTools.
	DeferredTools []tool.BaseTool

	// HandlerID is this handler's own composition-level registration ID
	// (composition.HandlerRegistration.ID) -- plain identity metadata, not a
	// capability. It is used only to tag durable audit records (see
	// authorizedRewriteRecord) with which sealed handler instance made a
	// content rewrite.
	HandlerID string

	// authorizeRewrite records a function_tool_result content change as
	// sanctioned content management (see authorizedRewriteSet), so
	// settlementSeal/verifySettledToolResults treat it as a deliberate
	// rewrite instead of an unauthorized one, and queues it for durable
	// audit (handler ID, call ID, kind, before/after digest -- drained by
	// adkModel.begin). It is unexported: only this package's own
	// content-management recipes (patchtoolcalls, reduction) are wrapped
	// with it (see wrapAuthorizedContentRewrites); an arbitrary
	// host-registered HandlerFactory is never given this authority.
	authorizeRewrite func(handlerID, kind, callID, beforeDigest, afterDigest string)

	// epochs is the narrow, unexported durable capability this package's OWN
	// summarization recipe uses (see contextEpochCapability's doc comment).
	// It intentionally never appears as an exported field: HandlerBuildContext
	// is handed to every registered HandlerFactory, host-provided ones
	// included, and none of them may reach session.Store or
	// session.ExecutionStore through it -- doing so would let a host factory
	// fabricate a durable ToolCall settlement or read checkpoint/provider-
	// private state (see this type's doc comment and the W6 round-1 review's
	// C1 finding). A handler factory that needs bounded durable reads/writes
	// beyond this must be one of this package's own recipes, given its own
	// narrow, purpose-built capability the same way.
	epochs contextEpochCapability
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
	// This is the mandatory FIRST handler, so its BeforeModelRewriteState is
	// the start of a fresh ReAct cycle: clear any authorization a
	// content-management recipe recorded last cycle before that recipe's
	// own BeforeModelRewriteState (registered after this one) runs again
	// and, if it still wants the same rewrite, re-authorizes it fresh -- see
	// authorizedRewriteSet.resetCycle's doc comment.
	h.engine.authorizedRewrites.resetCycle()
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

// authorizedRewriteRecord is one durably-auditable authorized content
// rewrite: a sanctioned content-management recipe (patchtoolcalls,
// reduction) changed a function_tool_result's content for one call ID in
// its own BeforeModelRewriteState. adkModel.begin drains these each cycle
// and durably records them as session.AuthorizedToolResultRewriteEventKind
// events (see adk_model.go), correlated to the dispatch that carries the
// rewrite.
type authorizedRewriteRecord struct {
	HandlerID    string
	Kind         string
	CallID       string
	BeforeDigest string
	AfterDigest  string
}

// authorizedRewriteSet is the per-cycle record of function_tool_result call
// IDs a sanctioned content-management recipe (patchtoolcalls, reduction --
// see wrapAuthorizedContentRewrites) deliberately rewrote in
// BeforeModelRewriteState, keyed to the EXACT post-rewrite content digest it
// produced. settlementSeal/verifySettledToolResults consult it to
// distinguish an authorized content-management rewrite (accepted only when
// the dispatched content's digest is byte-for-byte the one the sanctioned
// recipe itself produced) from an unauthorized one (any other handler
// substituting different content for the same call ID). digests is reset at
// the start of every ReAct cycle (durableBaselineHandler.BeforeModelRewriteState,
// the mandatory first handler) so an authorization never outlives the cycle
// that produced it -- a recipe that legitimately still wants the same
// rewrite next cycle re-authorizes it fresh, since its own
// BeforeModelRewriteState re-runs (and re-diffs) every cycle too.
type authorizedRewriteSet struct {
	mu      sync.Mutex
	digests map[string]string
	pending []authorizedRewriteRecord
}

func newAuthorizedRewriteSet() *authorizedRewriteSet {
	return &authorizedRewriteSet{digests: make(map[string]string)}
}

// record authorizes callID's current content (identified by afterDigest) for
// the remainder of this cycle and queues a durable audit entry (drained by
// adkModel.begin -- see drainPending).
func (s *authorizedRewriteSet) record(handlerID, kind, callID, beforeDigest, afterDigest string) {
	if s == nil || callID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.digests[callID] = afterDigest
	s.pending = append(s.pending, authorizedRewriteRecord{HandlerID: handlerID, Kind: kind, CallID: callID, BeforeDigest: beforeDigest, AfterDigest: afterDigest})
}

// authorizedDigest reports the exact content digest authorized for callID
// this cycle, if any.
func (s *authorizedRewriteSet) authorizedDigest(callID string) (string, bool) {
	if s == nil {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	digest, ok := s.digests[callID]
	return digest, ok
}

// resetCycle clears every authorization: called once per ReAct cycle by
// durableBaselineHandler.BeforeModelRewriteState, before any host handler
// (including the content-management recipes that populate this set) runs
// for that cycle.
func (s *authorizedRewriteSet) resetCycle() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.digests = make(map[string]string)
}

// drainPending returns and clears every authorization queued for durable
// audit since the last drain -- see adkModel.begin.
func (s *authorizedRewriteSet) drainPending() []authorizedRewriteRecord {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.pending
	s.pending = nil
	return out
}

// wrapAuthorizedContentRewrites decorates a sanctioned content-management
// middleware (this package's own patchtoolcalls/reduction recipes only --
// never an arbitrary host-registered handler) so that every
// function_tool_result content change it makes in its own
// BeforeModelRewriteState is recorded into authorize (handlerID/kind
// identify which sealed handler instance made the change, for the durable
// audit trail -- see authorizedRewriteRecord), which settlementSeal/
// verifySettledToolResults consult. It diffs state.Messages'
// function_tool_result content before and after delegating to inner, by
// call ID, so it works regardless of inner's own internal mechanism
// (patchtoolcalls' PatchedToolResultGenerator callback, reduction's
// Trunc/ClearHandler-driven rewriting, or any other upstream implementation
// detail).
func wrapAuthorizedContentRewrites(inner adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], handlerID, kind string, authorize func(handlerID, kindArg, callID, beforeDigest, afterDigest string)) adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage] {
	if inner == nil || authorize == nil {
		return inner
	}
	return &authorizedRewriteMiddleware{TypedChatModelAgentMiddleware: inner, handlerID: handlerID, kind: kind, authorize: authorize}
}

type authorizedRewriteMiddleware struct {
	adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
	handlerID string
	kind      string
	authorize func(handlerID, kind, callID, beforeDigest, afterDigest string)
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
			m.authorize(m.handlerID, m.kind, callID, before[callID], content)
		}
	}
	return ctx, next, nil
}

func snapshotToolResultContent(state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage]) map[string]string {
	if state == nil {
		return map[string]string{}
	}
	occurrences := toolResultOccurrencesByCallID(state.Messages)
	// wrapAuthorizedContentRewrites diffs by call ID assuming exactly one
	// occurrence per call (the recipes it wraps -- patchtoolcalls,
	// reduction -- only ever rewrite the single settled/patched occurrence
	// for a call ID); use the last occurrence's digest, matching the
	// pre-hardening map behavior for this diff-only use.
	result := make(map[string]string, len(occurrences))
	for callID, digests := range occurrences {
		if len(digests) == 0 {
			continue
		}
		result[callID] = digests[len(digests)-1]
	}
	return result
}

// toolResultOccurrencesByCallID returns, for every function_tool_result
// content block found across messages, the ordered list of canonical
// content digests for that call ID -- every occurrence, not just the last:
// a handler inserting a second, divergent function_tool_result block for an
// already-settled call ID (ahead of the genuine one) must not be hidden
// behind a map that only keeps the last write (see verifySettledToolResults).
func toolResultOccurrencesByCallID(messages []*einoschema.AgenticMessage) map[string][]string {
	result := make(map[string][]string)
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
			result[callID] = append(result[callID], canonicalFunctionToolResultContent(block.FunctionToolResult.Content))
		}
	}
	return result
}

// canonicalFunctionToolResultContent returns a sha256 digest of content's
// full canonical JSON encoding -- every content block's Type AND every
// media field (Image/Audio/Video/File, not just Text), so swapping a
// settled multimodal result's media bytes or URL is detected exactly like
// swapping its text would be. Extra is cleared before marshalling: it is
// model-specific/custom metadata, not part of the durably settled content
// this comparison protects.
func canonicalFunctionToolResultContent(content []*einoschema.FunctionToolResultContentBlock) string {
	cleaned := make([]*einoschema.FunctionToolResultContentBlock, 0, len(content))
	for _, part := range content {
		if part == nil {
			continue
		}
		cp := *part
		cp.Extra = nil
		cleaned = append(cleaned, &cp)
	}
	raw, err := json.Marshal(cleaned)
	if err != nil {
		// An unencodable block must never be treated as "matches nothing
		// meaningfully" in a way that could coincide with another
		// unencodable block's digest; the sentinel prefix is not a valid
		// JSON document byte sequence so it can never collide with a real
		// marshaled digest's preimage.
		raw = []byte("\x00unencodable:" + err.Error())
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// verifySettledToolResults is the mandatory-dispatch-authority enforcement
// of settlementSeal's two invariants (see that type's doc comment),
// extracted so it can be called from adkModel.prepareDispatchInput -- the
// innermost dispatch authority, which every physical attempt passes
// through regardless of how many handlers wrap WrapModel around it,
// including a host WrapModel installed after (outside) settlementSeal's own
// BeforeModelRewriteState hook (hooks and the WrapModel wrapper chain are
// two independent mechanisms in ADK; only the innermost point after both
// have run is authoritative -- see settlementSeal.BeforeModelRewriteState's
// doc comment for why that hook is kept too, as an early, non-authoritative
// check).
//
// For every function_tool_result occurrence in input (by call ID, every
// occurrence, not just one per call ID): it must either byte-for-byte match
// one of that call ID's occurrences in baseline (the fresh durable
// projection this cycle started from), or match the exact digest this
// cycle's authorized set recorded for that call ID. A call ID with no
// baseline occurrence at all is rejected unless every one of its
// occurrences matches the authorized digest (patchtoolcalls' legitimate
// patch of a dangling, unsettled call).
func verifySettledToolResults(baseline, input []*einoschema.AgenticMessage, authorized *authorizedRewriteSet) error {
	baselineOccurrences := toolResultOccurrencesByCallID(baseline)
	currentOccurrences := toolResultOccurrencesByCallID(input)
	for callID, occurrences := range currentOccurrences {
		baseDigests := baselineOccurrences[callID]
		authorizedDigest, isAuthorized := authorized.authorizedDigest(callID)
		for _, digest := range occurrences {
			if digest == authorizedDigest && isAuthorized {
				continue
			}
			matchesBaseline := false
			for _, base := range baseDigests {
				if digest == base {
					matchesBaseline = true
					break
				}
			}
			if matchesBaseline {
				continue
			}
			if len(baseDigests) == 0 {
				return fmt.Errorf("%w: call %s", errUnauthorizedFabricatedToolResult, callID)
			}
			return fmt.Errorf("%w: call %s", errSettledToolResultDiverged, callID)
		}
	}
	return nil
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
// handler's own BeforeModelRewriteState has already run.
//
// This hook is NOT the authority: a host handler's WrapModel wrapper (a
// SEPARATE mechanism from the BeforeModelRewriteState hook chain -- see ADK's
// adk/wrappers.go, wrapModel nests every handler's WrapModel result around
// the agent's Model AFTER every handler's hooks have already run) can still
// rewrite state.Messages between this hook returning and the physical
// dispatch. The real authority is verifySettledToolResults, called from
// adkModel.prepareDispatchInput -- the innermost dispatch authority every
// physical attempt passes through regardless of any WrapModel nesting (see
// that function's doc comment). This hook is kept only as an early,
// non-authoritative check: it fails a turn sooner, before a wasted physical
// dispatch, for the common case where no WrapModel rewrite happens after it.
//
// It enforces exactly two invariants, regardless of what a content-management
// handler otherwise does to the baseline (see durableBaselineHandler):
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

// BeforeModelRewriteState is the early, non-authoritative check described in
// this type's doc comment: it delegates to the exact same
// verifySettledToolResults adkModel.prepareDispatchInput uses as the real
// authority, compared against durableBaselineHandler's own reconstruction
// (e.engine.baselineMessages, computed this same cycle, before any host
// handler ran).
func (s *settlementSeal) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	if state == nil {
		return ctx, state, nil
	}
	if err := verifySettledToolResults(s.engine.baselineMessages, state.Messages, s.authorized); err != nil {
		return ctx, nil, err
	}
	return ctx, state, nil
}
