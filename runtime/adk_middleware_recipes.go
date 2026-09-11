package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/middlewares/agentsmd"
	"github.com/cloudwego/eino/adk/middlewares/dynamictool/toolsearch"
	filesystemmw "github.com/cloudwego/eino/adk/middlewares/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/patchtoolcalls"
	"github.com/cloudwego/eino/adk/middlewares/plantask"
	"github.com/cloudwego/eino/adk/middlewares/reduction"
	"github.com/cloudwego/eino/adk/middlewares/skill"
	"github.com/cloudwego/eino/adk/middlewares/summarization"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/compaction"
)

// This file provides tested wiring recipes for the upstream typed ADK
// middleware constructors (adk/middlewares/*), each behind a
// composition.HandlerDescriptor.Kind constant below. Every recipe:
//   - calls the upstream typed constructor directly (never reimplements its
//     parser/planner/search logic);
//   - any tool it injects into ChatModelAgentContext.Tools is discovered
//     once at plan-compile time (discoverHandlerTools) and sealed into the
//     frozen tool universe -- a durable runtime.Tool built from that sealed
//     identity is what actually dispatches to it, through the full durable
//     claim/permission/execute/settle pipeline (see
//     adkEngine.sealHandlerTools/handlerToolExecutor);
//   - patchtoolcalls/reduction, the two recipes that legitimately rewrite a
//     function_tool_result's content, are wrapped with
//     wrapAuthorizedContentRewrites so settlementSeal recognizes their
//     rewrites as sanctioned content management, not tampering;
//   - fails construction closed when a required backend/config is missing.
//
// Each NewXxxHandlerFactory returns a HandlerFactory suitable for
// composition.HandlerRegistration.Factory; XxxConfig is the bounded,
// JSON-serializable configuration a host marshals into
// composition.HandlerDescriptor.Config for that same registration (its
// canonical hash is what is actually sealed into the plan fingerprint).
const (
	HandlerKindAgentsMD       = "agentsmd"
	HandlerKindSkill          = "skill"
	HandlerKindFilesystem     = "filesystem"
	HandlerKindPlanTask       = "plantask"
	HandlerKindPatchToolCalls = "patchtoolcalls"
	HandlerKindReduction      = "reduction"
	HandlerKindSummarization  = "summarization"
	HandlerKindToolSearch     = "toolsearch"
)

// HandlerVersion1 is the config-schema version every recipe in this file
// currently declares.
const HandlerVersion1 = "1"

// errHandlerMissingBackend reports that a recipe's required host-provided
// backend (filesystem/skill read-only view, plantask/reduction scratch
// backend, or a durable store) was not available for this run -- e.g.
// because no workspace root is configured -- and so construction failed
// closed rather than operating unscoped.
var errHandlerMissingBackend = fmt.Errorf("%w: required backend unavailable", errADKUnsupportedBlock)

// --- agentsmd ---------------------------------------------------------

// AgentsMDConfig is the bounded, closed-schema configuration for the
// agentsmd recipe (Kind = HandlerKindAgentsMD).
type AgentsMDConfig struct {
	AgentsMDFiles       []string `json:"agents_md_files"`
	AllAgentsMDMaxBytes int      `json:"all_agents_md_max_bytes,omitempty"`
	PerAgentsMDMaxBytes int      `json:"per_agents_md_max_bytes,omitempty"`
}

// NewAgentsMDHandlerFactory injects Agents.md content (loaded from the
// admitted workspace's read-only filesystem view) into every model call via
// upstream agentsmd.NewTyped. Missing files/paths become construction-time
// or bounded load-time failures per upstream's own Backend.Read contract
// (os.ErrNotExist -> a load warning, any other error aborts loading).
func NewAgentsMDHandlerFactory(cfg AgentsMDConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.FilesystemBackend == nil {
			return nil, fmt.Errorf("%w: agentsmd requires a workspace filesystem backend", errHandlerMissingBackend)
		}
		if len(cfg.AgentsMDFiles) == 0 {
			return nil, fmt.Errorf("%w: agentsmd requires at least one AgentsMDFiles entry", errADKUnsupportedBlock)
		}
		mw, err := agentsmd.NewTyped[*einoschema.AgenticMessage](ctx, &agentsmd.Config{
			Backend: build.FilesystemBackend, AgentsMDFiles: cfg.AgentsMDFiles,
			AllAgentsMDMaxBytes: cfg.AllAgentsMDMaxBytes, PerAgentsMDMaxBytes: cfg.PerAgentsMDMaxBytes,
		})
		if err != nil {
			return nil, err
		}
		return mw, nil
	}
}

// --- skill --------------------------------------------------------------

// SkillConfig is the bounded, closed-schema configuration for the skill
// recipe (Kind = HandlerKindSkill).
type SkillConfig struct {
	SkillToolName string `json:"skill_tool_name,omitempty"`
}

// NewSkillHandlerFactory loads skills from the admitted workspace's
// read-only skill view via upstream skill.NewTyped. Bounded scope: this
// recipe does not wire AgentHub/ModelHub, so only inline-mode skills (no
// "context: fork"/"fork_with_context" frontmatter) are supported -- a skill
// requiring a fork fails with upstream's own "AgentHub required" error at
// invocation time, not silently.
//
// Every activation (the skill tool's own Backend.Get call, at the moment a
// skill is actually loaded for use -- see activationRecordingSkillBackend)
// is durably recorded as a SkillActivatedEventKind event: the skill's name
// and a content digest of what was actually loaded. This is a durable
// audit trail, not yet a resume-time enforcement: nothing today re-checks
// on resume that Get(name) still hashes to the recorded digest, so a
// SKILL.md edited between pause and resume is not detected and rejected --
// see docs/architecture/eino-feature-support.md's W6 section for this open
// limitation.
func NewSkillHandlerFactory(cfg SkillConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.SkillBackend == nil {
			return nil, fmt.Errorf("%w: skill requires a workspace skill backend", errHandlerMissingBackend)
		}
		backend := build.SkillBackend
		if build.epochs.ready() {
			backend = &activationRecordingSkillBackend{inner: backend, epochs: build.epochs}
		}
		skillCfg := &skill.TypedConfig[*einoschema.AgenticMessage]{Backend: backend}
		if cfg.SkillToolName != "" {
			name := cfg.SkillToolName
			skillCfg.SkillToolName = &name
		}
		mw, err := skill.NewTyped[*einoschema.AgenticMessage](ctx, skillCfg)
		if err != nil {
			return nil, err
		}
		return mw, nil
	}
}

// activationRecordingSkillBackend wraps a skill.Backend so every successful
// Get -- upstream's own skill tool calls Backend.Get(ctx, args.Skill)
// exactly at activation time, not at List-time rendering -- durably records
// {name, sha256(FrontMatter JSON + Content)} via the epochs capability
// before returning the skill to upstream.
type activationRecordingSkillBackend struct {
	inner  skill.Backend
	epochs contextEpochCapability
}

var _ skill.Backend = (*activationRecordingSkillBackend)(nil)

func (b *activationRecordingSkillBackend) List(ctx context.Context) ([]skill.FrontMatter, error) {
	return b.inner.List(ctx)
}

func (b *activationRecordingSkillBackend) Get(ctx context.Context, name string) (skill.Skill, error) {
	loaded, err := b.inner.Get(ctx, name)
	if err != nil {
		return skill.Skill{}, err
	}
	digest := skillContentDigest(loaded)
	if err := b.epochs.recordSkillActivation(ctx, name, digest); err != nil {
		return skill.Skill{}, err
	}
	return loaded, nil
}

func skillContentDigest(loaded skill.Skill) string {
	front, err := json.Marshal(loaded.FrontMatter)
	if err != nil {
		front = nil
	}
	sum := sha256.Sum256(append(front, []byte(loaded.Content)...))
	return hex.EncodeToString(sum[:])
}

// --- filesystem -----------------------------------------------------------

// FilesystemConfig is the bounded, closed-schema configuration for the
// filesystem recipe (Kind = HandlerKindFilesystem).
type FilesystemConfig struct {
	// UseMultiModalRead enables the multimodal read_file tool; MultiModalRead
	// results become media result parts (never text-flattened) through
	// workspaceFilesystemBackend's MultiModalReader implementation.
	UseMultiModalRead bool `json:"use_multi_modal_read,omitempty"`
}

// NewFilesystemHandlerFactory provides ls/read_file/write_file/edit_file/
// glob/grep tools over the admitted workspace's read-only view via upstream
// filesystem.NewTyped. Shell/StreamingShell are never wired (left nil): this
// runtime grants no generic shell authority through a middleware, matching
// "Runtime does not grant a generic shell or wider filesystem authority just
// because a middleware can use it" -- write_file/edit_file are present as
// tools (upstream's own API surface) but every call against them fails
// closed with errWorkspaceReadOnly, since the backend itself is read-only.
func NewFilesystemHandlerFactory(cfg FilesystemConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.FilesystemBackend == nil {
			return nil, fmt.Errorf("%w: filesystem requires a workspace filesystem backend", errHandlerMissingBackend)
		}
		mw, err := filesystemmw.NewTyped[*einoschema.AgenticMessage](ctx, &filesystemmw.MiddlewareConfig{
			Backend: build.FilesystemBackend, UseMultiModalRead: cfg.UseMultiModalRead,
		})
		if err != nil {
			return nil, err
		}
		return mw, nil
	}
}

// --- plantask -------------------------------------------------------------

// PlanTaskConfig is the bounded, closed-schema configuration for the
// plantask recipe (Kind = HandlerKindPlanTask). It currently has no fields:
// the backend is always the run's own private writable scratch area (see
// HandlerBuildContext.PlanTaskBackend), never host-supplied storage.
type PlanTaskConfig struct{}

// NewPlanTaskHandlerFactory provides task-list tools (TaskCreate/Get/Update/
// List) backed by a private, workspace-scoped scratch directory via upstream
// plantask.NewTyped. Plan/task state is agent state -- it is not host issue
// tracking or session event authority.
func NewPlanTaskHandlerFactory(PlanTaskConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.PlanTaskBackend == nil {
			return nil, fmt.Errorf("%w: plantask requires a workspace root", errHandlerMissingBackend)
		}
		mw, err := plantask.NewTyped[*einoschema.AgenticMessage](ctx, &plantask.Config{Backend: build.PlanTaskBackend, BaseDir: "."})
		if err != nil {
			return nil, err
		}
		return mw, nil
	}
}

// --- patchtoolcalls ---------------------------------------------------------

// PatchToolCallsConfig is the bounded, closed-schema configuration for the
// patchtoolcalls recipe (Kind = HandlerKindPatchToolCalls). PatchedText is
// the fixed, bounded text every dangling tool call gets patched with;
// keeping it a plain string (not a func) is what makes this recipe's
// Config JSON-serializable and thus sealable into the plan fingerprint.
type PatchToolCallsConfig struct {
	PatchedText string `json:"patched_text,omitempty"`
}

const defaultPatchedToolCallText = "This tool call's result is unavailable (its history was patched); treat it as unknown, not as a failure."

// NewPatchToolCallsHandlerFactory patches dangling tool calls in model input
// with a fixed bounded text via upstream patchtoolcalls.NewTyped. It never
// touches durable state: it can only make model INPUT well-formed for a
// dangling call this runtime's own history never recorded a settlement for
// (e.g. after external history editing) -- it cannot fabricate a durable
// session.ToolCall settlement. It injects no tools. Its patch is recorded
// as an authorized rewrite (see wrapAuthorizedContentRewrites) so
// settlementSeal treats it as a sanctioned content-management patch of an
// unsettled call, not a fabrication.
func NewPatchToolCallsHandlerFactory(cfg PatchToolCallsConfig) HandlerFactory {
	text := cfg.PatchedText
	if strings.TrimSpace(text) == "" {
		text = defaultPatchedToolCallText
	}
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		mw, err := patchtoolcalls.NewTyped[*einoschema.AgenticMessage](ctx, &patchtoolcalls.Config{
			PatchedToolResultGenerator: func(context.Context, string, string, *einoschema.ToolArgument) (*patchtoolcalls.PatchedToolResult, error) {
				return &patchtoolcalls.PatchedToolResult{Content: text}, nil
			},
		})
		if err != nil {
			return nil, err
		}
		return wrapAuthorizedContentRewrites(mw, build.HandlerID, HandlerKindPatchToolCalls, build.authorizeRewrite), nil
	}
}

// --- reduction ------------------------------------------------------------

// ReductionConfig is the bounded, closed-schema configuration for the
// reduction recipe (Kind = HandlerKindReduction).
type ReductionConfig struct {
	MaxLengthForTrunc int   `json:"max_length_for_trunc,omitempty"`
	MaxTokensForClear int64 `json:"max_tokens_for_clear,omitempty"`
}

// NewReductionHandlerFactory offloads/clears large tool output into a
// private, workspace-scoped scratch backend via upstream reduction.NewTyped,
// with the default (character-estimate) token counter.
//
// Only cfg.MaxTokensForClear is effective in this runtime; cfg.MaxLengthForTrunc
// is accepted (upstream requires it non-zero) but its truncation-via-tool-
// wrapper mechanism (upstream's WrapInvokableToolCall/WrapEnhancedInvokableToolCall,
// which intercepts a tool's own return value at execution time) does not
// survive this runtime's per-cycle durable baseline: durableBaselineHandler
// rebuilds state.Messages fresh from the durable settlement every cycle,
// discarding whatever a wrapped tool call's own in-flight return value held
// from an earlier cycle. Clearing works because it operates via
// BeforeModelRewriteState -- the same seam durableBaselineHandler/
// wrapAuthorizedContentRewrites are built for -- so it is applied fresh,
// every cycle, directly against the current baseline. Set MaxTokensForClear
// low enough (and remember upstream's own default ClearRetentionSuffixLimit
// of 1 always protects the single most-recent tool-call round from
// clearing) to actually shrink what the model sees; see
// TestReductionHandlerTruncatesLargeToolResultAsAuthorizedRewrite for a
// worked, non-false-positive example. Use
// NewReductionHandlerFactoryWithTokenCounter to inject a custom counter --
// funcs cannot round-trip through the JSON-serializable Config, so injection
// is a separate constructor rather than a Config field.
func NewReductionHandlerFactory(cfg ReductionConfig) HandlerFactory {
	return NewReductionHandlerFactoryWithTokenCounter(cfg, nil)
}

// TypedTokenCounter matches upstream reduction.TypedTokenCounterFunc for
// *schema.AgenticMessage.
type TypedTokenCounter func(ctx context.Context, msgs []*einoschema.AgenticMessage, tools []*einoschema.ToolInfo) (int64, error)

// NewReductionHandlerFactoryWithTokenCounter is NewReductionHandlerFactory
// with an injectable token counter (design: "injectable token counter").
func NewReductionHandlerFactoryWithTokenCounter(cfg ReductionConfig, counter TypedTokenCounter) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.ReductionBackend == nil {
			return nil, fmt.Errorf("%w: reduction requires a workspace root", errHandlerMissingBackend)
		}
		typedCfg := &reduction.TypedConfig[*einoschema.AgenticMessage]{
			// RootDir must be relative: build.ReductionBackend
			// (writableWorkspaceBackend) resolves every WriteRequest.FilePath
			// against its own workspace-contained root and rejects an
			// absolute path outside it (upstream's own default, "/tmp", is
			// exactly such a path).
			Backend: build.ReductionBackend, ReadFileToolName: "read_file", RootDir: ".",
			MaxLengthForTrunc: cfg.MaxLengthForTrunc, MaxTokensForClear: cfg.MaxTokensForClear,
		}
		if counter != nil {
			typedCfg.TokenCounter = counter
		}
		mw, err := reduction.NewTyped[*einoschema.AgenticMessage](ctx, typedCfg)
		if err != nil {
			return nil, err
		}
		return wrapAuthorizedContentRewrites(mw, build.HandlerID, HandlerKindReduction, build.authorizeRewrite), nil
	}
}

// --- summarization ----------------------------------------------------------

// SummarizationConfig is the bounded, closed-schema configuration for the
// summarization recipe (Kind = HandlerKindSummarization).
type SummarizationConfig struct {
	TriggerContextTokens   int    `json:"trigger_context_tokens,omitempty"`
	TriggerContextMessages int    `json:"trigger_context_messages,omitempty"`
	UserInstruction        string `json:"user_instruction,omitempty"`
	TranscriptFilePath     string `json:"transcript_file_path,omitempty"`
	// RetainTailCount is the number of most-recent durable conversational
	// messages this recipe's Finalize keeps out of the new epoch's summary
	// (TailStartID onward); the rest, from the session's first
	// conversational message through the message immediately before the
	// tail, is what SummarizedFromID/SummarizedToID cover.
	RetainTailCount int `json:"retain_tail_count,omitempty"`
}

// NewSummarizationHandlerFactory generates a conversation summary via
// upstream summarization.NewTyped, using build.Model -- for this recipe
// ONLY, adkEngine.buildAgentHandlers hands it a bounded internal-dispatch
// adapter (AgentPath "summarizer") instead of the turn's own conversational
// adapter (see adkModel.internalDispatch): the summary-generation call is
// still fully durably audited (its own ledger row, retried/failed over
// through the same path, usage charged) but can never claim the turn's
// assistant placeholder or persist as if the assistant said it to the user
// -- and maps the completed summary into an atomic session.ContextEpoch
// (Finalize below) via the narrow contextEpochCapability. On cancellation
// or a failed summary generation, upstream never calls Finalize at all, so
// no new epoch is created and the previously active epoch stays in force --
// see summarizationFinalize's doc comment for the exact epoch-boundary
// derivation.
func NewSummarizationHandlerFactory(cfg SummarizationConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.Model == nil || !build.epochs.ready() {
			return nil, fmt.Errorf("%w: summarization requires a durable store and model adapter", errHandlerMissingBackend)
		}
		typedCfg := &summarization.TypedConfig[*einoschema.AgenticMessage]{
			Model: build.Model, UserInstruction: cfg.UserInstruction, TranscriptFilePath: cfg.TranscriptFilePath,
			Finalize: summarizationFinalize(build, cfg.RetainTailCount),
		}
		if cfg.TriggerContextTokens > 0 || cfg.TriggerContextMessages > 0 {
			typedCfg.Trigger = &summarization.TriggerCondition{ContextTokens: cfg.TriggerContextTokens, ContextMessages: cfg.TriggerContextMessages}
		}
		mw, err := summarization.NewTyped[*einoschema.AgenticMessage](ctx, typedCfg)
		if err != nil {
			return nil, err
		}
		return mw, nil
	}
}

// errSummarizationTailUnsplittable reports that summarizationFinalize could
// not find any group boundary at or before the requested retain-tail cut
// point without splitting a function call from one of its results (see
// moveTailStartToGroupBoundary): every durable message from the session's
// start through the one immediately preceding the tail would have to be
// summarized away, including the very first message, which would leave
// nothing for SummarizedFromID/SummarizedToID to cover distinctly from the
// tail.
var errSummarizationTailUnsplittable = fmt.Errorf("%w: summarization tail cannot be trimmed without separating a function call from its result", errADKUnsupportedBlock)

// summarizationFinalize maps a completed summary into a new atomic
// session.ContextEpoch. It correlates upstream's in-memory originalMessages
// with this session's durable message history by loading the SAME
// epoch-projected agentic view ADK's own current-turn input was built from
// (build.epochs.loadCurrentAgenticProjection, applying whatever
// summarization epoch is already active for this session -- possibly none,
// possibly one from an earlier summarization on this same session): its
// SourceMessageIDs correlates 1:1, in order, with originalMessages. This
// holds for the expected case (summarizing the session's complete,
// uninterrupted conversational history, whether or not it has already been
// summarized once before) but is a documented, checked assumption, not a
// guarantee for every possible upstream Trigger/GenModelInput configuration:
// a mismatch fails Finalize closed (an error, which fails the run) rather
// than fabricating an incorrect boundary. Because summarization's own
// internal call now runs through a bounded internal-dispatch adapter that
// never persists a trailing assistant message (see adkModel.internalDispatch),
// this correlation no longer needs the "drop one trailing entry" workaround
// a prior revision required.
func summarizationFinalize(build HandlerBuildContext, retainTail int) summarization.TypedFinalizeFunc[*einoschema.AgenticMessage] {
	return func(ctx context.Context, originalMessages []*einoschema.AgenticMessage, summary *einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
		summaryText := summaryAgenticMessageText(summary)
		if strings.TrimSpace(summaryText) == "" {
			return nil, fmt.Errorf("%w: summary text is empty", errADKUnsupportedBlock)
		}
		activeEpoch, err := build.epochs.activeEpoch(ctx)
		if err != nil {
			return nil, err
		}
		projection, err := build.epochs.loadCurrentAgenticProjection(ctx, activeEpoch)
		if err != nil {
			return nil, err
		}
		// history.LoadAgentic has no notion of "unfinalized": unlike
		// adkEngine.buildDurableBaseline (which explicitly drops the turn's
		// own not-yet-committed assistant placeholder via
		// dropUnfinalizedAssistantPlaceholders before ADK ever sees it),
		// projectLegacyAgenticMessage happily projects a zero-part
		// assistant message as an empty AgenticMessage. ADK's own
		// originalMessages never includes that placeholder, so it must be
		// filtered out here the same way, in lockstep with SourceMessageIDs,
		// or the correlation below is off by one for every turn's first
		// summarization trigger.
		sourceIDs := make([]session.MessageID, 0, len(projection.SourceMessageIDs))
		for i, msg := range projection.Messages {
			if msg != nil && msg.Role == einoschema.AgenticRoleTypeAssistant && len(msg.ContentBlocks) == 0 {
				continue
			}
			sourceIDs = append(sourceIDs, projection.SourceMessageIDs[i])
		}
		if len(sourceIDs) != len(originalMessages) || len(sourceIDs) == 0 {
			return nil, fmt.Errorf("%w: summarization could not correlate %d in-memory messages with %d durable messages", errADKUnsupportedBlock, len(originalMessages), len(sourceIDs))
		}
		// A second pass over the FULL, unfiltered durable history (not
		// epoch-projected) gives role and content-block-kind lookups by
		// message ID for the tail-boundary logic below; every ID in
		// projection.SourceMessageIDs is necessarily present in it (the
		// epoch-projected view is a subset of the full durable history).
		batch, err := build.epochs.loadConversationalHistory(ctx)
		if err != nil {
			return nil, err
		}
		byID := make(map[session.MessageID]session.Message, len(batch.Messages))
		for _, msg := range batch.Messages {
			byID[msg.ID] = msg
		}
		owners, err := session.ResolveReplayPartOwners(batch.Parts, batch.PartOwnerMessageIDs)
		if err != nil {
			return nil, err
		}
		partsByMessage := make(map[session.MessageID][]session.Part, len(batch.Parts))
		for i, part := range batch.Parts {
			partsByMessage[owners[i]] = append(partsByMessage[owners[i]], part)
		}
		durable := make([]session.Message, len(sourceIDs))
		for i, id := range sourceIDs {
			msg, ok := byID[id]
			if !ok {
				return nil, fmt.Errorf("%w: summarization correlated durable message %s not found in full history reload", errADKUnsupportedBlock, id)
			}
			durable[i] = msg
		}
		if retainTail < 0 {
			retainTail = 0
		}
		if retainTail >= len(durable) {
			// Nothing to compact: the requested tail already covers the
			// whole conversation. Do not start a new epoch (it would have
			// SummarizedFromID == SummarizedToID == TailStartID, an
			// overlapping, meaningless range) -- leave history untouched.
			return originalMessages, nil
		}
		tailStart := len(durable) - retainTail
		tailStart = moveTailStartToGroupBoundary(durable, partsByMessage, tailStart)
		if tailStart <= 0 {
			return nil, errSummarizationTailUnsplittable
		}
		// Always keep the session's leading system-message prefix out of
		// the summarized range: system instructions are host-injected
		// context, not conversational history to compact away.
		systemPrefixLen := 0
		for systemPrefixLen < tailStart && durable[systemPrefixLen].Role == session.RoleSystem {
			systemPrefixLen++
		}
		summarizedFrom := systemPrefixLen
		if summarizedFrom >= tailStart {
			// The entire pre-tail range is a system prefix: there is
			// nothing conversational left to summarize.
			return originalMessages, nil
		}
		epoch := session.ContextEpoch{
			ID: build.epochs.ids.NewEpochID(), SessionID: build.SessionID,
			SummarizedFromID: durable[summarizedFrom].ID, SummarizedToID: durable[tailStart-1].ID,
			Trigger: "summarization", Reason: "context_budget", NextAction: session.EpochNextAutoContinue, CreatedAt: build.epochs.now(),
		}
		if tailStart < len(durable) {
			// tailStart == len(durable) means no tail is retained at all
			// (retainTail == 0): TailStartID stays empty, matching
			// applyEpoch's contract (a zero TailStartID plus a non-empty
			// SummaryMessageID projects the summary alone, with no tail).
			epoch.TailStartID = durable[tailStart].ID
		}
		ids := compaction.BoundaryIDs{MessageID: build.epochs.ids.NewMessageID(), PartID: build.epochs.ids.NewPartID()}
		if _, _, err := build.epochs.commitSummaryEpoch(ctx, epoch, ids, summaryText); err != nil {
			return nil, err
		}
		systemPrefix := append([]*einoschema.AgenticMessage(nil), originalMessages[:systemPrefixLen]...)
		tail := append([]*einoschema.AgenticMessage(nil), originalMessages[tailStart:]...)
		result := append(systemPrefix, summary)
		return append(result, tail...), nil
	}
}

// moveTailStartToGroupBoundary walks tailStart backward, never forward,
// until the boundary no longer separates an assistant message's
// function_tool_call from one of its function_tool_result messages: a
// result message (identified by owning a PartFunctionToolResult part)
// cannot become the tail's first entry unless its originating call is also
// in the tail, and a call-bearing assistant message cannot be the last
// entry excluded from the tail while any of its results are retained.
// Since each check only ever moves tailStart strictly backward and it is
// bounded below by 0, this always terminates.
func moveTailStartToGroupBoundary(durable []session.Message, partsByMessage map[session.MessageID][]session.Part, tailStart int) int {
	hasKind := func(id session.MessageID, kind session.PartKind) bool {
		for _, part := range partsByMessage[id] {
			if part.Kind == kind {
				return true
			}
		}
		return false
	}
	for tailStart > 0 {
		if tailStart < len(durable) && hasKind(durable[tailStart].ID, session.PartFunctionToolResult) {
			tailStart--
			continue
		}
		if hasKind(durable[tailStart-1].ID, session.PartFunctionToolCall) {
			tailStart--
			continue
		}
		break
	}
	return tailStart
}

func summaryAgenticMessageText(msg *einoschema.AgenticMessage) string {
	if msg == nil {
		return ""
	}
	var b strings.Builder
	for _, block := range msg.ContentBlocks {
		if block != nil && block.AssistantGenText != nil {
			b.WriteString(block.AssistantGenText.Text)
		}
	}
	return b.String()
}

// --- dynamictool/toolsearch -------------------------------------------------

// ToolSearchHandlerConfig is the bounded, closed-schema configuration for
// the toolsearch recipe (Kind = HandlerKindToolSearch).
type ToolSearchHandlerConfig struct {
	// UseModelToolSearch maps directly to upstream's UseModelToolSearch
	// (model-native tool search) vs this middleware's own list-filtering
	// tool_search behavior.
	UseModelToolSearch bool `json:"use_model_tool_search,omitempty"`
}

// NewToolSearchHandlerFactory provides upstream's own dynamictool/toolsearch
// middleware over this turn's frozen, already-durably-wrapped deferred tools
// (HandlerBuildContext.DeferredTools, the same W4 Deferred: true tools) via
// dynamictool/toolsearch.NewTyped. This is independent of and in addition to
// this runtime's own native tool-search tool (composition.Registrar.ToolSearch/
// runtime.ToolSearchConfig); a host registers at most whichever mechanism(s)
// it wants, and the frozen tool registry -- not this middleware -- remains
// the sole authority for which tools may ever actually execute.
func NewToolSearchHandlerFactory(cfg ToolSearchHandlerConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if len(build.DeferredTools) == 0 {
			return nil, fmt.Errorf("%w: toolsearch requires at least one deferred tool in the frozen registry", errADKUnsupportedBlock)
		}
		mw, err := toolsearch.NewTyped[*einoschema.AgenticMessage](ctx, &toolsearch.Config{
			DynamicTools: build.DeferredTools, UseModelToolSearch: cfg.UseModelToolSearch,
		})
		if err != nil {
			return nil, err
		}
		return mw, nil
	}
}
