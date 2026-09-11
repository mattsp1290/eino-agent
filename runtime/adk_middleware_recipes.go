package runtime

import (
	"context"
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
	"github.com/mattsp1290/eino-agent/session/history"
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
// invocation time, not silently. Activated skill names are recorded on the
// run-local ADK state (adk.SetRunLocalValue), which is carried through
// ADK's own checkpoint payload across interrupt/resume, so a resumed run
// can see which skills this turn activated without any new checkpoint
// field.
func NewSkillHandlerFactory(cfg SkillConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.SkillBackend == nil {
			return nil, fmt.Errorf("%w: skill requires a workspace skill backend", errHandlerMissingBackend)
		}
		skillCfg := &skill.TypedConfig[*einoschema.AgenticMessage]{Backend: build.SkillBackend}
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
		return wrapAuthorizedContentRewrites(mw, build.authorizeRewrite), nil
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
// with the default (character-estimate) token counter. Use
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
			Backend: build.ReductionBackend, ReadFileToolName: "read_file",
			MaxLengthForTrunc: cfg.MaxLengthForTrunc, MaxTokensForClear: cfg.MaxTokensForClear,
		}
		if counter != nil {
			typedCfg.TokenCounter = counter
		}
		mw, err := reduction.NewTyped[*einoschema.AgenticMessage](ctx, typedCfg)
		if err != nil {
			return nil, err
		}
		return wrapAuthorizedContentRewrites(mw, build.authorizeRewrite), nil
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
// upstream summarization.NewTyped, using build.Model (the same
// ledger-audited adapter as this turn's primary dispatch -- summarization's
// own summary-generation call is therefore fully durably audited) and maps
// the completed summary into an atomic session.ContextEpoch (Finalize below)
// via compaction.AppendBoundary. On cancellation or a failed summary
// generation, upstream never calls Finalize at all, so no new epoch is
// created and the previously active epoch stays in force -- see
// summarizationFinalize's doc comment for the exact epoch-boundary
// derivation and its one documented limitation.
func NewSummarizationHandlerFactory(cfg SummarizationConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.Model == nil || build.Store == nil || build.Execution == nil || build.IDs == nil || build.Now == nil {
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

// summarizationFinalize maps a completed summary into a new atomic
// session.ContextEpoch. It correlates upstream's in-memory originalMessages
// with this session's durable message history by position: it loads the
// full durable history (history.LoadBatch), filters it to conversational
// roles (system/user/assistant -- the same roles the agentic projection
// carries), and requires that filtered list to be exactly as long as
// originalMessages. This holds for the expected case (summarizing the
// session's complete, uninterrupted conversational history) but is a
// documented, checked assumption, not a guarantee for every possible
// upstream Trigger/GenModelInput configuration: a mismatch fails Finalize
// closed (an error, which fails the run) rather than fabricating an
// incorrect boundary.
func summarizationFinalize(build HandlerBuildContext, retainTail int) summarization.TypedFinalizeFunc[*einoschema.AgenticMessage] {
	return func(ctx context.Context, originalMessages []*einoschema.AgenticMessage, summary *einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
		batch, err := history.LoadBatch(ctx, build.Store, build.SessionID)
		if err != nil {
			return nil, err
		}
		var durable []session.Message
		for _, msg := range batch.Messages {
			switch msg.Role {
			case session.RoleSystem, session.RoleUser, session.RoleAssistant:
				durable = append(durable, msg)
			}
		}
		if len(durable) != len(originalMessages) || len(durable) == 0 {
			return nil, fmt.Errorf("%w: summarization could not correlate %d in-memory messages with %d durable messages", errADKUnsupportedBlock, len(originalMessages), len(durable))
		}
		if retainTail < 0 {
			retainTail = 0
		}
		if retainTail > len(durable) {
			retainTail = len(durable)
		}
		tailStart := len(durable) - retainTail
		summarizedTo := tailStart - 1
		if summarizedTo < 0 {
			summarizedTo = 0
		}
		epoch := session.ContextEpoch{
			ID: build.IDs.NewEpochID(), SessionID: build.SessionID,
			SummarizedFromID: durable[0].ID, SummarizedToID: durable[summarizedTo].ID,
			Trigger: "summarization", Reason: "context_budget", NextAction: session.EpochNextAutoContinue, CreatedAt: build.Now(),
		}
		if tailStart < len(durable) {
			epoch.TailStartID = durable[tailStart].ID
		}
		started, err := build.Execution.StartContextEpoch(ctx, epoch)
		if err != nil {
			return nil, err
		}
		ids := compaction.BoundaryIDs{MessageID: build.IDs.NewMessageID(), PartID: build.IDs.NewPartID()}
		if _, err := compaction.AppendBoundary(ctx, build.Execution, started, ids, build.RunID, build.Now(), summaryAgenticMessageText(summary)); err != nil {
			return nil, err
		}
		tail := append([]*einoschema.AgenticMessage(nil), originalMessages[tailStart:]...)
		return append([]*einoschema.AgenticMessage{summary}, tail...), nil
	}
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
