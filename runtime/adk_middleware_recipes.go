package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/cloudwego/eino/adk"
	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/agentsmd"
	"github.com/cloudwego/eino/adk/middlewares/dynamictool/toolsearch"
	filesystemmw "github.com/cloudwego/eino/adk/middlewares/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/patchtoolcalls"
	"github.com/cloudwego/eino/adk/middlewares/plantask"
	"github.com/cloudwego/eino/adk/middlewares/reduction"
	"github.com/cloudwego/eino/adk/middlewares/skill"
	"github.com/cloudwego/eino/adk/middlewares/summarization"
	"github.com/cloudwego/eino/components/tool"
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

// ErrHandlerConfiguration reports a host-facing agent-handler recipe
// configuration problem -- a missing required backend, toolsearch's empty
// deferred-tool registry, or a durable-history correlation summarization
// could not resolve -- distinct from errADKUnsupportedBlock's narrower "adk
// adapter cannot durably record this content block" meaning. Every site
// that previously wrapped only errADKUnsupportedBlock for one of these
// configuration-shaped failures now wraps BOTH sentinels (never in place of
// errADKUnsupportedBlock, so any existing errors.Is(err, errADKUnsupportedBlock)
// check keeps working), so a caller/log line can classify "this recipe was
// configured wrong" apart from "this adapter hit an unsupported content
// shape" -- see RW-S7 in the round-two W6 review.
var ErrHandlerConfiguration = errors.New("agent handler configuration problem")

// errHandlerMissingBackend reports that a recipe's required host-provided
// backend (filesystem/skill read-only view, plantask/reduction scratch
// backend, or a durable store) was not available for this run -- e.g.
// because no workspace root is configured -- and so construction failed
// closed rather than operating unscoped.
var errHandlerMissingBackend = fmt.Errorf("%w: %w: required backend unavailable", ErrHandlerConfiguration, errADKUnsupportedBlock)

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
// and a content digest of what was actually loaded. ResumeRun enforces this
// durable record against the CURRENT workspace state before ever claiming
// the run's fence (verifySkillActivationsUnchanged, scoped to the specific
// run being resumed -- see that function's doc comment): a SKILL.md edited
// between pause and resume is detected and rejected with
// ErrSkillChangedSinceActivation, leaving the run exactly as paused as it
// was found.
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

// ErrSkillChangedSinceActivation reports that ResumeRun's skill
// verification (verifySkillActivationsUnchanged) found a durably recorded
// SkillActivatedEventKind activation -- scoped to the specific run being
// resumed -- whose skill is now missing, or whose current content digest no
// longer matches what was recorded at activation time. The resumed run is
// left exactly as paused as it was found (its fence was never claimed): the
// only escape for a run stuck behind this error is StopPolicy{Abandon:
// true} (see runtime.StopPolicy's doc comment), which settles the durably
// paused run without resuming it and frees the session for a new run.
var ErrSkillChangedSinceActivation = errors.New("adk skill content changed since activation; resume this run with StopPolicy{Abandon: true} to abandon it instead")

// skillActivationRecord is one durably recorded SkillActivatedEventKind
// payload, decoded from EventRecord.Payload (see recordSkillActivation).
type skillActivationRecord struct {
	Name          string `json:"name"`
	ContentDigest string `json:"content_digest"`
}

// latestSkillActivations pages sessionID's full durable event history (via
// store.ListEvents, unbounded/unfiltered by Kind at the store layer) and
// returns the most recently recorded activation per skill name, SCOPED TO
// runID (round-two W6 review item 12): a skill activated by a DIFFERENT,
// unrelated run on the same session must never block resuming this one --
// SkillActivatedEventKind events are appended in the same order
// store.ListEvents replays them (ascending by CreatedAt, ID -- see
// store/internal/sqlstore/events.go's ListEvents), so folding forward and
// overwriting by name always keeps the latest activation THIS run recorded.
func latestSkillActivations(ctx context.Context, store session.Store, sessionID session.ID, runID session.RunID) (map[string]skillActivationRecord, error) {
	activations := make(map[string]skillActivationRecord)
	cursor := session.EventCursor{Limit: 200}
	for {
		batch, err := store.ListEvents(ctx, sessionID, cursor)
		if err != nil {
			return nil, err
		}
		for _, event := range batch.Events {
			if event.Kind != session.SkillActivatedEventKind || event.RunID != runID {
				continue
			}
			var record skillActivationRecord
			if err := json.Unmarshal(event.Payload, &record); err != nil || record.Name == "" {
				continue
			}
			activations[record.Name] = record
		}
		if batch.Next == (session.EventCursor{}) {
			return activations, nil
		}
		cursor = batch.Next
	}
}

// verifySkillActivationsUnchanged re-reads, from the CURRENT workspace
// state, every skill runID (the run being resumed -- round-two W6 review
// item 12) durably recorded activating (SkillActivatedEventKind), and fails
// closed with ErrSkillChangedSinceActivation the moment any of them is
// missing or its content digest no longer matches what was recorded at
// activation time. It is read-only (a fresh workspaceSkillBackend.Get per
// recorded skill name) and never mutates durable state, so ResumeRun's
// caller (which calls this before ever claiming the run's fence) can reject
// a resume this finds unsafe while leaving the run exactly as paused as it
// found it -- a changed or missing skill therefore fails resume
// predictably, not silently, and not by letting the model see stale or
// divergent skill content it was never actually re-shown. An unrelated
// paused run on the SAME session, whose own activations are unaffected, is
// never blocked by this check (it is scoped to runID, not the session as a
// whole).
//
// workspaceRoot == "" (no workspace configured for this run) skips
// verification outright: the skill recipe itself requires a non-nil
// SkillBackend to construct (see NewSkillHandlerFactory), which in turn
// requires a non-empty WorkspaceRoot, so no session ever recorded a
// SkillActivatedEventKind event without one.
func verifySkillActivationsUnchanged(ctx context.Context, store session.Store, sessionID session.ID, runID session.RunID, workspaceRoot string) error {
	if workspaceRoot == "" {
		return nil
	}
	activations, err := latestSkillActivations(ctx, store, sessionID, runID)
	if err != nil {
		return fmt.Errorf("resolving recorded skill activations: %w", err)
	}
	if len(activations) == 0 {
		return nil
	}
	_, skillBackend, err := newWorkspaceBackends(workspaceRoot)
	if err != nil {
		return fmt.Errorf("%w: resolving workspace for skill verification: %v", ErrSkillChangedSinceActivation, err)
	}
	for name, recorded := range activations {
		current, err := skillBackend.Get(ctx, name)
		if err != nil {
			return fmt.Errorf("%w: skill %q recorded active at digest %s is no longer available: %v", ErrSkillChangedSinceActivation, name, recorded.ContentDigest, err)
		}
		if digest := skillContentDigest(current); digest != recorded.ContentDigest {
			return fmt.Errorf("%w: skill %q content digest changed from %s to %s since it was activated", ErrSkillChangedSinceActivation, name, recorded.ContentDigest, digest)
		}
	}
	return nil
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
			return nil, fmt.Errorf("%w: plantask requires a scratch backend", errHandlerMissingBackend)
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
		return wrapAuthorizedContentRewrites(mw, build.HandlerID, HandlerKindPatchToolCalls, build.baselineToolResultDigests, build.authorizeRewrite), nil
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
// Both cfg.MaxLengthForTrunc and cfg.MaxTokensForClear are effective, but
// through two different mechanisms with two different timings:
//
//   - MaxLengthForTrunc (truncation) applies per call, at EXECUTION time,
//     before this runtime durably settles the tool call: mw's own
//     WrapInvokableToolCall/WrapEnhancedInvokableToolCall is threaded
//     through adkEngine.applyHandlerToolResultWrappers, called from
//     executeClaimedToolPipeline before buildToolSettlement/persistToolSettlement
//     -- the plan's "result transforms occur before settlement and event
//     emission" requirement. The settled row IS the truncated form (with
//     reduction's own offload reference), not an in-memory rewrite of an
//     already-settled result, so there is nothing for settlementSeal to
//     authorize here at all -- see
//     TestReductionTruncatesToolResultBeforeSettlement.
//   - MaxTokensForClear (clearing) applies across the WHOLE conversation's
//     accumulated size, which cannot be decided at any single tool's own
//     execution time, so it stays a post-settlement rewrite of the durable
//     baseline via BeforeModelRewriteState, explicitly authorized through
//     wrapAuthorizedContentRewrites/settlementSeal exactly like
//     patchtoolcalls' own patches. Set it low enough (and remember
//     upstream's own default ClearRetentionSuffixLimit of 1 always
//     protects the single most-recent tool-call round from clearing) to
//     actually shrink what the model sees; see
//     TestReductionClearsOlderSettledResultAsAuthorizedRewrite for a
//     worked, non-false-positive example.
//
// Use NewReductionHandlerFactoryWithTokenCounter to inject a custom
// counter -- funcs cannot round-trip through the JSON-serializable Config,
// so injection is a separate constructor rather than a Config field.
//
// Correlation dependency (round-four W6 correlation-and-seal review,
// Suggestion #2): this constructor -- and NewReductionHandlerFactoryWithTokenCounter
// below, which it wraps -- deliberately never sets upstream reduction's own
// ClearAtLeastTokens or ClearMessageRewriter fields on TypedConfig. Clearing
// today only ever happens through this recipe's own in-place rewrite
// (`setToolResultContent` on the existing message pointer, per upstream's
// `reduction.go`), which is what lets summarization's pointer-keyed
// correlation (cycleMessageSourceByPointer) still resolve a message
// reduction has cleared -- the pointer identity survives the rewrite.
// Upstream switches to allocating NEW message pointers
// (`copyMessagesGeneric`) once ClearAtLeastTokens > 0, and
// ClearMessageRewriter can restructure the slice outright; either would
// silently break summarization's pointer lookup for every message reduction
// touches in a cycle where both recipes are mounted and fire together --
// summarizationFinalize's own content-based fallback
// (correlateDurableSubsequence) cannot rescue this case either, since
// reduction's whole purpose is to change the content the fallback would
// need to match on. Leaving both fields unset is a real, load-bearing
// precondition for correlation to hold when reduction and summarization are
// co-mounted, not an incidental default -- keep it that way unless
// correlation is re-verified against whichever of these two upstream
// behaviors gets enabled.
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
			return nil, fmt.Errorf("%w: reduction requires a scratch backend", errHandlerMissingBackend)
		}
		typedCfg := &reduction.TypedConfig[*einoschema.AgenticMessage]{
			// RootDir must be relative: build.ReductionBackend
			// (scratchRootBackend) resolves every WriteRequest.FilePath
			// against its own os.Root-sandboxed scratch root and rejects an
			// absolute path outside it (upstream's own default, "/tmp", is
			// exactly such a path). ReadFileToolName points the offload
			// notice text at reductionOffloadReadToolName, NOT the
			// filesystem recipe's "read_file" -- reduction's offloads live
			// in this session's own runtime-owned scratch root, never the
			// workspace, so the workspace's read-only filesystem tool
			// cannot reach them at all (round-two W6 review I5/I6); see
			// reductionOffloadReadTool below.
			Backend: build.ReductionBackend, ReadFileToolName: reductionOffloadReadToolName, RootDir: ".",
			MaxLengthForTrunc: cfg.MaxLengthForTrunc, MaxTokensForClear: cfg.MaxTokensForClear,
		}
		if counter != nil {
			typedCfg.TokenCounter = counter
		}
		mw, err := reduction.NewTyped[*einoschema.AgenticMessage](ctx, typedCfg)
		if err != nil {
			return nil, err
		}
		wrapped := wrapAuthorizedContentRewrites(mw, build.HandlerID, HandlerKindReduction, build.baselineToolResultDigests, build.authorizeRewrite)
		return &toolAppendingMiddleware{
			TypedChatModelAgentMiddleware: wrapped,
			extra:                         []tool.BaseTool{reductionOffloadReadTool{backend: build.ReductionBackend}},
		}, nil
	}
}

// reductionOffloadReadToolName is the fixed, sealed tool name reduction's
// own offload notices point the model at (see NewReductionHandlerFactoryWithTokenCounter's
// ReadFileToolName) to read back the full content of a truncated or cleared
// tool result.
const reductionOffloadReadToolName = "reduction_read_offload"

// reductionOffloadReadTool lets the model read back the full content of a
// tool result THIS session's own reduction recipe previously truncated or
// cleared, by the offload file path named in reduction's own notice text.
// backend is this turn's own HandlerBuildContext.ReductionBackend -- itself
// sandboxed to this session's own scratch *os.Root (see
// scratchRootBackend/sessionScratchRoot) -- so this tool can never read
// another session's offloads: a sealed per-session tool, never a shared,
// workspace-wide one (round-two W6 review I5/I6).
type reductionOffloadReadTool struct {
	backend writableTaskBackend
}

var _ tool.InvokableTool = reductionOffloadReadTool{}

func (t reductionOffloadReadTool) Info(context.Context) (*einoschema.ToolInfo, error) {
	return &einoschema.ToolInfo{
		Name: reductionOffloadReadToolName,
		Desc: "Read back the full content of a tool result this session's own reduction recipe previously truncated or cleared. Pass the exact file path named in the offload notice (e.g. \"Full output saved to: trunc/<call_id>\").",
		ParamsOneOf: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
			"file_path": {Type: einoschema.String, Desc: "The offload file path named in reduction's own notice text", Required: true},
		}),
	}, nil
}

func (t reductionOffloadReadTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	var args struct {
		FilePath string `json:"file_path"`
	}
	if err := json.Unmarshal([]byte(argumentsInJSON), &args); err != nil || args.FilePath == "" {
		return "", fmt.Errorf("%w: %s requires a file_path argument", errADKUnsupportedBlock, reductionOffloadReadToolName)
	}
	if t.backend == nil {
		return "", fmt.Errorf("%w: %s has no backend configured", errHandlerMissingBackend, reductionOffloadReadToolName)
	}
	content, err := t.backend.Read(ctx, &adkfilesystem.ReadRequest{FilePath: args.FilePath})
	if err != nil {
		return "", err
	}
	return content.Content, nil
}

// toolAppendingMiddleware decorates inner so its own BeforeAgent's returned
// tool list always also includes extra -- used to seal
// reductionOffloadReadTool into reduction's own contributed tool list
// without upstream reduction.NewTyped itself ever needing to know about it.
type toolAppendingMiddleware struct {
	adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
	extra []tool.BaseTool
}

func (m *toolAppendingMiddleware) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	ctx, next, err := m.TypedChatModelAgentMiddleware.BeforeAgent(ctx, runCtx)
	if err != nil || next == nil {
		return ctx, next, err
	}
	next.Tools = append(next.Tools, m.extra...)
	return ctx, next, nil
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
// adapter (AgentPath == this entry's own HandlerID) instead of the turn's
// own conversational adapter (see adkModel.internalDispatch): the
// summary-generation call is still fully durably audited (its own ledger
// row, retried/failed over through the same path, usage charged) but can
// never claim the turn's assistant placeholder or persist as if the
// assistant said it to the user -- and maps the completed summary into an
// atomic session.ContextEpoch (Finalize below) via the narrow
// contextEpochCapability. On cancellation or a failed summary generation,
// upstream never calls Finalize at all, so no new epoch is created and the
// previously active epoch stays in force -- see summarizationFinalize's
// doc comment for the exact epoch-boundary derivation. Keeping the
// previous epoch does NOT mean the turn itself succeeds: upstream's own
// BeforeModelRewriteState propagates a failed or cancelled summary call's
// error, failing that cycle -- the turn's own main dispatch never runs for
// it. A failed summary call fails the turn (session.RunFailed); a
// cancelled one maps to an interrupted outcome (session.RunInterrupted) --
// see TestSummarizationFailedGenerationKeepsPreviousEpoch/
// TestSummarizationCancelledGenerationKeepsPreviousEpoch in
// examples/agentic-middleware.
//
// Trigger configuration (round-two W6 review item 9): at least one of
// TriggerContextTokens/TriggerContextMessages must be configured --
// construction fails closed rather than silently falling through to
// upstream's own hidden default (a 160000-token threshold with no
// message-count trigger at all). When only TriggerContextMessages is set,
// the token threshold is bound to math.MaxInt, not left at its zero value:
// upstream's own getTriggerContextTokens returns
// TriggerCondition.ContextTokens VERBATIM whenever Trigger is non-nil (the
// 160000 default applies only when Trigger is nil entirely) -- so a
// message-only Trigger with ContextTokens left at 0 does not disable the
// token check, it sets its threshold to zero, which very nearly every
// non-empty conversation already exceeds (tokens > 0), triggering
// summarization on almost every cycle instead of never on tokens at all.
// The mirror case needs no such correction: upstream's own message-count
// check is already gated behind `ContextMessages > 0`, so leaving it at 0
// when only tokens are configured already means "disabled".
func NewSummarizationHandlerFactory(cfg SummarizationConfig) HandlerFactory {
	return func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if build.Model == nil || !build.epochs.ready() {
			return nil, fmt.Errorf("%w: summarization requires a durable store and model adapter", errHandlerMissingBackend)
		}
		if cfg.TriggerContextTokens == 0 && cfg.TriggerContextMessages == 0 {
			return nil, fmt.Errorf("%w: summarization requires at least one of TriggerContextTokens or TriggerContextMessages", ErrHandlerConfiguration)
		}
		if cfg.TriggerContextTokens < 0 || cfg.TriggerContextMessages < 0 {
			return nil, fmt.Errorf("%w: summarization trigger thresholds must be non-negative", ErrHandlerConfiguration)
		}
		tokenThreshold := cfg.TriggerContextTokens
		if tokenThreshold == 0 {
			tokenThreshold = math.MaxInt
		}
		typedCfg := &summarization.TypedConfig[*einoschema.AgenticMessage]{
			Model: build.Model, UserInstruction: cfg.UserInstruction, TranscriptFilePath: cfg.TranscriptFilePath,
			Finalize: summarizationFinalize(build, cfg.RetainTailCount),
			Trigger:  &summarization.TriggerCondition{ContextTokens: tokenThreshold, ContextMessages: cfg.TriggerContextMessages},
		}
		mw, err := summarization.NewTyped[*einoschema.AgenticMessage](ctx, typedCfg)
		if err != nil {
			return nil, err
		}
		return &summarizeAtMostOnceMiddleware{
			TypedChatModelAgentMiddleware: mw,
			summarizedRange:               build.summarizedRange,
			sourceMessageID:               build.sourceMessageID,
			baselineMessages:              build.baselineMessages,
		}, nil
	}
}

// summarizeAtMostOnceMiddleware bounds summarization to GENERATING at most
// once per turn (round-three W6 summarization-correctness review,
// Important #2) while still keeping every later cycle of a long tool loop
// compacted (round-four W6 correlation-and-seal review, Important #2):
// upstream summarization.TypedMiddleware re-evaluates its own trigger
// condition on every ReAct cycle, with no way to disable that after it has
// already fired once (TriggerCondition carries no per-call override). Left
// unguarded, a multi-cycle turn that crosses the configured threshold once
// would re-trigger -- and re-bill a real summary generation model call --
// on every later cycle of the SAME turn.
//
// Once summarizedRange (backed by adkEngine's summarized* fields, set by
// summarizationFinalize's own markSummarized call right after a successful
// commit) reports ok == true, this wrapper never again delegates to the
// inner middleware's BeforeModelRewriteState (never re-bills a summary
// generation call) -- instead it re-applies the SAME already-committed
// compaction to every later cycle's state.Messages via
// reapplyDurableSummary, so the model-visible context stays collapsed for
// the rest of the turn instead of regrowing back to the full baseline.
type summarizeAtMostOnceMiddleware struct {
	adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
	summarizedRange  func() (fromID, toID, boundaryID session.MessageID, summary *einoschema.AgenticMessage, ok bool)
	sourceMessageID  func(msg *einoschema.AgenticMessage) (session.MessageID, bool)
	baselineMessages func() ([]*einoschema.AgenticMessage, []session.MessageID)
}

func (m *summarizeAtMostOnceMiddleware) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	if m.summarizedRange != nil {
		if fromID, toID, boundaryID, summary, ok := m.summarizedRange(); ok {
			recompacted, err := reapplyDurableSummary(state.Messages, m.sourceMessageID, m.baselineMessages, fromID, toID, boundaryID, summary)
			if err != nil {
				return ctx, nil, err
			}
			afterState := *state
			afterState.Messages = recompacted
			return ctx, &afterState, nil
		}
	}
	return m.TypedChatModelAgentMiddleware.BeforeModelRewriteState(ctx, state, mc)
}

// reapplyDurableSummary re-projects originalMessages through a compaction
// this turn already committed once: the durable range [fromID, toID]
// (inclusive, by durable id) collapses to a single copy of summary, and the
// boundary message itself (boundaryID -- a synthetic system message the
// model does not need to see twice) is dropped. It shares
// correlateDurableSubsequence with summarizationFinalize's own first-commit
// path, so it fails exactly as closed: an unresolved durable baseline
// message here means the same "an earlier handler replaced content/pointers
// without preserving identity" hazard Important #1/#3 (round-four W6
// correlation-and-seal review) guard against at commit time, and a cycle
// that cannot safely re-apply a compaction it already promised the model
// must not silently hand back the full, uncompacted baseline instead.
func reapplyDurableSummary(
	originalMessages []*einoschema.AgenticMessage,
	sourceMessageID func(msg *einoschema.AgenticMessage) (session.MessageID, bool),
	baselineMessages func() ([]*einoschema.AgenticMessage, []session.MessageID),
	fromID, toID, boundaryID session.MessageID,
	summary *einoschema.AgenticMessage,
) ([]*einoschema.AgenticMessage, error) {
	if sourceMessageID == nil || baselineMessages == nil {
		return nil, fmt.Errorf("%w: %w: summarization requires per-cycle durable source correlation (sourceMessageID/baselineMessages)", ErrHandlerConfiguration, errADKUnsupportedBlock)
	}
	baselineMsgs, baselineIDs := baselineMessages()
	sourceIDs, durableOriginalIndices, err := correlateDurableSubsequence(originalMessages, sourceMessageID, baselineMsgs, baselineIDs)
	if err != nil {
		return nil, err
	}
	collapse := make(map[int]bool, len(durableOriginalIndices))
	dropIndex := -1
	inRange := false
	for _, origIndex := range durableOriginalIndices {
		id := sourceIDs[origIndex]
		if id == boundaryID {
			dropIndex = origIndex
			continue
		}
		if id == fromID {
			inRange = true
		}
		if inRange {
			collapse[origIndex] = true
		}
		if id == toID {
			inRange = false
		}
	}
	if len(collapse) == 0 && dropIndex < 0 {
		// The already-summarized range AND the boundary itself are both
		// absent from this cycle's own projection -- this package never
		// deletes durable history, so every one of fromID/toID/boundaryID
		// committed by this SAME turn's own earlier Finalize call should
		// always still resolve here. Reaching this branch means
		// correlateDurableSubsequence's own fail-closed guarantee was
		// satisfied against a baseline that itself no longer contains the
		// range this function was asked to re-collapse -- a genuine
		// invariant violation, not a degrade case. Failing closed here
		// (round-four W6 correlation-and-seal review followup, Suggestion
		// S-D-2) keeps this function's own contract -- a cycle that cannot
		// safely re-apply a compaction it already promised the model must
		// not silently hand back the full, uncompacted baseline instead --
		// true in code, not just in the doc comment above.
		return nil, fmt.Errorf("%w: summarization could not find the already-committed compaction range (from=%s to=%s boundary=%s) in this cycle's own projection; refusing to re-apply", errADKUnsupportedBlock, fromID, toID, boundaryID)
	}
	result := make([]*einoschema.AgenticMessage, 0, len(originalMessages))
	summaryEmitted := false
	for i, msg := range originalMessages {
		if i == dropIndex {
			continue
		}
		if collapse[i] {
			if !summaryEmitted {
				result = append(result, summary)
				summaryEmitted = true
			}
			continue
		}
		result = append(result, msg)
	}
	return result, nil
}

// correlateDurableSubsequence resolves every originalMessages entry to its
// durable session.MessageID, if any, in TWO STRICT PASSES -- shared by
// summarizationFinalize (the first-commit path) and reapplyDurableSummary
// (later cycles re-applying an already-committed compaction), and the fix
// for round-four W6 correlation-and-seal review Important #1.
//
// Pass 1 resolves every message it can via sourceMessageID's pointer-keyed
// lookup (the fast, exact path) and marks the corresponding baseline slot
// consumed. Pass 2 then runs a content-based FALLBACK, but ONLY over
// messages pass 1 could not resolve, and ONLY against baseline slots pass 1
// did not already claim: baseline order is always preserved by every recipe
// in this package (messages are only ever inserted/appended around it,
// never reordered), so the next not-yet-consumed baseline entry -- if its
// content matches an unresolved message exactly -- is the same durable
// message under a new pointer.
//
// Running the fallback strictly after pass 1 (rather than interleaved, one
// message at a time) matters: an interleaved fallback lets an EPHEMERAL
// message (agentsmd/skill injected content with no durable id at all) that
// happens to match the content at the fallback cursor steal a DURABLE
// baseline slot before pass 1 has claimed it for the real message that
// actually owns it -- which the previous interleaved implementation could
// not distinguish from a genuine pointer-replaced durable message, and
// which defeated the very fail-closed check below (a genuinely absent
// durable message and a false consume from a coincidental duplicate
// canceled out in the unresolved count; round-four W6 correlation-and-seal
// review, Important #1 -- demonstrated as `PROBE2 RESULT: fail-closed check
// DEFEATED -- compacted and committed an epoch on a partial view`, and
// separately, with the pointer map intact, as one durable id resolving to
// two different messages with no error). Two strict passes close both --
// WHEN pointer correlation is available for the real owner: an ephemeral
// message then finds no unconsumed slot once the real durable message has
// already claimed its own via pass 1, and cannot mis-claim one pass 1
// already gave to someone else.
//
// This does NOT close the case where the pointer map is broken for the
// REAL owner too (an earlier handler cloned/replaced its pointer) AND an
// ephemeral duplicate with matching content is also present: with no
// pointer to resolve either message, pass 2 is all that runs, and content
// alone cannot distinguish "the real message under a new pointer" from "an
// ephemeral coincidental duplicate" -- whichever reaches the fallback
// cursor first wins the slot (round-four W6 correlation-and-seal review
// followup, Suggestion S-A). This is strictly narrower than the original
// finding (which needed no broken pointer at all) and is not silent in the
// case that matters most: with distinct content and a genuinely absent
// durable message, the fail-closed check below still fires correctly.
//
// unresolved (a baseline slot no message in either pass claimed) fails
// closed rather than compacting on a partial view -- the fabricated-slot
// hazard this whole function exists to close. This does not disambiguate
// two genuinely IDENTICAL adjacent baseline messages from each other, but
// that case is harmless: either assignment is correct, since durable ids
// are positional and durable order is preserved either way. The harmful
// case -- an ephemeral message stealing a DURABLE slot -- is what the
// strict ordering above closes.
func correlateDurableSubsequence(
	originalMessages []*einoschema.AgenticMessage,
	sourceMessageID func(msg *einoschema.AgenticMessage) (session.MessageID, bool),
	baselineMsgs []*einoschema.AgenticMessage,
	baselineIDs []session.MessageID,
) (sourceIDs []session.MessageID, durableOriginalIndices []int, err error) {
	baselineIndexByID := make(map[session.MessageID]int, len(baselineIDs))
	for idx, id := range baselineIDs {
		if id != "" {
			baselineIndexByID[id] = idx
		}
	}
	consumed := make([]bool, len(baselineMsgs))
	sourceIDs = make([]session.MessageID, len(originalMessages))
	durableOriginalIndices = make([]int, 0, len(originalMessages))
	unresolvedIndices := make([]int, 0, len(originalMessages))
	// Pass 1: pointer-keyed, exact.
	for i, msg := range originalMessages {
		if id, ok := sourceMessageID(msg); ok && id != "" {
			sourceIDs[i] = id
			durableOriginalIndices = append(durableOriginalIndices, i)
			if idx, ok := baselineIndexByID[id]; ok {
				consumed[idx] = true
			}
			continue
		}
		unresolvedIndices = append(unresolvedIndices, i)
	}
	// Pass 2: content fallback, only for what pass 1 could not resolve, and
	// only against baseline entries pass 1 did not already claim.
	fallbackCursor := 0
	for _, i := range unresolvedIndices {
		msg := originalMessages[i]
		for fallbackCursor < len(baselineMsgs) && consumed[fallbackCursor] {
			fallbackCursor++
		}
		if fallbackCursor < len(baselineMsgs) && reflect.DeepEqual(msg, baselineMsgs[fallbackCursor]) {
			sourceIDs[i] = baselineIDs[fallbackCursor]
			durableOriginalIndices = append(durableOriginalIndices, i)
			consumed[fallbackCursor] = true
			fallbackCursor++
		}
	}
	sort.Ints(durableOriginalIndices)
	var unresolved int
	for _, ok := range consumed {
		if !ok {
			unresolved++
		}
	}
	if unresolved > 0 {
		return nil, nil, fmt.Errorf("%w: summarization could not correlate %d of %d durable baseline messages to this cycle's projected messages (an earlier handler likely replaced message content/pointers without preserving identity); refusing to compact", errADKUnsupportedBlock, unresolved, len(baselineMsgs))
	}
	return sourceIDs, durableOriginalIndices, nil
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
// with this session's durable message history via build.sourceMessageID --
// a pointer-keyed lookup the current cycle's durable baseline handler
// records fresh every cycle (round-two W6 review item 8), NOT by
// re-deriving a separately-ordered projection and assuming it correlates
// positionally 1:1 with originalMessages. A message with no durable id
// (agentsmd/skill injected content, or anything else a host handler added
// that this run never durably committed) is simply never resolved here and
// is always retained -- present in originalMessages, but never counted
// toward or included in the summarized range. This is what makes
// summarization correct mid-turn after tool calls (this cycle's own new
// tool-loop progress resolves through the same map, since
// adkEngine.buildDurableBaseline threads its durable ids through exactly
// like the turn-admission prefix does) and with agentsmd/skill mounted
// (their injected messages simply resolve to "no durable id" instead of
// silently shifting every later index and failing Finalize closed).
func summarizationFinalize(build HandlerBuildContext, retainTail int) summarization.TypedFinalizeFunc[*einoschema.AgenticMessage] {
	return func(ctx context.Context, originalMessages []*einoschema.AgenticMessage, summary *einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
		summaryText := summaryAgenticMessageText(summary)
		if strings.TrimSpace(summaryText) == "" {
			return nil, fmt.Errorf("%w: summary text is empty", errADKUnsupportedBlock)
		}
		if build.sourceMessageID == nil || build.baselineMessages == nil {
			return nil, fmt.Errorf("%w: %w: summarization requires per-cycle durable source correlation (sourceMessageID/baselineMessages)", ErrHandlerConfiguration, errADKUnsupportedBlock)
		}
		baselineMsgs, baselineIDs := build.baselineMessages()
		sourceIDs, durableOriginalIndices, err := correlateDurableSubsequence(originalMessages, build.sourceMessageID, baselineMsgs, baselineIDs)
		if err != nil {
			return nil, err
		}
		if len(durableOriginalIndices) == 0 {
			// Nothing durable at all this cycle (a genuinely degenerate
			// case -- no baseline existed yet, e.g. the very first cycle
			// ever). unresolved == 0 here only because len(baselineMsgs)
			// == 0 too (the loop above would otherwise have forced a
			// failure). Nothing to summarize; leave history untouched.
			return originalMessages, nil
		}
		// A second pass over the FULL, unfiltered durable history (not
		// epoch-projected) gives role and content-block-kind lookups by
		// message ID for the tail-boundary logic below.
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
		// durable is the durable subsequence backing originalMessages, in
		// order -- possibly non-contiguous in originalMessages' own index
		// space (see durableOriginalIndices), but always contiguous and
		// correctly ordered as a CONVERSATION, since it is built purely
		// from resolved durable ids.
		durable := make([]session.Message, len(durableOriginalIndices))
		for k, origIndex := range durableOriginalIndices {
			msg, ok := byID[sourceIDs[origIndex]]
			if !ok {
				return nil, fmt.Errorf("%w: summarization correlated durable message %s not found in full history reload", errADKUnsupportedBlock, sourceIDs[origIndex])
			}
			durable[k] = msg
		}
		if retainTail < 0 {
			retainTail = 0
		}
		if retainTail >= len(durable) {
			// Nothing to compact: the requested tail already covers the
			// whole durable conversation. Do not start a new epoch (it
			// would have SummarizedFromID == SummarizedToID == TailStartID,
			// an overlapping, meaningless range) -- leave history untouched.
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
		boundaryIDs := compaction.BoundaryIDs{MessageID: build.epochs.ids.NewMessageID(), PartID: build.epochs.ids.NewPartID()}
		if _, _, err := build.epochs.commitSummaryEpoch(ctx, epoch, boundaryIDs, summaryText); err != nil {
			return nil, err
		}
		if build.markSummarized != nil {
			build.markSummarized(epoch.SummarizedFromID, epoch.SummarizedToID, boundaryIDs.MessageID, summary)
		}
		// Reassemble originalMessages by ORIGINAL index: every non-durable
		// message is always retained verbatim, wherever it falls; a
		// durable message whose durable position is within
		// [systemPrefixLen, tailStart) is replaced by exactly one copy of
		// summary (emitted the first time such a position is reached, so
		// a non-contiguous durable range still collapses to a single
		// summary message, not one per gap); every other durable message
		// (before systemPrefixLen, or at/after tailStart) is retained
		// verbatim.
		summarizedPositions := make(map[int]bool, tailStart-systemPrefixLen)
		for k := systemPrefixLen; k < tailStart; k++ {
			summarizedPositions[durableOriginalIndices[k]] = true
		}
		result := make([]*einoschema.AgenticMessage, 0, len(originalMessages))
		summaryEmitted := false
		for i, msg := range originalMessages {
			if summarizedPositions[i] {
				if !summaryEmitted {
					result = append(result, summary)
					summaryEmitted = true
				}
				continue
			}
			result = append(result, msg)
		}
		return result, nil
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
			return nil, fmt.Errorf("%w: %w: toolsearch requires at least one deferred tool in the frozen registry", ErrHandlerConfiguration, errADKUnsupportedBlock)
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
