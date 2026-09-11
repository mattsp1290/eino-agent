// Package agenticmiddleware demonstrates wiring every W6 typed ADK
// middleware recipe (agentsmd, skill, filesystem, plantask, patchtoolcalls,
// reduction, summarization, dynamictool/toolsearch) through the public
// composition.Registrar.Handler surface, and exercises the resulting turns
// end to end through the public runtime.StreamingOrchestrator -- never by
// reaching into the runtime package's own internals.
//
// Mount installs all eight recipes, session-scoped, with the bounded
// configuration this example's tests exercise. A real application would
// typically mount a smaller, purposeful subset; this example mounts all
// eight so every recipe's wiring is visible in one place.
package agenticmiddleware

import (
	"context"
	"encoding/json"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// Config selects which recipes Mount installs and their per-recipe
// settings. Zero-value fields fall back to the bounded defaults used by
// this example's positive-path tests.
type Config struct {
	AgentsMDFiles              []string
	SkillToolName              string
	FilesystemUseMultiModal    bool
	PatchToolCallsPatchedText  string
	ReductionMaxLengthForTrunc int
	ReductionMaxTokensForClear int64
	SummarizationTriggerMsgs   int
	SummarizationRetainTail    int
	ToolSearchUseModelNative   bool

	// Disable skips mounting the recipe whose HandlerKind* name appears
	// here (see runtime.HandlerKindAgentsMD etc.), so a test can mount a
	// deliberately narrower set (e.g. to exercise one recipe's own
	// missing-backend failure without every other recipe also failing).
	Disable map[string]bool
}

// Mount installs the selected typed ADK handler recipes for exactly one
// durable session. Deactivate (via the returned *composition.Mount) stops
// new plans from selecting it; Close waits for already-admitted plans.
func Mount(ctx context.Context, registry *composition.Registry, sessionID session.ID, cfg Config) (*composition.Mount, error) {
	if len(cfg.AgentsMDFiles) == 0 {
		cfg.AgentsMDFiles = []string{"AGENTS.md"}
	}
	if cfg.ReductionMaxLengthForTrunc == 0 {
		cfg.ReductionMaxLengthForTrunc = 64
	}
	if cfg.ReductionMaxTokensForClear == 0 {
		cfg.ReductionMaxTokensForClear = 1 << 30
	}
	if cfg.SummarizationTriggerMsgs == 0 {
		cfg.SummarizationTriggerMsgs = 1 << 30 // effectively never, unless a test overrides it
	}

	scope := extension.SessionScope(string(sessionID))
	component := extension.Component{
		InstanceID: "example.agentic-middleware/" + string(sessionID),
		Artifact: extension.Artifact{
			Name: "example-agentic-middleware", Version: "1.0.0",
			Hash: "example-agentic-middleware-artifact-v1", ConfigHash: "example-agentic-middleware-config-v1",
			SourceKind: extension.SourceNative,
		},
	}
	return registry.Mount(ctx, component, composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
		order := 0
		// register seals configValue (the same value the corresponding
		// NewXHandlerFactory call below is built from) into
		// HandlerDescriptor.Config, marshaled to canonical JSON -- this is
		// what session.AgentHandlerPlanIdentity.ConfigHash is derived from
		// (composition.handlerConfigHash), NOT the factory closure itself
		// (which is never inspectable for hashing). A caller that changes a
		// recipe's real behavior without also updating what it declares
		// here would seal a fingerprint that does not actually reflect
		// that behavior; this Mount keeps the two in lockstep by
		// construction.
		register := func(id, kind string, configValue any, factory runtime.HandlerFactory) error {
			if cfg.Disable[kind] {
				return nil
			}
			order++
			configJSON, err := json.Marshal(configValue)
			if err != nil {
				return err
			}
			return registrar.Handler(composition.HandlerRegistration{
				ID: id, Order: order, Scope: scope,
				Descriptor: composition.HandlerDescriptor{Kind: kind, Version: runtime.HandlerVersion1, Config: configJSON},
				Factory:    factory,
			})
		}
		agentsMDConfig := runtime.AgentsMDConfig{AgentsMDFiles: cfg.AgentsMDFiles}
		if err := register("agentsmd", runtime.HandlerKindAgentsMD, agentsMDConfig,
			runtime.NewAgentsMDHandlerFactory(agentsMDConfig)); err != nil {
			return err
		}
		skillConfig := runtime.SkillConfig{SkillToolName: cfg.SkillToolName}
		if err := register("skill", runtime.HandlerKindSkill, skillConfig,
			runtime.NewSkillHandlerFactory(skillConfig)); err != nil {
			return err
		}
		filesystemConfig := runtime.FilesystemConfig{UseMultiModalRead: cfg.FilesystemUseMultiModal}
		if err := register("filesystem", runtime.HandlerKindFilesystem, filesystemConfig,
			runtime.NewFilesystemHandlerFactory(filesystemConfig)); err != nil {
			return err
		}
		if err := register("plantask", runtime.HandlerKindPlanTask, runtime.PlanTaskConfig{},
			runtime.NewPlanTaskHandlerFactory(runtime.PlanTaskConfig{})); err != nil {
			return err
		}
		patchToolCallsConfig := runtime.PatchToolCallsConfig{PatchedText: cfg.PatchToolCallsPatchedText}
		if err := register("patchtoolcalls", runtime.HandlerKindPatchToolCalls, patchToolCallsConfig,
			runtime.NewPatchToolCallsHandlerFactory(patchToolCallsConfig)); err != nil {
			return err
		}
		reductionConfig := runtime.ReductionConfig{
			MaxLengthForTrunc: cfg.ReductionMaxLengthForTrunc, MaxTokensForClear: cfg.ReductionMaxTokensForClear,
		}
		if err := register("reduction", runtime.HandlerKindReduction, reductionConfig,
			runtime.NewReductionHandlerFactory(reductionConfig)); err != nil {
			return err
		}
		summarizationConfig := runtime.SummarizationConfig{
			TriggerContextMessages: cfg.SummarizationTriggerMsgs, RetainTailCount: cfg.SummarizationRetainTail,
		}
		if err := register("summarization", runtime.HandlerKindSummarization, summarizationConfig,
			runtime.NewSummarizationHandlerFactory(summarizationConfig)); err != nil {
			return err
		}
		toolSearchConfig := runtime.ToolSearchHandlerConfig{UseModelToolSearch: cfg.ToolSearchUseModelNative}
		if err := register("toolsearch", runtime.HandlerKindToolSearch, toolSearchConfig,
			runtime.NewToolSearchHandlerFactory(toolSearchConfig)); err != nil {
			return err
		}
		return nil
	}))
}
