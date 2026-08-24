package runtime

import (
	"context"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

func mustTestRunPlan(spec RunPlanSpec) *RunPlan {
	plan, err := NewRunPlan(spec)
	if err != nil {
		panic(err)
	}
	return plan
}

func newTestToolPlan(registry staticToolRegistry) *RunPlan {
	return mustTestRunPlan(RunPlanSpec{Tools: testPlanTools(registry)})
}

func testPlanTools(registry staticToolRegistry) []PlanTool {
	capabilities := make([]PlanTool, len(registry.tools))
	for index, candidate := range registry.tools {
		tool, err := cloneToolChecked(candidate)
		if err != nil {
			panic(err)
		}
		capabilities[index] = PlanTool{
			Identity: testToolIdentity(tool.Name),
			Resolve: func(context.Context, ToolScopeContext) (Tool, error) {
				return cloneToolChecked(tool)
			},
		}
	}
	return capabilities
}

func newTestToolPlanWithDispatch(registry staticToolRegistry, dispatch *extension.Plan, release func()) *RunPlan {
	return mustTestRunPlan(RunPlanSpec{Tools: testPlanTools(registry), Dispatch: dispatch, Release: release})
}

func newTestDispatchPlan(dispatch *extension.Plan) *RunPlan {
	return mustTestRunPlan(RunPlanSpec{Dispatch: dispatch})
}

func configureTestTools(orchestrator *StreamingOrchestrator, registry staticToolRegistry) {
	orchestrator.plans = staticRunPlanProvider{plan: newTestToolPlan(registry)}
}

func testToolIdentity(name string) session.ExtensionPlanEntry {
	return session.ExtensionPlanEntry{
		InstanceID: "test-tools", Kind: session.ExtensionTool,
		Artifact: session.ArtifactIdentity{Name: "test-tools", Version: "1", Hash: "test-tools-hash", ConfigHash: "test-tools-config", SourceKind: string(extension.SourceNative)},
		Required: true, Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)}, CapabilityID: name + "/test",
	}
}

func testPromptIdentity(name, instance string) session.ExtensionPlanEntry {
	return session.ExtensionPlanEntry{
		InstanceID: instance, Kind: session.ExtensionPrompt,
		Artifact: session.ArtifactIdentity{Name: "test-prompts", Version: "1", Hash: "test-prompts-hash", ConfigHash: "test-prompts-config", SourceKind: string(extension.SourceNative)},
		Required: true, Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)}, CapabilityID: name + "/test",
	}
}

func testGuardIdentity(id string) session.ExtensionPlanEntry {
	return session.ExtensionPlanEntry{
		InstanceID: "test-guards", Kind: session.ExtensionGuard,
		Artifact: session.ArtifactIdentity{Name: "test-guards", Version: "1", Hash: "test-guards-hash", ConfigHash: "test-guards-config", SourceKind: string(extension.SourceNative)},
		Required: true, Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)}, CapabilityID: id + "/test",
	}
}

func testEchoPlanDescriptor() session.ExtensionPlanDescriptor {
	return newTestToolPlan(staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{}, nil
	})}}}).Descriptor()
}
