package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

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

func testToolIdentity(name string) session.ToolPlanIdentity {
	return session.ToolPlanIdentity{
		InstanceID: "test-tools",
		Artifact:   session.ArtifactIdentity{Name: "test-tools", Version: "1", Hash: "test-tools-hash", ConfigHash: "test-tools-config", SourceKind: string(extension.SourceNative)},
		Name:       name, RegistrationID: "test", Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)}, SchemaHash: "test-schema", ExecutorHash: "test-executor",
	}
}

func testPromptIdentity(name, instance string, order int) session.PromptPlanIdentity {
	return session.PromptPlanIdentity{
		InstanceID: instance,
		Artifact:   session.ArtifactIdentity{Name: "test-prompts", Version: "1", Hash: "test-prompts-hash", ConfigHash: "test-prompts-config", SourceKind: string(extension.SourceNative)},
		Name:       name, RegistrationID: "test", Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)}, Order: order,
	}
}

func testGuardIdentity(id string) session.GuardPlanIdentity {
	return session.GuardPlanIdentity{
		InstanceID:     "test-guards",
		Artifact:       session.ArtifactIdentity{Name: "test-guards", Version: "1", Hash: "test-guards-hash", ConfigHash: "test-guards-config", SourceKind: string(extension.SourceNative)},
		RegistrationID: id, Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)},
	}
}

func emptyTestRunPlanProvider() RunPlanProvider {
	return staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{})}
}

func emptyTestPlanDescriptor() session.ExtensionPlanDescriptor {
	return mustTestRunPlan(RunPlanSpec{}).Descriptor()
}

func testEchoPlanDescriptor() session.ExtensionPlanDescriptor {
	return newTestToolPlan(staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{}, nil
	})}}}).Descriptor()
}

func TestCanonicalizeRestrictionRules(t *testing.T) {
	first, err := CanonicalizeRestrictionRules([]string{"zeta", "alpha", "zeta"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CanonicalizeRestrictionRules([]string{"alpha", "zeta"}, []string{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Allowed, []string{"alpha", "zeta"}) || first.Denied != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("canonical rules differ: first=%#v second=%#v", first, second)
	}
}

func TestCanonicalizeRestrictionRulesRejectsInvalidPolicies(t *testing.T) {
	tests := map[string]struct {
		allowed []string
		denied  []string
	}{
		"empty":   {},
		"blank":   {allowed: []string{" "}},
		"overlap": {allowed: []string{"echo"}, denied: []string{"echo"}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := CanonicalizeRestrictionRules(test.allowed, test.denied); err == nil {
				t.Fatal("invalid restriction policy was accepted")
			}
		})
	}
}

func TestNewRunPlanRejectsRestrictionHashThatDoesNotMatchCanonicalRules(t *testing.T) {
	identity := session.RestrictionPlanIdentity{
		InstanceID: "restrictions",
		Artifact: session.ArtifactIdentity{
			Name: "restrictions", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: string(extension.SourceNative),
		},
		RegistrationID: "policy", RulesHash: "stale", Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)},
	}
	_, err := NewRunPlan(RunPlanSpec{Restrictions: []PlanRestriction{{Identity: identity, Allowed: []string{"echo"}}}})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("NewRunPlan error = %v, want ErrExtensionPlanMismatch", err)
	}
}
