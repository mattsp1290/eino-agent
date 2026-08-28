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
			Component: testPlanComponent("test-tools"),
			Identity:  testToolIdentity(tool.Name),
			Resolve: func(context.Context, ToolScopeContext) (Tool, error) {
				return cloneToolChecked(tool)
			},
		}
	}
	return capabilities
}

func newTestToolPlanWithDispatch(registry staticToolRegistry, dispatch *extension.Plan) *RunPlan {
	return mustTestRunPlan(RunPlanSpec{Tools: testPlanTools(registry), Dispatch: dispatch})
}

func newTestDispatchPlan(dispatch *extension.Plan) *RunPlan {
	return mustTestRunPlan(RunPlanSpec{Dispatch: dispatch})
}

func configureTestTools(orchestrator *StreamingOrchestrator, registry staticToolRegistry) {
	orchestrator.plans = staticRunPlanProvider{plan: newTestToolPlan(registry)}
}

func testToolIdentity(name string) session.ToolPlanIdentity {
	return session.ToolPlanIdentity{
		Name: name, RegistrationID: "test", Scope: extension.GlobalScope(), SchemaHash: "test-schema", ExecutorHash: "test-executor",
	}
}

func testPromptIdentity(name, _ string, order int) session.PromptPlanIdentity {
	return session.PromptPlanIdentity{
		Name: name, RegistrationID: "test", Scope: extension.GlobalScope(), Order: order,
	}
}

func testGuardIdentity(id string) session.GuardPlanIdentity {
	return session.GuardPlanIdentity{
		RegistrationID: id, Scope: extension.GlobalScope(),
	}
}

func testPlanComponent(id string) extension.Component {
	return extension.Component{InstanceID: id, Artifact: extension.Artifact{Name: id, Version: "1", Hash: id + "-hash", ConfigHash: id + "-config", SourceKind: extension.SourceNative}}
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
		RegistrationID: "policy", RulesHash: "stale", Scope: extension.GlobalScope(),
	}
	_, err := NewRunPlan(RunPlanSpec{Restrictions: []PlanRestriction{{Component: testPlanComponent("restrictions"), Identity: identity, Allowed: []string{"echo"}}}})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("NewRunPlan error = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestNewRunPlanRetainsOwnersForHandlerlessCapabilities(t *testing.T) {
	rules, err := CanonicalizeRestrictionRules([]string{"echo"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	toolOwner := testPlanComponent("tool-only")
	promptOwner := testPlanComponent("prompt-only")
	guardOwner := testPlanComponent("guard-only")
	restrictionOwner := testPlanComponent("restriction-only")
	plan, err := NewRunPlan(RunPlanSpec{
		Tools: []PlanTool{{
			Component: toolOwner,
			Identity:  session.ToolPlanIdentity{Name: "echo", RegistrationID: "tool", Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor"},
			Resolve:   func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: "echo"}, nil },
		}},
		Prompts: []PlanPrompt{{
			Component: promptOwner,
			Identity:  session.PromptPlanIdentity{Name: "prompt", RegistrationID: "prompt", Scope: extension.GlobalScope()},
			Prompt: MountedPrompt{Name: "prompt", InstanceID: promptOwner.InstanceID, Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) {
				return "prompt", nil
			})},
		}},
		Guards: []PlanGuard{{
			Component: guardOwner,
			Identity:  session.GuardPlanIdentity{RegistrationID: "guard", Scope: extension.GlobalScope()},
			Guard: MountedToolGuard{ID: "guard", InstanceID: guardOwner.InstanceID, Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
				return ToolGuardResult{Decision: ToolGuardAbstain}, nil
			})},
		}},
		Restrictions: []PlanRestriction{{
			Component: restrictionOwner,
			Identity:  session.RestrictionPlanIdentity{RegistrationID: "restriction", RulesHash: rules.Hash, Scope: extension.GlobalScope()},
			Allowed:   rules.Allowed,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	descriptor := plan.Descriptor()
	if len(descriptor.Components) != 4 {
		t.Fatalf("descriptor components = %#v", descriptor.Components)
	}
	wantKinds := map[string]string{
		toolOwner.InstanceID:        "tool",
		promptOwner.InstanceID:      "prompt",
		guardOwner.InstanceID:       "guard",
		restrictionOwner.InstanceID: "restriction",
	}
	for _, component := range descriptor.Components {
		if component.Artifact != testPlanComponent(component.InstanceID).Artifact {
			t.Fatalf("component owner lost: %#v", component)
		}
		switch wantKinds[component.InstanceID] {
		case "tool":
			if len(component.Tools) != 1 {
				t.Fatalf("tool owner = %#v", component)
			}
		case "prompt":
			if len(component.Prompts) != 1 {
				t.Fatalf("prompt owner = %#v", component)
			}
		case "guard":
			if len(component.Guards) != 1 {
				t.Fatalf("guard owner = %#v", component)
			}
		case "restriction":
			if len(component.Restrictions) != 1 {
				t.Fatalf("restriction owner = %#v", component)
			}
		default:
			t.Fatalf("unexpected component = %#v", component)
		}
	}
}

func TestNewRunPlanRejectsBehaviorWithDifferentComponentOwner(t *testing.T) {
	owner := testPlanComponent("owner")
	_, err := NewRunPlan(RunPlanSpec{Prompts: []PlanPrompt{{
		Component: owner,
		Identity:  session.PromptPlanIdentity{Name: "prompt", RegistrationID: "prompt", Scope: extension.GlobalScope()},
		Prompt: MountedPrompt{Name: "prompt", InstanceID: "foreign", Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) {
			return "", nil
		})},
	}}})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("NewRunPlan error = %v, want ErrExtensionPlanMismatch", err)
	}
}
