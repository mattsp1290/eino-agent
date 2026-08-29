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
	return mustTestRunPlan(testToolPlanSpec(registry, nil))
}

func testPlanTools(registry staticToolRegistry) []PlanTool {
	capabilities := make([]PlanTool, len(registry.tools))
	for index, candidate := range registry.tools {
		tool, err := cloneToolChecked(candidate)
		if err != nil {
			panic(err)
		}
		identity := testToolIdentity(tool.Name)
		identity.Order = index
		capabilities[index] = PlanTool{
			Identity: identity,
			Resolve: func(context.Context, ToolScopeContext) (Tool, error) {
				return cloneToolChecked(tool)
			},
		}
	}
	return capabilities
}

func newTestToolPlanWithDispatch(registry staticToolRegistry, dispatch *extension.Plan) *RunPlan {
	return mustTestRunPlan(testToolPlanSpec(registry, dispatch))
}

func newTestDispatchPlan(dispatch *extension.Plan) *RunPlan {
	return mustTestRunPlan(testDispatchPlanSpec(dispatch))
}

func testDispatchComponents(dispatch *extension.Plan) []PlanComponent {
	return nil
}

func testDispatchPlanSpec(dispatch *extension.Plan) RunPlanSpec {
	return RunPlanSpec{Dispatch: dispatch, Components: testDispatchComponents(dispatch)}
}

func testToolPlanSpec(registry staticToolRegistry, dispatch *extension.Plan) RunPlanSpec {
	components := testDispatchComponents(dispatch)
	if tools := testPlanTools(registry); len(tools) > 0 {
		components = append(components, PlanComponent{Component: testPlanComponent("test-tools"), Tools: tools})
	}
	return RunPlanSpec{Dispatch: dispatch, Components: components}
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

func testGuardIdentityAt(id string, order int) session.GuardPlanIdentity {
	identity := testGuardIdentity(id)
	identity.Order = order
	return identity
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
	_, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{{Component: testPlanComponent("restrictions"), Restrictions: []PlanRestriction{{Identity: identity, Allowed: []string{"echo"}}}}}})
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
		Components: []PlanComponent{
			{Component: toolOwner, Tools: []PlanTool{{
				Identity: session.ToolPlanIdentity{Name: "echo", RegistrationID: "tool", Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor"},
				Resolve:  func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: "echo"}, nil },
			}}},
			{Component: promptOwner, Prompts: []PlanPrompt{{
				Identity: session.PromptPlanIdentity{Name: "prompt", RegistrationID: "prompt", Scope: extension.GlobalScope()},
				Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) {
					return "prompt", nil
				}),
			}}},
			{Component: guardOwner, Guards: []PlanGuard{{
				Identity: session.GuardPlanIdentity{RegistrationID: "guard", Scope: extension.GlobalScope()},
				Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
					return ToolGuardResult{Decision: ToolGuardAbstain}, nil
				}),
			}}},
			{Component: restrictionOwner, Restrictions: []PlanRestriction{{
				Identity: session.RestrictionPlanIdentity{RegistrationID: "restriction", RulesHash: rules.Hash, Scope: extension.GlobalScope()},
				Allowed:  rules.Allowed,
			}}},
		},
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

func TestNewRunPlanRejectsDuplicateComponentOwner(t *testing.T) {
	owner := testPlanComponent("owner")
	prompt := PlanPrompt{Identity: session.PromptPlanIdentity{Name: "prompt", RegistrationID: "prompt", Scope: extension.GlobalScope()}, Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "", nil })}
	_, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{{Component: owner, Prompts: []PlanPrompt{prompt}}, {Component: owner, Prompts: []PlanPrompt{prompt}}}})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("NewRunPlan error = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestNewRunPlanOrdersInterleavedComponentCapabilities(t *testing.T) {
	tool := func(name string, order int) PlanTool {
		return PlanTool{Identity: session.ToolPlanIdentity{Name: name, RegistrationID: name, Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor", Order: order}, Resolve: func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: name}, nil }}
	}
	prompt := func(name string, order int) PlanPrompt {
		return PlanPrompt{Identity: session.PromptPlanIdentity{Name: name, RegistrationID: name, Scope: extension.GlobalScope(), Order: order}, Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return name, nil })}
	}
	guard := func(name string, order int) PlanGuard {
		return PlanGuard{Identity: session.GuardPlanIdentity{RegistrationID: name, Scope: extension.GlobalScope(), Order: order}, Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})}
	}
	components := []PlanComponent{
		{Component: testPlanComponent("a"), Tools: []PlanTool{tool("a-1", 0), tool("a-3", 2)}, Prompts: []PlanPrompt{prompt("a-1", 0), prompt("a-3", 2)}, Guards: []PlanGuard{guard("a-1", 0), guard("a-3", 2)}},
		{Component: testPlanComponent("b"), Tools: []PlanTool{tool("b-2", 1)}, Prompts: []PlanPrompt{prompt("b-2", 1)}, Guards: []PlanGuard{guard("b-2", 1)}},
	}
	plan, err := NewRunPlan(RunPlanSpec{Components: components})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	if got := []string{plan.tools.capabilities[0].Identity.Name, plan.tools.capabilities[1].Identity.Name, plan.tools.capabilities[2].Identity.Name}; !reflect.DeepEqual(got, []string{"a-1", "b-2", "a-3"}) {
		t.Fatalf("tool order = %v", got)
	}
	if got := []string{plan.prompts[0].Name, plan.prompts[1].Name, plan.prompts[2].Name}; !reflect.DeepEqual(got, []string{"a-1", "b-2", "a-3"}) {
		t.Fatalf("prompt order = %v", got)
	}
	if got := []string{plan.guards[0].ID, plan.guards[1].ID, plan.guards[2].ID}; !reflect.DeepEqual(got, []string{"a-1", "b-2", "a-3"}) {
		t.Fatalf("guard order = %v", got)
	}
	permuted, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{components[1], components[0]}})
	if err != nil {
		t.Fatal(err)
	}
	defer permuted.Release()
	if permuted.Descriptor().Fingerprint != plan.Descriptor().Fingerprint {
		t.Fatalf("component permutation changed fingerprint: %s != %s", permuted.Descriptor().Fingerprint, plan.Descriptor().Fingerprint)
	}
	if got := []string{permuted.tools.capabilities[0].Identity.Name, permuted.tools.capabilities[1].Identity.Name, permuted.tools.capabilities[2].Identity.Name}; !reflect.DeepEqual(got, []string{"a-1", "b-2", "a-3"}) {
		t.Fatalf("permuted tool order = %v", got)
	}
}

func TestNewRunPlanOrdersGlobalGuardBeforeSessionGuard(t *testing.T) {
	guard := func(scope extension.Scope) PlanGuard {
		return PlanGuard{Identity: session.GuardPlanIdentity{RegistrationID: "guard", Scope: scope}, Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})}
	}
	plan, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{{
		Component: testPlanComponent("guard-owner"),
		Guards:    []PlanGuard{guard(extension.SessionScope("session")), guard(extension.GlobalScope())},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	if got := []extension.Scope{plan.guards[0].Scope, plan.guards[1].Scope}; !reflect.DeepEqual(got, []extension.Scope{extension.GlobalScope(), extension.SessionScope("session")}) {
		t.Fatalf("guard scope order = %#v", got)
	}
}

func TestNewRunPlanRejectsConflictingDispatchAndCapabilityOwner(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	mount, err := registry.Mount(context.Background(), testExtensionComponent("handler-owner"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, ModelRequestedPoint, extension.Registration{ID: "handler", Scope: extension.GlobalScope()}, func(context.Context, ModelRequestedNotice) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	owner := testPlanComponent("handler-owner")
	owner.Artifact.Hash = "conflicting"
	spec := RunPlanSpec{Dispatch: dispatch, Components: []PlanComponent{{
		Component: owner,
		Prompts: []PlanPrompt{{
			Identity: session.PromptPlanIdentity{Name: "prompt", RegistrationID: "prompt", Scope: extension.GlobalScope()},
			Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "", nil }),
		}},
	}}}
	if _, err := NewRunPlan(spec); !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("conflicting owner error = %v", err)
	}
	if err := mount.Close(context.Background()); err != nil {
		t.Fatalf("failed construction leaked dispatch lease: %v", err)
	}
}
