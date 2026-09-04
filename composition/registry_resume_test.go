package composition

import (
	"context"
	"errors"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

func TestResumeMismatchBeforePlanExecution(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), component("mismatch"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: definition("tool", "v1")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	persisted := plan.Descriptor()
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertResumePlanDrift(t, registry, "session-a", persisted)
}

func TestCapabilityPlanIdentityComesFromMountedComponent(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mounted := component("canonical-capability")
	mount, err := registry.Mount(context.Background(), mounted, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: definition("echo", "v1")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	descriptor := plan.Descriptor()
	owned := descriptorComponent(descriptor, mounted.InstanceID)
	if owned == nil || owned.Artifact != mounted.Artifact || len(owned.Tools) != 1 {
		t.Fatalf("plan descriptor = %#v", descriptor)
	}
}

func TestStrictResumeRejectsChangedConvertedToolSchema(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	component := component("schema-fingerprint")
	mountSchema := func(parameterType einoschema.DataType) *Mount {
		mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
			tool := definition("schema-tool", "stable")
			tool.Parameters = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
				"value": {Type: parameterType, Required: true},
			})
			return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: tool})
		}))
		if err != nil {
			t.Fatal(err)
		}
		return mount
	}

	firstMount := mountSchema(einoschema.String)
	first, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	persisted := first.Descriptor()
	first.Release()
	if err := firstMount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	secondMount := mountSchema(einoschema.Integer)
	defer func() { _ = secondMount.Close(context.Background()) }()
	second, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	secondDescriptor := second.Descriptor()
	if persisted.Fingerprint == secondDescriptor.Fingerprint || persisted.Components[0].Tools[0].SchemaHash == secondDescriptor.Components[0].Tools[0].SchemaHash {
		t.Fatalf("changed schemas collided: persisted=%#v current=%#v", persisted, secondDescriptor)
	}
	assertResumePlanDrift(t, registry, "session-a", persisted)
}

func TestStrictResumeRejectsChangedToolRuntimePolicy(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tools.Definition)
	}{
		{name: "max inline bytes", mutate: func(definition *tools.Definition) { definition.Retention.MaxInlineBytes++ }},
		{name: "external storage", mutate: func(definition *tools.Definition) { definition.Retention.StoreExternal = false }},
		{name: "redaction", mutate: func(definition *tools.Definition) { definition.Retention.Redact = false }},
		{name: "metadata", mutate: func(definition *tools.Definition) { definition.Metadata["policy"] = "changed" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := NewRegistry(nil, compositionNotice)
			if err != nil {
				t.Fatal(err)
			}
			component := component("policy-fingerprint")
			newDefinition := func() tools.Definition {
				tool := definition("policy-tool", "stable")
				tool.Retention = runtime.RetentionPolicy{MaxInlineBytes: 1024, StoreExternal: true, Redact: true}
				tool.Metadata = map[string]string{"policy": "stable", "source": "test"}
				return tool
			}
			mountDefinition := func(tool tools.Definition) *Mount {
				mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
					return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: tool})
				}))
				if err != nil {
					t.Fatal(err)
				}
				return mount
			}

			firstMount := mountDefinition(newDefinition())
			first, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			persisted := first.Descriptor()
			first.Release()
			if err := firstMount.Close(context.Background()); err != nil {
				t.Fatal(err)
			}

			changed := newDefinition()
			test.mutate(&changed)
			secondMount := mountDefinition(changed)
			defer func() { _ = secondMount.Close(context.Background()) }()
			second, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			defer second.Release()
			secondDescriptor := second.Descriptor()
			if persisted.Fingerprint == secondDescriptor.Fingerprint || persisted.Components[0].Tools[0].SchemaHash == secondDescriptor.Components[0].Tools[0].SchemaHash {
				t.Fatalf("changed policy collided: persisted=%#v current=%#v", persisted, secondDescriptor)
			}
			assertResumePlanDrift(t, registry, "session-a", persisted)
		})
	}
}

func TestStrictResumeCanonicalizesRestrictionRuleSets(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mountedComponent := component("restriction-canonical")
	mountRules := func(allowed, denied []string) *Mount {
		mount, err := registry.Mount(context.Background(), mountedComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
			return registrar.RestrictTools(RestrictionRegistration{ID: "policy", Scope: extension.GlobalScope(), Allowed: allowed, Denied: denied})
		}))
		if err != nil {
			t.Fatal(err)
		}
		return mount
	}

	firstMount := mountRules([]string{"zeta", "alpha", "zeta"}, nil)
	first, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	persisted := first.Descriptor()
	first.Release()
	if err := firstMount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	equivalentMount := mountRules([]string{"alpha", "zeta"}, []string{})
	resumed, err := registry.AcquireResumePlan(context.Background(), resumeRequest(t, "session-a", persisted))
	if err != nil {
		t.Fatalf("equivalent reordered rules did not resume: %v", err)
	}
	resumed.Release()
	if err := equivalentMount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	changedMount := mountRules([]string{"alpha"}, nil)
	defer func() { _ = changedMount.Close(context.Background()) }()
	assertResumePlanDrift(t, registry, "session-a", persisted)
}

func TestAcquireResumePlanRejectsTamperedPersistedDescriptorBeforeSelection(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), component("tamper-check"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "echo", Scope: extension.GlobalScope(), Definition: definition("echo", "stable")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	persisted := plan.Descriptor()
	plan.Release()
	persisted.Components[0].Artifact.Hash = "tampered"
	if _, err := session.VerifyExtensionPlanForSession("session-a", persisted); err == nil {
		t.Fatal("tampered descriptor unexpectedly verified")
	}
}

func TestAcquireResumePlanRejectsDurableSessionMismatch(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), component("resume-session"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "session-tool", Scope: extension.SessionScope("session-a"), Definition: definition("session-tool", "ok")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	plan.Release()
	verified := resumeRequest(t, "session-a", descriptor)
	if resumed, err := registry.AcquireResumePlan(context.Background(), runtime.ResumePlanRequest{SessionID: "session-b", Plan: verified.Plan}); !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		if resumed != nil {
			resumed.Release()
		}
		t.Fatalf("AcquireResumePlan = %v, want mismatch", err)
	}
}

func TestStrictResumeRecoversSessionScopeFromNestedHandlerRegistrations(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	component := component("mixed-handlers")
	mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "global", Scope: extension.GlobalScope()}, func(context.Context, string) error { return nil }); err != nil {
			return err
		}
		return extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "session", Scope: extension.SessionScope("session-a")}, func(context.Context, string) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	persisted := plan.Descriptor()
	plan.Release()
	owned := descriptorComponent(persisted, component.InstanceID)
	if owned == nil || len(owned.Handlers) != 2 {
		t.Fatalf("expected grouped mixed-scope handlers, got %#v", persisted.Components)
	}

	resumed, err := registry.AcquireResumePlan(context.Background(), resumeRequest(t, "session-a", persisted))
	if err != nil {
		t.Fatalf("AcquireResumePlan = %v", err)
	}
	resumed.Release()
}

func TestStrictResumeRejectsConflictingNestedSessionScopes(t *testing.T) {
	persisted := session.ExtensionPlanDescriptor{Components: []session.ComponentPlan{{
		InstanceID: "mixed", Artifact: extension.Artifact{Name: "mixed", Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: extension.SourceNative},
		Handlers: []session.RegistrationIdentity{
			{ID: "a", Contract: compositionNotice.Contract().ID, Version: compositionNotice.Contract().Version, Scope: extension.SessionScope("session-a"), Kind: extension.HandlerNotification},
			{ID: "b", Contract: compositionNotice.Contract().ID, Version: compositionNotice.Contract().Version, Scope: extension.SessionScope("session-b"), Kind: extension.HandlerNotification},
		},
	}}}
	if _, err := session.SealExtensionPlanForSession("session-a", persisted); err == nil {
		t.Fatal("conflicting nested scopes unexpectedly sealed")
	}
}

func TestPromptShadowRestrictionsAndGuardsFreezeTogether(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	global, err := registry.Mount(context.Background(), component("cap-global"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Tool(ToolRegistration{ID: "a", Scope: extension.GlobalScope(), Definition: definition("a", "a")}); err != nil {
			return err
		}
		if err := registrar.Tool(ToolRegistration{ID: "b", Scope: extension.GlobalScope(), Definition: definition("b", "b")}); err != nil {
			return err
		}
		if err := registrar.Prompt(PromptRegistration{ID: "prompt", Name: "policy", Scope: extension.GlobalScope(), Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) { return "global", nil })}); err != nil {
			return err
		}
		return registrar.Guard(GuardRegistration{ID: "guard", Scope: extension.GlobalScope(), Guard: runtime.ToolGuardFunc(func(context.Context, runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
			return runtime.ToolGuardResult{Decision: runtime.ToolGuardAbstain}, nil
		})})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = global.Close(context.Background()) }()
	sessionMount, err := registry.Mount(context.Background(), component("cap-session"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Prompt(PromptRegistration{ID: "prompt", Name: "policy", Scope: extension.SessionScope("session-a"), Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) { return "session", nil })}); err != nil {
			return err
		}
		return registrar.RestrictTools(RestrictionRegistration{ID: "only-b", Scope: extension.SessionScope("session-a"), Allowed: []string{"b"}})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessionMount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	prompts, guards := plan.Prompts(), plan.Guards()
	if err != nil || len(resolved) != 1 || resolved[0].Name != "b" || len(prompts) != 1 || prompts[0].InstanceID != "cap-session" || len(guards) != 1 {
		t.Fatalf("plan tools=%#v prompts=%#v guards=%#v err=%v", resolved, prompts, guards, err)
	}
	descriptor := plan.Descriptor()
	globalPlan := descriptorComponent(descriptor, "cap-global")
	sessionPlan := descriptorComponent(descriptor, "cap-session")
	if globalPlan == nil || sessionPlan == nil || len(globalPlan.Tools) != 2 || len(sessionPlan.Prompts) != 1 || len(globalPlan.Guards) != 1 || len(sessionPlan.Restrictions) != 1 {
		t.Fatalf("descriptor is missing a typed capability collection: %#v", descriptor)
	}
}

func TestPromptAndGuardOrderParticipateInStrictFingerprint(t *testing.T) {
	for _, kind := range []string{"prompt", "guard"} {
		t.Run(kind, func(t *testing.T) {
			registry, err := NewRegistry(nil, compositionNotice)
			if err != nil {
				t.Fatal(err)
			}
			mountOrdered := func(order int) *Mount {
				mount, err := registry.Mount(context.Background(), component("ordered"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
					switch kind {
					case "prompt":
						return registrar.Prompt(PromptRegistration{ID: "prompt", Name: "policy", Order: order, Scope: extension.GlobalScope(), Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) { return "policy", nil })})
					default:
						return registrar.Guard(GuardRegistration{ID: "guard", Order: order, Scope: extension.GlobalScope(), Guard: runtime.ToolGuardFunc(func(context.Context, runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
							return runtime.ToolGuardResult{Decision: runtime.ToolGuardAbstain}, nil
						})})
					}
				}))
				if err != nil {
					t.Fatal(err)
				}
				return mount
			}

			firstMount := mountOrdered(10)
			first, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			persisted := first.Descriptor()
			first.Release()
			entryOrder := 0
			owned := descriptorComponent(persisted, "ordered")
			if owned == nil {
				t.Fatalf("ordered descriptor = %#v", persisted)
			}
			if kind == "prompt" {
				entryOrder = owned.Prompts[0].Order
			} else {
				entryOrder = owned.Guards[0].Order
			}
			if entryOrder != 10 || persisted.Fingerprint == "" {
				t.Fatalf("ordered descriptor = %#v", persisted)
			}
			if err := firstMount.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			secondMount := mountOrdered(20)
			defer func() { _ = secondMount.Close(context.Background()) }()
			assertResumePlanDrift(t, registry, "session-a", persisted)
		})
	}
}
