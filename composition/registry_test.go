package composition

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

var compositionNotice = extension.NewNotification(extension.Contract{ID: "test/composition-notice", Version: "1"}, extension.NotificationContained, func(value string) string { return value })

func component(id string) extension.Component {
	return extension.Component{InstanceID: id, Artifact: extension.Artifact{Name: id, Version: "1", Hash: id + "-hash", ConfigHash: id + "-config", SourceKind: extension.SourceNative}}
}

func definition(name, marker string) tools.Definition {
	return tools.Definition{
		Name: name, Description: marker,
		Decode: func(_ context.Context, raw json.RawMessage) (any, error) {
			return append(json.RawMessage(nil), raw...), nil
		},
		Encode:  func(_ context.Context, value any) (json.RawMessage, error) { return value.(json.RawMessage), nil },
		Execute: func(context.Context, tools.Execution) (any, error) { return json.RawMessage(`{"ok":true}`), nil },
	}
}

func TestAtomicScopedCompositionAndStrictResume(t *testing.T) {
	registry := NewRegistry(nil)
	global, err := registry.Mount(context.Background(), component("global"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "notice", InstanceID: "global", Scope: extension.GlobalScope()}, func(context.Context, string) error { return nil }); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "echo-global", InstanceID: "global", Scope: extension.GlobalScope(), Definition: definition("echo", "global")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = global.Close(context.Background()) }()
	scoped, err := registry.Mount(context.Background(), component("scoped"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "echo-session", InstanceID: "scoped", Scope: extension.SessionScope("session-a"), Definition: definition("echo", "session")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scoped.Close(context.Background()) }()

	planA, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	toolsA, err := planA.Tools.ResolveTools(context.Background(), runtime.TurnSnapshot{SessionID: "session-a", Config: config.Snapshot{Model: model.Selection{}}})
	if err != nil || len(toolsA) != 1 || toolsA[0].Info.Desc != "session" {
		t.Fatalf("session tools = %#v, %v", toolsA, err)
	}
	persisted := planA.Descriptor.Clone()
	planA.Dispatch.Release()

	planB, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-b"})
	if err != nil {
		t.Fatal(err)
	}
	toolsB, _ := planB.Tools.ResolveTools(context.Background(), runtime.TurnSnapshot{SessionID: "session-b", Config: config.Snapshot{}})
	if len(toolsB) != 1 || toolsB[0].Info.Desc != "global" {
		t.Fatalf("other session tools = %#v", toolsB)
	}
	planB.Dispatch.Release()

	resumed, err := registry.AcquireResumePlan(context.Background(), persisted)
	if err != nil || resumed.Descriptor.Fingerprint != persisted.Fingerprint {
		t.Fatalf("AcquireResumePlan = %#v, %v", resumed, err)
	}
	resumed.Dispatch.Release()
}

func TestUnmountPreventsNewPlansAndDrainsExistingLease(t *testing.T) {
	registry := NewRegistry(nil)
	cleaned := false
	mount, err := registry.Mount(context.Background(), component("drain"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Defer(func(context.Context) error { cleaned = true; return nil }); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: "drain", Scope: extension.GlobalScope(), Definition: definition("tool", "tool")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	mount.Deactivate()
	newPlan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	resolved, _ := newPlan.Tools.ResolveTools(context.Background(), runtime.TurnSnapshot{})
	if len(resolved) != 0 {
		t.Fatalf("new plan retained tools: %#v", resolved)
	}
	newPlan.Dispatch.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := mount.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close before release = %v", err)
	}
	plan.Dispatch.Release()
	if err := mount.Close(context.Background()); err != nil || !cleaned {
		t.Fatalf("Close = %v cleaned=%t", err, cleaned)
	}
}

func TestResumeMismatchBeforePlanExecution(t *testing.T) {
	registry := NewRegistry(nil)
	mount, err := registry.Mount(context.Background(), component("mismatch"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: "mismatch", Scope: extension.GlobalScope(), Definition: definition("tool", "v1")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	persisted := plan.Descriptor.Clone()
	plan.Dispatch.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.AcquireResumePlan(context.Background(), persisted); !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		t.Fatalf("resume mismatch = %v", err)
	}
}

func TestStrictResumeRecoversSessionScopeFromNestedHandlerRegistrations(t *testing.T) {
	registry := NewRegistry(nil)
	component := component("mixed-handlers")
	mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "global", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(context.Context, string) error { return nil }); err != nil {
			return err
		}
		return extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "session", InstanceID: component.InstanceID, Scope: extension.SessionScope("session-a")}, func(context.Context, string) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	persisted := plan.Descriptor.Clone()
	plan.Dispatch.Release()
	if len(persisted.Entries) != 1 || persisted.Entries[0].Scope.Kind != string(extension.ScopeGlobal) || len(persisted.Entries[0].Registrations) != 2 {
		t.Fatalf("expected grouped mixed-scope handlers, got %#v", persisted.Entries)
	}

	resumed, err := registry.AcquireResumePlan(context.Background(), persisted)
	if err != nil {
		t.Fatalf("AcquireResumePlan = %v", err)
	}
	resumed.Dispatch.Release()
}

func TestStrictResumeRejectsConflictingNestedSessionScopes(t *testing.T) {
	registry := NewRegistry(nil)
	persisted := session.ExtensionPlanDescriptor{SchemaVersion: 1, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{
		InstanceID: "mixed", Kind: session.ExtensionHandlers, Required: true, Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)},
		Registrations: []session.RegistrationIdentity{
			{ID: "a", Contract: compositionNotice.Contract().ID, Version: compositionNotice.Contract().Version, Scope: session.ExtensionScope{Kind: string(extension.ScopeSession), Key: "session-a"}},
			{ID: "b", Contract: compositionNotice.Contract().ID, Version: compositionNotice.Contract().Version, Scope: session.ExtensionScope{Kind: string(extension.ScopeSession), Key: "session-b"}},
		},
	}}}
	persisted.Fingerprint, _ = session.FingerprintExtensionPlan(persisted)
	if _, err := registry.AcquireResumePlan(context.Background(), persisted); !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		t.Fatalf("conflicting nested scopes = %v", err)
	}
}

func TestPromptShadowRestrictionsAndGuardsFreezeTogether(t *testing.T) {
	registry := NewRegistry(nil)
	global, err := registry.Mount(context.Background(), component("cap-global"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Tool(ToolRegistration{ID: "a", InstanceID: "cap-global", Scope: extension.GlobalScope(), Definition: definition("a", "a")}); err != nil {
			return err
		}
		if err := registrar.Tool(ToolRegistration{ID: "b", InstanceID: "cap-global", Scope: extension.GlobalScope(), Definition: definition("b", "b")}); err != nil {
			return err
		}
		if err := registrar.Prompt(PromptRegistration{ID: "prompt", InstanceID: "cap-global", Name: "policy", Scope: extension.GlobalScope(), Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) { return "global", nil })}); err != nil {
			return err
		}
		return registrar.Guard(GuardRegistration{ID: "guard", InstanceID: "cap-global", Scope: extension.GlobalScope(), Guard: runtime.ToolGuardFunc(func(context.Context, runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
			return runtime.ToolGuardResult{Decision: runtime.ToolGuardAbstain}, nil
		})})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = global.Close(context.Background()) }()
	sessionMount, err := registry.Mount(context.Background(), component("cap-session"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Prompt(PromptRegistration{ID: "prompt", InstanceID: "cap-session", Name: "policy", Scope: extension.SessionScope("session-a"), Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) { return "session", nil })}); err != nil {
			return err
		}
		return registrar.RestrictTools(RestrictionRegistration{ID: "only-b", InstanceID: "cap-session", Scope: extension.SessionScope("session-a"), Allowed: []string{"b"}})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessionMount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Dispatch.Release()
	resolved, err := plan.Tools.ResolveTools(context.Background(), runtime.TurnSnapshot{SessionID: "session-a", Config: config.Snapshot{}})
	if err != nil || len(resolved) != 1 || resolved[0].Name != "b" || len(plan.Prompts) != 1 || plan.Prompts[0].InstanceID != "cap-session" || len(plan.Guards) != 1 {
		t.Fatalf("plan tools=%#v prompts=%#v guards=%#v err=%v", resolved, plan.Prompts, plan.Guards, err)
	}
	var kinds = map[session.ExtensionKind]bool{}
	for _, entry := range plan.Descriptor.Entries {
		kinds[entry.Kind] = true
	}
	for _, kind := range []session.ExtensionKind{session.ExtensionTool, session.ExtensionPrompt, session.ExtensionGuard, session.ExtensionRestriction} {
		if !kinds[kind] {
			t.Fatalf("descriptor missing %s: %#v", kind, plan.Descriptor)
		}
	}
}
