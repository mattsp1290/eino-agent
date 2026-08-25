package composition

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

var compositionNotice = extension.NewNotification(extension.Contract{ID: "test/composition-notice", Version: "1"}, extension.NotificationContained, func(value string) (string, error) { return value, nil })

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

func TestMountRejectsInvalidToolDefinitionsAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tools.Definition)
	}{
		{name: "missing decoder", mutate: func(definition *tools.Definition) { definition.Decode = nil }},
		{name: "missing encoder", mutate: func(definition *tools.Definition) { definition.Encode = nil }},
		{name: "missing executor", mutate: func(definition *tools.Definition) { definition.Execute = nil }},
		{name: "malformed schema", mutate: func(definition *tools.Definition) {
			definition.Parameters = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"broken": nil})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry(nil)
			component := component("invalid-tool")
			invalid := definition("broken", "broken")
			test.mutate(&invalid)
			_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
				return registrar.Tool(ToolRegistration{ID: "broken", InstanceID: component.InstanceID, Scope: extension.GlobalScope(), Definition: invalid})
			}))
			if !errors.Is(err, tools.ErrInvalidDefinition) {
				t.Fatalf("Mount error = %v, want ErrInvalidDefinition", err)
			}
			if len(registry.tools) != 0 {
				t.Fatalf("mounted tools = %d, want 0", len(registry.tools))
			}
			plan, planErr := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if planErr != nil {
				t.Fatalf("AcquireRunPlan error = %v", planErr)
			}
			defer plan.Release()
			if len(plan.Descriptor().Entries) != 0 {
				t.Fatalf("plan entries = %#v, want none", plan.Descriptor().Entries)
			}
		})
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
		return registrar.Tool(ToolRegistration{ID: "echo-session", InstanceID: "scoped", Scope: extension.SessionScope("session-a"), Definition: definition("session_echo", "session")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scoped.Close(context.Background()) }()

	planA, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	toolsA, err := planA.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	if err != nil || len(toolsA) != 2 || toolsA[0].Info.Desc != "global" || toolsA[1].Info.Desc != "session" {
		t.Fatalf("session tools = %#v, %v", toolsA, err)
	}
	persisted := planA.Descriptor()
	planA.Release()

	planB, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-b"})
	if err != nil {
		t.Fatal(err)
	}
	toolsB, _ := planB.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-b"})
	if len(toolsB) != 1 || toolsB[0].Info.Desc != "global" {
		t.Fatalf("other session tools = %#v", toolsB)
	}
	planB.Release()

	resumed, err := registry.AcquireResumePlan(context.Background(), persisted)
	if err != nil || resumed.Descriptor().Fingerprint != persisted.Fingerprint {
		t.Fatalf("AcquireResumePlan = %#v, %v", resumed, err)
	}
	resumed.Release()
}

func TestMountAcceptsOpaqueSessionScopeKeys(t *testing.T) {
	for index, key := range []string{"user@example.com==", strings.Repeat("opaque", 60) + "=="} {
		registry := NewRegistry(nil)
		mountedComponent := component("opaque-scope-" + string(rune('a'+index)))
		mount, err := registry.Mount(context.Background(), mountedComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
			if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "notice", InstanceID: mountedComponent.InstanceID, Scope: extension.SessionScope(key)}, func(context.Context, string) error { return nil }); err != nil {
				return err
			}
			return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: mountedComponent.InstanceID, Scope: extension.SessionScope(key), Definition: definition("echo", "opaque")})
		}))
		if err != nil {
			t.Fatalf("Mount(%q) = %v", key, err)
		}

		plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: session.ID(key)})
		if err != nil {
			t.Fatalf("AcquireRunPlan(%q) = %v", key, err)
		}
		resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: session.ID(key)})
		if err != nil || len(resolved) != 1 || resolved[0].Name != "echo" {
			t.Fatalf("matching scope tools = %#v, %v", resolved, err)
		}
		plan.Release()

		other, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "other"})
		if err != nil {
			t.Fatal(err)
		}
		resolved, err = other.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "other"})
		if err != nil || len(resolved) != 0 {
			t.Fatalf("other scope tools = %#v, %v", resolved, err)
		}
		other.Release()
		if err := mount.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAcquireRunPlanFreezesRequestedToolSelection(t *testing.T) {
	registry := NewRegistry(nil)
	component := component("selected-tools")
	mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Tool(ToolRegistration{ID: "a", InstanceID: component.InstanceID, Scope: extension.GlobalScope(), Definition: definition("a", "a")}); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "b", InstanceID: component.InstanceID, Scope: extension.GlobalScope(), Definition: definition("b", "b")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	tests := []struct {
		name      string
		config    config.ToolConfig
		wantTools []string
	}{
		{name: "disabled wins", config: config.ToolConfig{Enabled: []string{"a", "b"}, Disabled: []string{"b"}}, wantTools: []string{"a"}},
		{name: "explicit empty enabled", config: config.ToolConfig{Enabled: []string{}}, wantTools: []string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{Config: config.Snapshot{Tools: test.config}})
			if err != nil {
				t.Fatal(err)
			}
			defer plan.Release()
			resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{})
			if err != nil {
				t.Fatal(err)
			}
			gotTools := make([]string, len(resolved))
			for index, tool := range resolved {
				gotTools[index] = tool.Name
			}
			if !reflect.DeepEqual(gotTools, test.wantTools) {
				t.Fatalf("resolved tools = %v, want %v", gotTools, test.wantTools)
			}
			var descriptorTools int
			for _, entry := range plan.Descriptor().Entries {
				if kind, _ := entry.Kind(); kind == session.ExtensionTool {
					descriptorTools++
				}
			}
			if descriptorTools != len(test.wantTools) {
				t.Fatalf("descriptor tools=%d, want %d", descriptorTools, len(test.wantTools))
			}
		})
	}
}

func TestMountRejectsGlobalAndSessionToolNameCollision(t *testing.T) {
	registry := NewRegistry(nil)
	baseComponent := component("scope-collision")
	_, err := registry.Mount(context.Background(), baseComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Tool(ToolRegistration{ID: "global", InstanceID: baseComponent.InstanceID, Scope: extension.GlobalScope(), Definition: definition("echo", "global")}); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "session", InstanceID: baseComponent.InstanceID, Scope: extension.SessionScope("session-a"), Definition: definition("echo", "session")})
	}))
	if !errors.Is(err, tools.ErrDuplicateRegistration) {
		t.Fatalf("Mount error = %v, want ErrDuplicateRegistration", err)
	}
	if len(registry.tools) != 0 {
		t.Fatalf("collision mount published %d tools", len(registry.tools))
	}
}

func TestMountRejectsCrossMountToolCollisionAndRemainsReusable(t *testing.T) {
	registry := NewRegistry(nil)
	globalComponent := component("global-collision")
	global, err := registry.Mount(context.Background(), globalComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "global", InstanceID: globalComponent.InstanceID, Scope: extension.GlobalScope(), Definition: definition("echo", "global")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = global.Close(context.Background()) }()
	sessionComponent := component("session-collision")
	_, err = registry.Mount(context.Background(), sessionComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "session", InstanceID: sessionComponent.InstanceID, Scope: extension.SessionScope("session-a"), Definition: definition("echo", "session")})
	}))
	if !errors.Is(err, tools.ErrDuplicateRegistration) {
		t.Fatalf("collision Mount error = %v", err)
	}
	validComponent := component("valid-after-collision")
	valid, err := registry.Mount(context.Background(), validComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "valid", InstanceID: validComponent.InstanceID, Scope: extension.GlobalScope(), Definition: definition("other", "valid")})
	}))
	if err != nil {
		t.Fatalf("valid Mount after collision = %v", err)
	}
	defer func() { _ = valid.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	if err != nil || len(resolved) != 2 || resolved[0].Name != "echo" || resolved[1].Name != "other" {
		t.Fatalf("resolved tools after collision = %#v, %v", resolved, err)
	}
}

func TestMountedToolCallbacksReceiveMountContext(t *testing.T) {
	registry := NewRegistry(nil)
	mountedComponent := component("callback-context")
	var mounted *Mount
	definition := definition("probe", "probe")
	definition.Decode = func(ctx context.Context, raw json.RawMessage) (any, error) {
		if mounted == nil {
			return nil, errors.New("mount unavailable")
		}
		if err := mounted.extension.CheckClose(ctx); !errors.Is(err, extension.ErrSelfClose) {
			return nil, errors.New("callback context does not identify its mount")
		}
		return append(json.RawMessage(nil), raw...), nil
	}
	var err error
	mounted, err = registry.Mount(context.Background(), mountedComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "probe", InstanceID: mountedComponent.InstanceID, Scope: extension.GlobalScope(), Definition: definition})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mounted.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{})
	if err != nil || len(resolved) != 1 {
		t.Fatalf("resolved tools = %#v, %v", resolved, err)
	}
	if resolved[0].InputDecoder == nil {
		t.Fatal("mounted tool has no input decoder")
	}
	if _, err := resolved[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("mounted Decode = %v", err)
	}
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
	resolved, _ := newPlan.ResolveTools(context.Background(), runtime.ToolScopeContext{})
	if len(resolved) != 0 {
		t.Fatalf("new plan retained tools: %#v", resolved)
	}
	newPlan.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := mount.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close before release = %v", err)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil || !cleaned {
		t.Fatalf("Close = %v cleaned=%t", err, cleaned)
	}
}

func TestSessionScopedCallbackOnlyMountIgnoresUnrelatedPlan(t *testing.T) {
	registry := NewRegistry(nil)
	mountedComponent := component("callback-only")
	mount, err := registry.Mount(context.Background(), mountedComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return extension.On(registrar.Extensions(), compositionNotice, extension.Registration{
			ID: "notice", InstanceID: mountedComponent.InstanceID, Scope: extension.SessionScope("session-a"),
		}, func(context.Context, string) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	other, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-b"})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	mount.Deactivate()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatalf("unrelated plan retained callback-only mount: %v", err)
	}
}

func TestResumePlanDoesNotLeaseLaterSameSessionMount(t *testing.T) {
	registry := NewRegistry(nil)
	componentA := component("resume-lease-a")
	mountA, err := registry.Mount(context.Background(), componentA, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "tool-a", InstanceID: componentA.InstanceID, Scope: extension.SessionScope("session-a"), Definition: definition("tool-a", "a")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	planA, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	persisted := planA.Descriptor()
	planA.Release()

	componentB := component("resume-lease-b")
	mountB, err := registry.Mount(context.Background(), componentB, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "tool-b", InstanceID: componentB.InstanceID, Scope: extension.SessionScope("session-a"), Definition: definition("tool-b", "b")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := registry.AcquireResumePlan(context.Background(), persisted)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Release()

	mountB.Deactivate()
	if err := mountB.Close(context.Background()); err != nil {
		t.Fatalf("resumed A plan retained later B mount: %v", err)
	}
	resumed.Release()
	mountA.Deactivate()
	if err := mountA.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMountInstallerAndRollbackCanReenterRegistry(t *testing.T) {
	registry := NewRegistry(nil)
	var nested *Mount
	done := make(chan error, 1)
	go func() {
		_, err := registry.Mount(context.Background(), component("outer"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
			_ = registry.Diagnostics()
			plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				return err
			}
			plan.Release()
			nested, err = registry.Mount(context.Background(), component("nested"), InstallerFunc(func(_ context.Context, nestedRegistrar *Registrar) error {
				return nestedRegistrar.Tool(ToolRegistration{ID: "nested", InstanceID: "nested", Scope: extension.GlobalScope(), Definition: definition("nested", "nested")})
			}))
			if err != nil {
				return err
			}
			if err := registrar.Defer(func(context.Context) error {
				_ = registry.Diagnostics()
				plan, acquireErr := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
				if acquireErr == nil {
					plan.Release()
				}
				return acquireErr
			}); err != nil {
				return err
			}
			return errors.New("rollback")
		}))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || err.Error() != "rollback" {
			t.Fatalf("Mount error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("re-entrant installer or rollback deadlocked")
	}
	if nested == nil {
		t.Fatal("nested mount did not commit")
	}
	if err := nested.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCapabilityConflictRollbackReentersWithoutPublishingHandlers(t *testing.T) {
	registry := NewRegistry(nil)
	first, err := registry.Mount(context.Background(), component("first"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: "first", Scope: extension.GlobalScope(), Definition: definition("duplicate", "first")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close(context.Background()) }()
	cleaned := false
	done := make(chan error, 1)
	go func() {
		_, mountErr := registry.Mount(context.Background(), component("rejected"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
			if err := registrar.Defer(func(context.Context) error {
				_ = registry.Diagnostics()
				cleaned = true
				return nil
			}); err != nil {
				return err
			}
			if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "notice", InstanceID: "rejected", Scope: extension.GlobalScope()}, func(context.Context, string) error {
				return nil
			}); err != nil {
				return err
			}
			return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: "rejected", Scope: extension.GlobalScope(), Definition: definition("duplicate", "rejected")})
		}))
		done <- mountErr
	}()
	select {
	case err := <-done:
		if !errors.Is(err, tools.ErrDuplicateRegistration) || !cleaned {
			t.Fatalf("conflicting Mount = %v cleaned=%t", err, cleaned)
		}
	case <-time.After(time.Second):
		t.Fatal("conflict rollback deadlocked")
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	for _, entry := range plan.Descriptor().Entries {
		if entry.InstanceID == "rejected" {
			t.Fatal("rejected mount handler was published")
		}
	}
}

func TestMountedCapabilitiesRejectSelfCloseBeforeDeactivation(t *testing.T) {
	registry := NewRegistry(nil)
	var mount *Mount
	definition := definition("self-close", "self-close")
	definition.Normalize = func(ctx context.Context, _ any) (json.RawMessage, error) {
		return nil, mount.Close(ctx)
	}
	definition.Execute = func(ctx context.Context, _ tools.Execution) (any, error) {
		return nil, mount.Close(ctx)
	}
	var err error
	mount, err = registry.Mount(context.Background(), component("self-close"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Tool(ToolRegistration{ID: "tool", InstanceID: "self-close", Scope: extension.GlobalScope(), Definition: definition}); err != nil {
			return err
		}
		if err := registrar.Prompt(PromptRegistration{ID: "prompt", InstanceID: "self-close", Name: "prompt", Scope: extension.GlobalScope(), Provider: runtime.PromptProviderFunc(func(ctx context.Context, _ runtime.PromptContext) (string, error) {
			return "", mount.Close(ctx)
		})}); err != nil {
			return err
		}
		return registrar.Guard(GuardRegistration{ID: "guard", InstanceID: "self-close", Scope: extension.GlobalScope(), Guard: runtime.ToolGuardFunc(func(ctx context.Context, _ runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
			return runtime.ToolGuardResult{}, mount.Close(ctx)
		})})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	prompts := plan.Prompts()
	guards := plan.Guards()
	if _, err := prompts[0].Provider.ProvidePrompt(context.Background(), runtime.PromptContext{}); !errors.Is(err, extension.ErrSelfClose) {
		t.Fatalf("prompt self-close = %v", err)
	}
	if _, err := guards[0].Guard.GuardTool(context.Background(), runtime.ToolGuardRequest{}); !errors.Is(err, extension.ErrSelfClose) {
		t.Fatalf("guard self-close = %v", err)
	}
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{})
	if err != nil || len(resolved) != 1 {
		t.Fatalf("ResolveTools = %#v, %v", resolved, err)
	}
	if _, err := resolved[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{}`)); !errors.Is(err, extension.ErrSelfClose) {
		t.Fatalf("decoder self-close = %v", err)
	}
	if _, err := resolved[0].Executor.Execute(context.Background(), runtime.ToolCall{Input: json.RawMessage(`{}`)}); !errors.Is(err, extension.ErrSelfClose) {
		t.Fatalf("executor self-close = %v", err)
	}
	diagnostics := registry.Diagnostics()
	if len(diagnostics.Tools) != 1 {
		t.Fatalf("self-close deactivated mount: %#v", diagnostics)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
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
	persisted := plan.Descriptor()
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.AcquireResumePlan(context.Background(), persisted); !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		t.Fatalf("resume mismatch = %v", err)
	}
}

func TestStrictResumeRejectsChangedConvertedToolSchema(t *testing.T) {
	registry := NewRegistry(nil)
	component := component("schema-fingerprint")
	mountSchema := func(parameterType einoschema.DataType) *Mount {
		mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
			tool := definition("schema-tool", "stable")
			tool.Parameters = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
				"value": {Type: parameterType, Required: true},
			})
			return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: component.InstanceID, Scope: extension.GlobalScope(), Definition: tool})
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
	if persisted.Fingerprint == secondDescriptor.Fingerprint || persisted.Entries[0].Tool.SchemaHash == secondDescriptor.Entries[0].Tool.SchemaHash {
		t.Fatalf("changed schemas collided: persisted=%#v current=%#v", persisted, secondDescriptor)
	}
	if _, err := registry.AcquireResumePlan(context.Background(), persisted); !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		t.Fatalf("changed schema resume = %v, want ErrExtensionPlanMismatch", err)
	}
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
			registry := NewRegistry(nil)
			component := component("policy-fingerprint")
			newDefinition := func() tools.Definition {
				tool := definition("policy-tool", "stable")
				tool.Retention = runtime.RetentionPolicy{MaxInlineBytes: 1024, StoreExternal: true, Redact: true}
				tool.Metadata = map[string]string{"policy": "stable", "source": "test"}
				return tool
			}
			mountDefinition := func(tool tools.Definition) *Mount {
				mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
					return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: component.InstanceID, Scope: extension.GlobalScope(), Definition: tool})
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
			if persisted.Fingerprint == secondDescriptor.Fingerprint || persisted.Entries[0].Tool.SchemaHash == secondDescriptor.Entries[0].Tool.SchemaHash {
				t.Fatalf("changed policy collided: persisted=%#v current=%#v", persisted, secondDescriptor)
			}
			if _, err := registry.AcquireResumePlan(context.Background(), persisted); !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
				t.Fatalf("changed policy resume = %v, want ErrExtensionPlanMismatch", err)
			}
		})
	}
}

func TestToolSchemaHashCanonicalizesMetadataOrder(t *testing.T) {
	first := definition("metadata-tool", "stable")
	first.Metadata = map[string]string{"alpha": "one", "beta": "two"}
	second := definition("metadata-tool", "stable")
	second.Metadata = map[string]string{"beta": "two", "alpha": "one"}
	firstHash, err := toolSchemaHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := toolSchemaHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("equivalent metadata hashes differ: %s != %s", firstHash, secondHash)
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
	persisted := plan.Descriptor()
	plan.Release()
	if len(persisted.Entries) != 1 || persisted.Entries[0].Handlers == nil || len(persisted.Entries[0].Handlers.Registrations) != 2 {
		t.Fatalf("expected grouped mixed-scope handlers, got %#v", persisted.Entries)
	}

	resumed, err := registry.AcquireResumePlan(context.Background(), persisted)
	if err != nil {
		t.Fatalf("AcquireResumePlan = %v", err)
	}
	resumed.Release()
}

func TestStrictResumeRejectsConflictingNestedSessionScopes(t *testing.T) {
	registry := NewRegistry(nil)
	persisted := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Entries: []session.ExtensionPlanEntry{{
		InstanceID: "mixed", Artifact: session.ArtifactIdentity{Name: "mixed", Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: string(extension.SourceNative)},
		Handlers: &session.HandlerPlanIdentity{Registrations: []session.RegistrationIdentity{
			{ID: "a", Contract: compositionNotice.Contract().ID, Version: compositionNotice.Contract().Version, Scope: session.ExtensionScope{Kind: string(extension.ScopeSession), Key: "session-a"}, Kind: session.HandlerNotification},
			{ID: "b", Contract: compositionNotice.Contract().ID, Version: compositionNotice.Contract().Version, Scope: session.ExtensionScope{Kind: string(extension.ScopeSession), Key: "session-b"}, Kind: session.HandlerNotification},
		}},
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
	defer plan.Release()
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	prompts, guards := plan.Prompts(), plan.Guards()
	if err != nil || len(resolved) != 1 || resolved[0].Name != "b" || len(prompts) != 1 || prompts[0].InstanceID != "cap-session" || len(guards) != 1 {
		t.Fatalf("plan tools=%#v prompts=%#v guards=%#v err=%v", resolved, prompts, guards, err)
	}
	var kinds = map[session.ExtensionKind]bool{}
	descriptor := plan.Descriptor()
	for _, entry := range descriptor.Entries {
		kind, err := entry.Kind()
		if err != nil {
			t.Fatal(err)
		}
		kinds[kind] = true
	}
	for _, kind := range []session.ExtensionKind{session.ExtensionTool, session.ExtensionPrompt, session.ExtensionGuard, session.ExtensionRestriction} {
		if !kinds[kind] {
			t.Fatalf("descriptor missing %s: %#v", kind, descriptor)
		}
	}
}

func TestPromptAndGuardOrderParticipateInStrictFingerprint(t *testing.T) {
	for _, kind := range []session.ExtensionKind{session.ExtensionPrompt, session.ExtensionGuard} {
		t.Run(string(kind), func(t *testing.T) {
			registry := NewRegistry(nil)
			mountOrdered := func(order int) *Mount {
				mount, err := registry.Mount(context.Background(), component("ordered"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
					switch kind {
					case session.ExtensionPrompt:
						return registrar.Prompt(PromptRegistration{ID: "prompt", InstanceID: "ordered", Name: "policy", Order: order, Scope: extension.GlobalScope(), Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) { return "policy", nil })})
					default:
						return registrar.Guard(GuardRegistration{ID: "guard", InstanceID: "ordered", Order: order, Scope: extension.GlobalScope(), Guard: runtime.ToolGuardFunc(func(context.Context, runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
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
			if kind == session.ExtensionPrompt {
				entryOrder = persisted.Entries[0].Prompt.Order
			} else {
				entryOrder = persisted.Entries[0].Guard.Order
			}
			if len(persisted.Entries) != 1 || entryOrder != 10 || persisted.SchemaVersion != session.ExtensionPlanSchemaVersion {
				t.Fatalf("ordered descriptor = %#v", persisted)
			}
			if err := firstMount.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			secondMount := mountOrdered(20)
			defer func() { _ = secondMount.Close(context.Background()) }()
			if _, err := registry.AcquireResumePlan(context.Background(), persisted); !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
				t.Fatalf("changed order resume = %v, want mismatch", err)
			}
		})
	}
}

func TestUnsupportedVersionTwoPlanIsRejected(t *testing.T) {
	registry := NewRegistry(nil)
	component := component("unsupported-version")
	mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "notice", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(context.Context, string) error { return nil }); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "tool", InstanceID: component.InstanceID, Scope: extension.GlobalScope(), Definition: definition("echo", "v1")})
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
	persisted.SchemaVersion = 2
	resumed, err := registry.AcquireResumePlan(context.Background(), persisted)
	if !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		t.Fatalf("AcquireResumePlan schema v2 callback/tool plan = %v, want mismatch", err)
	}
	if resumed != nil {
		resumed.Release()
	}
}
