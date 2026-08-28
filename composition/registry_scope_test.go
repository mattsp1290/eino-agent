package composition

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

func TestAtomicScopedCompositionAndStrictResume(t *testing.T) {
	registry := NewRegistry(nil)
	global, err := registry.Mount(context.Background(), component("global"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "notice", Scope: extension.GlobalScope()}, func(context.Context, string) error { return nil }); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "echo-global", Scope: extension.GlobalScope(), Definition: definition("echo", "global")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = global.Close(context.Background()) }()
	scoped, err := registry.Mount(context.Background(), component("scoped"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "echo-session", Scope: extension.SessionScope("session-a"), Definition: definition("session_echo", "session")})
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

	resumed, err := registry.AcquireResumePlan(context.Background(), resumeRequest("session-a", persisted))
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
			if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "notice", Scope: extension.SessionScope(key)}, func(context.Context, string) error { return nil }); err != nil {
				return err
			}
			return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.SessionScope(key), Definition: definition("echo", "opaque")})
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
		if err := registrar.Tool(ToolRegistration{ID: "a", Scope: extension.GlobalScope(), Definition: definition("a", "a")}); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "b", Scope: extension.GlobalScope(), Definition: definition("b", "b")})
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
			descriptorTools := len(plan.Descriptor().Tools)
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
		if err := registrar.Tool(ToolRegistration{ID: "global", Scope: extension.GlobalScope(), Definition: definition("echo", "global")}); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "session", Scope: extension.SessionScope("session-a"), Definition: definition("echo", "session")})
	}))
	if !errors.Is(err, tools.ErrDuplicateRegistration) {
		t.Fatalf("Mount error = %v, want ErrDuplicateRegistration", err)
	}
	assertRegistryEmpty(t, registry)
}

func TestMountRejectsCrossMountToolCollisionAndRemainsReusable(t *testing.T) {
	registry := NewRegistry(nil)
	globalComponent := component("global-collision")
	global, err := registry.Mount(context.Background(), globalComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "global", Scope: extension.GlobalScope(), Definition: definition("echo", "global")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = global.Close(context.Background()) }()
	sessionComponent := component("session-collision")
	_, err = registry.Mount(context.Background(), sessionComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "session", Scope: extension.SessionScope("session-a"), Definition: definition("echo", "session")})
	}))
	if !errors.Is(err, tools.ErrDuplicateRegistration) {
		t.Fatalf("collision Mount error = %v", err)
	}
	validComponent := component("valid-after-collision")
	valid, err := registry.Mount(context.Background(), validComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "valid", Scope: extension.GlobalScope(), Definition: definition("other", "valid")})
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
	definition.Normalize = func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
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
		return registrar.Tool(ToolRegistration{ID: "probe", Scope: extension.GlobalScope(), Definition: definition})
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
		return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: definition("tool", "tool")})
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
			ID: "notice", Scope: extension.SessionScope("session-a"),
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
		return registrar.Tool(ToolRegistration{ID: "tool-a", Scope: extension.SessionScope("session-a"), Definition: definition("tool-a", "a")})
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
		return registrar.Tool(ToolRegistration{ID: "tool-b", Scope: extension.SessionScope("session-a"), Definition: definition("tool-b", "b")})
	}))
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := registry.AcquireResumePlan(context.Background(), resumeRequest("session-a", persisted))
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
			plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				return err
			}
			plan.Release()
			nested, err = registry.Mount(context.Background(), component("nested"), InstallerFunc(func(_ context.Context, nestedRegistrar *Registrar) error {
				return nestedRegistrar.Tool(ToolRegistration{ID: "nested", Scope: extension.GlobalScope(), Definition: definition("nested", "nested")})
			}))
			if err != nil {
				return err
			}
			if err := registrar.Defer(func(context.Context) error {
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
		return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: definition("duplicate", "first")})
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
				cleaned = true
				return nil
			}); err != nil {
				return err
			}
			if err := extension.On(registrar.Extensions(), compositionNotice, extension.Registration{ID: "notice", Scope: extension.GlobalScope()}, func(context.Context, string) error {
				return nil
			}); err != nil {
				return err
			}
			return registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: definition("duplicate", "rejected")})
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
	descriptor := plan.Descriptor()
	for _, identity := range descriptor.Handlers {
		if identity.InstanceID == "rejected" {
			t.Fatal("rejected mount handler was published")
		}
	}
	for _, identity := range descriptor.Tools {
		if identity.InstanceID == "rejected" {
			t.Fatal("rejected mount handler was published")
		}
	}
}

func TestMountedCapabilitiesRejectSelfCloseBeforeDeactivation(t *testing.T) {
	registry := NewRegistry(nil)
	var mount *Mount
	definition := definition("self-close", "self-close")
	definition.Normalize = func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		return nil, mount.Close(ctx)
	}
	definition.Execute = func(ctx context.Context, _ tools.Execution) (json.RawMessage, error) {
		return nil, mount.Close(ctx)
	}
	var err error
	mount, err = registry.Mount(context.Background(), component("self-close"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Tool(ToolRegistration{ID: "tool", Scope: extension.GlobalScope(), Definition: definition}); err != nil {
			return err
		}
		if err := registrar.Prompt(PromptRegistration{ID: "prompt", Name: "prompt", Scope: extension.GlobalScope(), Provider: runtime.PromptProviderFunc(func(ctx context.Context, _ runtime.PromptContext) (string, error) {
			return "", mount.Close(ctx)
		})}); err != nil {
			return err
		}
		return registrar.Guard(GuardRegistration{ID: "guard", Scope: extension.GlobalScope(), Guard: runtime.ToolGuardFunc(func(ctx context.Context, _ runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
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
	activePlan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	activeTools, err := activePlan.ResolveTools(context.Background(), runtime.ToolScopeContext{})
	activePlan.Release()
	if err != nil || len(activeTools) != 1 {
		t.Fatalf("self-close deactivated mount: tools=%d err=%v", len(activeTools), err)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
