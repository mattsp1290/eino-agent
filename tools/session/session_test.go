package sessiontools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestSessionPlanIsScopedPerSession(t *testing.T) {
	t.Parallel()

	registry := mountSessionTools(t, extension.GlobalScope(), Options{})
	toolsA := resolve(t, registry, "session-a")
	toolsB := resolve(t, registry, "session-b")
	execute(t, toolsA[NamePlanSet], `{"items":[{"id":"1","text":"ship","status":"doing"}]}`)
	resultA := execute(t, toolsA[NamePlanGet], `{}`)
	resultB := execute(t, toolsB[NamePlanGet], `{}`)
	if !strings.Contains(resultA.Output, "ship") {
		t.Fatalf("session A plan = %s, want ship", resultA.Output)
	}
	if strings.Contains(resultB.Output, "ship") {
		t.Fatalf("session B leaked plan state: %s", resultB.Output)
	}
}

func TestRetainedOutputIsBoundedAndSessionScoped(t *testing.T) {
	t.Parallel()

	state := mustState(t, Limits{MaxRetainedBytes: 4, MaxSessionBytes: 100})
	registry := mountSessionTools(t, extension.GlobalScope(), Options{State: state})
	toolsA := resolve(t, registry, "session-a")
	result := execute(t, toolsA[NameRetainedOutput], `{"id":"out-1","content":"abcdef"}`)
	if !strings.Contains(result.Output, `"content":"abcd"`) || !strings.Contains(result.Output, `"truncated":true`) {
		t.Fatalf("retained output = %s", result.Output)
	}
	state.mu.RLock()
	_, leaked := state.outputs["session-b"]["out-1"]
	retained := state.outputs["session-a"]["out-1"]
	state.mu.RUnlock()
	if leaked {
		t.Fatal("retained output leaked across sessions")
	}
	if retained.Content != "abcd" || !retained.Truncated {
		t.Fatalf("retained = %+v", retained)
	}
	got, ok := state.GetRetainedOutput("session-a", "out-1")
	if !ok || got.Content != "abcd" {
		t.Fatalf("GetRetainedOutput = %+v ok=%t", got, ok)
	}
}

func TestRetainedOutputTruncatesAtUTF8Boundary(t *testing.T) {
	t.Parallel()

	state := mustState(t, Limits{MaxRetainedBytes: 4, MaxSessionBytes: 100})
	registry := mountSessionTools(t, extension.GlobalScope(), Options{State: state})
	tools := resolve(t, registry, "session-a")
	result := execute(t, tools[NameRetainedOutput], `{"id":"out-utf8","content":"ééé"}`)
	if !strings.Contains(result.Output, `"content":"éé"`) || strings.Contains(result.Output, "�") {
		t.Fatalf("retained output = %s", result.Output)
	}
}

func TestRetainedOutputHonorsZeroPerOutputLimit(t *testing.T) {
	t.Parallel()

	state := mustState(t, Limits{MaxRetainedBytes: 0, MaxSessionBytes: 3})
	registry := mountSessionTools(t, extension.GlobalScope(), Options{State: state})
	tools := resolve(t, registry, "session-a")
	execute(t, tools[NameRetainedOutput], `{"id":"first","content":"abcdef"}`)
	if first, ok := state.GetRetainedOutput("session-a", "first"); !ok || first.Content != "" || !first.Truncated {
		t.Fatalf("first retained = %+v ok=%t", first, ok)
	}
}

func TestRetainedOutputHonorsAggregateAndZeroSessionLimits(t *testing.T) {
	t.Parallel()

	state := mustState(t, Limits{MaxRetainedBytes: 10, MaxSessionBytes: 3})
	registry := mountSessionTools(t, extension.GlobalScope(), Options{State: state})
	tools := resolve(t, registry, "session-a")
	execute(t, tools[NameRetainedOutput], `{"id":"second","content":"abcdef"}`)
	if second, ok := state.GetRetainedOutput("session-a", "second"); !ok || second.Content != "abc" || !second.Truncated {
		t.Fatalf("second retained = %+v ok=%t", second, ok)
	}

	zeroState := mustState(t, Limits{MaxRetainedBytes: 10, MaxSessionBytes: 0})
	zeroRegistry := mountSessionTools(t, extension.GlobalScope(), Options{State: zeroState})
	zeroTools := resolve(t, zeroRegistry, "session-zero")
	execute(t, zeroTools[NameRetainedOutput], `{"id":"zero","content":"abcdef"}`)
	if output, ok := zeroState.GetRetainedOutput("session-zero", "zero"); !ok || output.Content != "" || !output.Truncated {
		t.Fatalf("zero aggregate retained = %+v ok=%t", output, ok)
	}
}

func TestNewStateRejectsNegativeLimitsAndDefaultsAreBounded(t *testing.T) {
	t.Parallel()
	for _, limits := range []Limits{{MaxRetainedBytes: -1}, {MaxSessionBytes: -1}} {
		if _, err := NewState(limits); err == nil {
			t.Fatalf("NewState(%+v) accepted negative limit", limits)
		}
	}
	defaults := DefaultLimits()
	if defaults.MaxRetainedBytes <= 0 || defaults.MaxSessionBytes <= 0 {
		t.Fatalf("DefaultLimits = %+v", defaults)
	}
}

func TestSessionToolsExposePermissionsAndDoNotDuplicateLeafTools(t *testing.T) {
	t.Parallel()

	registry := mountSessionTools(t, extension.GlobalScope(), Options{})
	tools := resolve(t, registry, "session")
	forbidden := map[string]bool{
		"apply_patch":   true,
		"file_edit":     true,
		"file_list":     true,
		"file_read":     true,
		"file_write":    true,
		"glob":          true,
		"search":        true,
		"shell":         true,
		"tracker_write": true,
		"url_fetch":     true,
		"user_interact": true,
	}
	for name, tool := range tools {
		if forbidden[name] {
			t.Fatalf("session tools registered leaf tool %s", name)
		}
		if len(tool.Scope.Permissions) == 0 {
			t.Fatalf("tool %s missing permissions", name)
		}
		if tool.Metadata["source"] != "session" {
			t.Fatalf("tool %s metadata = %#v", name, tool.Metadata)
		}
	}
}

func TestSessionHooksReceiveSessionID(t *testing.T) {
	t.Parallel()

	subagent := subagentFunc(func(_ context.Context, request SubagentRequest) (SubagentResult, error) {
		if request.SessionID != "session-hooks" || request.Task != "test" {
			t.Fatalf("subagent request = %+v", request)
		}
		return SubagentResult{ID: "task-1", Status: "queued"}, nil
	})
	skills := skillFunc(func(_ context.Context, request SkillRequest) (SkillResult, error) {
		if request.SessionID != "session-hooks" || request.Name != "go" {
			t.Fatalf("skill request = %+v", request)
		}
		return SkillResult{Name: request.Name, Status: "loaded"}, nil
	})
	registry := mountSessionTools(t, extension.GlobalScope(), Options{Subagent: subagent, Skills: skills})
	tools := resolve(t, registry, "session-hooks")
	if result := execute(t, tools[NameSubagent], `{"task":"test"}`); !strings.Contains(result.Output, "queued") {
		t.Fatalf("subagent result = %s", result.Output)
	}
	if result := execute(t, tools[NameSkillLoad], `{"name":"go"}`); !strings.Contains(result.Output, "loaded") {
		t.Fatalf("skill result = %s", result.Output)
	}
	normalized, err := tools[NameSkillLoad].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"name":"go"}`))
	if err != nil {
		t.Fatalf("DecodeToolInput skill error = %v", err)
	}
	if strings.Contains(string(normalized), "permission_pattern") {
		t.Fatalf("normalized skill input leaked permission metadata = %s", normalized)
	}
	pattern, err := tools[NameSkillLoad].Pattern.ResolvePermissionPattern(context.Background(), normalized)
	if err != nil || pattern != "go" {
		t.Fatalf("permission pattern = %q err=%v", pattern, err)
	}
}

func TestSessionToolMountDeactivationPreservesAcquiredPlan(t *testing.T) {
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	component := sessionToolComponent()
	mount, err := Mount(context.Background(), registry, component, extension.GlobalScope(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	mount.Deactivate()
	future, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	resolvedFuture, err := future.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	future.Release()
	if err != nil || len(resolvedFuture) != 0 {
		t.Fatalf("future tools = %#v, %v", resolvedFuture, err)
	}
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	if err != nil || len(resolved) != 3 {
		t.Fatalf("acquired plan tools = %#v, %v", resolved, err)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSessionToolMountRoutesExactSessionScope(t *testing.T) {
	registry := mountSessionTools(t, extension.SessionScope("session-a"), Options{})
	if got := resolve(t, registry, "session-a"); len(got) != 3 {
		t.Fatalf("session-a tools = %d, want 3", len(got))
	}
	if got := resolve(t, registry, "session-b"); len(got) != 0 {
		t.Fatalf("session-b tools = %d, want 0", len(got))
	}
}

func TestSessionToolMountResumesAcrossEquivalentRegistry(t *testing.T) {
	component := sessionToolComponent()
	first, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	firstMount, err := Mount(context.Background(), first, component, extension.GlobalScope(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := first.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	plan.Release()
	if err := firstMount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, err := composition.NewRegistry(nil)

	if err != nil {

		t.Fatal(err)

	}
	secondMount, err := Mount(context.Background(), second, component, extension.GlobalScope(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = secondMount.Close(context.Background()) }()
	resumed, err := second.AcquireResumePlan(context.Background(), resumePlanRequest("session-a", descriptor))
	if err != nil {
		t.Fatalf("AcquireResumePlan error = %v", err)
	}
	defer resumed.Release()
	tools, err := resumed.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	if err != nil || len(tools) != 3 {
		t.Fatalf("resumed tools = %#v, %v", tools, err)
	}
}

func TestSessionToolMountResumeRejectsIdentityDrift(t *testing.T) {
	subagent := subagentFunc(func(context.Context, SubagentRequest) (SubagentResult, error) {
		return SubagentResult{ID: "task", Status: "queued"}, nil
	})
	component := sessionToolComponent()
	original, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	originalMount, err := Mount(context.Background(), original, component, extension.GlobalScope(), Options{Subagent: subagent})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := original.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	plan.Release()
	if err := originalMount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		component extension.Component
		scope     extension.Scope
		options   Options
	}{
		{name: "missing optional definition", component: component, scope: extension.GlobalScope(), options: Options{}},
		{name: "component config", component: func() extension.Component {
			changed := component
			changed.Artifact.ConfigHash = "changed"
			return changed
		}(), scope: extension.GlobalScope(), options: Options{Subagent: subagent}},
		{name: "scope", component: component, scope: extension.SessionScope("session-a"), options: Options{Subagent: subagent}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := composition.NewRegistry(nil)
			if err != nil {
				t.Fatal(err)
			}
			mount, err := Mount(context.Background(), registry, test.component, test.scope, test.options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = mount.Close(context.Background()) }()
			resumed, err := registry.AcquireResumePlan(context.Background(), resumePlanRequest("session-a", descriptor))
			if err != nil {
				t.Fatalf("AcquireResumePlan error = %v", err)
			}
			if resumed.Descriptor().Fingerprint == descriptor.Fingerprint {
				resumed.Release()
				t.Fatal("drifted session tool composition retained persisted fingerprint")
			}
			resumed.Release()
		})
	}
}

func resumePlanRequest(sessionID session.ID, descriptor session.ExtensionPlanDescriptor) runtime.ResumePlanRequest {
	plan, _ := session.VerifyExtensionPlanForSession(sessionID, descriptor)
	return runtime.ResumePlanRequest{SessionID: sessionID, Plan: plan}
}

func resolve(t *testing.T, registry *composition.Registry, id session.ID) map[string]runtime.Tool {
	t.Helper()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: id})
	if err != nil {
		t.Fatalf("AcquireRunPlan error = %v", err)
	}
	defer plan.Release()
	materialized, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{
		SessionID: id, WorkspaceID: "workspace-" + string(id), WorkspaceRoot: "/workspace/" + string(id),
	})
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	result := make(map[string]runtime.Tool, len(materialized))
	for _, tool := range materialized {
		result[tool.Name] = tool
	}
	return result
}

func mountSessionTools(t *testing.T, scope extension.Scope, options Options) *composition.Registry {
	t.Helper()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	component := sessionToolComponent()
	mount, err := Mount(context.Background(), registry, component, scope, options)
	if err != nil {
		t.Fatalf("Mount error = %v", err)
	}
	t.Cleanup(func() {
		if err := mount.Close(context.Background()); err != nil {
			t.Errorf("close mount: %v", err)
		}
	})
	return registry
}

func mustState(t *testing.T, limits Limits) *State {
	t.Helper()
	state, err := NewState(limits)
	if err != nil {
		t.Fatalf("NewState error = %v", err)
	}
	return state
}

func sessionToolComponent() extension.Component {
	return extension.Component{
		InstanceID: "session-tools",
		Artifact: extension.Artifact{
			Name: "session-tools", Version: "1", Hash: "session-tools-artifact", ConfigHash: "session-tools-config", SourceKind: extension.SourceNative,
		},
	}
}

func execute(t *testing.T, tool runtime.Tool, input string) runtime.ToolResult {
	t.Helper()
	normalized, err := tool.InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(input))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	result, err := tool.Executor.Execute(context.Background(), runtime.ToolCall{Input: normalized})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	return result
}

type subagentFunc func(context.Context, SubagentRequest) (SubagentResult, error)

func (fn subagentFunc) RunSubagent(ctx context.Context, request SubagentRequest) (SubagentResult, error) {
	return fn(ctx, request)
}

type skillFunc func(context.Context, SkillRequest) (SkillResult, error)

func (fn skillFunc) LoadSkill(ctx context.Context, request SkillRequest) (SkillResult, error) {
	return fn(ctx, request)
}
