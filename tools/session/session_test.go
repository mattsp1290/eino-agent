package sessiontools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func TestSessionPlanIsScopedPerSession(t *testing.T) {
	t.Parallel()

	registry := agenttools.NewRegistry()
	if _, err := Register(context.Background(), registry, Options{}); err != nil {
		t.Fatalf("Register error = %v", err)
	}
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
	if toolsA[NamePlanSet].Scope.ConcurrencyKey == toolsB[NamePlanSet].Scope.ConcurrencyKey {
		t.Fatalf("session tools share concurrency key %q", toolsA[NamePlanSet].Scope.ConcurrencyKey)
	}
}

func TestRetainedOutputIsBoundedAndSessionScoped(t *testing.T) {
	t.Parallel()

	state := NewState()
	state.MaxRetainedBytes = 4
	registry := agenttools.NewRegistry()
	if _, err := Register(context.Background(), registry, Options{State: state}); err != nil {
		t.Fatalf("Register error = %v", err)
	}
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

	state := NewState()
	state.MaxRetainedBytes = 4
	registry := agenttools.NewRegistry()
	if _, err := Register(context.Background(), registry, Options{State: state}); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	tools := resolve(t, registry, "session-a")
	result := execute(t, tools[NameRetainedOutput], `{"id":"out-utf8","content":"ééé"}`)
	if !strings.Contains(result.Output, `"content":"éé"`) || strings.Contains(result.Output, "�") {
		t.Fatalf("retained output = %s", result.Output)
	}
}

func TestRetainedOutputHonorsAggregateSessionLimitAndZeroLimit(t *testing.T) {
	t.Parallel()

	state := NewState()
	state.SetMaxRetainedBytes(0)
	state.MaxSessionBytes = 3
	registry := agenttools.NewRegistry()
	if _, err := Register(context.Background(), registry, Options{State: state}); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	tools := resolve(t, registry, "session-a")
	execute(t, tools[NameRetainedOutput], `{"id":"first","content":"abcdef"}`)
	if first, ok := state.GetRetainedOutput("session-a", "first"); !ok || first.Content != "" || !first.Truncated {
		t.Fatalf("first retained = %+v ok=%t", first, ok)
	}

	state.SetMaxRetainedBytes(10)
	execute(t, tools[NameRetainedOutput], `{"id":"second","content":"abcdef"}`)
	if second, ok := state.GetRetainedOutput("session-a", "second"); !ok || second.Content != "abc" || !second.Truncated {
		t.Fatalf("second retained = %+v ok=%t", second, ok)
	}
	if listed := state.ListRetainedOutputs("session-a"); len(listed) != 2 {
		t.Fatalf("listed retained outputs = %d, want 2", len(listed))
	}
}

func TestSessionToolsExposePermissionsAndDoNotDuplicateLeafTools(t *testing.T) {
	t.Parallel()

	registry := agenttools.NewRegistry()
	if _, err := Register(context.Background(), registry, Options{}); err != nil {
		t.Fatalf("Register error = %v", err)
	}
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
	registry := agenttools.NewRegistry()
	if _, err := Register(context.Background(), registry, Options{Subagent: subagent, Skills: skills}); err != nil {
		t.Fatalf("Register error = %v", err)
	}
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
	if !strings.Contains(string(normalized), `"permission_pattern":"go"`) {
		t.Fatalf("normalized skill input = %s", normalized)
	}
}

func resolve(t *testing.T, registry *agenttools.Registry, id session.ID) map[string]runtime.Tool {
	t.Helper()
	materialized, err := registry.ResolveTools(context.Background(), runtime.TurnSnapshot{
		SessionID: id,
		Config: config.Snapshot{Metadata: map[string]string{
			"workspace_id":   "workspace-" + string(id),
			"workspace_root": "/workspace/" + string(id),
		}},
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
