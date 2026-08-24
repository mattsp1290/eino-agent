package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestRegisterValidatesDefinitions(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.Register(Definition{}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("Register empty error = %v, want ErrInvalidDefinition", err)
	}
	malformed := testDefinition("malformed")
	malformed.Parameters = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"broken": nil})
	if err := ValidateDefinition(malformed); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("ValidateDefinition malformed error = %v, want ErrInvalidDefinition", err)
	}
	definition := testDefinition("echo")
	if _, err := registry.Register(definition); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	if _, err := registry.Register(definition); !errors.Is(err, ErrDuplicateRegistration) {
		t.Fatalf("Register duplicate error = %v, want ErrDuplicateRegistration", err)
	}
}

func TestReplaceRejectsStaleRegistration(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	first, err := registry.Register(testDefinition("echo"))
	if err != nil {
		t.Fatalf("Register error = %v", err)
	}
	second, err := registry.Replace(first, testDefinition("echo"))
	if err != nil {
		t.Fatalf("Replace error = %v", err)
	}
	if _, err := registry.Replace(first, testDefinition("echo")); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("stale Replace error = %v, want ErrStaleRegistration", err)
	}
	if _, err := registry.Replace(second, testDefinition("echo")); err != nil {
		t.Fatalf("fresh Replace error = %v", err)
	}
}

func TestUnregisterRequiresExactGenerationAndSnapshotIsStable(t *testing.T) {
	t.Parallel()
	registry := NewRegistry()
	first, err := registry.Register(testDefinition("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Register(testDefinition("second"))
	if err != nil {
		t.Fatal(err)
	}
	frozen := registry.Snapshot()
	if err := registry.Unregister(Registration{Name: first.Name, Generation: first.Generation + 1}); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("stale Unregister = %v", err)
	}
	if err := registry.Unregister(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Unregister(first); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("repeated Unregister = %v", err)
	}
	if entries := frozen.Entries(); len(entries) != 2 || entries[0].Registration != first || entries[1].Registration != second {
		t.Fatalf("frozen entries = %#v", entries)
	}
	materialized, err := frozen.ResolveTools(context.Background(), snapshot("session"))
	if err != nil || len(materialized) != 2 {
		t.Fatalf("frozen ResolveTools = %#v, %v", materialized, err)
	}
}

func TestResolveToolsMaterializesPerSessionScopes(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.Register(testDefinition("echo")); err != nil {
		t.Fatalf("Register error = %v", err)
	}

	toolsA, err := registry.ResolveTools(context.Background(), snapshot("session-a"))
	if err != nil {
		t.Fatalf("ResolveTools A error = %v", err)
	}
	if toolsA[0].Scope.WorkspaceID != "workspace-session-a" || toolsA[0].Scope.Root != "project://session-a" {
		t.Fatalf("scope A = %#v", toolsA[0].Scope)
	}
}

func TestResolveToolsHonorsEnabledDisabledAndClonesModelInfo(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	for _, name := range []string{"echo", "search"} {
		if _, err := registry.Register(testDefinition(name)); err != nil {
			t.Fatalf("Register %s error = %v", name, err)
		}
	}
	snap := snapshot("session")
	snap.EnabledTools = []string{"echo", "search"}
	snap.DisabledTools = []string{"search"}

	materialized, err := registry.ResolveTools(context.Background(), snap)
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if len(materialized) != 1 || materialized[0].Name != "echo" {
		t.Fatalf("materialized = %#v", names(materialized))
	}
	materialized[0].Info.Name = "mutated"
	again, err := registry.ResolveTools(context.Background(), snap)
	if err != nil {
		t.Fatalf("ResolveTools again error = %v", err)
	}
	if again[0].Info.Name != "echo" {
		t.Fatalf("model-facing info shared mutable state: %q", again[0].Info.Name)
	}
}

func TestExplicitEmptyEnabledListMaterializesNoTools(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.Register(testDefinition("echo")); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	snap := snapshot("session")
	snap.EnabledTools = []string{}

	materialized, err := registry.ResolveTools(context.Background(), snap)
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if len(materialized) != 0 {
		t.Fatalf("materialized %d tools, want none", len(materialized))
	}
}

func TestDecodeAndExecuteTypedTool(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.Register(testDefinition("echo")); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot("session"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	normalized, err := materialized[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"text":"hi"}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	if string(normalized) != `{"text":"hi"}` {
		t.Fatalf("normalized = %s", normalized)
	}
	result, err := materialized[0].Executor.Execute(context.Background(), runtime.ToolCall{Input: normalized})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if string(result.Structured) != `{"text":"hi"}` || result.Output != `{"text":"hi"}` {
		t.Fatalf("result = %#v", result)
	}
}

func TestDecodeToolInputDoesNotUseOutputEncoder(t *testing.T) {
	t.Parallel()

	type input struct {
		Query string `json:"query"`
	}
	type output struct {
		Results []string `json:"results"`
	}
	registry := NewRegistry()
	if _, err := registry.Register(Definition{
		Name: "search",
		Decode: func(_ context.Context, raw json.RawMessage) (any, error) {
			var value input
			return value, json.Unmarshal(raw, &value)
		},
		Encode: func(_ context.Context, value any) (json.RawMessage, error) {
			result, ok := value.(output)
			if !ok {
				return nil, fmt.Errorf("expected output, got %T", value)
			}
			return json.Marshal(result)
		},
		Execute: func(_ context.Context, execution Execution) (any, error) {
			value := execution.Input.(input)
			return output{Results: []string{value.Query}}, nil
		},
	}); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot("session"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	normalized, err := materialized[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"query":"needle"}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	result, err := materialized[0].Executor.Execute(context.Background(), runtime.ToolCall{Input: normalized})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if string(result.Structured) != `{"results":["needle"]}` {
		t.Fatalf("result = %s", result.Structured)
	}
}

func TestMalformedToolInputReturnsTypedError(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.Register(testDefinition("echo")); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot("session"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if _, err := materialized[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{`)); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("DecodeToolInput error = %v, want ErrMalformedInput", err)
	}
	_, err = materialized[0].Executor.Execute(context.Background(), runtime.ToolCall{Input: json.RawMessage(`{"wrong":true}`)})
	if !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("Execute malformed error = %v, want ErrMalformedInput", err)
	}
}

func TestScopeResolverCanReplaceWithoutDeadlock(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	registration, err := registry.Register(testDefinition("echo"))
	if err != nil {
		t.Fatalf("Register error = %v", err)
	}
	replaced := make(chan error, 1)
	definition := testDefinition("echo")
	definition.Scope = func(runtime.ToolScopeContext) runtime.ToolScope {
		next := testDefinition("echo")
		_, err := registry.Replace(registration, next)
		replaced <- err
		return runtime.ToolScope{}
	}
	if registration, err = registry.Replace(registration, definition); err != nil {
		t.Fatalf("Replace scoped definition error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := registry.ResolveTools(context.Background(), snapshot("session"))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ResolveTools error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ResolveTools deadlocked while ScopeResolver replaced registration")
	}
	if err := <-replaced; err != nil {
		t.Fatalf("ScopeResolver Replace error = %v", err)
	}
}

func TestParameterSchemasAreClonedPerMaterialization(t *testing.T) {
	t.Parallel()

	params := einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
		"text": {Type: einoschema.String, Required: true},
	})
	definition := testDefinition("echo")
	definition.Parameters = params
	registry := NewRegistry()
	if _, err := registry.Register(definition); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	first, err := registry.ResolveTools(context.Background(), snapshot("session-a"))
	if err != nil {
		t.Fatalf("ResolveTools first error = %v", err)
	}
	second, err := registry.ResolveTools(context.Background(), snapshot("session-b"))
	if err != nil {
		t.Fatalf("ResolveTools second error = %v", err)
	}
	if first[0].Info.ParamsOneOf == nil || second[0].Info.ParamsOneOf == nil {
		t.Fatal("expected cloned parameter schemas")
	}
	if first[0].Info.ParamsOneOf == params || second[0].Info.ParamsOneOf == params || first[0].Info.ParamsOneOf == second[0].Info.ParamsOneOf {
		t.Fatal("parameter schema pointer shared across registration or materializations")
	}
}

func testDefinition(name string) Definition {
	return Definition{
		Name:        name,
		Description: "echo input",
		Decode: func(_ context.Context, raw json.RawMessage) (any, error) {
			var value struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			if value.Text == "" {
				return nil, errors.New("text required")
			}
			return value, nil
		},
		Encode: func(_ context.Context, value any) (json.RawMessage, error) {
			return json.Marshal(value)
		},
		Execute: func(_ context.Context, execution Execution) (any, error) {
			return execution.Input, nil
		},
		RetrySafe:   true,
		Permissions: []string{"workspace:read"},
		Metadata:    map[string]string{"kind": "test"},
	}
}

func snapshot(id session.ID) runtime.ToolScopeContext {
	return runtime.ToolScopeContext{
		SessionID: id, WorkspaceID: "workspace-" + string(id), WorkspaceRoot: "project://" + string(id),
	}
}

func names(values []runtime.Tool) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = value.Name
	}
	return result
}
