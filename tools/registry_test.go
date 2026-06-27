package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestRegisterValidatesDefinitions(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.Register(Definition{}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("Register empty error = %v, want ErrInvalidDefinition", err)
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
	toolsB, err := registry.ResolveTools(context.Background(), snapshot("session-b"))
	if err != nil {
		t.Fatalf("ResolveTools B error = %v", err)
	}
	if toolsA[0].Scope.ConcurrencyKey == toolsB[0].Scope.ConcurrencyKey {
		t.Fatalf("concurrency keys should be per-session, got %q", toolsA[0].Scope.ConcurrencyKey)
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
	snap.Config.Tools.Enabled = []string{"echo", "search"}
	snap.Config.Tools.Disabled = []string{"search"}

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

func TestConcurrentSessionsMaterializeIndependently(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.Register(testDefinition("echo")); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	const sessions = 16
	var wg sync.WaitGroup
	keys := make(chan string, sessions)
	for i := range sessions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			materialized, err := registry.ResolveTools(context.Background(), snapshot(session.ID(fmt.Sprintf("session-%02d", i))))
			if err != nil {
				t.Errorf("ResolveTools error = %v", err)
				return
			}
			keys <- materialized[0].Scope.ConcurrencyKey
		}(i)
	}
	wg.Wait()
	close(keys)
	seen := map[string]bool{}
	for key := range keys {
		if seen[key] {
			t.Fatalf("duplicate concurrency key %q", key)
		}
		seen[key] = true
	}
	if len(seen) != sessions {
		t.Fatalf("keys = %d, want %d", len(seen), sessions)
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
		Concurrency: runtime.ToolConcurrencySequential,
		Permissions: []string{"workspace:read"},
		Metadata:    map[string]string{"kind": "test"},
	}
}

func snapshot(id session.ID) runtime.TurnSnapshot {
	return runtime.TurnSnapshot{
		SessionID: id,
		Config: config.Snapshot{
			Metadata: map[string]string{
				"workspace_id":   "workspace-" + string(id),
				"workspace_root": "project://" + string(id),
			},
		},
	}
}

func names(values []runtime.Tool) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = value.Name
	}
	return result
}
