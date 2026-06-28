package agui

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	einoschema "github.com/cloudwego/eino/schema"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func TestRegistryCombinesServerAndClientTools(t *testing.T) {
	t.Parallel()

	server := agenttools.NewRegistry()
	if _, err := server.Register(serverDefinition("server_echo")); err != nil {
		t.Fatalf("Register server tool error = %v", err)
	}
	registry := NewRegistry(server, testDispatcher())
	if err := registry.SetClientTools(agentagui.ClientToolSnapshot{
		SessionID:  "session-1",
		Generation: 1,
		Tools:      []aguitypes.Tool{clientTool("client_lookup")},
	}); err != nil {
		t.Fatalf("SetClientTools error = %v", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot("session-1"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if got := names(materialized); len(got) != 2 || got[0] != "server_echo" || got[1] != "client_lookup" {
		t.Fatalf("materialized = %#v", got)
	}
}

func TestRegistryClientToolsAreSessionScoped(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(nil, testDispatcher())
	if err := registry.SetClientTools(agentagui.ClientToolSnapshot{
		SessionID:  "session-a",
		Generation: 1,
		Tools:      []aguitypes.Tool{clientTool("client_a")},
	}); err != nil {
		t.Fatalf("SetClientTools error = %v", err)
	}
	a, err := registry.ResolveTools(context.Background(), snapshot("session-a"))
	if err != nil {
		t.Fatalf("ResolveTools A error = %v", err)
	}
	b, err := registry.ResolveTools(context.Background(), snapshot("session-b"))
	if err != nil {
		t.Fatalf("ResolveTools B error = %v", err)
	}
	if len(a) != 1 || a[0].Name != "client_a" {
		t.Fatalf("session A tools = %#v", names(a))
	}
	if len(b) != 0 {
		t.Fatalf("session B tools = %#v", names(b))
	}
}

func TestRegistryRejectsStaleClientDefinitions(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(nil, testDispatcher())
	if err := registry.SetClientTools(agentagui.ClientToolSnapshot{SessionID: "session-1", Generation: 2, Tools: []aguitypes.Tool{clientTool("new")}}); err != nil {
		t.Fatalf("SetClientTools error = %v", err)
	}
	err := registry.SetClientTools(agentagui.ClientToolSnapshot{SessionID: "session-1", Generation: 1, Tools: []aguitypes.Tool{clientTool("old")}})
	if !errors.Is(err, agenttools.ErrStaleRegistration) {
		t.Fatalf("stale SetClientTools error = %v, want ErrStaleRegistration", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot("session-1"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if len(materialized) != 1 || materialized[0].Name != "new" {
		t.Fatalf("materialized = %#v", names(materialized))
	}
}

func TestRegistryRejectsStaleClientDefinitionsAfterClear(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(nil, testDispatcher())
	if err := registry.SetClientTools(agentagui.ClientToolSnapshot{SessionID: "session-1", Generation: 5, Tools: []aguitypes.Tool{clientTool("new")}}); err != nil {
		t.Fatalf("SetClientTools error = %v", err)
	}
	registry.ClearClientTools("session-1")
	err := registry.SetClientTools(agentagui.ClientToolSnapshot{SessionID: "session-1", Generation: 3, Tools: []aguitypes.Tool{clientTool("old")}})
	if !errors.Is(err, agenttools.ErrStaleRegistration) {
		t.Fatalf("stale SetClientTools after clear error = %v, want ErrStaleRegistration", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot("session-1"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if len(materialized) != 0 {
		t.Fatalf("materialized = %#v, want none after clear", names(materialized))
	}
}

func TestRegistryRejectsEmptySessionClientDefinitions(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(nil, testDispatcher())
	err := registry.SetClientTools(agentagui.ClientToolSnapshot{Generation: 1, Tools: []aguitypes.Tool{clientTool("client")}})
	if !errors.Is(err, agenttools.ErrInvalidDefinition) {
		t.Fatalf("empty session SetClientTools error = %v, want ErrInvalidDefinition", err)
	}
}

func TestRegistryHonorsEnabledDisabledForClientTools(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(nil, testDispatcher())
	if err := registry.SetClientTools(agentagui.ClientToolSnapshot{
		SessionID:  "session-1",
		Generation: 1,
		Tools:      []aguitypes.Tool{clientTool("client_a"), clientTool("client_b")},
	}); err != nil {
		t.Fatalf("SetClientTools error = %v", err)
	}
	snap := snapshot("session-1")
	snap.Config.Tools.Enabled = []string{"client_a", "client_b"}
	snap.Config.Tools.Disabled = []string{"client_b"}
	materialized, err := registry.ResolveTools(context.Background(), snap)
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if len(materialized) != 1 || materialized[0].Name != "client_a" {
		t.Fatalf("materialized = %#v", names(materialized))
	}
}

func TestRegistryRequiresDispatcherForClientTools(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(nil, nil)
	if err := registry.SetClientTools(agentagui.ClientToolSnapshot{SessionID: "session-1", Generation: 1, Tools: []aguitypes.Tool{clientTool("client")}}); err != nil {
		t.Fatalf("SetClientTools error = %v", err)
	}
	_, err := registry.ResolveTools(context.Background(), snapshot("session-1"))
	if !errors.Is(err, agentagui.ErrClientToolDispatchRequired) {
		t.Fatalf("ResolveTools error = %v, want ErrClientToolDispatchRequired", err)
	}
}

func TestRegistryServerToolWinsNameConflictWithoutMutation(t *testing.T) {
	t.Parallel()

	server := agenttools.NewRegistry()
	if _, err := server.Register(serverDefinition("shared")); err != nil {
		t.Fatalf("Register server tool error = %v", err)
	}
	registry := NewRegistry(server, testDispatcher())
	if err := registry.SetClientTools(agentagui.ClientToolSnapshot{SessionID: "session-1", Generation: 1, Tools: []aguitypes.Tool{clientTool("shared")}}); err != nil {
		t.Fatalf("SetClientTools error = %v", err)
	}
	first, err := registry.ResolveTools(context.Background(), snapshot("session-1"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	if len(first) != 1 || first[0].Name != "shared" || first[0].Metadata[agentagui.MetadataClientTool] == "true" {
		t.Fatalf("materialized = %#v", first)
	}
	first[0].Info.Name = "mutated"
	again, err := registry.ResolveTools(context.Background(), snapshot("session-1"))
	if err != nil {
		t.Fatalf("ResolveTools again error = %v", err)
	}
	if again[0].Info.Name != "shared" {
		t.Fatalf("shared model info mutation leaked: %q", again[0].Info.Name)
	}
}

func TestClientNames(t *testing.T) {
	t.Parallel()

	names := ClientNames([]aguitypes.Tool{clientTool("client_lookup"), clientTool("server_tool"), aguitypes.Tool{}}, map[string]bool{"server_tool": true})
	if !names["client_lookup"] || len(names) != 1 {
		t.Fatalf("ClientNames = %#v", names)
	}
}

func serverDefinition(name string) agenttools.Definition {
	return agenttools.Definition{
		Name:        name,
		Description: "server tool",
		Parameters:  einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
		Decode: func(_ context.Context, raw json.RawMessage) (any, error) {
			var value map[string]any
			return value, json.Unmarshal(raw, &value)
		},
		Encode: func(_ context.Context, value any) (json.RawMessage, error) {
			return json.Marshal(value)
		},
		Execute: func(context.Context, agenttools.Execution) (any, error) {
			return map[string]string{"ok": "true"}, nil
		},
	}
}

func clientTool(name string) aguitypes.Tool {
	return aguitypes.Tool{
		Name:        name,
		Description: "client tool",
		Parameters:  map[string]any{"type": "object"},
	}
}

func snapshot(id session.ID) runtime.TurnSnapshot {
	return runtime.TurnSnapshot{
		SessionID: id,
		Config: config.Snapshot{
			Metadata: map[string]string{
				"workspace_id":   "workspace-" + string(id),
				"workspace_root": "/workspace/" + string(id),
			},
		},
	}
}

func names(tools []runtime.Tool) []string {
	result := make([]string, 0, len(tools))
	for _, tool := range tools {
		result = append(result, tool.Name)
	}
	return result
}

func testDispatcher() agentagui.ClientToolDispatcher {
	return agentagui.ClientToolDispatcherFunc(func(_ context.Context, call runtime.ToolCall) (runtime.ToolResult, error) {
		return runtime.ToolResult{Output: string(call.Input)}, nil
	})
}
