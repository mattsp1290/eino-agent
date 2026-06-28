package agui

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func TestClientToolSnapshotMaterializesModelFacingTools(t *testing.T) {
	t.Parallel()

	tools, err := ClientToolSnapshot{
		SessionID:  "session-1",
		Generation: 7,
		Tools:      []aguitypes.Tool{clientTool("client_lookup")},
	}.RuntimeTools(testDispatcher(t))
	if err != nil {
		t.Fatalf("RuntimeTools error = %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "client_lookup" {
		t.Fatalf("tools = %#v", tools)
	}
	if tools[0].Metadata[MetadataClientTool] != "true" || tools[0].Metadata[MetadataClientToolGeneration] != "7" {
		t.Fatalf("metadata = %#v", tools[0].Metadata)
	}
	if got := tools[0].Scope.Permissions; len(got) != 1 || got[0] != PermissionClientTool {
		t.Fatalf("permissions = %#v", got)
	}
	normalized, err := tools[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"query":"hi"}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	if string(normalized) != `{"query":"hi"}` {
		t.Fatalf("normalized = %s", normalized)
	}
	result, err := tools[0].Executor.Execute(context.Background(), runtime.ToolCall{ID: "call-1", Input: normalized})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if result.Output != `{"client":true}` {
		t.Fatalf("client executor result = %#v", result)
	}
}

func TestClientToolSnapshotRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	tools, err := ClientToolSnapshot{SessionID: "session-1", Generation: 1, Tools: []aguitypes.Tool{clientTool("client_lookup")}}.RuntimeTools(testDispatcher(t))
	if err != nil {
		t.Fatalf("RuntimeTools error = %v", err)
	}
	_, err = tools[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{`))
	if !errors.Is(err, agenttools.ErrMalformedInput) {
		t.Fatalf("DecodeToolInput error = %v, want ErrMalformedInput", err)
	}
}

func TestClientToolSnapshotClonesDefinitions(t *testing.T) {
	t.Parallel()

	snapshot := ClientToolSnapshot{
		SessionID:  "session-1",
		Generation: 1,
		Tools:      []aguitypes.Tool{clientTool("client_lookup")},
	}
	tools, err := snapshot.RuntimeTools(testDispatcher(t))
	if err != nil {
		t.Fatalf("RuntimeTools error = %v", err)
	}
	tools[0].Info.Name = "mutated"
	again, err := snapshot.RuntimeTools(testDispatcher(t))
	if err != nil {
		t.Fatalf("RuntimeTools again error = %v", err)
	}
	if again[0].Info.Name != "client_lookup" {
		t.Fatalf("tool info shared mutable state: %q", again[0].Info.Name)
	}
	cloned := snapshot.Clone()
	snapshot.Tools[0].Parameters.(map[string]any)["properties"].(map[string]any)["query"] = map[string]any{"type": "number"}
	fromClone, err := cloned.RuntimeTools(testDispatcher(t))
	if err != nil {
		t.Fatalf("RuntimeTools clone error = %v", err)
	}
	schema, err := fromClone[0].Info.ToJSONSchema()
	if err != nil {
		t.Fatalf("ToJSONSchema error = %v", err)
	}
	raw, _ := json.Marshal(schema)
	if !strings.Contains(string(raw), `"type":"string"`) {
		t.Fatalf("cloned schema was mutated: %s", raw)
	}
}

func TestClientToolSnapshotRequiresDispatcher(t *testing.T) {
	t.Parallel()

	_, err := ClientToolSnapshot{SessionID: "session-1", Generation: 1, Tools: []aguitypes.Tool{clientTool("client_lookup")}}.RuntimeTools(nil)
	if !errors.Is(err, ErrClientToolDispatchRequired) {
		t.Fatalf("RuntimeTools error = %v, want ErrClientToolDispatchRequired", err)
	}
}

func clientTool(name string) aguitypes.Tool {
	return aguitypes.Tool{
		Name:        name,
		Description: "client lookup",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string"},
			},
		},
	}
}

func testDispatcher(t *testing.T) ClientToolDispatcher {
	t.Helper()
	return ClientToolDispatcherFunc(func(_ context.Context, call runtime.ToolCall) (runtime.ToolResult, error) {
		if call.ID == "" {
			t.Fatal("client tool call missing id")
		}
		return runtime.ToolResult{Output: `{"client":true}`}, nil
	})
}

var _ = session.ID("")
