package agui

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"

	"github.com/mattsp1290/eino-agent/runtime"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func TestClientToolSnapshotBuildsCanonicalDefinitions(t *testing.T) {
	definitions, err := ClientToolSnapshot{
		SessionID: "session-1", Generation: 7, DispatcherArtifactID: "dispatcher-v1",
		Tools: []aguitypes.Tool{clientTool("client_lookup")},
	}.Definitions(testDispatcher(t, json.RawMessage(`{"client":true}`)))
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 1 || definitions[0].Name != "client_lookup" {
		t.Fatalf("definitions = %#v", definitions)
	}
	definition := definitions[0]
	if definition.Metadata[MetadataClientTool] != "true" || definition.Metadata[MetadataClientToolGeneration] != "7" {
		t.Fatalf("metadata = %#v", definition.Metadata)
	}
	if len(definition.Permissions) != 1 || definition.Permissions[0] != PermissionClientTool {
		t.Fatalf("permissions = %#v", definition.Permissions)
	}
	decoded, err := definition.Decode(context.Background(), json.RawMessage(`{"query":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	output, err := definition.Execute(context.Background(), agenttools.Execution{Input: decoded, Call: runtime.ToolCall{ID: "call-1"}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := definition.Encode(context.Background(), output)
	if err != nil || string(encoded) != `{"client":true}` {
		t.Fatalf("encoded = %s, %v", encoded, err)
	}
}

func TestClientToolDefinitionRejectsMalformedInputAndResult(t *testing.T) {
	definitions, err := ClientToolSnapshot{SessionID: "session", Generation: 1, DispatcherArtifactID: "dispatcher", Tools: []aguitypes.Tool{clientTool("client")}}.Definitions(testDispatcher(t, json.RawMessage(`{`)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definitions[0].Decode(context.Background(), json.RawMessage(`{`)); !errors.Is(err, agenttools.ErrMalformedInput) {
		t.Fatalf("decode error = %v", err)
	}
	output, err := definitions[0].Execute(context.Background(), agenttools.Execution{Call: runtime.ToolCall{ID: "call"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definitions[0].Encode(context.Background(), output); err == nil {
		t.Fatal("invalid dispatcher JSON result accepted")
	}
}

func TestClientToolSnapshotCloneIsCheckedAndIsolated(t *testing.T) {
	snapshot := ClientToolSnapshot{SessionID: "session", Generation: 1, DispatcherArtifactID: "dispatcher", Tools: []aguitypes.Tool{clientTool("client")}}
	cloned, err := snapshot.Clone()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Tools[0].Parameters.(map[string]any)["properties"].(map[string]any)["query"] = map[string]any{"type": "number"}
	definitions, err := cloned.Definitions(testDispatcher(t, json.RawMessage(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := definitions[0].Parameters.ToJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(schema)
	if !strings.Contains(string(raw), `"type":"string"`) {
		t.Fatalf("cloned schema was mutated: %s", raw)
	}

	unsupported := snapshot
	unsupported.Tools = []aguitypes.Tool{{Name: "bad", Parameters: map[string]any{"bad": make(chan int)}}}
	if _, err := unsupported.Clone(); err == nil {
		t.Fatal("unsupported parameter graph cloned without error")
	}
}

func TestClientToolSnapshotRequiresDispatcher(t *testing.T) {
	_, err := ClientToolSnapshot{SessionID: "session", Generation: 1, DispatcherArtifactID: "dispatcher", Tools: []aguitypes.Tool{clientTool("client")}}.Definitions(nil)
	if !errors.Is(err, ErrClientToolDispatchRequired) {
		t.Fatalf("error = %v", err)
	}
}

func TestClientToolDefinitionPropagatesDispatcherError(t *testing.T) {
	want := errors.New("dispatch failed")
	dispatcher := ClientToolDispatcherFunc(func(context.Context, runtime.ToolCall) (json.RawMessage, error) { return nil, want })
	definitions, err := ClientToolSnapshot{SessionID: "session", Generation: 1, DispatcherArtifactID: "dispatcher", Tools: []aguitypes.Tool{clientTool("client")}}.Definitions(dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definitions[0].Execute(context.Background(), agenttools.Execution{Call: runtime.ToolCall{ID: "call"}}); !errors.Is(err, want) {
		t.Fatalf("execute error = %v, want dispatcher error", err)
	}
}

func clientTool(name string) aguitypes.Tool {
	return aguitypes.Tool{Name: name, Description: "client lookup", Parameters: map[string]any{
		"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}},
	}}
}

func testDispatcher(t *testing.T, result json.RawMessage) ClientToolDispatcher {
	t.Helper()
	return ClientToolDispatcherFunc(func(_ context.Context, call runtime.ToolCall) (json.RawMessage, error) {
		if call.ID == "" {
			t.Fatal("client tool call missing id")
		}
		return result, nil
	})
}
