package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestValidateDefinitionRejectsIncompleteAndMalformedDefinitions(t *testing.T) {
	t.Parallel()
	if err := ValidateDefinition(Definition{}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("ValidateDefinition empty error = %v, want ErrInvalidDefinition", err)
	}
	malformed := testDefinition("malformed")
	malformed.Parameters = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"broken": nil})
	if err := ValidateDefinition(malformed); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("ValidateDefinition malformed error = %v, want ErrInvalidDefinition", err)
	}
}

func TestMaterializeUsesBoundedScopeAndReturnsIndependentContainers(t *testing.T) {
	t.Parallel()
	definition := testDefinition("echo")
	definition.Scope = func(scope runtime.ToolScopeContext) runtime.ToolScope {
		return runtime.ToolScope{WorkspaceID: scope.WorkspaceID, Root: "session://" + string(scope.SessionID)}
	}
	first, err := Materialize(context.Background(), definition, toolScope("session-a"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Materialize(context.Background(), definition, toolScope("session-b"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Scope.WorkspaceID != "workspace-session-a" || first.Scope.Root != "session://session-a" || second.Scope.Root != "session://session-b" {
		t.Fatalf("scopes = %#v, %#v", first.Scope, second.Scope)
	}
	first.Info.Name = "mutated"
	first.Scope.Permissions[0] = "mutated"
	first.Metadata["kind"] = "mutated"
	if second.Info.Name != "echo" || second.Scope.Permissions[0] != "workspace:read" || second.Metadata["kind"] != "test" {
		t.Fatalf("materializations shared containers: %#v", second)
	}
}

func TestMaterializeDecodeAndExecuteTypedTool(t *testing.T) {
	t.Parallel()
	tool, err := Materialize(context.Background(), testDefinition("echo"), toolScope("session"))
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := tool.InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"text":"hi"}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	if string(normalized) != `{"text":"hi"}` {
		t.Fatalf("normalized = %s", normalized)
	}
	result, err := tool.Executor.Execute(context.Background(), runtime.ToolCall{Input: normalized})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if string(result.Structured) != `{"text":"hi"}` || result.Output != `{"text":"hi"}` {
		t.Fatalf("result = %#v", result)
	}
}

func TestMaterializeTypedExecutorEncodesOutput(t *testing.T) {
	t.Parallel()
	type input struct {
		Query string `json:"query"`
	}
	type output struct {
		Results []string `json:"results"`
	}
	definition := Definition{
		Name: "search",
		Execute: TypedExecutor[input, output](func(_ context.Context, execution TypedExecution[input]) (output, error) {
			return output{Results: []string{execution.Input.Query}}, nil
		}),
	}
	tool, err := Materialize(context.Background(), definition, toolScope("session"))
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := tool.InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"query":"needle"}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	result, err := tool.Executor.Execute(context.Background(), runtime.ToolCall{Input: normalized})
	if err != nil || string(result.Structured) != `{"results":["needle"]}` {
		t.Fatalf("result = %s, error = %v", result.Structured, err)
	}
}

func TestMaterializeReturnsTypedInputErrorsAndHonorsCancellation(t *testing.T) {
	t.Parallel()
	tool, err := Materialize(context.Background(), testDefinition("echo"), toolScope("session"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{`)); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("DecodeToolInput error = %v, want ErrMalformedInput", err)
	}
	if _, err := tool.Executor.Execute(context.Background(), runtime.ToolCall{Input: json.RawMessage(`{"wrong":true}`)}); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("Execute malformed error = %v, want ErrMalformedInput", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Materialize(canceled, testDefinition("echo"), toolScope("session")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Materialize canceled error = %v", err)
	}
}

func TestMaterializeClonesParameterSchemas(t *testing.T) {
	t.Parallel()
	params := einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
		"text": {Type: einoschema.String, Required: true},
	})
	definition := testDefinition("echo")
	definition.Parameters = params
	first, err := Materialize(context.Background(), definition, toolScope("session-a"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Materialize(context.Background(), definition, toolScope("session-b"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Info.ParamsOneOf == nil || second.Info.ParamsOneOf == nil || first.Info.ParamsOneOf == params || second.Info.ParamsOneOf == params || first.Info.ParamsOneOf == second.Info.ParamsOneOf {
		t.Fatal("parameter schema pointer shared across definition or materializations")
	}
}

func testDefinition(name string) Definition {
	type input struct {
		Text string `json:"text"`
	}
	return Definition{
		Name: name, Description: "echo input",
		Normalize: TypedNormalizer(func(_ context.Context, value input) (input, error) {
			if value.Text == "" {
				return input{}, errors.New("text required")
			}
			return value, nil
		}),
		Execute: TypedExecutor[input, input](func(_ context.Context, execution TypedExecution[input]) (input, error) {
			if execution.Input.Text == "" {
				return input{}, fmt.Errorf("%w: text required", ErrMalformedInput)
			}
			return execution.Input, nil
		}),
		RetrySafe: true, Permissions: []string{"workspace:read"}, Metadata: map[string]string{"kind": "test"},
	}
}

func toolScope(id session.ID) runtime.ToolScopeContext {
	return runtime.ToolScopeContext{SessionID: id, WorkspaceID: "workspace-" + string(id), WorkspaceRoot: "project://" + string(id)}
}
