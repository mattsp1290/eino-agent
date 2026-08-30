package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/tools"
)

func TestMaterializedToolPassesPlanPrepareAndExecuteValidation(t *testing.T) {
	materialized, err := tools.Materialize(context.Background(), tools.Definition{
		Name: "echo",
		Execute: func(_ context.Context, execution tools.Execution) (json.RawMessage, error) {
			return execution.Input, nil
		},
	}, runtime.ToolScopeContext{SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}

	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "tool-validation", Artifact: extension.Artifact{Name: "tool-validation", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.OnTransform(registrar, runtime.ToolPreparePoint, extension.Registration{ID: "prepare", Scope: extension.GlobalScope()}, func(_ context.Context, input runtime.PreparedToolCall) (runtime.PreparedToolCall, error) {
			return input, nil
		}); err != nil {
			return err
		}
		return extension.OnAround(registrar, runtime.ToolExecutePoint, extension.Registration{ID: "execute", Scope: extension.GlobalScope()}, func(ctx context.Context, _ runtime.ToolExecution, proceed extension.Proceed) error {
			return proceed(ctx)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()

	call := runtime.ToolCall{ID: "call", Name: "echo", Input: json.RawMessage(`{"value":1}`)}
	preparedTool := materialized
	preparedTool.Executor = nil
	preparedTool.InputDecoder = nil
	preparedTool.Pattern = nil
	prepared, err := extension.ApplyTransforms(plan, context.Background(), runtime.ToolPreparePoint, runtime.PreparedToolCall{Tool: preparedTool, Call: call})
	if err != nil {
		t.Fatalf("ToolPreparePoint rejected unchanged registry tool: %v", err)
	}

	terminalErr := errors.New("terminal reached")
	terminalCalled := false
	_, err = extension.InvokeAround(plan, context.Background(), runtime.ToolExecutePoint, runtime.ToolExecution(prepared), func(context.Context) (runtime.ToolResult, error) {
		terminalCalled = true
		return runtime.ToolResult{}, terminalErr
	})
	if !terminalCalled || !errors.Is(err, terminalErr) {
		t.Fatalf("ToolExecutePoint did not reach terminal: called=%t err=%v", terminalCalled, err)
	}
}
