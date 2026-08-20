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

func TestRegistryToolPassesPlanPrepareAndExecuteValidation(t *testing.T) {
	toolRegistry := tools.NewRegistry()
	_, err := toolRegistry.Register(tools.Definition{
		Name: "echo",
		Decode: func(_ context.Context, raw json.RawMessage) (any, error) {
			return append(json.RawMessage(nil), raw...), nil
		},
		Encode: func(_ context.Context, value any) (json.RawMessage, error) {
			return value.(json.RawMessage), nil
		},
		Execute: func(_ context.Context, execution tools.Execution) (any, error) {
			return execution.Input, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := toolRegistry.ResolveTools(context.Background(), runtime.TurnSnapshot{SessionID: "session"})
	if err != nil || len(materialized) != 1 {
		t.Fatalf("ResolveTools = %#v, %v", materialized, err)
	}

	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "tool-validation", Artifact: extension.Artifact{Name: "tool-validation", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.Use(registrar, runtime.ToolPreparePoint, extension.Registration{ID: "prepare", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input runtime.PreparedToolCall, next extension.Next[runtime.PreparedToolCall, runtime.PreparedToolCall]) (runtime.PreparedToolCall, error) {
			return next(ctx, input)
		}); err != nil {
			return err
		}
		return extension.Use(registrar, runtime.ToolExecutePoint, extension.Registration{ID: "execute", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input runtime.ToolExecution, next extension.Next[runtime.ToolExecution, runtime.ToolOutcome]) (runtime.ToolOutcome, error) {
			return next(ctx, input)
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
	prepared, err := extension.Invoke(plan, context.Background(), runtime.ToolPreparePoint, runtime.PreparedToolCall{Tool: materialized[0], Call: call}, func(_ context.Context, input runtime.PreparedToolCall) (runtime.PreparedToolCall, error) {
		return input, nil
	})
	if err != nil {
		t.Fatalf("ToolPreparePoint rejected unchanged registry tool: %v", err)
	}

	terminalErr := errors.New("terminal reached")
	terminalCalled := false
	_, err = extension.Invoke(plan, context.Background(), runtime.ToolExecutePoint, runtime.ToolExecution(prepared), func(_ context.Context, _ runtime.ToolExecution) (runtime.ToolOutcome, error) {
		terminalCalled = true
		return runtime.ToolOutcome{}, terminalErr
	})
	if !terminalCalled || !errors.Is(err, terminalErr) {
		t.Fatalf("ToolExecutePoint did not reach terminal: called=%t err=%v", terminalCalled, err)
	}
}
