// Package nativeextension demonstrates one reversible, session-scoped plugin
// mounted through the same typed runtime points used by curated Wasm adapters.
package nativeextension

import (
	"context"
	"encoding/json"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

// Mount installs a complete example plugin for exactly one durable session.
// Deactivate prevents new runs from selecting it; Close waits for already
// admitted plans and then runs cleanup.
func Mount(ctx context.Context, registry *composition.Registry, sessionID session.ID, cleaned func()) (*composition.Mount, error) {
	const instanceID = "example.native/session-tools"
	component := extension.Component{InstanceID: instanceID, Artifact: extension.Artifact{
		Name: "example-native-extension", Version: "1.0.0", Hash: "example-artifact-v1", ConfigHash: "example-config-v1", SourceKind: extension.SourceNative,
	}}
	scope := extension.SessionScope(string(sessionID))
	return registry.Mount(ctx, component, composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
		if cleaned != nil {
			if err := registrar.Defer(func(context.Context) error { cleaned(); return nil }); err != nil {
				return err
			}
		}
		if err := registrar.Tool(composition.ToolRegistration{ID: "tool/echo", Order: runtime.OrderApplication, Scope: scope, Definition: echoDefinition()}); err != nil {
			return err
		}
		if err := registrar.Prompt(composition.PromptRegistration{ID: "prompt/policy", Name: "example/policy", Order: runtime.OrderApplication, Scope: scope, Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) {
			return "Use the session echo tool only for explicitly requested text.", nil
		})}); err != nil {
			return err
		}
		if err := registrar.Guard(composition.GuardRegistration{ID: "guard/blocked-input", Order: runtime.OrderApplication, Scope: scope, Guard: runtime.ToolGuardFunc(func(_ context.Context, request runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
			if jsonContainsTrue(request.Call.Input, "blocked") {
				return runtime.ToolGuardResult{Decision: runtime.ToolGuardDeny, Code: "example_blocked", Message: "example plugin blocked this input"}, nil
			}
			return runtime.ToolGuardResult{Decision: runtime.ToolGuardAbstain}, nil
		})}); err != nil {
			return err
		}
		if err := extension.Use(registrar.Extensions(), runtime.ContextAssemblePoint, extension.Registration{ID: "context/session", Order: runtime.OrderApplication, Scope: scope}, func(ctx context.Context, assembly runtime.ContextAssembly, next extension.Next[runtime.ContextAssembly, runtime.ContextAssembly]) (runtime.ContextAssembly, error) {
			assembly.Contributions = append(assembly.Contributions, runtime.ContextContribution{Source: instanceID + "/context", Order: runtime.OrderApplication, Message: einoschema.SystemMessage("Native example extension is active for this session.")})
			return next(ctx, assembly)
		}); err != nil {
			return err
		}
		if err := extension.Use(registrar.Extensions(), runtime.ToolPreparePoint, extension.Registration{ID: "tool/prepare", Order: runtime.OrderApplication, Scope: scope}, func(ctx context.Context, prepared runtime.PreparedToolCall, next extension.Next[runtime.PreparedToolCall, runtime.PreparedToolCall]) (runtime.PreparedToolCall, error) {
			var input map[string]any
			if err := json.Unmarshal(prepared.Call.Input, &input); err != nil {
				return runtime.PreparedToolCall{}, err
			}
			input["prepared_by"] = instanceID
			prepared.Call.Input, _ = json.Marshal(input)
			return next(ctx, prepared)
		}); err != nil {
			return err
		}
		return extension.On(registrar.Extensions(), runtime.ToolSettledPoint, extension.Registration{ID: "tool/settled", Order: runtime.OrderApplication, Scope: scope}, func(context.Context, runtime.ToolSettledNotice) error {
			return nil
		})
	}))
}

func echoDefinition() tools.Definition {
	return tools.Definition{
		Name: "example_echo", Description: "Echo text from the session-scoped native extension.",
		Execute: tools.TypedExecutor[map[string]any, map[string]any](func(_ context.Context, execution tools.TypedExecution[map[string]any]) (map[string]any, error) {
			return map[string]any{"text": execution.Input["text"]}, nil
		}),
		Permissions: []string{"example.echo"},
	}
}

func jsonContainsTrue(raw json.RawMessage, key string) bool {
	var input map[string]any
	return json.Unmarshal(raw, &input) == nil && input[key] == true
}
