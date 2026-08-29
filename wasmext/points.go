package wasmext

import (
	"context"
	"fmt"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
)

func registerContextSource(registrar extension.Registrar, spec extension.Registration, source *loadedContextSource) error {
	if source == nil {
		return fmt.Errorf("nil Wasm context source")
	}
	instanceID := registrar.InstanceID()
	return extension.OnTransform(registrar, runtime.ContextAssemblePoint, spec, func(ctx context.Context, assembly runtime.ContextAssembly) (runtime.ContextAssembly, error) {
		messages, err := source.loadBoundedContext(ctx, assembly.Metadata)
		if err != nil {
			return runtime.ContextAssembly{}, err
		}
		for index, message := range messages {
			assembly.Contributions = append(assembly.Contributions, runtime.ContextContribution{Source: contextContributionSource(instanceID, spec, index), Order: spec.Order, Message: message})
		}
		return assembly, nil
	})
}

// RegisterEventSink adapts event-sink@0.1.0 to immutable runtime event
// observation. Infrastructure event delivery remains a separate EventSink.
func RegisterEventSink(registrar extension.Registrar, spec extension.Registration, sink runtime.EventSink) error {
	if sink == nil {
		return fmt.Errorf("nil Wasm event sink")
	}
	return extension.On(registrar, runtime.EventPublishedPoint, spec, sink.Emit)
}

// registerHook maps the curated hook world to run admission and settled-run
// notifications. Both notices carry bounded metadata directly.
func registerHook(registrar extension.Registrar, spec extension.Registration, hook *loadedHook) error {
	if hook == nil {
		return fmt.Errorf("nil Wasm hook")
	}
	admitted := spec
	admitted.ID += "/before-run"
	if err := extension.On(registrar, runtime.RunAdmittedPoint, admitted, func(ctx context.Context, notice runtime.RunAdmittedNotice) error {
		return hook.beforeRunBounded(ctx, notice.Metadata)
	}); err != nil {
		return err
	}
	settled := spec
	settled.ID += "/after-run"
	return extension.On(registrar, runtime.RunSettledPoint, settled, func(ctx context.Context, notice runtime.RunSettledNotice) error {
		return hook.finish(ctx, notice.Metadata)
	})
}

func contextContributionSource(instanceID string, spec extension.Registration, index int) string {
	parts := []string{instanceID, spec.ID, string(spec.Scope.Kind), spec.Scope.Key}
	return fmt.Sprintf("wasm-context/%d:%s/%d:%s/%d:%s/%d:%s/%06d", len(parts[0]), parts[0], len(parts[1]), parts[1], len(parts[2]), parts[2], len(parts[3]), parts[3], index)
}

// registerToolMiddleware maps tool-middleware@0.1.0 only to prepare and result
// transformation. Around execution remains native-only.
func registerToolMiddleware(registrar extension.Registrar, spec extension.Registration, middleware *loadedToolMiddleware) error {
	if middleware == nil {
		return fmt.Errorf("nil Wasm tool middleware")
	}
	prepare := spec
	prepare.ID += "/prepare"
	if err := extension.OnTransform(registrar, runtime.ToolPreparePoint, prepare, func(ctx context.Context, prepared runtime.PreparedToolCall) (runtime.PreparedToolCall, error) {
		input, err := middleware.beforeToolCall(ctx, prepared.Tool, prepared.Call)
		if err != nil {
			return runtime.PreparedToolCall{}, err
		}
		prepared.Call.Input = input
		return prepared, nil
	}); err != nil {
		return err
	}
	result := spec
	result.ID += "/result"
	return extension.OnTransform(registrar, runtime.ToolResultTransformPoint, result, func(ctx context.Context, input runtime.ToolResultTransform) (runtime.ToolResultTransform, error) {
		transformed, err := middleware.afterToolCall(ctx, runtime.Tool{Name: input.ToolName}, input.Call, input.Result)
		if err != nil {
			return runtime.ToolResultTransform{}, err
		}
		input.Result = transformed
		return input, nil
	})
}
