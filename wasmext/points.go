package wasmext

import (
	"context"
	"errors"
	"fmt"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
)

// RegisterContextSource adapts context-source@0.1.0 to the native context
// assembly point.
func RegisterContextSource(registrar extension.Registrar, spec extension.Registration, source *LoadedContextSource) error {
	if source == nil {
		return fmt.Errorf("nil Wasm context source")
	}
	return extension.Use(registrar, runtime.ContextAssemblePoint, spec, func(ctx context.Context, assembly runtime.ContextAssembly, next extension.Next[runtime.ContextAssembly, runtime.ContextAssembly]) (runtime.ContextAssembly, error) {
		messages, err := source.loadBoundedContext(ctx, assembly.Metadata)
		if err != nil {
			return runtime.ContextAssembly{}, err
		}
		for index, message := range messages {
			assembly.Contributions = append(assembly.Contributions, runtime.ContextContribution{Source: contextContributionSource(spec, index), Order: spec.Order, Message: message})
		}
		return next(ctx, assembly)
	})
}

// RegisterEventSink adapts event-sink@0.1.0 to immutable runtime event
// observation. Infrastructure event delivery remains a separate EventSink.
func RegisterEventSink(registrar extension.Registrar, spec extension.Registration, sink *LoadedEventSink) error {
	if sink == nil {
		return fmt.Errorf("nil Wasm event sink")
	}
	return extension.On(registrar, runtime.EventPublishedPoint, spec, sink.Emit)
}

// RegisterHook maps the curated hook world to run admission, context assembly,
// and settled-run points. It reuses LoadedHook's bounded per-run metadata cache.
func RegisterHook(registrar extension.Registrar, spec extension.Registration, hook *LoadedHook) error {
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
	turn := spec
	turn.ID += "/before-turn"
	if err := extension.Use(registrar, runtime.TurnPreparePoint, turn, func(ctx context.Context, metadata runtime.BoundedTurnMetadata, next extension.Next[runtime.BoundedTurnMetadata, runtime.BoundedTurnMetadata]) (runtime.BoundedTurnMetadata, error) {
		if err := hook.beforeTurnBounded(ctx, metadata); err != nil {
			return runtime.BoundedTurnMetadata{}, err
		}
		return next(ctx, metadata)
	}); err != nil {
		return err
	}
	settled := spec
	settled.ID += "/after-run"
	return extension.On(registrar, runtime.RunSettledPoint, settled, func(ctx context.Context, notice runtime.RunSettledNotice) error {
		return finishRegisteredHook(ctx, hook, notice)
	})
}

func contextContributionSource(spec extension.Registration, index int) string {
	parts := []string{spec.InstanceID, spec.ID, string(spec.Scope.Kind), spec.Scope.Key}
	return fmt.Sprintf("wasm-context/%d:%s/%d:%s/%d:%s/%d:%s/%06d", len(parts[0]), parts[0], len(parts[1]), parts[1], len(parts[2]), parts[2], len(parts[3]), parts[3], index)
}

func finishRegisteredHook(ctx context.Context, hook *LoadedHook, notice runtime.RunSettledNotice) error {
	snapshot := runtime.TurnSnapshot{RunID: notice.Result.RunID, SessionID: notice.SessionID}
	return errors.Join(hook.afterTurn(ctx, snapshot, notice.Result), hook.afterRun(ctx, notice.Result))
}

// RegisterToolMiddleware maps tool-middleware@0.1.0 only to prepare and result
// transformation. Around execution remains native-only.
func RegisterToolMiddleware(registrar extension.Registrar, spec extension.Registration, middleware *LoadedToolMiddleware) error {
	if middleware == nil {
		return fmt.Errorf("nil Wasm tool middleware")
	}
	prepare := spec
	prepare.ID += "/prepare"
	if err := extension.Use(registrar, runtime.ToolPreparePoint, prepare, func(ctx context.Context, prepared runtime.PreparedToolCall, next extension.Next[runtime.PreparedToolCall, runtime.PreparedToolCall]) (runtime.PreparedToolCall, error) {
		input, err := middleware.beforeToolCall(ctx, prepared.Tool, prepared.Call)
		if err != nil {
			return runtime.PreparedToolCall{}, err
		}
		prepared.Call.Input = input
		return next(ctx, prepared)
	}); err != nil {
		return err
	}
	result := spec
	result.ID += "/result"
	return extension.Use(registrar, runtime.ToolResultTransformPoint, result, func(ctx context.Context, outcome runtime.ToolOutcome, next extension.Next[runtime.ToolOutcome, runtime.ToolOutcome]) (runtime.ToolOutcome, error) {
		transformed, err := middleware.afterToolCall(ctx, runtime.Tool{Name: outcome.Call.Name}, outcome.Call, outcome.Result, outcome.RawError)
		if err != nil {
			return runtime.ToolOutcome{}, err
		}
		outcome.Result = transformed
		return next(ctx, outcome)
	})
}
