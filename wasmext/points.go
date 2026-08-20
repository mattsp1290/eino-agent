package wasmext

import (
	"context"
	"fmt"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// RegisterContextSource adapts context-source@0.1.0 to the native context
// assembly point while preserving the legacy LoadContext constructor surface.
func RegisterContextSource(registrar extension.Registrar, spec extension.Registration, source *LoadedContextSource) error {
	if source == nil {
		return fmt.Errorf("nil Wasm context source")
	}
	return extension.Use(registrar, runtime.ContextAssemblePoint, spec, func(ctx context.Context, assembly runtime.ContextAssembly, next extension.Next[runtime.ContextAssembly, runtime.ContextAssembly]) (runtime.ContextAssembly, error) {
		messages, err := source.LoadContext(ctx, runtime.TurnSnapshot{RunID: assembly.RunID, SessionID: assembly.SessionID, EpochID: assembly.EpochID, Messages: assembly.Base})
		if err != nil {
			return runtime.ContextAssembly{}, err
		}
		for index, message := range messages {
			assembly.Contributions = append(assembly.Contributions, runtime.ContextContribution{Source: fmt.Sprintf("%s/%06d", spec.ID, index), Order: spec.Order, Message: message})
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
		return hook.BeforeRun(ctx, session.Run{ID: notice.RunID, SessionID: notice.SessionID, ContextEpoch: ""})
	}); err != nil {
		return err
	}
	turn := spec
	turn.ID += "/before-turn"
	if err := extension.Use(registrar, runtime.ContextAssemblePoint, turn, func(ctx context.Context, assembly runtime.ContextAssembly, next extension.Next[runtime.ContextAssembly, runtime.ContextAssembly]) (runtime.ContextAssembly, error) {
		if _, err := hook.BeforeTurn(ctx, runtime.TurnSnapshot{RunID: assembly.RunID, SessionID: assembly.SessionID, EpochID: assembly.EpochID, Messages: assembly.Base}); err != nil {
			return runtime.ContextAssembly{}, err
		}
		return next(ctx, assembly)
	}); err != nil {
		return err
	}
	settled := spec
	settled.ID += "/after-run"
	return extension.On(registrar, runtime.RunSettledPoint, settled, func(ctx context.Context, notice runtime.RunSettledNotice) error {
		snapshot := runtime.TurnSnapshot{RunID: notice.Result.RunID, SessionID: notice.SessionID}
		if err := hook.AfterTurn(ctx, snapshot, notice.Result); err != nil {
			return err
		}
		return hook.AfterRun(ctx, notice.Result)
	})
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
		input, err := middleware.BeforeToolCall(ctx, prepared.Tool, prepared.Call)
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
		transformed, err := middleware.AfterToolCall(ctx, runtime.Tool{Name: outcome.Call.Name}, outcome.Call, outcome.Result, outcome.RawError)
		if err != nil {
			return runtime.ToolOutcome{}, err
		}
		outcome.Result = transformed
		return next(ctx, outcome)
	})
}
