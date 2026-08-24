package wasmext

import (
	"context"
	"errors"
	"sync"

	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

// Loader owns all modules opened through it and provides one-call shutdown.
type Loader struct {
	mu      sync.Mutex
	closed  bool
	modules []*module
	factory engineFactory
}

// NewLoader returns an empty module owner.
func NewLoader() *Loader { return &Loader{factory: newEngine} }

// LoadTool loads and tracks a Wasm-backed native tool definition.
func (l *Loader) LoadTool(ctx context.Context, cfg ModuleConfig) (tools.Definition, error) {
	loaded, err := openTool(ctx, cfg, l.engineFactory())
	if err != nil {
		return tools.Definition{}, err
	}
	if err := l.track(loaded.module); err != nil {
		_ = loaded.Close()
		return tools.Definition{}, err
	}
	return loaded.Definition()
}

// LoadPermissionsPolicy loads and tracks a Wasm-backed native policy.
func (l *Loader) LoadPermissionsPolicy(ctx context.Context, cfg ModuleConfig) (permissions.Policy, error) {
	policy, err := loadPermissionsPolicy(ctx, cfg, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(policy.module); err != nil {
		_ = policy.Close()
		return nil, err
	}
	return policy, nil
}

// LoadContextSource loads and tracks a context-source component.
func (l *Loader) LoadContextSource(ctx context.Context, cfg ModuleConfig) (*LoadedContextSource, error) {
	module, err := loadModule(ctx, cfg, contextSourceContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	component, err := componentAs[contextComponent](module, "context-source.load-context")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedContextSource{module: module, component: component}, nil
}

// LoadEventSink loads and tracks an event-sink component.
func (l *Loader) LoadEventSink(ctx context.Context, cfg ModuleConfig) (runtime.EventSink, error) {
	module, err := loadModule(ctx, cfg, eventSinkContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	component, err := componentAs[eventComponent](module, "event-sink.emit")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedEventSink{module: module, component: component}, nil
}

// LoadHook loads and tracks a hook component.
func (l *Loader) LoadHook(ctx context.Context, cfg ModuleConfig) (*LoadedHook, error) {
	module, err := loadModule(ctx, cfg, hookContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	component, err := componentAs[hookComponent](module, "hook.before-run")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedHook{module: module, component: component, turns: make(map[session.RunID]wittypes.TurnMetadata)}, nil
}

// LoadToolMiddleware loads and tracks a tool-middleware component.
func (l *Loader) LoadToolMiddleware(ctx context.Context, cfg ModuleConfig) (*LoadedToolMiddleware, error) {
	module, err := loadModule(ctx, cfg, toolMiddlewareContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	component, err := componentAs[middlewareComponent](module, "tool-middleware.before-tool-call")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedToolMiddleware{module: module, component: component}, nil
}

func (l *Loader) engineFactory() engineFactory {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.factory == nil {
		l.factory = newEngine
	}
	return l.factory
}

func (l *Loader) track(module *module) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return extensionError(ErrorClosed, module.identity, "load", nil)
	}
	l.modules = append(l.modules, module)
	return nil
}

// Close stops new loads, interrupts in-flight calls, and closes every tracked
// module exactly once. The supplied context bounds the aggregate shutdown.
func (l *Loader) Close(ctx context.Context) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	modules := append([]*module(nil), l.modules...)
	l.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		var errs []error
		for index := len(modules) - 1; index >= 0; index-- {
			errs = append(errs, modules[index].Close())
		}
		done <- errors.Join(errs...)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
