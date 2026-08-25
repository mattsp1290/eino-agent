package wasmext

import (
	"context"
	"errors"
	"sync"

	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/tools"
)

// Loader owns all modules opened through it and provides one-call shutdown.
type Loader struct {
	mu           sync.Mutex
	closed       bool
	modules      []ownedModule
	factory      engineFactory
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

type ownedModule struct {
	module  *module
	cleanup func()
}

// NewLoader returns an empty module owner.
func NewLoader() *Loader { return &Loader{factory: newEngine, shutdownDone: make(chan struct{})} }

// LoadTool loads and tracks a Wasm-backed native tool definition. The
// definition remains valid only until Loader.Close completes.
func (l *Loader) LoadTool(ctx context.Context, cfg ModuleConfig) (tools.Definition, error) {
	loaded, err := openTool(ctx, cfg, l.engineFactory())
	if err != nil {
		return tools.Definition{}, err
	}
	definition, err := loaded.Definition()
	if err != nil {
		_ = loaded.close()
		return tools.Definition{}, err
	}
	if err := l.track(loaded.module, nil); err != nil {
		_ = loaded.close()
		return tools.Definition{}, err
	}
	return definition, nil
}

// LoadPermissionsPolicy loads and tracks a Wasm-backed native policy. The
// policy remains valid only until Loader.Close completes.
func (l *Loader) LoadPermissionsPolicy(ctx context.Context, cfg ModuleConfig) (permissions.Policy, error) {
	policy, err := loadPermissionsPolicy(ctx, cfg, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(policy.module, nil); err != nil {
		_ = policy.close()
		return nil, err
	}
	return policy, nil
}

// LoadContextSource loads and tracks a context-source component.
func (l *Loader) LoadContextSource(ctx context.Context, cfg ModuleConfig) (*LoadedContextSource, error) {
	loaded, err := openContextSource(ctx, cfg, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(loaded.module, nil); err != nil {
		_ = loaded.close()
		return nil, err
	}
	return loaded, nil
}

// LoadEventSink loads and tracks an event-sink component. The sink remains
// valid only until Loader.Close completes.
func (l *Loader) LoadEventSink(ctx context.Context, cfg ModuleConfig) (runtime.EventSink, error) {
	loaded, err := openEventSink(ctx, cfg, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(loaded.module, nil); err != nil {
		_ = loaded.close()
		return nil, err
	}
	return loaded, nil
}

// LoadHook loads and tracks a hook component.
func (l *Loader) LoadHook(ctx context.Context, cfg ModuleConfig) (*LoadedHook, error) {
	loaded, err := openHook(ctx, cfg, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(loaded.module, loaded.cleanup); err != nil {
		_ = loaded.close()
		return nil, err
	}
	return loaded, nil
}

// LoadToolMiddleware loads and tracks a tool-middleware component.
func (l *Loader) LoadToolMiddleware(ctx context.Context, cfg ModuleConfig) (*LoadedToolMiddleware, error) {
	loaded, err := openToolMiddleware(ctx, cfg, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(loaded.module, nil); err != nil {
		_ = loaded.close()
		return nil, err
	}
	return loaded, nil
}

func (l *Loader) engineFactory() engineFactory {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.factory == nil {
		l.factory = newEngine
	}
	return l.factory
}

func (l *Loader) track(module *module, cleanup func()) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return extensionError(ErrorClosed, module.identity, "load", nil)
	}
	l.modules = append(l.modules, ownedModule{module: module, cleanup: cleanup})
	return nil
}

// Close stops new loads, interrupts in-flight calls, and closes every tracked
// module exactly once. The supplied context bounds the aggregate shutdown.
func (l *Loader) Close(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.shutdownOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		modules := append([]ownedModule(nil), l.modules...)
		if l.shutdownDone == nil {
			l.shutdownDone = make(chan struct{})
		}
		l.mu.Unlock()
		go func() {
			for index := len(modules) - 1; index >= 0; index-- {
				if modules[index].cleanup != nil {
					modules[index].cleanup()
				}
				modules[index].module.beginShutdown()
			}
			var errs []error
			for index := len(modules) - 1; index >= 0; index-- {
				errs = append(errs, modules[index].module.waitFinalized(context.Background()))
			}
			l.mu.Lock()
			l.shutdownErr = errors.Join(errs...)
			l.mu.Unlock()
			close(l.shutdownDone)
		}()
	})
	l.mu.Lock()
	done := l.shutdownDone
	l.mu.Unlock()
	select {
	case <-done:
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
