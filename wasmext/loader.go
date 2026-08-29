package wasmext

import (
	"context"
	"errors"
	"sync"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/permissions"
)

// Loader owns all modules opened through it and provides one-call shutdown.
type Loader struct {
	mu           sync.Mutex
	closed       bool
	modules      []*ownedModule
	factory      engineFactory
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

type ownedModule struct {
	module    *module
	cleanup   func()
	beginOnce sync.Once
}

func (o *ownedModule) beginShutdown() {
	if o == nil || o.module == nil {
		return
	}
	o.beginOnce.Do(func() {
		if o.cleanup != nil {
			o.cleanup()
		}
		o.module.beginShutdown()
	})
}

// NewLoader returns an empty module owner.
func NewLoader() *Loader { return &Loader{factory: newEngine, shutdownDone: make(chan struct{})} }

// RegisterTool loads, registers, and owns one Wasm tool for the lifetime of
// the prepared composition mount or Loader, whichever closes first.
func (l *Loader) RegisterTool(ctx context.Context, registrar *composition.Registrar, registration composition.ToolRegistration, cfg ModuleConfig) error {
	if registrar == nil {
		return errors.New("wasm tool registration requires registrar")
	}
	loaded, err := openTool(ctx, cfg, l.engineFactory())
	if err != nil {
		return err
	}
	definition, err := loaded.definition.Clone()
	if err != nil {
		_ = loaded.close()
		return err
	}
	registration.Definition = definition
	owned := &ownedModule{module: loaded.module}
	return l.registerOwned(ctx, registrar, owned, func() error { return registrar.Tool(registration) })
}

// LoadHostPermissionsPolicy loads and tracks a Wasm-backed host-global policy.
// The policy remains valid only until Loader.Close completes.
func (l *Loader) LoadHostPermissionsPolicy(ctx context.Context, cfg ModuleConfig) (permissions.Policy, error) {
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

// RegisterEventSink loads, registers, and owns one Wasm event observer for the
// lifetime of the prepared mount or Loader, whichever closes first.
func (l *Loader) RegisterEventSink(ctx context.Context, registrar *composition.Registrar, spec extension.Registration, cfg ModuleConfig) error {
	if registrar == nil {
		return errors.New("wasm event sink registration requires registrar")
	}
	loaded, err := openEventSink(ctx, cfg, l.engineFactory())
	if err != nil {
		return err
	}
	owned := &ownedModule{module: loaded.module}
	return l.registerOwned(ctx, registrar, owned, func() error { return registerEventSink(registrar.Extensions(), spec, loaded) })
}

// RegisterContextSource loads, registers, and owns one Wasm context source for
// the lifetime of the prepared mount or Loader, whichever closes first.
func (l *Loader) RegisterContextSource(ctx context.Context, registrar *composition.Registrar, spec extension.Registration, cfg ModuleConfig) error {
	if registrar == nil {
		return errors.New("wasm context source registration requires registrar")
	}
	loaded, err := openContextSource(ctx, cfg, l.engineFactory())
	if err != nil {
		return err
	}
	owned := &ownedModule{module: loaded.module}
	return l.registerOwned(ctx, registrar, owned, func() error { return registerContextSource(registrar.Extensions(), spec, loaded) })
}

// RegisterHook loads, registers, and owns one Wasm lifecycle hook for the
// lifetime of the prepared mount or Loader, whichever closes first.
func (l *Loader) RegisterHook(ctx context.Context, registrar *composition.Registrar, spec extension.Registration, cfg ModuleConfig) error {
	if registrar == nil {
		return errors.New("wasm hook registration requires registrar")
	}
	loaded, err := openHook(ctx, cfg, l.engineFactory())
	if err != nil {
		return err
	}
	owned := &ownedModule{module: loaded.module}
	return l.registerOwned(ctx, registrar, owned, func() error { return registerHook(registrar.Extensions(), spec, loaded) })
}

// RegisterToolMiddleware loads, registers, and owns one Wasm tool middleware
// for the lifetime of the prepared mount or Loader, whichever closes first.
func (l *Loader) RegisterToolMiddleware(ctx context.Context, registrar *composition.Registrar, spec extension.Registration, cfg ModuleConfig) error {
	if registrar == nil {
		return errors.New("wasm tool middleware registration requires registrar")
	}
	loaded, err := openToolMiddleware(ctx, cfg, l.engineFactory())
	if err != nil {
		return err
	}
	owned := &ownedModule{module: loaded.module}
	return l.registerOwned(ctx, registrar, owned, func() error { return registerToolMiddleware(registrar.Extensions(), spec, loaded) })
}

type cleanupRegistrar interface {
	Defer(extension.Cleanup) error
}

func (l *Loader) registerOwned(ctx context.Context, registrar cleanupRegistrar, owned *ownedModule, register func() error) error {
	if registrar == nil {
		owned.beginShutdown()
		return errors.Join(errors.New("wasm registration requires registrar"), owned.module.waitFinalized(context.WithoutCancel(ctx)))
	}
	cleanup := func(cleanupCtx context.Context) error { return l.release(cleanupCtx, owned) }
	if err := registrar.Defer(cleanup); err != nil {
		owned.beginShutdown()
		return errors.Join(err, owned.module.waitFinalized(context.WithoutCancel(ctx)))
	}
	if err := register(); err != nil {
		return errors.Join(err, l.release(context.WithoutCancel(ctx), owned))
	}
	if err := l.trackOwned(owned); err != nil {
		return errors.Join(err, l.release(context.WithoutCancel(ctx), owned))
	}
	return nil
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
	return l.trackOwned(&ownedModule{module: module, cleanup: cleanup})
}

func (l *Loader) trackOwned(owned *ownedModule) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return extensionError(ErrorClosed, owned.module.identity, "load", nil)
	}
	l.modules = append(l.modules, owned)
	return nil
}

func (l *Loader) release(ctx context.Context, owned *ownedModule) error {
	if owned == nil || owned.module == nil {
		return nil
	}
	l.mu.Lock()
	for index, candidate := range l.modules {
		if candidate == owned {
			copy(l.modules[index:], l.modules[index+1:])
			l.modules[len(l.modules)-1] = nil
			l.modules = l.modules[:len(l.modules)-1]
			break
		}
	}
	l.mu.Unlock()
	owned.beginShutdown()
	return owned.module.waitFinalized(ctx)
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
		modules := append([]*ownedModule(nil), l.modules...)
		l.modules = nil
		if l.shutdownDone == nil {
			l.shutdownDone = make(chan struct{})
		}
		l.mu.Unlock()
		go func() {
			for index := len(modules) - 1; index >= 0; index-- {
				modules[index].beginShutdown()
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
