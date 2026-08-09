package wasmext

import (
	"context"
	"errors"
	"sync"

	wasmtime "github.com/bytecodealliance/wasmtime-go/v47"
)

type wasmtimeEngine struct {
	mu     sync.Mutex
	engine *wasmtime.Engine
	closed bool
}

func newWasmtimeEngine(Limits) (engine, error) {
	config := wasmtime.NewConfig()
	config.SetWasmComponentModel(true)
	config.SetEpochInterruption(true)
	return &wasmtimeEngine{engine: wasmtime.NewEngineWithConfig(config)}, nil
}

func (e *wasmtimeEngine) Compile(_ context.Context, wasm []byte, contract worldContract) (compiledComponent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, errors.New("wasmtime engine closed")
	}
	component, err := wasmtime.NewComponent(e.engine, wasm)
	if err != nil {
		return nil, err
	}
	export := component.GetExportIndex(nil, contract.exportName)
	if export == nil {
		component.Close()
		return nil, errors.New("required world export missing")
	}
	defer export.Close()
	for _, name := range contract.functions {
		function := component.GetExportIndex(export, name)
		if function == nil {
			component.Close()
			return nil, errors.New("required world function missing")
		}
		function.Close()
	}
	return &wasmtimeComponent{component: component, engine: e.engine}, nil
}

func (e *wasmtimeEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	e.engine.Close()
	return nil
}

type wasmtimeComponent struct {
	component *wasmtime.Component
	engine    *wasmtime.Engine
	once      sync.Once
}

func (c *wasmtimeComponent) Call(context.Context, string, any, any) error {
	// wasmtime-go v47 exposes component compilation, reflection, linking, and
	// instantiation, but not ComponentFunc/value lifting and lowering yet.
	// Keep this limitation isolated behind the engine boundary so a later
	// upstream release can add calls without changing any wrapper API.
	return errors.New("wasmtime-go component function calls are unavailable")
}

func (c *wasmtimeComponent) Interrupt() { c.engine.IncrementEpoch() }

func (c *wasmtimeComponent) Close() error {
	c.once.Do(func() { c.component.Close() })
	return nil
}
