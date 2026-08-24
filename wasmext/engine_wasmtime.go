//go:build cgo

package wasmext

import (
	"context"
	"errors"
	"sync"

	wasmtime "github.com/bytecodealliance/wasmtime-go/v47"
)

const cgoEnabled = true

type wasmtimeEngine struct {
	mu     sync.Mutex
	engine *wasmtime.Engine
	limits Limits
	closed bool
}

func newWasmtimeEngine(limits Limits) (engine, error) {
	config := wasmtime.NewConfig()
	config.SetWasmComponentModel(true)
	config.SetEpochInterruption(true)
	return &wasmtimeEngine{engine: wasmtime.NewEngineWithConfig(config), limits: limits}, nil
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
	return &wasmtimeComponent{
		component: component,
		engine:    e.engine,
		limits:    e.limits,
		contract:  contract,
	}, nil
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
	limits    Limits
	contract  worldContract
	once      sync.Once
}

func (c *wasmtimeComponent) Interrupt() { c.engine.IncrementEpoch() }

func (c *wasmtimeComponent) Close() error {
	c.once.Do(func() { c.component.Close() })
	return nil
}
