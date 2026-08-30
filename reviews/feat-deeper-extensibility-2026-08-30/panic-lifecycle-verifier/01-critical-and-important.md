# Critical and Important Findings

## Critical

None.

## Important

None.

The prior Important finding is resolved at `wasmext/module.go:181`: the goroutine that directly invokes the component now owns panic recovery, communicates a bounded error through the buffered `done` channel, and then runs the existing gate and in-flight release defers. `wasmext/wasmtime_abi.go:45` separately prevents a host exporter panic from escaping the exported cgo callback. The regression tests at `wasmext/wasmext_test.go:220` and `wasmext/wasmext_test.go:568` exercise both boundaries.
