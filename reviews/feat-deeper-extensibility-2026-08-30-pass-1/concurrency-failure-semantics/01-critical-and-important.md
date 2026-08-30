# Critical and Important Findings

## Critical

None.

## Important: Recover panics inside the Wasm invocation goroutine

- Severity: Important
- References: `wasmext/module.go:179`, `wasmext/module.go:181`, `wasmext/module.go:184`, `wasmext/wasmtime_abi.go:56`

### Problem

`module.call` moves `invoke(callCtx)` into a new goroutine to enforce time and shutdown bounds. The goroutine releases `inFlight` and `callGate`, but it does not recover a panic. Go panic recovery is goroutine-local, so a panic here bypasses the caller's extension/runtime recovery boundaries and can terminate the entire host process.

This is reachable through more than test-only machinery. The Wasmtime invocation path enters host callbacks, and `wasmextHostLog` calls the host-supplied `einoobs.Exporter.Export` through `observeGuestLog`. A panicking exporter or an unexpected panic in the ABI/component wrapper therefore escapes from the invocation goroutine instead of becoming the module's classified `ErrorTrap`. That violates the branch's otherwise consistent rule that extension and transport failures are contained and classified.

### Suggested fix

Recover in the goroutine that directly invokes the component and send the recovered failure through the existing buffered result channel. Keep the existing `inFlight` and gate release defers so shutdown cannot strand either resource. Also contain the exported host-log callback itself so a host observer panic never crosses the cgo callback boundary.

```go
go func() {
	defer m.inFlight.Done()
	defer func() { m.callGate <- struct{}{} }()
	defer func() {
		if recovered := recover(); recovered != nil {
			done <- fmt.Errorf("component invocation panic: %v", recovered)
		}
	}()
	done <- invoke(callCtx)
}()
```

```go
//export wasmextHostLog
func wasmextHostLog(/* existing arguments */) {
	defer func() { _ = recover() }()
	// existing bounded log projection and best-effort export
}
```

Add a fake-engine regression test whose component invocation panics and assert that the call returns an `ErrorTrap`, releases the serialization gate, and allows a subsequent call/close to complete. A cgo-enabled test should additionally use a panicking guest-log exporter and verify process-safe containment at the callback boundary.
