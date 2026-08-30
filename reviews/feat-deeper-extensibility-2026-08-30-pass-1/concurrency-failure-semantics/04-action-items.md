# Action Items

## Critical

- [x] No Critical findings.

## Important

- [ ] In `wasmext/module.go:179`, recover panics in the goroutine that directly calls `invoke(callCtx)`, return them through `done` so they become `ErrorTrap`, and preserve exactly-once `inFlight.Done` and `callGate` release.

- [ ] In `wasmext/wasmtime_abi.go:56`, contain panics from the host-supplied guest-log exporter at the exported cgo callback boundary so they cannot cross into C or terminate the process.

- [ ] Add regression coverage proving a panicking component invocation is classified, the module gate is reusable, and shutdown completes; add cgo coverage for a panicking guest-log exporter when the Wasmtime path is enabled.

## Suggestions

- [ ] In `runtime/orchestrator_tool_test.go:112`, replace the millisecond polling loop with a channel signaling the third tool-event delivery attempt.

- [ ] In `runtime/event_queue.go:43`, expose a bounded drop counter or observer diagnostic while retaining non-blocking, best-effort queue semantics.
