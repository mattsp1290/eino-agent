# Positive Notes

- `wasmext/module.go:181` places recovery in the only goroutine capable of recovering `invoke` panics. The recovered value is deliberately not included in the returned error, preserving the module error's bounded and non-secret-bearing surface.

- The defer order at `wasmext/module.go:182` through `wasmext/module.go:188` is correct. Panic recovery sends the result first, then the serialization token is restored, then `inFlight.Done` executes. A caller may return just before the last two defers complete, but a subsequent call waits safely on the token and close waits safely on the reference.

- `wasmext/wasmext_test.go:220` proves the complete repaired lifecycle rather than only checking an error string: the first panic is classified as `ErrorTrap`, a second call succeeds through the same gate, close succeeds, and the component is finalized.

- `wasmext/wasmtime_abi.go:45` contains a panic at the exported cgo callback boundary, where outer Go recovery cannot be relied upon safely. `wasmext/wasmext_test.go:568` uses a real checked-in Wasm component and a panicking host exporter, then proves guest execution still returns the expected result.

- `runtime/orchestrator_tool_test.go:117` replaces scheduler polling with a completion channel. The sink signals on the third pending/running/terminal delivery attempt and still panics after each signal increment, so the test continues to verify post-commit best-effort containment.

- `runtime/tool_preparation.go:45`, `store/sqlite/execution.go:109`, and `runtime/orchestrator.go:432` preserve the branch's core durable ordering: transition state and its canonical event commit atomically, publication follows commit, and run settlement refuses unfinished tool calls.

- `extension/registry.go:431` acquires mount references under the publication lock, while queued notifications retain their owning mount before enqueue at `extension/notification_dispatcher.go:36`. Deactivation therefore cannot finalize resources still reachable by a plan or accepted callback.

- `Makefile:25` now includes `windows-compile` in `check`, and `Makefile:88` compiles the Windows einotools package with cgo disabled. This caught and fixed the previously uncompiled short-declaration error at `tools/einotools/einotools_windows_test.go:25`.

- The complete repository passed race execution at the reviewed HEAD, including SQLite settlement contention, runtime lease/event queues, extension snapshot close/drain, keyed einotools locking, stream tails, and Wasm call/close lifecycle tests.
