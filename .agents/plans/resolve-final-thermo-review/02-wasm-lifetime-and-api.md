# Wasm Lifetime and Ownership API

## Goal and prerequisites

Prevent timed-out work from outliving its synchronization/resource ownership, and make every public load path expose its lifetime owner. No stored-data or ABI compatibility is required.

## Worker-owned call lifetime

### Evidence

- `wasmext/module.go`, `module.call`, increments `inFlight` and defers both `Done` and gate release in the caller while `invoke` runs in a goroutine.
- After the timeout and `CloseDrain` timers expire, the caller returns even if the worker ignored interruption.
- `module.Close` can then close the component and engine after the same bounded wait.

### Proposed lifecycle

```text
open -> call acquires gate -> worker owns gate + inFlight
 worker exits -------------------------------> open
 timeout; worker exits during drain ---------> open, return timeout
 timeout; drain expires -> quarantined/closing
 quarantined -> reject new calls -> worker exits -> close component+engine -> closed
```

### Exact change surface

- Change `wasmext/module.go`:
  - Under `mu`, reject a closing module or add one in-flight reference before unlocking. The caller owns this reference while queued.
  - Wait for `callGate`, caller cancellation, or a module-closing channel. Cancellation/closure before worker start releases the caller-owned reference.
  - After gate acquisition, recheck closing under `mu`. If closing won, return the gate and release the reference; otherwise transfer ownership to the newly spawned worker.
  - The invocation goroutine alone releases the gate and transferred in-flight reference.
  - Shutdown marks closing and closes the module-closing signal under the same mutex before waiting, so no positive `WaitGroup.Add` can race or follow `Wait` and queued background-context calls wake promptly.
  - Add an exactly-once shutdown/finalization path (`proposed` internal lifecycle fields/helpers) that marks the module closed to new calls, interrupts active work, waits without a destructive deadline, then closes component and engine.
  - Keep each public `Close` call bounded by `CloseDrain`; if finalization is still pending, return the timeout classification while finalization continues.
  - Let later `Close` calls observe final completion and its stored close error.
  - On a call timeout, keep the module usable only when the worker exits during the drain window. If the drain window expires, quarantine and begin finalization.
- Extend existing fakes/tests in `wasmext/module_test.go` or adjacent test files.

### Invariants and edge cases

- Only a worker that actually exited releases the serialization gate and in-flight reference.
- A quarantined module rejects new calls with `ErrorClosed` and never invokes the component again.
- Component and engine close only after every invocation goroutine exits.
- Concurrent and repeated `Close` calls cannot double-close resources or race on the stored result.
- Call admission is linearized with shutdown under one mutex; `WaitGroup.Add` cannot overlap a shutdown `Wait` that observed zero.
- Normal call success, component errors, context cancellation, and a timeout whose worker promptly exits preserve existing caller-visible classifications.
- A permanently stuck host component can retain resources permanently; bounded caller return is preserved without unsafe destruction.

### Tests and acceptance

- Add an interrupt-ignoring component controlled by `started` and `release` channels.
- Assert a timeout returns within `Timeout + CloseDrain` plus a documented scheduler tolerance, a second call is rejected, and component/engine close flags remain false before `release`.
- Release the worker and assert deferred finalization closes component and engine exactly once.
- Cover repeated/concurrent Close before and after finalization.
- Cover Close racing worker startup, a queued background-context call waking on closure, and queued-call cancellation concurrent with Close.
- Run `go test ./wasmext` and `go test -race ./wasmext`.

## Loader-level shutdown completion

- Change `wasmext/loader.go` so the Loader owns one persistent shutdown operation, completion channel, and stored aggregate result.
- On the first `Loader.Close`, atomically stop tracking/loading, initiate shutdown for every tracked module in reverse order without waiting on each one, then wait for actual module finalization in reverse order and store the joined result.
- Each `Loader.Close(ctx)` waits for that shared completion or its own context. A timed-out first call does not erase shutdown state; a later call can observe successful finalization or the stored close error.
- Aggregate completion means component and engine destruction finished, not merely that bounded `module.Close` returned.
- Test first-close context timeout, stubborn-worker release, later close completion/error observation, reverse-order initiation, and concurrent idempotent calls.

## Ownership-visible loader API

### Evidence

- `wasmext/wrappers.go` exposes `OpenTool`, `OpenContextSource`, `OpenEventSink`, and `OpenHook` as concrete closeable handles.
- Free `LoadTool`, `LoadPermissionsPolicy`, and `LoadEventSink` return a definition/interface while hiding or discarding the close handle.
- Receiver methods on `wasmext.Loader` retain loaded resources for `Loader.Close`.

### Exact change surface

- Delete free `wasmext.LoadTool`, `wasmext.LoadPermissionsPolicy`, and `wasmext.LoadEventSink`.
- Rename/export the concrete permissions wrapper as `LoadedPermissionsPolicy` (`proposed`) and add `OpenPermissionsPolicy` (`proposed`) returning its pointer.
- Retain `Loader.LoadTool`, `Loader.LoadPermissionsPolicy`, and `Loader.LoadEventSink`; their receiver is the explicit owner. Strengthen comments to state returned callables are valid only until `Loader.Close`.
- Update tests, examples, package documentation, and symbol references without aliases or deprecated shims.

### Tests and acceptance

- Every free `Open*` path returns a type with `Close`.
- Every `Load*` path is a `Loader` method whose documentation names the Loader-owned lifetime.
- Structural searches find no deleted free functions and no public function that discards a newly opened close handle.
- Existing loader reverse-close-order and idempotence tests still pass.
- Search current prompt/design documents as well as architecture docs for deleted signatures.

## Dependencies and exclusions

- Implement worker ownership before API cleanup so all handles share the corrected close behavior.
- Do not redesign component interfaces or the Wasm wire protocol.
- Do not add finalizers based on garbage collection.
