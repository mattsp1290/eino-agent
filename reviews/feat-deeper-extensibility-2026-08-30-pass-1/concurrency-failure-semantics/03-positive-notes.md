# Positive Notes

- `runtime/orchestrator_tool_test.go:103` correctly gives the delivery-attempt test a 16-slot queue. `newEventQueue` intentionally maps a non-positive size to one slot (`runtime/event_queue.go:25`), while the real constructor defaults to 64 (`runtime/options.go:22`); the repair removes an accidental test-helper artifact without changing the post-commit best-effort assertion.

- `tools/einotools/einotools_test.go:291` creates per-test executable fixtures under `t.TempDir()` and wires both `SearchOptions.RGBinary` and `ShellOptions.ShellBinary`. The catalog is still constructed normally and all original catalog order, mount rollback, runtime execution, permission, and durable-settlement assertions remain intact.

- `runtime/tool_preparation.go:45` commits assistant parts and every pending tool state/event in one fenced transaction. Publications begin only after that transaction succeeds at `runtime/tool_preparation.go:63`, preventing live consumers from observing a state transition that rolled back.

- `store/sqlite/execution.go:109` derives terminal run state and its `run_finished` event under one transaction/fence, rejects settlement while unfinished tools remain, and verifies identical terminal retries instead of appending duplicate terminal events.

- `runtime/model_stream.go:46` recovers provider-stream panics in the invoking goroutine, retains usage already received, and finalizes the request ledger from its durable state. `runtime/model_stream.go:114` makes the ledger state the sole transition authority and prevents a completion notice when terminal persistence fails.

- `extension/registry.go:431` acquires mount references while holding the registry lock, and `extension/registry.go:551` closes the drain channel only after deactivation and the final reference release. Notification tasks retain their owning mount before enqueue (`extension/notification_dispatcher.go:36`), so asynchronous callbacks cannot race module cleanup.

- `runtime/event_queue.go:43` serializes enqueue against close, clones accepted records, never blocks the run on a full queue, and isolates sink panics in `runtime/event_sink.go:30`. The queue tests pin payload ownership, panic continuation, bounded dropping, and non-blocking close.

- `wasmext/module.go:145` takes the in-flight reference before releasing the closing lock, and `wasmext/module.go:237` flips closing state, cancels calls, interrupts the component, waits for in-flight work, and finalizes component-before-engine exactly once. The remaining finding is narrowly about panic conversion inside the spawned invocation goroutine, not the shutdown ordering.

- Race verification passed for the complete repository at `78f4a2541a62e97d0c85cb2f8ac8d80c57c4b491`, including runtime concurrency, SQLite claim/settlement contention, extension close/drain behavior, tails, keyed einotools locking, and Wasm lifecycle tests.
