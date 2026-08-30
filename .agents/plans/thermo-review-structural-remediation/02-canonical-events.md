# Canonical Events

## Goal and prerequisite state

Use `session.EventRecord` as the only event envelope and make runtime event delivery explicitly best-effort. Preserve durable storage as the source of truth and preserve AG-UI replay error reporting.

Prerequisites:

- The application context in `00-overview.md` remains valid.
- Beads issue `eino-agent-tn9` is claimed.
- The existing durable event schema and store contracts remain unchanged.

## Repository evidence

- `runtime.Event` duplicates `session.EventRecord` with two renamed fields and without `ToolTransition`.
- `runtime.runtimeEventRecord` and `agui.runtimeEvent` duplicate field-by-field conversion.
- `runtime.eventQueue` discards `EventSink.Emit` errors because emission occurs in a worker goroutine.
- `runtime.runEventSink.publishPersisted` also discards infrastructure errors because durable state is already committed.
- `agui.Bridge` stores transport and encoding failure in the emitter and exposes `Err()` and `EncErr()`.
- `stream.Tail.Emit` disconnects slow subscribers and currently cannot make asynchronous runtime delivery transactional.

## Exact change surface

- `runtime/types.go`
  - Delete `Event` and `EventKind`.
  - Keep untyped named runtime event constants for run start, message delta, tool update, run finish, and tail overflow.
  - Delete duplicate `Usage`, `EventError`, `RedactionClass`, and redaction constants.
  - Change `Result.Usage` and `ModelCompletedNotice.Usage` to `session.Usage`.
  - Change `EventSink.Emit(context.Context, session.EventRecord)` to return no value.
  - Change `EventSinkFunc` accordingly.
- `runtime/event_sink.go`
  - Delete `runtimeEventRecord`.
  - Clone the canonical record before infrastructure delivery and before extension notification.
  - Add one private helper that recovers infrastructure sink panics and intentionally drops them.
  - Keep persisted and live delivery explicitly best-effort.
- `runtime/event_queue.go`
  - Call the void sink method in the worker.
  - Clone the record before enqueue so the queue owns its payload bytes.
  - Keep `emit` errors limited to context cancellation and inability to enqueue.
- `runtime/extension_execution.go` and `runtime/orchestrator.go`
  - Publish committed `session.EventRecord` values directly.
- `runtime/model_stream.go`, `runtime/observability.go`, and runtime tests
  - Rename `EventID` to `ID` and `Time` to `CreatedAt`.
  - Use `session.Usage`, `session.EventError`, and `session.RedactionClass` directly where explicit construction is needed.
- `runtime/extension_lifecycle.go`
  - Type `EventPublishedPoint` directly on `session.EventRecord` and clone its payload.
- `agui/bridge.go`
  - Consume `session.EventRecord` directly.
  - Keep protocol mapping behavior unchanged.
- `agui/replay.go`
  - Delete `runtimeEvent` and send stored records directly to the bridge.
  - After each synchronous bridge emission in replay or reconnect, return `Bridge.Err()` if non-nil.
  - Continue to skip durable records marked `LiveOnly` and deduplicate by `Event.ID`.
- `stream/tail.go`
  - Implement the void best-effort sink method.
  - Clone the record separately for each subscriber.
  - Preserve slow-subscriber disconnect and tail-overflow behavior.
- `wasmext/wrappers.go`, `wasmext/projections.go`, and `wasmext/points.go`
  - Project `session.EventRecord` directly into the bounded WIT event.
  - Keep the private WASM observer method error-returning so `extension.Notify` can report module callback failures.
  - Stop treating `loadedEventSink` as a runtime infrastructure sink.
- Tests in `runtime`, `agui`, and `stream`
  - Update sink functions and canonical field names.

## Intended behavior and invariants

- A live event and a replayed durable event have the same Go envelope.
- `ToolTransition` survives publication and replay because no projection drops it.
- Mutable payload bytes are cloned for independent consumers.
- A runtime sink cannot fail a run after admission or after a durable transition commits.
- A runtime sink panic is isolated and intentionally dropped. It cannot skip extension notification or terminate the queue worker.
- Queue producers and live-tail subscribers never share writable payload backing arrays.
- Live queue enqueue cancellation can still fail the producer through `eventQueue.emit`.
- AG-UI replay and reconnect still stop on a synchronous transport error reported by the bridge.
- Encoder errors remain observable through `Bridge.EncErr()` under the existing bridge contract; do not silently convert them into runtime errors unless existing tests establish that behavior.

## Tests and acceptance criteria

- `go test ./runtime -run 'TestRunEventSink|TestEventQueue|TestStreamingOrchestratorUsesCanonicalEventSink|TestStreamingOrchestratorPublishes'`
- `go test ./agui -run 'TestBridge|TestReplay|TestReconnect'`
- `go test ./stream -run 'TestTail'`
- Add or adapt tests proving:
  - live and persisted events reach sinks as canonical records;
  - `ToolTransition` and payload bytes survive publication;
  - one sink mutating its payload cannot mutate extension notification input or stored test fixtures;
  - producer mutation after enqueue cannot change the queued payload;
  - one tail subscriber mutating its payload cannot change another subscriber's payload;
  - a sink has no error return and cannot become a run failure source;
  - a sink panic during persisted admission publication does not prevent handle creation or extension notification;
  - a sink panic during one queued event does not prevent later delivery or queue closure;
  - replay returns a bridge transport error after the failing emission;
  - tail overflow remains detectable during reconnect.
- Add a reporter-backed WASM test proving an observer module failure is reported without failing the run.
- Acceptance is observable when searches find no `runtimeEventRecord`, `agui.runtimeEvent`, `runtime.Event`, `runtime.EventKind`, `runtime.Usage`, `runtime.EventError`, or `runtime.RedactionClass`.

## Dependencies, risks, and exclusions

- This work can begin independently of run-plan sealing, but both touch runtime fixtures; apply it after the plan refactor to reduce churn.
- Do not alter SQLite event storage, cursor semantics, or durable event selection.
- Do not add retries or acknowledgements to `EventSink`.
- Do not make AG-UI transport errors authoritative over already committed runtime state.
- Do not edit `docs/`. Keep any `examples/` edit mechanical and limited to removed-API compilation.
