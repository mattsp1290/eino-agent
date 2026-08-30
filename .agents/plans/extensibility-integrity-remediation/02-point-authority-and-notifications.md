# Point Authority and Notifications

## Goal and prerequisites

Make point semantics host-owned and make observer-only callbacks unable to block run execution. Preserve typed dispatch, callback ordering, diagnostics, and mount lifetime safety.

Prerequisite: the atomic tool-turn work must define when pending and terminal events publish.

## Repository evidence

- `extension/types.go` stores each typed point's immutable `pointDefinition` behind a pointer.
- `extension/registry.go:CommitMount` currently adds unknown point definitions while publishing a component.
- `extension/dispatch.go:matchingEntries` intentionally uses exact pointer equality.
- `extension.Plan` owns `releases`, and `Plan.Release` currently releases mount references immediately.
- `extension.Notify` is the only observer-only dispatch form. Hooks, transforms, gates, and around callbacks affect execution results.
- Runtime persisted-event delivery calls `Notify` synchronously and sometimes with `context.WithoutCancel`.

## Host-declared point catalog

### Change surface

- `extension/types.go`
  - Add a proposed exported `Point` interface implemented only by the typed point handle types through an unexported method.
  - Keep exact typed point handles as runtime dispatch authority.
- `extension/registry.go` and `extension/notification_dispatcher.go`
  - Change `NewRegistry` to accept the complete host catalog and return an error for invalid, duplicate, or conflicting definitions.
  - Freeze `pointDefinitions` at construction.
  - Make `CommitMount` reject any registration whose durable key is absent or whose definition pointer differs.
  - Delete candidate authority election and mutation of `pointDefinitions` during publication.
- `runtime/extension_lifecycle.go`
  - Add a proposed `ExtensionPoints` function that returns every canonical runtime point as `[]extension.Point`.
- `composition/registry.go`
  - Change `NewRegistry` to combine `runtime.ExtensionPoints()` with caller-supplied custom host points before constructing the underlying registry.
  - Propagate catalog-construction errors directly.
- Test helpers in `extension`, `runtime`, `composition`, and `wasmext`
  - Declare test-owned points before mounting callbacks.

### Invariants

- Mounted components cannot create authority.
- The registry catalog does not change after construction.
- Unknown point contracts fail before component publication.
- A different handle with the same contract and kind fails deterministically regardless of mount order.
- Closing every mount does not erase host authority.

## Plan-owned notification dispatcher

### Change surface

- `extension/registry.go`
  - Add a private dispatcher owned by each snapshot `Plan`.
  - Use one bounded FIFO queue and one worker for accepted notification tasks.
  - Give each accepted task one reference to only its target mount and release that reference when the callback returns or panics.
  - Make `Plan.Release` stop new admission, release baseline snapshot references immediately, and return without waiting for callbacks.
  - Add a context-bounded public flush method only if tests or host shutdown require an observable drain boundary.
- `extension/dispatch.go`
  - Clone each notification payload before enqueueing it.
  - Enqueue one callback task per matching registration without blocking.
  - Execute callback diagnostics through the dispatcher worker so a blocking reporter cannot block the run caller.
  - Keep hooks, transforms, gates, and around dispatch synchronous.
- `runtime/event_sink.go`, `runtime/event_queue.go`, `runtime/extension_execution.go`, and `runtime/orchestrator.go`
  - Route extension notifications through the nonblocking plan dispatcher.
  - Create exactly one bounded infrastructure dispatcher per run before publishing the admitted event.
  - Route admission, live deltas, pending/running/terminal tool transitions, and final-run events through that dispatcher.
  - Make enqueue nonblocking and FIFO for accepted events, dropping the new event on overflow.
  - Stop admission during execution release without waiting for the sink worker or a native callback; remove the per-model-stream blocking close path.

The exact private dispatcher type and capacity constant are proposed implementation details. Keep capacity finite and test the selected value without exposing compatibility options.

### Lifecycle and failure behavior

- Accepted tasks execute in registration and emission order.
- Queue overflow drops the new notification and never blocks the caller.
- `Plan.Release` is idempotent and never sends on a closed queue.
- The plan owns baseline mount references only until `Plan.Release`; each accepted task owns only its target mount until completion.
- A callback that never returns may cause its own `Mount.Close(ctx)` to time out, but cannot delay another mount's cleanup, run completion, or `Handle.Done`.
- Callback panics and errors remain bounded diagnostics.
- No goroutine is spawned per emitted notification.

## Tests and acceptance criteria

- Replace mount-order authority tests with catalog-order invariance tests.
- Prove an undeclared or alternate point cannot publish a mount regardless of which component prepares first.
- Prove copied canonical point handles still dispatch.
- Block a notification callback and prove `Notify`, persisted-event publication, run settlement, and `Handle.Done` return.
- While callback A is blocked, prove mount A close respects its context and unrelated mount B closes normally. Release A and prove a later close completes cleanup exactly once.
- Fill the notification queue and prove the caller does not block and accepted tasks preserve FIFO order.
- Block the diagnostic reporter and separately block the infrastructure event sink. Prove admission returns a handle, live deltas and tool transitions progress, final settlement completes, plan release runs, and `Handle.Done` returns.
- Prove the infrastructure queue preserves FIFO order for accepted events and deterministically drops new events after capacity is reached.
- Run `go test -race` for `extension`, `runtime`, `composition`, and `wasmext`.

## Risks and exclusions

- Stop if any synchronous notification currently determines runtime behavior. Repository inspection shows notification return values are ignored.
- Do not make gates, transforms, hooks, or around callbacks asynchronous.
- Do not add unbounded buffers, retries, or delivery guarantees.
