# Model Stream Attempt

## Goal

Replace implicit, defer-driven stream lifecycle state with one explicit attempt result and isolate provider reading from durable request coordination.

Prerequisite: none. This work can be implemented independently of capability-plan compilation.

## Existing evidence

- `runtime/model_stream.go:16` returns named values and also maintains `streamErr` as a second error authority.
- The deferred closure at lines 22-52 performs five distinct responsibilities: panic conversion, usage accumulation, durable request settlement, observer settlement, and extension completion notification.
- Every error exit manually assigns `streamErr`; `modelRequested` and `ledgerTransitionOK` encode whether notification is legal.
- `runtime.streamModelAttempts` uses `receivedDelta` to decide retry eligibility, so the refactor must preserve partial-stream semantics.
- `updateModelRequest` is the canonical durable transition helper, and `prepareModelRequest` owns record creation.

## Exact change surface

### `runtime/model_stream.go`

- Add proposed private `modelStreamResult` with:
  - `message *schema.Message`;
  - `usage model.Usage`;
  - `receivedDelta bool`;
  - `err error`.
- `streamModel` owns one mutable `modelStreamResult`; pass it by pointer into the proposed private `receiveModelStream` so accepted-delta state survives unwind.
- Change the private signature to proposed `streamModel(...) (result modelStreamResult)`. The deferred recovery/finalizer mutates this named single result; do not retain the old returned triplet alongside it.
- Add proposed private `receiveModelStream` that:
  - accepts the provider reader and the immutable event identity needed for live deltas;
  - accepts a narrow synchronous `onDelta(index int64, message *schema.Message)` callback owned by `streamModel`;
  - checks context before and after receive;
  - progressively merges per-delta usage and sets `receivedDelta` on the caller-owned result;
  - rejects nil message chunks;
  - invokes `onDelta` after validating each chunk;
  - concatenates chunks or returns the canonical empty assistant message;
  - invokes reader close exactly once;
  - performs no ledger, observer, live-event, or extension notification work directly.
- The `onDelta` callback owns both `observeStreamChunk` and live delta-event emission, keeping their current synchronous order and zero-based index.
- Add proposed private attempt-finalization input or a focused `finalizeModelStreamAttempt` method containing the model-request record, observation handle, notice identity, and result. Do not include `dispatchStarted` or another lifecycle boolean.
- Refactor `streamModel` into an explicit coordinator:
  1. start observation;
  2. prepare/audit/persist request;
  3. mark dispatch started;
  4. emit `ModelRequested`;
  5. invoke the around terminal and call `receiveModelStream`;
  6. guarantee one finalizer that settles ledger, observation, usage, and optional `ModelCompleted`.
- Delete `streamErr`, `modelRequested`, and `ledgerTransitionOK`.
- Use `session.ModelRequestRecord.State` as the sole authority for prepared versus dispatch-started finalization. Switch on that state before the terminal transition; do not mirror it into a boolean.
- Preserve a small direct deferred function around panic recovery because Go `recover` must execute in the deferred call path. Convert a recovered panic into the caller-owned attempt result, clear only its message, preserve usage/received-delta fields, and then call the focused finalizer.

No public API change is proposed. A new file is not required; proposed `runtime/model_stream_receive.go` is allowed only if keeping result/receiver code in `model_stream.go` obscures the coordinator.

### `runtime/orchestrator.go`

- Update existing `streamModelAttempts` to consume the single `modelStreamResult`.
- Return `result.message` on success.
- Use `result.err` and `result.receivedDelta` for retry eligibility and final failure.
- Do not mirror the result back into a separate error/delta tuple.

## Explicit state model

```text
not prepared
  -> preparation failure
       observer failed; no model-request terminal transition; no ModelCompleted
  -> prepared
       -> dispatch-start transition failure
            settle prepared record failed; observer failed; no ModelCompleted
       -> dispatch started + ModelRequested
            -> provider/receive success
            -> provider/receive failure
            -> panic converted to failure
          finalizer attempts completed/failed durable transition
            -> transition success: settle observer and emit ModelCompleted
            -> transition failure: replace canonical returned error, fail observer, omit ModelCompleted
```

## Intended behavior and invariants

- Each prepared model request receives at most one terminal state transition.
- `ModelRequested` is emitted only after durable dispatch-start succeeds.
- `ModelCompleted` is emitted only after `ModelRequested` and a successful durable terminal transition.
- A durable terminal-transition failure replaces the provider/stream error exactly as today.
- Provider panics during invocation, receive, or close become `provider stream panic: ...`, clear the returned message, preserve accumulated usage and `receivedDelta`, settle the request failed when prepared, and finish observation failed.
- Context cancellation before or after `Recv` returns interruption semantics through the existing caller.
- Usage observed before EOF or failure is accumulated exactly once into the run total.
- An error after any delta sets `receivedDelta=true`, preventing retry.
- A pre-delta retryable provider failure retains `receivedDelta=false`.
- Nil readers, nil message chunks, malformed concatenation, and empty streams retain their current canonical results.
- Reader close is invoked exactly once. It has no error result; a close panic supersedes success with the canonical panic error while retaining accumulated usage and delta state.
- The receiver contains no durable store calls, observer types, `observe*` calls, live event-sink calls, or extension lifecycle notifications.
- No replacement lifecycle flag such as `dispatchStarted`, `requestStarted`, or `terminalTransitionOK` is introduced.
- Deferred recovery and terminal-ledger failure mutations to the named result are the exact values observed by `streamModelAttempts`.

## Tests and acceptance criteria

- Preserve existing retry and ledger tests.
- Add or strengthen focused tests for:
  - audited request preparation failure;
  - dispatch-start transition failure;
  - provider invocation failure before a reader exists;
  - nil reader;
  - pre-delta receive failure remains retryable;
  - post-delta receive failure is not retried;
  - cancellation before receive and after one delta;
  - nil delta message;
  - message concatenation failure;
  - empty stream;
  - provider panic during invocation and receive;
  - panic on a second receive after one successful delta retains usage, sets `receivedDelta=true`, prevents retry, settles failed once, and preserves observer/notification cardinality;
  - close panic after a completed stream invokes close once, supersedes success, clears the message, and retains usage/delta state;
  - terminal ledger-transition failure replacing the stream error;
  - usage accumulation on success and failure;
  - exact `ModelRequested`/`ModelCompleted` cardinality and ordering;
  - first-token observation, zero-based chunk indices/count, and ordering relative to live delta events;
  - observer success/failure settlement.
- Use the real SQLite store for at least one successful attempt and one terminal-transition failure path where practical; mocks must not be the only proof of ledger behavior.
- Supplemental static analysis must not report `runtime.streamModel` under default `funlen` or `gocognit` thresholds.
- `rg -n 'streamErr|modelRequested|ledgerTransitionOK|dispatchStarted|terminalTransitionOK' runtime/model_stream*.go` must return no lifecycle-flag matches outside enum comparisons or test names.
- `receiveModelStream` must remain below the same thresholds and have no store, observer, `observe`, event-sink, or `extension.Notify` references.
- `streamModelAttempts` must consume `modelStreamResult` directly and must not reconstruct the removed return tuple as local mirrored state.

## Risks and exclusions

- Do not change the public provider stream contract.
- Do not join terminal ledger-transition errors with provider errors unless existing canonical store behavior requires it; current behavior replaces the provider error.
- Do not move retry decisions into the receiver.
- Do not buffer or reorder live delta events.
- Do not drop first-token or per-chunk observability while isolating the receiver.
- Do not change usage merge semantics.
- Do not add a generic state-machine framework or interface for one lifecycle.
