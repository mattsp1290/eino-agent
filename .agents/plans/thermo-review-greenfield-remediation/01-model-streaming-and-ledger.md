# Model Streaming and Ledger

## Goal and prerequisites

Replace the dual callback/reader provider contract with one normalized delta reader. Preserve usage already received when a later operation fails. Prove model-request terminal settlement survives request cancellation.

Prerequisite: none. Complete the interface and adapter conversion before changing runtime accounting tests.

## Repository evidence

- `model/provider.go` defines `StreamDelta`, `StreamObserver`, `Request.Observer`, `Streamer`, and `IdempotentStreamer`.
- `model/eino_streamer.go` converts Eino messages and invokes only start, delta, and error callbacks.
- `providers/fake/provider.go` sends message values through the reader and usage through callbacks.
- `runtime/model_stream.go` owns receive/EOF/error lifecycle but captures observer usage only after successful completion.
- `runtime/ledger.go:updateModelRequest` already strips cancellation before the fenced write.

## Work package A: one provider stream result

Change surface:

- `model/provider.go`
  - Keep `StreamDelta` as the sole per-chunk result, containing only `Message` and `Usage`.
  - Define `Usage` as the latest cumulative attempt-to-date snapshot, not an increment.
  - Remove the unused `Index` and `Done` fields; runtime derives order and EOF is authoritative.
  - Remove `StreamObserver`, `Request.Observer`, `Response`, and `IdempotentStreamer`.
  - Change `Streamer.StreamProvider` to return `*schema.StreamReader[StreamDelta]`.
  - Update `Request.IdempotencyKey` documentation to say adapters may read it directly.
- `model/eino_streamer.go`
  - Convert each Eino message into `StreamDelta`.
  - Use one conversion helper for `Message.ResponseMeta.Usage` that maps prompt, completion, completion-detail reasoning, and prompt-detail cached tokens to input, output, reasoning, and cache-read usage. Leave cache-write and cost unknown when Eino does not expose them.
  - Preserve upstream errors as reader errors without observer notifications.
- `providers/fake/provider.go`
  - Return scripted `StreamDelta` values directly.
  - Treat scripted `Step.Usage` as an increment, accumulate it, and put the cumulative snapshot on every emitted delta.
  - Remove observer panic and lifecycle machinery.
- Update all `model.Streamer` implementations in `runtime` tests, `model` tests, examples, `tools/einotools`, and `wasmext`.

Invariants:

- The reader is the only stream result channel.
- Every delta's usage is cumulative for the current provider attempt. Adapters must normalize provider-specific incremental data before emission.
- A successful receive may contain a nil message only if the runtime rejects it as malformed; usage on that delta is still accounted before rejection.
- EOF is the sole successful terminal signal.
- A reader error is the sole provider terminal error signal.
- Eino readers may return a value and error together. The delta's usage remains authoritative for accounting, while its message is not emitted or concatenated.
- `Request.IdempotencyKey` remains transport metadata and is excluded from the audited model-visible projection.

Acceptance tests:

- Eino conversion returns deltas with prompt, completion, reasoning, and cache-read usage and propagates an upstream error.
- Fake provider returns cumulative usage snapshots and a normalized scripted error through the reader.
- Multiple sparse usage increments become monotonic cumulative snapshots; repeated cumulative values do not increase the final attempt total twice.
- A streamer that records its request sees the durable ledger ID in `Request.IdempotencyKey` through the only method.
- A streamer that ignores the field still completes normally.

## Work package B: runtime-owned lifecycle and partial usage

Change surface:

- `runtime/provider.go`: remove the observer parameter from `TurnSnapshot.ProviderRequest`.
- `runtime/model_stream.go`
  - Delete `streamObserver`, its mutex, callback methods, and `openStream`'s type assertion.
  - Call `snapshot.Model.Streamer.StreamProvider(ctx, request)` directly after the extension around-point.
  - After every `Recv`, merge the latest non-zero fields from cumulative `delta.Usage` into `streamUsage` before checking a non-EOF error, context, message, concatenation, or queue conditions.
  - If `Recv` returns both a delta and an error, account for usage but do not process the message. Return the error.
  - Append `delta.Message` to message chunks and emit its content/reasoning as the live delta.
  - After successful concatenation, merge message response-metadata usage as a fallback for fields the delta stream did not report.
  - Keep the existing defer as the single terminal path for ledger, observed-stream, run-usage, and `ModelCompletedNotice` updates.
- `runtime/extension_model.go`, `runtime/extension_lifecycle.go`, and model-stream cases in `runtime/extensions_test.go`
  - Change the required-around output and reader validators to `*schema.StreamReader[model.StreamDelta]`.
  - Preserve the rule that an around extension must return the exact delegated reader.

Behavior and error precedence:

- Preserve the provider or cancellation error unless terminal ledger persistence fails.
- If terminal ledger persistence fails, return that persistence error because durable attempt state is uncertain. Report accumulated usage with that error.
- If usage arrived before any later failure, add it exactly once to the caller's run-total accumulator.
- Retry policy remains based on the returned provider error. A failed attempt's usage remains in the run total before a retry begins.
- Do not add cumulative snapshots. Replace each non-zero field with the latest value, then fill only still-zero fields from the concatenated message fallback.

Acceptance tests in `runtime/ledger_test.go`, `runtime/orchestrator_provider_test.go`, and/or a focused new runtime test file:

- At least two sparse, non-zero cumulative usage snapshots followed by a retryable stream error appear exactly once in the failed `ModelCompletedNotice` and observed-stream completion.
- A delta carrying cumulative usage together with its terminal error contributes that usage exactly once and does not emit its message.
- After a retry succeeds, `Result.Usage` and the unique `run_finished` event equal failed-attempt usage plus successful-attempt usage.
- Cancellation after dispatch leaves the SQLite model request in `failed`, not `dispatch_started`.
- A forced terminal ledger write failure becomes the returned run error while retaining partial usage.
- Eino `ResponseMeta` conversion and final result/event assertions cover prompt, completion, reasoning, and cached prompt tokens when provider delta usage is otherwise absent.

## Dependencies, risks, and exclusions

- Complete Work package A atomically across the repository because the public interface will not compile mid-change.
- Use Eino stream primitives rather than adding a new goroutine unless conversion cannot preserve upstream error semantics.
- Do not add lifecycle callbacks under a new name.
- Do not retain provider-supplied sequence or completion fields; runtime receive order and EOF/error define lifecycle.
- Do not change retry count, retry classification, event durability policy, or provider transport idempotency claims.
