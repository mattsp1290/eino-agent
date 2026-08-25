# Single Model Transport

## Repository evidence

- `model.Adapter.Build` returns an Eino client while adapters may separately
  implement `model.Streamer`.
- `model.AdapterResolver.Resolve` always builds the client, even when runtime
  later ignores it in favor of the adapter streamer.
- `runtime.openStream` branches between the two fields. Its client branch calls
  `WithTools` and also passes `einomodel.WithTools`, binding tools twice.
- `providers/fake.Provider` implements both paths with separate state capture.

## Exact change surface

- `model/provider.go`, `model/types.go`
  - Change `Adapter.Build` to return `Streamer` and reject a nil result.
  - Remove `Resolved.Client`, `streamerFor`, and the claim that streaming is an
    optional adapter capability.
  - Add a small Eino-chat-model-backed streamer constructor in `model`; it owns
    system-message insertion, one `WithTools` call, upstream-reader closure, and
    observer forwarding. Use Eino's close-propagating
    `StreamReaderWithConvert` with EOF/error hooks rather than a pipe goroutine
    that can block forever in upstream `Recv`. It snapshots adapter/provider
    state during `Build`, not later during `StreamProvider`.
  - Preserve optional idempotency as a capability of the one built streamer.
- `runtime/model_stream.go`
  - Remove the Eino model import and client branch.
  - Require `resolved.Streamer`; use the idempotent method only when the one
    transport implements it and a durable key exists.
- `providers/fake`
  - Build an immutable request streamer containing the selected provider/model,
    cloned runtime data, and cloned scripted steps.
  - Delete the parallel fake chat-model path and tests that only exercise it.
- Update model/runtime/example/Wasm test resolvers to populate the one field.

## Invariants and tests

- `Build` runs once per resolution and the returned object is the exact object
  runtime streams through.
- System and tool inputs are cloned before provider use; tools are bound once.
- Observer start occurs exactly once before provider dispatch. For a consumed
  stream, exactly one of observer end/error follows; deltas are observed only
  for successfully converted chunks. Synchronous `WithTools`/`Stream` errors
  and `Recv` errors report error, while EOF reports end with merged usage.
- Context cancellation and early downstream reader close propagate directly to
  the upstream reader. Consumer abandonment guarantees resource closure but no
  synthetic terminal observer callback because `StreamReader.Close` has no
  error/result channel. Observer panics are recovered and converted to a
  normalized provider-stream error.
- Add a spy Eino model test that fails if tools are bound or supplied twice.
- Test synchronous binding/stream errors, receive failure, empty EOF,
  cancellation, early close without cancellation, and observer panic.
- Preserve idempotency-key tests against the single transport.

## Dependencies and risks

- This package can be implemented independently of plan identities and tools.
- Do not move request auditing into the transport; runtime canonicalization
  remains authoritative before `StreamProvider` is called.
