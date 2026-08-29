# Thermo Review Greenfield Remediation

Status: Implemented and verified after plan review.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "b105070409a3af52b816d2f433b70c2ce3947ab672dc4b349c23fd9e3a6cf9fb",
    "confirmed_at": "2026-08-29T11:15:12-04:00"
  }
}
```

The user explicitly declared this a greenfield API. Replace weak contracts directly. Do not add adapters, database migrations, deprecated methods, feature flags, or dual-write behavior.

## Change type and affected areas

This is a cross-cutting correctness and maintainability repair. It changes the public model streaming boundary, runtime stream accounting, the public run-settlement store contract, the SQLite schema, internal execution capability construction, contract tests, examples, and consumer documentation.

Affected areas:

- `model`: make normalized deltas the only streaming result and remove callback and idempotency compatibility surfaces.
- `runtime`: own stream lifecycle, preserve partial-attempt usage, make ledger cancellation behavior executable, and construct run executions with a fence.
- `session` and `store`: model run settlement as one required terminal state/event transition and enforce one terminal event per run.
- `providers`, `examples`, `tools`, and `wasmext`: compile against the single greenfield streaming method.
- `docs`: describe the actual event-sink and run-plan-provider contracts.

## Requested outcome

Fix the six Beads findings `eino-agent-hve`, `eino-agent-51o`, `eino-agent-9hl`, `eino-agent-mw8`, `eino-agent-o4y`, and `eino-agent-1rb` without preserving unused compatibility paths.

Success means:

- A provider returns `model.StreamDelta` values with cumulative attempt-to-date usage, so it cannot report usage through a second callback channel.
- Usage received before an error contributes to the attempt notice, observed stream, run result, and final durable event.
- A canceled request settles its model-request ledger record to `failed` through SQLite.
- Run settlement always derives and commits one canonical `run_finished` event atomically with terminal state.
- Replaying the identical run settlement returns the original event, while a different terminal event conflicts.
- Every `runExecution` receives its fenced `session.ExecutionStore` at construction, with no rebinding or top-level-store recovery path.
- `model.Streamer.StreamProvider` receives `Request.IdempotencyKey`; no second optional method exists.
- Consumer docs match the constructor, event persistence, and live-tail behavior demonstrated by the examples.
- `make check` and `git diff --check` pass.

## Scope

In scope:

- Direct breaking changes to the model and store interfaces.
- Direct edits to SQLite schema version 1 because there are no deployed databases to migrate.
- Updates to all repository implementations, fakes, examples, and tests.
- Focused failure, retry, replay, cancellation, and contract tests.

Non-goals:

- Exactly-once provider delivery.
- A generic provider lifecycle event bus.
- Persisting live-only stream deltas.
- Compatibility adapters for the prior `StreamObserver`, `IdempotentStreamer`, or nullable run-settlement event contracts.
- A schema upgrade path for existing SQLite files.
- New transport behavior or AG-UI approval semantics.

## Repository-grounded findings

- `model.Request.Observer` and `model.StreamObserver` make adapters responsible for start/delta/error/end, but `runtime.streamModel` independently owns open, receive, EOF, error, and close. `model.NewEinoStreamer` never calls `OnProviderEnd`.
- `runtime.streamModel` snapshots observer usage only at successful EOF. A fake provider can emit usage and then fail, but the failed attempt contributes zero usage to the run.
- `runtime.updateModelRequest` already calls `ExecutionStore.UpdateModelRequest` with `context.WithoutCancel(ctx)`. The missing work for `eino-agent-51o` is an integration test proving this survives cancellation and a test fixing terminal-ledger-error precedence.
- `session.ExecutionStore.SettleRun` accepts a nullable `*EventRecord`, while configured fresh and resume paths always build a final event.
- `store/sqlite.executionStore.SettleRun` accepts a second event ID after an identical terminal run replay. The `events` table has phase uniqueness for tools but no uniqueness for `run_finished`.
- `runtime.runExecution` can reacquire a fence through `ensureStore`, even though fresh admission and resume already know the authoritative claim token before mutations begin.
- `model.Request.IdempotencyKey` and `model.IdempotentStreamer.StreamProviderWithIdempotencyKey` carry the same value through two APIs.
- `docs/integrations/ag-ui-go-server-example.md` assigns durable persistence to `EventSink`, while runtime persists selected events before forwarding them to the sink. `docs/consumer-guide.md` calls the required run-plan provider optional.

## Key decisions

1. Change `model.Streamer` to return `*schema.StreamReader[model.StreamDelta]`. Remove `StreamObserver`, `Request.Observer`, `model.Response`, and the unused `StreamDelta.Index` and `StreamDelta.Done` fields. Runtime derives ordering, start, error, and end from the call and receive loop.
2. Define `StreamDelta.Usage` as a cumulative attempt-to-date snapshot. Merge the latest non-zero fields into `streamUsage` immediately after every receive, before handling a co-returned error or any later failure path.
3. Keep `updateModelRequest` as the single cancellation-stripping helper. Add SQLite-backed tests rather than layering another detached context at its callers.
4. Add `session.RunSettlement`, `session.SettleRunRequest`, `session.RunSettlementEvent`, and `session.RunSettlementResult`, patterned after tool transition requests. Callers submit only terminal fields and non-derivable event fields. The store retains current fenced ownership and lease state while deriving the canonical terminal run and `run_finished` record.
5. Enforce `UNIQUE(run_id, kind) WHERE kind = 'run_finished'` in schema version 1. Reserve `run_finished` for `SettleRun` by rejecting it from ordinary `AppendEvent`.
6. Construct `runExecution` only after admission or claim, passing the admitted run to derive its immutable fenced store. Publish the admission event through the frozen plan before constructing the execution.
7. Delete `model.IdempotentStreamer` and always call `StreamProvider(ctx, request)`. Provider transports opt in by reading `request.IdempotencyKey`.

Rejected alternatives:

- Adding a missing Eino `OnProviderEnd` call leaves two lifecycle authorities and still permits custom adapters to omit callbacks.
- Keeping a callback only for usage preserves the failure-prone side channel.
- Keeping nullable settlement events preserves the invalid terminal-state-without-event state.
- Looking up a run in `ensureStore` hides an execution-construction defect and can reacquire mutation authority from mutable global state.
- Adding a schema version 2 migration serves no user or deployed database in the confirmed application context.

## Target flow

```text
provider adapter
  -> StreamProvider(Request{IdempotencyKey})
  -> StreamReader[StreamDelta{Message, cumulative Usage}]
runtime receive loop
  -> merge usage before processing message
  -> emit live delta / concatenate messages
  -> finalize ledger with cancellation-free context
  -> report success or error with accumulated usage
run completion
  -> SettleRun(SettleRunRequest{RunSettlement, RunSettlementEvent})
  -> atomically write terminal run + unique canonical run_finished event
  -> publish committed event to observation/live sink
```

Fresh execution construction becomes:

```text
acquire frozen plan -> resolve model -> admit run -> publish admission
-> newRunExecution(host, plan, admitted run) -> execute
```

Resume execution construction becomes:

```text
load run -> acquire frozen plan if nonterminal -> claim run if needed
-> newRunExecution(host, plan, loaded/claimed run) -> execute resume
```

## Risks and gates

- Blocking decisions: none. The user resolved users, compatibility, and feature flags.
- Stop if Eino's generic stream utilities cannot preserve terminal errors while converting messages to `StreamDelta`; choose a small local pipe wrapper and test close/error propagation before editing every adapter.
- Stop if a non-SQLite store implementation exists outside the repository. The user authorized breaking the current codebase, not unseen external consumers; repository inspection currently finds only SQLite plus test doubles.
- The new uniqueness constraint is the final defense. Store logic must still return the existing canonical event on identical replay instead of relying on a constraint error.
- Concurrent identical settlement requests must return the same canonical result. Concurrent conflicting requests must produce one winner and one `ErrConflict`.
- Settlement must retain store-owned lease and ownership fields even when the caller began from an older run snapshot.
- Partial usage is only as accurate as adapter deltas. The runtime must not manufacture usage for providers that do not report it.
- The constructor refactor affects many tests. Tests may use a helper that admits or supplies a valid run, but production code must not regain a nil or fallback store path.

## Document map

- `01-model-streaming-and-ledger.md`: replace the streaming boundary, preserve partial usage, and prove cancellation-safe ledger settlement.
- `02-run-settlement-and-execution.md`: make run settlement required and unique, then seal execution mutation authority.
- `03-docs-and-verification.md`: correct public guidance and run integration-wide verification.
- `04-execution-handoff.md`: order the implementation, define commands, and state completion gates.
