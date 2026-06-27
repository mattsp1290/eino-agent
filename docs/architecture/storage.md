# Storage Architecture

Date: 2026-06-27

This document defines durable storage semantics for `eino-agent` store
implementations. It extends `docs/architecture/runtime.md` with the transaction,
idempotency, replay, pending tool-call, and recovery rules required by
downstream implementation beads.

## Scope

The public store boundary is `session.Store` plus optional
`session.Transactor`. Concrete backends may be SQLite, embedded log, in-memory
test stores, or hosted databases, but they must expose the same behavior to the
runtime.

The reusable contract suite lives in `store/storetest`. Each concrete backend
must call:

```go
storetest.Run(t, func(t testing.TB) storetest.Subject {
    return storetest.Subject{Store: newStore(t), Transactor: newTransactor(t)}
})
```

## Durable Facts

Stores persist these durable facts:

- `session.Session`: conversation/session metadata.
- `session.Run`: admitted execution attempts and their terminal state.
- `session.Message`: replayable conversation envelopes.
- `session.Part`: ordered replayable message content.
- `session.EventRecord`: durable runtime event projection for recovery,
  observability, and replay audit.
- `session.ToolCall`: pending, claimed, completed, failed, and interrupted tool
  calls.
- `session.ContextEpoch`: compaction and context-history boundaries.

Live AG-UI SSE frames are not durable facts. Replay is projected from messages,
parts, context epochs, and durable events.

## Atomic Run Admission

`session.Store.AdmitRun` is both creation and ownership acquisition.

Required behavior:

- The operation is atomic for one `SessionID`.
- Exactly one nonterminal run may own a session.
- A second nonterminal admission returns `session.ErrSessionBusy`.
- Terminal statuses are `RunInterrupted`, `RunFailed`, and `RunCompleted`.
- After `FinishRun` records a terminal state, the next run may be admitted.
- `OwnerID` and `LeaseUntil` are stored so recovery can distinguish stale
  registrations from current owners.

`ActiveRun` returns the current nonterminal owner or `session.ErrNotFound`.
`ListUnfinishedRuns` returns every nonterminal run so process startup can mark
or reconcile interrupted work.

## Transactions

Backends that can provide transactions should implement `session.Transactor`.
`WithinTx` must commit if `fn` returns nil and roll back if `fn` returns a
non-nil error or panics.

Transaction boundaries matter for:

- admitting a run and writing its first durable event;
- appending a message and its initial parts;
- creating, claiming, and writing the first tool-call part;
- finishing a run and settling unfinished tool calls;
- creating a context epoch and writing compaction summary/tail metadata.

Stores without transaction support must document their weaker guarantees and
should still pass the non-transactional contract tests.

## Replay Ordering

Replay order is stable and deterministic:

- messages are ordered by the store's canonical message order;
- parts are ordered by message order and `Part.Ordinal`;
- pages use `ReplayCursor` without repeating or skipping records;
- replay reads only committed records.

IDs may be caller-supplied, backend-generated, or monotonic, but ordering must
not depend on map iteration or undefined database row order.

## Durable Events

`session.EventRecord` is the durable event projection that keeps `session`
independent from `runtime.Event`.

Durable events must:

- keep session/run/message/part/tool/epoch IDs when available;
- preserve `Kind`, `Correlation`, `LiveOnly`, and `CreatedAt`;
- be listable in stable event order with `EventCursor`;
- be usable after restart to explain recovery and replay decisions.

Live-only events may be persisted as audit records, but replay clients should
not treat them as the source of conversation content.

## Tool-Call Claim Semantics

Tool calls use a create, claim, finish lifecycle:

1. `CreateToolCall` writes a pending call before execution.
2. `ClaimToolCall` atomically changes pending to running and records
   `ClaimedBy`, `ClaimToken`, and `LeaseUntil`.
3. A second claim for the same pending/running call returns `session.ErrConflict`
   unless the backend has a documented expired-lease takeover policy.
4. `FinishToolCall` records completed, failed, or interrupted state.
5. `ListUnfinishedToolCalls` returns pending/running calls for a run.

This prevents double execution and gives recovery enough data to decide whether
to mark a call interrupted or retry it when `RetrySafe` is true.

## Idempotency

Stores should make caller-supplied IDs idempotent where possible:

- repeating `CreateSession`, `AdmitRun`, `AppendMessage`, `AppendPart`,
  `AppendEvent`, or `CreateToolCall` with identical IDs and compatible payloads
  may return the existing record;
- repeating those calls with incompatible payloads must return
  `session.ErrConflict`;
- finishing an already-terminal run or tool call with the same terminal payload
  may succeed; changing terminal state must return `session.ErrConflict`.

The first concrete backend bead should decide exact idempotent replay behavior
and encode it in backend-specific tests in addition to `store/storetest`.

## Interrupt and Resume Invariants

On startup or resume:

- `ListUnfinishedRuns` identifies interrupted provider requests.
- `ListUnfinishedToolCalls` identifies pending/running tool work.
- non-idempotent running tool calls are settled as `ToolCallInterrupted`;
- retry-safe pending calls may be retried if no claim has settled;
- replay uses committed messages/parts, not live transport state;
- context epochs explain compacted history before a new turn snapshot is built.

These rules keep recovery conservative until a future backend has stronger
journaling for provider responses or tool execution.

## Contract Tests

`store/storetest.Run` currently describes these required behaviors:

- atomic per-session run ownership and `ErrSessionBusy`;
- transaction rollback hides writes;
- stable replay ordering by message and part order;
- pending tool-call creation, single-owner claim, conflict on second claim, and
  terminal settlement;
- unfinished run discovery and durable event listing.

Every backend should run the contract suite in its own package and add
backend-specific tests for ID generation, migrations, persistence across
process restart, and database-level isolation.
