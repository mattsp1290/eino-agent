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

`session.Store.AdmitRun` is both creation and ownership acquisition. It accepts
a lease duration; the store clock stamps the absolute deadline.

Required behavior:

- The operation is atomic for one `SessionID`.
- Exactly one nonterminal run may own a session.
- A second nonterminal admission returns `session.ErrSessionBusy`.
- Terminal statuses are `RunInterrupted`, `RunFailed`, and `RunCompleted`.
- After scoped `SettleRun` records terminal state and its durable event, the next
  run may be admitted.
- `ClaimToken` is mutation authority. `OwnerID` is diagnostic metadata.
- `ClaimRun` atomically replaces owner/token only when the store clock observes
  an expired nonterminal lease.
- `Store.Execution(RunFence)` returns the only execution mutation capability;
  every write validates the current run token in the same transaction.

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
- finishing a run together with its final durable event;
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
- keep provider/model IDs, parent/correlation IDs, usage, error, and redaction
  fields as first-class data;
- preserve `Kind`, `Correlation`, `LiveOnly`, and `CreatedAt`;
- be listable in stable event order with `EventCursor`;
- be usable after restart to explain recovery and replay decisions.

Runtime does not persist live-only transport events. Replay clients reconstruct
conversation content from committed messages, parts, epochs, and durable event
projections rather than old transport frames.

## Tool-Call Claim Semantics

Tool calls use a create, claim, atomic-settlement lifecycle:

1. `CreateToolCall` writes a pending call before execution.
2. Scoped `ClaimToolCall` atomically changes pending to running, records
   `ClaimedBy`, `ClaimToken`, and a store-clock deadline, and extends the owning
   run lease to at least the same deadline.
3. A second claim for the same pending/running call returns
   `session.ErrConflict`.
4. `SettleToolCall` atomically records completed, failed, or interrupted state
   together with the reserved tool-result message and part.
5. `SettleToolCall` verifies both the successful tool claim and current run
   claim token, and returns
   `session.ErrConflict` for stale or stolen settlement attempts. Repeating an
   identical settlement is idempotent; contradictory state or envelopes conflict.
6. `ListUnfinishedToolCalls` returns pending/running calls for a run.

This prevents double execution and gives recovery enough data to decide whether
to mark a call interrupted or retry it when `RetrySafe` is true.

## Idempotency

Stores must make caller-supplied IDs idempotent:

- repeating `CreateSession`, `AppendMessage`, `AppendPart`, `AppendEvent`, or
  `CreateToolCall` with identical IDs and compatible payloads returns the
  existing record;
- repeating `AdmitRun` with the same run ID and compatible payload returns the
  existing run when it is already the active owner, or its terminal record after
  finish;
- repeating those calls with incompatible payloads must return
  `session.ErrConflict`;
- finishing an already-terminal run or tool call with the same terminal payload
  succeeds; changing terminal state or settling with the wrong claim owner/token
  must return `session.ErrConflict`.

Backend-specific tests may add stronger constraints, but they must not weaken
this portable duplicate-write behavior.

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

- atomic per-session run ownership under concurrent admission and
  `ErrSessionBusy`;
- transaction commit and rollback behavior when a transactor is exposed;
- stable replay ordering by message and part order, including paged reads;
- mandatory compatible duplicate idempotency and incompatible duplicate
  conflicts;
- pending tool-call creation, single-owner claim, conflict on second claim,
  claim-token fencing, and terminal settlement;
- unfinished run discovery and durable event listing with paged reads.

Transactional backends should also call `storetest.RunTransactional`, which
requires a non-nil transactor and repeats the transaction-specific contract.

Every backend should run the contract suite in its own package and add
backend-specific tests for ID generation, migrations, persistence across
process restart, and database-level isolation.
