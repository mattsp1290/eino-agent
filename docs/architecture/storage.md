# Storage Architecture

Date: 2026-06-27

This document defines durable storage semantics for `eino-agent` store
implementations. It extends `docs/architecture/runtime.md` with the transaction,
idempotency, replay, pending tool-call, and recovery rules required by
downstream implementation beads.

## Scope

The public store boundary is transactional `session.Store`. Concrete backends
may be SQLite, embedded log, in-memory test stores, or hosted databases, but
they must expose the same atomic behavior to the runtime.

The reusable contract suite lives in `store/storetest`. Each concrete backend
must call:

```go
storetest.Run(t, func(t testing.TB) storetest.Subject {
    return storetest.Subject{Store: newStore(t)}
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
- `session.ModelRequestRecord`: optional provider-attempt audit records and
  their lifecycle state.

Live AG-UI SSE frames are not durable facts. Replay is projected from messages,
parts, context epochs, and durable events.

## Atomic Run Admission

`session.Store.AdmitRun` is both creation and ownership acquisition. It accepts
a lease duration; the store clock stamps the absolute deadline.

Required behavior:

- The operation is atomic for one `SessionID`.
- Fresh-run admission commits the run and fence, context epoch, current user
  message and text part, assistant placeholder, and canonical `run_started`
  event as one set. Creating a new session is part of that same transaction.
- `Run.ParentMsgID` and the assistant placeholder's `ParentID` both name the
  runtime-generated user message. No consumer-supplied message ID participates
  in this relationship.
- The current user message sorts strictly after all existing session messages;
  its assistant placeholder sorts strictly after that user. Stores represent
  this canonical order with UTC timestamps at nanosecond precision and use ID
  only as the deterministic tie-breaker for equal timestamps.
- Every pre-existing run ID returns `session.ErrConflict`, regardless of
  whether the submitted record is identical.
- Exactly one nonterminal run may own a session.
- A second nonterminal admission returns `session.ErrSessionBusy`.
- Terminal statuses are `RunInterrupted`, `RunFailed`, and `RunCompleted`.
- After scoped `SettleRun` records terminal state and its durable event, the next
  run may be admitted.
- Any error or panic before admission commits rolls back the entire new set. A
  failed later admission leaves the already-committed session and transcript
  unchanged. Failures or interruptions after admission retain the admitted
  user and assistant placeholder.
- `run_finished` is a reserved canonical event kind. Exactly one exists per run,
  and only `SettleRun` may commit it.
- `ClaimToken` is mutation authority. `OwnerID` is diagnostic metadata.
- `ClaimRun` atomically replaces owner/token only when the store clock observes
  an expired nonterminal lease.
- `Store.Execution(RunFence)` returns the only execution mutation capability;
  every write validates the current run token in the same transaction.
- `Store` exposes `ModelRequestReader`; model-request creates and updates are
  available only from the `ModelRequestWriter` embedded in `ExecutionStore`.

`ActiveRun` returns the current nonterminal owner or `session.ErrNotFound`.
`ListUnfinishedRuns` returns every nonterminal run so process startup can mark
or reconcile interrupted work.

## Transactions

Every backend implements `session.Store.WithinTx`. The outermost call commits
if `fn` returns nil and rolls back if `fn` returns a non-nil error or panics.
Nested calls reuse the current transaction without a savepoint; only an error
or panic that escapes the outermost callback forces rollback.

Transaction boundaries matter for:

- admitting a run and writing its first durable event;
- appending a message and its initial parts;
- creating, claiming, and settling a tool call together with its canonical
  pending, running, or terminal event;
- finishing a run together with its final durable event;
- creating a context epoch and writing compaction summary/tail metadata.
- creating and transitioning a model-request ledger record.

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

1. `CreateToolCall(CreateToolCallRequest)` writes a pending call and its
   canonical pending event in one fenced transaction.
2. Scoped `ClaimToolCall(ClaimToolCallRequest)` atomically changes pending to running, records
   `ClaimedBy`, `ClaimToken`, and a store-clock deadline, and extends the owning
   run lease to at least the same deadline. The running event commits in that
   same transaction.
3. A second claim for the same pending/running call returns
   `session.ErrConflict`.
4. `SettleToolCall(SettleToolCallRequest)` atomically records completed, failed,
   or interrupted state together with the reserved tool-result message, part,
   and one canonical terminal event.
5. `SettleToolCall` verifies both the successful tool claim and current run
   claim token, and returns
   `session.ErrConflict` for stale or stolen settlement attempts. Repeating an
   identical settlement is idempotent; contradictory state or envelopes conflict.
6. `ListUnfinishedToolCalls` returns pending/running calls for a run.

Tool transition events have a caller-supplied non-empty event ID and a distinct
`pending`, `running`, or `terminal` phase. The event payload and correlation
fields are derived from the authoritative request state. Replaying the same
phase, event ID, and state is idempotent. A different ID for an already
committed phase, or the same ID with different state, returns
`session.ErrConflict`. `AppendEvent` rejects tool transitions; only the three
typed mutations can persist them. Runtime publishes the already-committed event
to live sinks and extensions afterward, on a best-effort basis.

This prevents double execution and gives recovery enough data to decide whether
to mark a call interrupted or retry it when `RetrySafe` is true.

## Model-Request Ledger

Each provider attempt creates a bounded `prepared` record through
`ExecutionStore`, transitions it to
`dispatch_started` before the provider call, and settles it `completed` or
`failed`. These writes validate the owning run fence atomically. Top-level
`Store` readers expose individual records and stable run-scoped pagination.

## Idempotency

Stores make most caller-supplied record IDs idempotent, with admission as an
intentional exception:

- repeating `CreateSession`, `AppendMessage`, `AppendPart`, `AppendEvent`, or a
  typed tool transition with identical IDs and compatible payloads returns the
  existing record;
- repeating `AdmitRun` with any existing run ID returns
  `session.ErrConflict`; a start request is a one-shot admission attempt, not a
  replay command;
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
- compatible duplicate idempotency and incompatible duplicate conflicts for
  ordinary durable records, plus insert-only `AdmitRun` conflicts;
- pending tool-call creation, single-owner claim, conflict on second claim,
  claim-token fencing, atomic canonical events for every phase,
  event-ID/phase idempotency, generic append bypass rejection, and atomic
  terminal result-envelope persistence;
- model-request creation and state transitions through a valid execution fence,
  top-level reads and pagination, invalid-transition rejection, and rollback;
- unfinished run discovery and durable event listing with paged reads.


Every backend should run the contract suite in its own package and add
backend-specific tests for ID generation, migrations, persistence across
process restart, and database-level isolation.
