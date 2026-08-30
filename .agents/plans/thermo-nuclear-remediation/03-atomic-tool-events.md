# Atomic Tool Events

## Goal and prerequisite

Make the tool-call state machine and its durable replay event one fenced transaction. This package can be developed independently of work packages 1 and 2, but final runtime integration follows both to avoid overlapping test churn.

## Existing evidence

- `runtime/tool_preparation.go` calls `CreateToolCall`, `ClaimToolCall`, and settlement before separate `emitToolCall` calls and discards all three errors.
- `runtime/event_sink.go` persists non-live intermediate events through `ExecutionStore.AppendEvent`, so each tool transition currently opens a second transaction.
- `store/sqlite/execution.go` wraps each state mutation in `withFence`.
- `ExecutionStore.SettleRun` already accepts a final event and writes state plus event in one transaction.
- Resume tool claims and interrupted settlements currently do not create equivalent durable transition events.

## Typed mutation contract decision

Define three concrete session-owned request types, one per phase. Each has exactly one authoritative state value plus bounded event-envelope metadata; a canonical session encoder derives the durable tool-transition event from that state. Runtime does not hand stores an independent `EventRecord` or arbitrary JSON to correlate.

```text
CreateToolCallRequest
  Call ToolCall                         # complete pending state, including pattern, retry safety, and reserved result IDs
  EventID, EpochID, ProviderID, ModelID # bounded event envelope

ClaimToolCallRequest
  ID, ClaimedBy, ClaimToken, StartedAt  # authoritative claim delta
  LeaseDuration                         # store derives run/tool LeaseUntil and returns the complete running call
  EventID, EpochID, ProviderID, ModelID # bounded event envelope

SettleToolCallRequest
  Settlement ToolSettlement             # authoritative terminal state plus result message/part
  EventID, EpochID, ProviderID, ModelID # bounded event envelope
```

Change existing `session.ExecutionStore` methods to accept those requests:

```text
CreateToolCall(ctx, request CreateToolCallRequest) (ToolCall, error)
ClaimToolCall(ctx, request ClaimToolCallRequest) (ToolCall, error)
SettleToolCall(ctx, request SettleToolCallRequest) error
```

The store may generate only `LeaseUntil` while renewing the run lease; all other state comes from the phase request. Event metadata is mandatory, not a nullable compatibility option.

## Atomic invariants

- Create requires a pending call and a matching pending event.
- Claim requires a running candidate and a matching running event.
- Settlement requires a terminal settlement and a matching terminal event.
- Phase, event ID, `SessionID`, `RunID`, `MessageID`, `ToolCallID`, status, name, input/output, error, metadata, and timestamp are derived from or validated against the authoritative phase state and canonical event projection.
- Create commits the call and pending event together. Claim commits run-lease renewal, tool lease/status, and running event together. Settlement commits the terminal call, reserved result message, reserved result part, and terminal event together.
- If any validation or write fails, every write in that operation rolls back, including run-lease renewal and the result envelope.
- Event ID is the phase idempotency key. Repeating the same phase with the same event ID and state succeeds idempotently; a different event ID for an already committed phase returns `ErrConflict`, even if all other fields match. A same-ID request with different state also returns `ErrConflict`.
- Stale run or tool claim tokens reject both writes.
- Durable events use a caller-supplied non-empty ID and the same typed timestamp/status data used by the transition.

## Persistence uniqueness

- Update the greenfield `001_sqlite_store.sql` directly: add nullable `tool_call_id` and `tool_transition` columns to `events` and a partial unique index on `(tool_call_id, tool_transition)` for tool-transition rows.
- Update schema verification for the new columns and unique partial index; do not add a migration for the old schema.
- Only the three tool mutation APIs may persist tool-transition events. `AppendEvent` rejects records declaring a tool transition so generic event persistence cannot bypass uniqueness or correlation validation.
- A replay with a new event ID for an already committed phase returns `ErrConflict`; it must never append a duplicate event.

## Runtime publication split

- Refactor `runtime.emitToolCall` into a pure event builder plus a publication path.
- Build each concrete phase request before calling the store mutation; derive the runtime `Event` and durable `EventRecord` from the committed request/returned call through the same canonical projection.
- After commit, publish the exact event to the infrastructure sink and `EventPublishedPoint` without persisting it again.
- Infrastructure sink failure remains best-effort and cannot change durable state or the run result.
- General `runEventSink.Emit` keeps its current behavior for non-tool runtime events; add a private method or focused publisher for already-persisted tool events rather than a public bypass flag.
- Fresh execution may publish live transitions externally. Resume must commit durable claim/settlement events but may retain its documented transport policy.

## Exact change surface

- `session/types.go`: add the typed transition contract and change `ExecutionStore`.
- `session/tool_settlement.go`: validate terminal transition correlation at the durable contract layer.
- `store/sqlite/execution.go` and `store/sqlite/tool_calls.go`: perform atomic writes and validate matching records.
- `store/storetest/contract.go`: make atomic state/event behavior portable to every backend.
- `runtime/tool_preparation.go`, `runtime/tool_execution.go`, `runtime/interrupt.go`, `runtime/event_sink.go`, and focused helpers.
- Runtime fake stores and every direct `ExecutionStore` call in tests.
- `docs/architecture/storage.md`, `docs/architecture/extension-points.md`, and `docs/architecture/agui-events.md`.

## Tests and acceptance criteria

- Store contract tests mutate event ID, phase, session, run, message, tool-call ID, kind, status, name, input/output, error, metadata, claim ownership/token/time, reserved result IDs, result envelope, and timestamp independently and prove invalid requests write nothing.
- Injected create failure leaves no call or event; claim failure leaves tool status/lease and run lease unchanged; settlement failure leaves call, result message, result part, and event unchanged.
- SQLite tests prove exactly one matching event appears with each successful transition.
- Same-ID equivalent replay succeeds with no duplicate. Different-ID equivalent replay returns `ErrConflict` with no duplicate. Same-ID conflicting replay returns `ErrConflict` with no mutation.
- Successful/idempotent settlement proves exactly one result message, result part, and terminal event.
- Stale fences and claim tokens create neither state nor event changes.
- Runtime tests prove infrastructure sink failure occurs after commit and does not fail tool execution.
- Runtime tests prove persistent event failure fails the state mutation rather than being discarded.
- No tool-transition call site uses `_ = o.emitToolCall`.

## Risks and exclusions

- Do not make external transport delivery part of the database transaction.
- Do not keep the old overloads for tests or third-party stores.
- Do not infer event status by reparsing arbitrary JSON inside SQLite; use the session-owned typed transition and canonical encoder.
