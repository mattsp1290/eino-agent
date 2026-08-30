# Atomic Tool Transitions

## Goal and prerequisite state

Return the exact tool call and event committed by each fenced transition, then publish only that returned event. This work is independent of run-plan grouping and may proceed after the plan is accepted.

## Existing evidence

- `session.ExecutionStore` returns a `ToolCall` for create/claim and only `error` for settle.
- `store/sqlite/execution.go` derives and appends each event inside the fenced transaction but discards the canonical `appendEvent` return.
- `runtime/tool_preparation.go:publishToolTransition` reconstructs events and silently returns on reconstruction failure.
- `runtime/tool_execution.go:settlementCall` discards `ToolSettlement.Apply` errors and reconstructs the terminal event after commit.
- SQLite `appendEvent` returns the already-persisted record on an identical idempotent replay, so its return is the canonical publication value.

## Exact change surface

- Add `session.ToolTransitionResult` in `session/tool_transition.go` with `Call ToolCall` and `Event EventRecord`.
- Change all three methods in `session.ExecutionStore`:
  - `CreateToolCall(...) (ToolTransitionResult, error)`
  - `ClaimToolCall(...) (ToolTransitionResult, error)`
  - `SettleToolCall(...) (ToolTransitionResult, error)`
- Update `store/sqlite/execution.go`, `store/storetest/contract.go`, SQLite tests, runtime fakes, wrappers, and direct call sites.
- Update `runtime/tool_preparation.go`, `runtime/tool_execution.go`, and `runtime/interrupt.go` to consume `.Call` and `.Event`.
- Delete production `publishToolTransition` and `settlementCall`.

## Store invariants

Within one fenced transaction:

1. Derive the phase call and event from validated request data.
2. Persist or idempotently recover the authoritative tool call.
3. Append or idempotently recover the authoritative event.
4. Return both canonical records only after every write succeeds.
5. Return a zero result on any error; rollback all writes.

For claim, derive the running event after the store-derived lease has been applied to the call, even though `LeaseUntil` is not serialized into the event payload. For settle, use the successful `ToolSettlement.Apply` result as the returned call and never recompute it outside the transaction.

Idempotent replay must return the records already stored, not merely the incoming candidates. Conflicting event IDs, calls, claim identities, settlements, or fences return `session.ErrConflict` and no publishable result.

## Runtime publication

- Fresh create publishes `result.Event` immediately after the store method returns successfully.
- Fresh claim preserves the exact sequence: claim commit, `ToolStartedPoint` notification, then publication of `result.Event`.
- Fresh settlement returns `result.Event` through `settledTool`; the existing caller publishes it once.
- Resume claim publishes the returned running event through the same persisted-event path if that path currently emits transition events; preserve existing observer/event ordering.
- Resume interruption settlement may ignore the returned event only where replay intentionally does not have a live sink publication, but it must still consume the result type without reconstructing data.
- Never swallow transition construction errors; they are store errors before commit.

## Tests and acceptance criteria

Extend store contract and SQLite tests to assert exact returned call/event equality for create, claim, settle, and identical replay. Add failure-injection assertions that a failed event insert yields no state mutation and a zero result.

Add runtime tests with a fake store that deliberately canonicalizes an otherwise non-semantic event field and prove the sink receives the returned record, not a runtime reconstruction. Verify one publish per successful fresh phase and zero publishes on store failure. Use one ordered recorder across the extension notification and event sink to assert the fresh claim sequence.

Run:

```text
go test ./session ./store/storetest ./store/sqlite ./runtime
go test -race ./runtime ./store/sqlite
```

Acceptance: every published tool transition is byte-for-byte the record returned by the atomic store mutation, and no post-commit derivation helper remains.

## Risks and exclusions

- Do not change event schemas, SQLite tables, IDs, redaction, or sink retry behavior.
- Preserve observer ordering relative to durable persistence.
- Update every interface fake directly; no compatibility embedding adapter is allowed.
