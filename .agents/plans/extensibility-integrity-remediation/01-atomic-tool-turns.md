# Atomic Tool Turns

## Goal and prerequisites

Persist one model-produced assistant turn and all requested tool lifecycles atomically before any tool executes. Before the run settles, every committed call must be terminal.

Prerequisite: preserve the current run fence and SQLite nested-transaction behavior.

## Repository evidence

- `runtime/tool_preparation.go:persistAssistant` owns assistant parts but not tool-call creation.
- `runtime/tool_preparation.go:executePreparedTools` interleaves creation, claim, execution, and settlement.
- `session/tool_transition.go:CreateToolCallRequest` contains only `Call` and `Event`.
- `store/sqlite/execution.go:CreateToolCall` already combines call and pending event in one fenced transaction.
- `session.ToolSettlement` and `store/sqlite.settleToolCall` provide the canonical pattern for storing a call transition with its message part.

## Change surface

- `session/types.go`
  - Add a request-part identity to `ToolCall`, or replace it with an equivalent single canonical linkage chosen during implementation.
  - Keep result message and part identities unchanged.
- `session/tool_transition.go`
  - Extend `CreateToolCallRequest` to carry the canonical assistant `PartToolCall` part.
  - Validate call, request part, and pending event as one envelope.
- `store/sqlite/execution.go` and `store/sqlite/tool_calls.go`
  - Make `CreateToolCall` append the request part in the same transaction as the pending call and event.
  - Preserve exact idempotent replay of all three records.
  - Reject generic `AppendPart` calls for `PartToolCall` and `PartToolResult`; their lifecycle transition methods are the only writers.
  - Make `SettleRun` reject terminal settlement transactionally whenever the run has a pending or running call.
- `runtime/tool_preparation.go`
  - Replace `persistAssistant` plus creation inside `executePreparedTools` with a proposed `persistAssistantTurn` boundary.
  - Reserve request-part, result-message, and result-part IDs before entering the transaction.
  - Append text/reasoning parts and invoke `CreateToolCall` for every prepared call through one outer `WithinTx` call.
  - Collect canonical `ToolTransitionResult` values and publish them only after the outer transaction succeeds.
  - Execute the returned canonical calls without recreating request state.
- `runtime/tool_execution.go`
  - Add a shared private lifecycle-only helper for a committed call skipped after cancellation, resume recovery, or another fatal outcome.
  - The helper claims and settles the call as interrupted using `context.WithoutCancel`, without invoking the executor, guards, middleware, or result transforms.
- `runtime/orchestrator.go`
  - Pass prepared calls into the atomic persistence boundary.
  - Keep the first fatal error as the run result while continuing lifecycle terminalization for remaining calls.
- `runtime/interrupt.go`
  - Use the same ordered lifecycle-only terminalization path for the current and remaining resume calls after any fatal recovery outcome.
  - Preserve a nonterminal run when terminalization cannot be persisted; let its lease expire so a later resume can retry recovery.

All proposed helpers are private and live beside the current symbols they replace.

## Invariants and failure behavior

- No assistant tool-call part commits without its pending `ToolCall` and pending transition event.
- No pending transition event is published before the containing transaction commits.
- Returned store records, not request copies, drive claim and execution.
- A normal tool failure remains model-visible and does not abort later calls.
- A panic, cancellation, or lifecycle persistence failure stops new tool behavior.
- Calls not executed after that point become interrupted through lifecycle-only transitions.
- The original fatal error remains discoverable if cleanup also fails; join cleanup errors without replacing the primary error.
- Run settlement succeeds only after all committed calls are terminal. This is a SQLite/store invariant, not only a runtime convention.
- If lifecycle-only terminalization fails, terminal settlement is rejected and the run remains reclaimable after lease expiry.

## Tests and acceptance criteria

- Extend `session/tool_transition_test.go` for invalid request-part identity and canonical payload validation.
- Extend `store/storetest/contract.go` so every backend must atomically create the request part, call, and pending event.
- Add store-contract coverage that generic `AppendPart` rejects both tool-call and tool-result parts, while the specialized envelopes remain exactly idempotent.
- Add store-contract coverage that `SettleRun` rejects a terminal state while any call is pending or running.
- Add SQLite rollback tests for forced part, call, and event failures.
- Add a runtime test with at least two calls where the first panics or observes cancellation. Assert both calls exist, both are terminal, both result parts exist, and the run is terminal only afterward.
- Add a runtime test that forces the second creation to fail. Assert no assistant parts, tool calls, or pending events from the turn commit.
- Add fresh-run and resume tests with injected claim/settlement cleanup failures. Assert terminal settlement is rejected, the lease remains recoverable after expiry, and a later resume can finish cleanup.
- Assert emitted pending events appear only after successful transaction completion and retain call order.

## Dependencies and exclusions

- Implement after point authority and before asynchronous dispatch. Rerun event-order assertions after dispatch lands using explicit test synchronization.
- Do not parallelize tool behavior.
- Do not add a migration. Modify schema version 1 only if a relational request-part column becomes necessary; prefer keeping the linkage inside the canonical record when no query requires a column.
