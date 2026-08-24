# Atomic Tool Lifecycle

## Goal and prerequisite state

After work package 1 removes compatibility modes, make every tool call use one result-ID allocation and atomic settlement path. Make final normalized input the sole source for permission patterns.

## Repository evidence

- `runtime/tool_execution.go:commitToolSettlement` chooses atomic or ordered writes from descriptor state.
- `runtime/tool_preparation.go` reserves result IDs only for strict plans.
- `runtime/interrupt.go` scans `ListUnreconciledToolSettlements` before resume.
- `store/sqlite/store.go` already implements transactional `SettleToolCall`.
- `runtime/tool_preparation.go` overwrites the pattern returned by `ToolPreparePoint`.

## Exact change surface

- `session/types.go`, `session/tool_settlement.go`
  - Add `SettleToolCall(context.Context, ToolSettlement) error` to `session.Store`; remove the optional `ToolSettlementStore` interface entirely.
  - Keep claim fencing and idempotent `ToolSettlement.Apply` behavior.
- `runtime/options.go`, `runtime/orchestrator.go`, `runtime/interrupt.go`
  - Call the required store method directly so option-built and direct-struct `Start`/`Resume` share the same contract.
  - Remove reconciliation before resume.
- `runtime/tool_preparation.go`
  - Allocate `ResultMessageID` and `ResultPartID` for every tool call before `CreateToolCall`.
  - Remove descriptor-based settlement branches and redundant type checks.
  - Accept only `PreparedToolCall.Call.Input` from the prepare interceptor.
  - Derive `ToolCall.Pattern` once from final normalized input after all transforms.
- `runtime/extension_tool.go`
  - Treat `ToolCall.Pattern` as protected in `validatePreparedToolCallInput`.
  - Update public comments to state that prepare interceptors rewrite JSON input, not permission authority independently.
- `runtime/tool_execution.go`
  - Remove `ensureToolResultIDs` branching and the non-atomic fallback.
  - Commit every terminal envelope through the required `session.Store` method using `context.WithoutCancel`.
- `runtime/interrupt.go`
  - Resume only unfinished calls. Pending calls reuse reserved IDs; running calls settle interrupted atomically without re-execution.
- `store/sqlite/store.go`
  - Delete `ListUnreconciledToolSettlements`, repair helpers used only by it, and compatibility comments.
- Tests in `runtime`, `session`, `store/sqlite`, and `tools`.

## Intended behavior and invariants

- No tool call can become terminal without its result message and part in the same store transaction.
- Every settlement presents the durable claim owner, token, and reserved result IDs.
- Cancellation after tool execution does not cancel settlement.
- Running calls found on resume settle interrupted atomically and never execute again.
- Pending calls found on resume preserve their admitted normalized input and reserved result IDs.
- Permission guards, host policy, approval requests, execution, durable events, and observation all see the same derived pattern and JSON input.
- A prepare interceptor that attempts to change `Pattern` fails with `extension.ErrProtectedMutation`.

## Tests and acceptance criteria

- Replace reconciliation tests with atomicity tests proving call/message/part commit together or not at all.
- Add real SQLite fault-injection cases at result-message and result-part insertion after the terminal update attempt; assert call, message, and part all roll back, then prove a retry succeeds.
- Test fresh and pending-resume calls produce equivalent settlement envelopes.
- Test running-resume interruption commits one atomic envelope and does not execute.
- Test settlement after cancellation uses a non-cancelled context.
- Test a prepare interceptor can rewrite input and that permissions receive the pattern derived from that final input.
- Test direct `Pattern` mutation fails before the terminal and permissions are reached.
- `rg -n 'ListUnreconciledToolSettlements|descriptorRequiresToolSettlement|FinishToolCall\(ctx, terminal\)|AppendMessage\(ctx, settlement.ResultMessage\)' runtime session store/sqlite --glob '*.go' --glob '!**/*_test.go'` returns no compatibility path.

## Dependencies, risks, and exclusions

- Requires sealed current-only plans from work package 1.
- Preserve the current model-visible `ToolOutput` JSON and metadata keys.
- Preserve SQLite transaction and claim-fencing semantics. All in-repository store implementations and test stores must implement the required settlement method.
- Do not add retry or compensation around a failed atomic store transaction; return the store error.
