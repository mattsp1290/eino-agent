# Atomic Settlement And Dead Concurrency

## Goal and prerequisites

Make the durable settlement invariant unrepresentable through `session.Store`, then delete concurrency metadata that never controls runtime behavior. This work can begin immediately.

## Repository evidence

- `session/types.go`: `Store` exposes `FinishToolCall` and `SettleToolCall` together.
- `store/sqlite/store.go`: `SettleToolCall` calls exported `FinishToolCall` inside its transaction.
- `store/storetest/contract.go`: the reusable contract independently approves terminal call-only writes.
- `runtime/tool_preparation.go`: prepared calls execute in slice order in a single loop.
- `tools/einotools/einotools.go`: non-concurrent workspace tools already use `spec.locker.Do`.
- `tools/session/session.go`: shared session state already uses its own mutex.

## Exact change surface

- `session/types.go`: remove `Store.FinishToolCall`.
- `session/tool_settlement.go`: rewrite comments that mention `FinishToolCall`.
- `docs/architecture/storage.md`: describe `SettleToolCall` as the sole atomic terminal transition and remove guidance for the deleted call-only operation.
- `store/sqlite/store.go`: rename `FinishToolCall` to a private helper such as proposed `finishToolCall`; call it only from `SettleToolCall` while `s.tx != nil`.
- `store/storetest/contract.go`: delete the standalone finish contract cases; keep claim fencing, conflicting settlement, idempotent settlement, and atomic result-envelope cases.
- `store/sqlite/store_test.go`: migrate or delete tests that invoke the removed method directly.
- Runtime test stores: remove `FinishToolCall`; implement `SettleToolCall` directly against their in-memory records and reserved result envelopes.
- `runtime/types.go`: remove `ToolConcurrency`, `Tool.Concurrency`, and `ToolScope.ConcurrencyKey`.
- `tools/registry.go`: remove `Definition.Concurrency`, validation, defaults, and materialization copies.
- `tools/einotools/einotools.go`, `tools/session/session.go`, `agui/client_tools.go`, and affected tests: remove concurrency assignments and key-only assertions while preserving actual internal locking.
- `composition/registry.go`: remove concurrency from `toolSchemaHash` so the sealed descriptor hashes only behavior that exists.
- `docs/architecture/tools.md`, `docs/architecture/runtime.md`, and `docs/consumer-guide.md`: remove the runtime concurrency contract and describe tool-owned locking as the only synchronization authority.

## Intended invariants

- Every terminal tool call commits the call, reserved result message, and reserved result part in one store operation.
- Repeating the identical settlement remains idempotent.
- A stale claim token or conflicting envelope returns `session.ErrConflict` without partial mutation.
- No public type advertises parallelism or serialization that runtime does not enforce.
- Tool implementations remain free to serialize access internally where they own the mutable resource.

## Tests and acceptance

- `go test ./session ./store/sqlite ./store/storetest ./runtime ./tools/... ./composition`
- `go test -race ./runtime ./store/sqlite ./tools/...`
- `rg -n '\bFinishToolCall\b|ToolConcurrency|ConcurrencyKey' --glob '*.go' --glob '*.md' . --glob '!docs/prompts/**' --glob '!.agents/plans/**'` returns no current code or documentation hits.
- Focused documentation assertions/searches find no advice to request sequential runtime execution and do find the replacement tool-owned-locking contract.
- SQLite fault-injection tests prove a failed message or part append rolls back the terminal call update.

## Risks and exclusions

- Do not parallelize tool execution in this package. The deleted contract never controlled behavior.
- Do not weaken SQLite transaction fencing to simplify the helper extraction.
- Do not retain compatibility aliases.
