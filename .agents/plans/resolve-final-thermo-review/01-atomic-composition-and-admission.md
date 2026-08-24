# Atomic Composition and Admission

## Goal and prerequisites

Make both in-memory mount publication and durable run admission all-or-nothing. This package depends only on the no-compatibility application decision in [00-overview.md](00-overview.md).

## Composition mount publication

### Evidence

- `composition/registry.go`, `Registry.Mount`, validates staged entries under `Registry.mu`, calls `r.extensions.CommitMount`, and then calls fallible `mountToolDefinition` while appending tools.
- `extension/registrar.go`, `Registrar.Tool`, already clones each `tools.Definition` before staging it.
- `mountToolDefinition` clones again only to replace callbacks with mount-context wrappers.

### Exact change surface

- Change `composition/registry.go`:
  - Make `mountToolDefinition` an infallible value transformation over the already-frozen definition.
  - Delete the second clone, commit the extension mount, then perform only infallible value wrapping and slice appends while `Registry.mu` remains held; callback wrapping needs the committed `extension.Mount`.
  - Keep validation and one atomic commit section under `Registry.mu`.
  - Guarantee unlock through a single structured exit after locking.
- Change focused composition tests in `composition/registry_test.go` or the existing mount test files.

### Intended invariants

- All cloning, schema validation, collision validation, and extension preparation finish before publication.
- A returned error publishes neither the extension mount nor any tool or prompt registration.
- No code path after `CommitMount` can fail before all capability slices are updated.
- Mounted callbacks still receive `extension.Mount` callback context and retain the frozen schema/metadata snapshot.

### Tests and acceptance

- Preserve or add a test proving mutation of the caller's definition after registration does not affect the mount.
- Add a callback-context regression test for the infallible wrapper path.
- Add a failure/reuse test: a preparation or validation failure leaves no capabilities visible and a subsequent valid mount succeeds without blocking.
- Run `go test ./composition ./extension` and `go test -race ./composition`.

## Mandatory store-owned admission transaction

### Evidence

- `session/types.go` separates `Store`, `Tx`, and `Transactor`.
- `runtime/admission.go`, `Admitter.Admit`, uses `a.transactor()` when available and otherwise calls `admitDurable` directly.
- `runtime.StreamingOrchestrator` and the minimal server can inject a separate transactor.
- `store/sqlite/store.go` implements the only production transaction boundary; `store/storetest/contract.go` makes transaction coverage optional.

### Exact change surface

- Change `session/types.go`:
  - Add `WithinTx(ctx context.Context, fn func(context.Context, Store) error) error` to `Store`.
  - Delete the `Tx` and `Transactor` interfaces.
- Change `store/sqlite/store.go`:
  - Update `WithinTx` to pass `session.Store`.
  - If the receiver already wraps a SQL transaction, invoke the callback with that receiver instead of opening a nested transaction.
  - Preserve rollback on callback error and panic, and commit only after a nil callback result.
- Change `runtime/admission.go`:
  - Delete `Admitter.Transactor` and `admitter.transactor`.
  - Always execute `admitDurable` through `a.Store.WithinTx`.
- Change `store/storetest/contract.go`:
  - Remove the optional transactor field and compatibility suite.
  - Run rollback, commit, panic, and nested-transaction behavior through every subject's `Store`.
- Update all fake stores, SQLite tests, runtime tests, examples, and constructor options for the new single interface.

### Intended invariants and error paths

- Session creation/update, run creation, epoch creation, assistant-message append, and the admission event commit together.
- Any write or callback failure rolls back every admission write.
- A panic rolls back before propagating.
- Nested `WithinTx` calls share the current transaction without a savepoint and cannot deadlock by trying to begin another SQLite transaction.
- Only an error or panic that escapes the outermost callback rolls back the transaction. If an outer callback deliberately swallows a nested callback error, the outer transaction may commit its accumulated writes.
- The store used for reads/writes is necessarily the store that owns the transaction.

### Tests and acceptance

- Make transactional contract tests mandatory for SQLite.
- Keep the admission injected-failure tests and assert no durable partial state at every write boundary.
- Test successful nesting, a propagated nested error, a deliberately swallowed nested error, and a nested panic. The propagated error and panic roll back the outermost transaction; the swallowed error follows the documented no-savepoint semantics.
- Audit every embedded or decorated `session.Store`. Add explicit transaction forwarding or a real implementation wherever runtime admission can reach it; compilation through a nil embedded interface is not sufficient.
- Structural searches find no `session.Tx`, `session.Transactor`, `WithTransactor`, `Admitter.Transactor`, or non-transactional `admitDurable` invocation.
- Run `go test ./session ./store/... ./runtime` and the corresponding race tests.

## Dependencies and exclusions

- Complete the store interface change before the orchestrator sealing package so constructor call sites move once.
- Do not add a transaction adapter for non-transactional stores.
- Do not change admission event contents or introduce a new persisted schema.
