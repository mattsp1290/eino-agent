# Execution Handoff

## Dependency-ordered work packages

### 1. Seal storage and split ledger interfaces

Files and symbols:

- `session/model_request.go`: proposed `ModelRequestReader`, proposed `ModelRequestWriter`; delete `ModelRequestStore`.
- `session/types.go`: embed the reader in `Store` and writer in `ExecutionStore`.
- `store/sqlite/store.go`: privatize run-owned writers and model-request writers.
- `store/sqlite/execution.go`: delegate fenced writes and remove ledger reads.
- `store/storetest/contract.go`: add model-request reader/writer, pagination, transition, rollback, and stale-fence contract coverage.
- Affected store, replay, transport, and runtime test fixtures.

Prerequisites: none.

Verification: focused `session`, `store`, `agui`, and `transport` tests plus stale-fence tests.

Done when no direct concrete-store mutation remains and every fixture uses an admitted run fence.

### 2. Correct the optional model-request ledger boundary

Files and symbols:

- `runtime/options.go`: retain `WithModelRequestLedger` and delete only the top-level writer assertion.
- `runtime/ledger.go`, `runtime/model_stream.go`: use `ModelRequestWriter` from `ExecutionStore` when enabled.
- `runtime/ledger_test.go` and fake stores: remove duplicate top-level writer scaffolding and retain optional-path and failure-path coverage.
- Ledger documentation named in `03-settlement-docs-and-verification.md`.

Prerequisite: work package 1 establishes the interface boundary.

Verification: `go test ./runtime/... ./store/...` and focused ledger lifecycle tests.

Done when disabled persistence creates no record, enabled persistence creates and terminally updates each attempt, and neither path requires writer methods on the top-level store.

### 3. Replace the mutable tool registry with direct materialization

Files and symbols:

- `tools/registry.go`: delete after extracting retained definition behavior.
- `tools/definition.go` (proposed new): definition contracts and proposed `Materialize`.
- `tools/registry_test.go`: replace with `tools/definition_test.go` (proposed new).
- `composition/registry.go`: direct definition materialization.
- `runtime/extensions_registry_test.go`: remove direct legacy-registry use.
- `wasmext/wasmext_test.go`: replace `wasmTestPlanProvider` with composition-backed plans.
- Composition, runtime plan, and Wasm tests.

Prerequisite: none. Do not run concurrently with work package 4 because both modify tool tests and documentation.

Verification: `go test ./tools/... ./composition/... ./runtime/... ./wasmext/...`.

Done when composition is the only live registry and strict plan identity remains unchanged.

### 4. Mount session tools through composition

Files and symbols:

- `tools/session/session.go`: replace `Register` with proposed `Mount`.
- `tools/session/session_test.go`: composition-backed lifecycle and isolation tests.
- Related tool documentation.

Prerequisite: work package 3 provides direct materialization.

Verification: `go test -race ./tools/session ./composition`, including a fresh-registry `AcquireResumePlan` round trip and drift rejection cases.

Done when session tools enter immutable run plans, obey mount lifecycle and scope, and resume after an equivalent registry reconstruction while rejecting identity drift.

### 5. Delete the output facade and finish documentation

Files and symbols:

- Delete `tools/output.go` and `tools/output_test.go`.
- Replace `tools/output_sqlite_test.go` with `runtime/tool_settlement_sqlite_test.go` (proposed new).
- Add or extend runtime settlement tests for unique facade coverage.
- Update all documentation listed in work file 03.

Prerequisites: work packages 1 through 4.

Verification: deleted-symbol search, focused runtime/SQLite tests, and documentation link inspection.

Done when runtime exclusively owns settlement encoding and documentation contains no deleted API guidance.

### 6. Integrate and deliver

Prerequisite: all prior work packages.

Run:

```bash
make fmt
make check
git diff --check
git status --short
```

Then inspect the diff, close `eino-agent-0go`, commit, rebase, push Beads data, push Git, and verify synchronization.

## Integration gates

- No compatibility aliases, forwarding methods, feature flags, or migrations were added.
- No top-level SQLite method can perform a run-owned mutation.
- No model-request write is available outside `ExecutionStore`.
- Composition selects tools once and materializes sealed definitions without a second registry.
- Acquired run plans keep working until release after their source mount deactivates.
- Session-tool descriptors resume against equivalent remounts and reject component, scope, order, and optional-definition drift.
- Enabled ledger persistence and tool settlement remain fail-closed.
- `make check` and `git diff --check` pass.

## Definition of done

- Every success criterion in `00-overview.md` is observable in code or tests.
- All accepted reviewer corrections are incorporated into the relevant plan files before implementation.
- The Beads issue is closed with no untracked follow-up work.
- The implementation and plan are committed and pushed.
- `git status` reports a clean branch up to date with origin.

## Deferred work

None. If implementation uncovers unrelated defects, create separate Beads issues rather than expanding this change.
