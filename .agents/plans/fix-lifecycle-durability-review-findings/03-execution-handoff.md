# Execution Handoff

## Dependency-ordered work packages

1. Opaque session scopes
   - Change: `extension/types.go:validateScope`.
   - Tests: `extension/extension_test.go`, `composition/registry_test.go`.
   - Verify: `go test ./extension ./composition`.

2. SQLite partial settlement reconciliation
   - Change: `store/sqlite/store.go:ListUnreconciledToolSettlements` and optional private part lookup helper.
   - Tests: `store/sqlite/store_test.go`.
   - Verify: `go test ./store/sqlite`.

3. Durable settlement notification gate
   - Change: private execution/finalization signatures in `runtime/orchestrator.go` and `runtime/interrupt.go`.
   - Tests: runtime extension/orchestrator tests and controlled store failure fields in test doubles.
   - Verify: `go test ./runtime`.

4. Tool phase separation
   - Change: `runtime/orchestrator.go:executePreparedTools`, `afterToolOutcome`, and a shared result-transform helper.
   - Tests: `runtime/orchestrator_test.go` and, if the extension interceptor setup is clearer there, `runtime/extensions_test.go`.
   - Verify: `go test ./runtime`.

5. Provider panic ledger failure
   - Change: `runtime/orchestrator.go:streamModel` finalization defer.
   - Tests: `runtime/ledger_test.go`.
   - Verify: `go test ./runtime`.

Packages 1 and 2 have no code dependency. Packages 3 through 5 touch `runtime/orchestrator.go` and should be applied sequentially to keep lifecycle changes reviewable.

## Integration and regression gates

After focused tests pass:

1. Run `make fmt` and inspect the diff for unrelated generated or fixture changes.
2. Run `make check`, which includes format, vet, unit tests, race tests, module tidiness, lint, and WIT generation checks.
3. Run `git diff --check`.
4. Confirm `git status --short` contains only the reviewed plan, source, and test changes plus expected Beads state outside Git if applicable.
5. Close `eino-agent-drs`, stage only the reviewed plan/source/test files, inspect `git diff --cached`, commit the changes, and record `git rev-parse HEAD`.
6. Run push preflight, then follow the repository protocol with individually checked commands: `git pull --rebase`, `bd dolt push`, `git push`, and verify the branch is up to date with its upstream.

## Definition of done

- All five supplied findings have regression tests that fail against the pre-fix behavior and pass after implementation.
- Opaque session keys work through extension registration and composition mounting.
- `RunSettledPoint` is absent on non-durable fresh/resume failures and present once on durable completion.
- Legacy after middleware is never invoked for either preparation failure source.
- Panicking dispatched model requests are durably failed.
- SQLite repairs the missing-part-only legacy state without an idempotency conflict.
- `make check` and `git diff --check` pass.
- The Beads issue is closed, changes are committed, and the commit is pushed and verified against the upstream branch.

## Deferred work

None. Any newly discovered unrelated defect must be recorded as a separate Beads issue rather than folded into this fix.
