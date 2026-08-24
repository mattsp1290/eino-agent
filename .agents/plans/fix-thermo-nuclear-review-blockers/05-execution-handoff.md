# Execution Handoff

## Dependency-ordered work packages

1. **Remove terminal settlement bypass and concurrency metadata.** Change `session`, SQLite, store contracts, runtime/tool types, typed definitions, built-in tools, composition hashing, and focused tests.
2. **Install bounded tool context.** Change runtime context derivation, typed tool scope/execution contracts, Wasm projection, admission cloning, and mutation tests.
3. **Replace AG-UI aggregate registry with composition mounts.** Change client dispatcher/definition conversion, `tools/agui`, the integration example, docs, and end-to-end tests.
4. **Rebuild sealed-plan test fixtures.** Remove direct plan-field construction across runtime tests and add production-shaped integration coverage.
5. **Run final gates and deliver.** Format, test, lint, inspect the diff, close the bead, commit selectively, rebase, push Beads and Git, and verify clean/up-to-date state.

Work packages 2 and 3 depend on the removed concurrency fields. Work package 4 follows all API changes to avoid repeated fixture churn.

## Per-package verification

- Package 1: `go test ./session ./store/sqlite ./store/storetest ./runtime ./tools/... ./composition`.
- Package 2: `go test ./runtime ./tools/... ./wasmext` plus focused nested-mutation tests.
- Package 3: `go test ./agui ./tools/agui ./composition ./runtime ./examples/ag-ui-go-server-example`.
- Package 4: `go test ./...` and the structural searches below.
- Package 5: `make fmt`, `make check`, and `git diff --check`.

## Structural searches

```bash
rg -n '\bFinishToolCall\b|ToolConcurrency|ConcurrencyKey' --glob '*.go' --glob '*.md' . --glob '!docs/prompts/**' --glob '!.agents/plans/**'
rg -n 'testRunPlanWithTools|setTestTools|strictToolDescriptor|&RunPlan\{' runtime --glob '*_test.go'
rg -n 'runtime.ToolRegistry|ToolRegistryFunc|RuntimeTools|SetClientTools|ClearClientTools' runtime tools/agui agui examples docs --glob '*.go' --glob '*.md' --glob '!docs/prompts/**'
rg -n 'execution\.Snapshot|func\([^)]*runtime\.TurnSnapshot|func\([^)]*TurnSnapshot' runtime tools wasmext --glob '*.go'
rg -n 'sequential concurrency|container-only|materializ(e|ation).*TurnSnapshot|full turn snapshot' docs/architecture docs/consumer-guide.md --glob '*.md'
```

Every command must return no obsolete production or test references, except a documentation sentence that explicitly names a removed API as removed is allowed only when necessary.

## Integration invariants

- Plan identity and executable behavior remain constructed together.
- Session-scoped client tools appear only in matching session plans.
- All terminal tool writes are atomic and claim-fenced.
- Tool code sees only bounded data required for execution.
- Built-in stateful tools retain their existing internal synchronization.
- Plan leases release exactly once on success, error, cancellation, and resume.

## Definition of done

- Every success criterion in `00-overview.md` has a direct test or structural check.
- `make check` and `git diff --check` pass.
- No compatibility alias, migration, feature flag, or deprecated path was added.
- `eino-agent-fcf` is closed with the final gate result.
- Related files are committed with the actual commit SHA reported.
- `git pull --rebase`, `bd dolt push`, and `git push` succeed.
- `git status --short --branch` reports a clean branch up to date with `origin/feat/deeper-extensibility`.

## Deferred work

No deferred compatibility or rollout work is expected. File a new Beads issue only for a newly discovered requirement outside these five accepted review blockers.
