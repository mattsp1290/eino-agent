# Verification

## Goal

Prove that both structural simplifications preserve scope/identity behavior, ownership cleanup, model-request durability, retry behavior, and repository quality.

## Focused gates

Run after capability-plan compilation:

```bash
go test ./extension ./composition ./runtime
go test -race ./extension ./composition ./runtime
```

Run after model-stream refactoring:

```bash
go test ./runtime ./model ./store/sqlite
go test -race ./runtime ./model ./store/sqlite
```

Run structural checks after both work packages compile:

```bash
rg -n 'capabilityApplies|func scopeApplies' composition extension --glob '*.go'
rg -n 'streamErr|modelRequested|ledgerTransitionOK|dispatchStarted|terminalTransitionOK' runtime/model_stream*.go
target_pattern="composition/registry.go:.*(Function 'acquire'|func .?\\(\\*Registry\\)\\.acquire)|runtime/model_stream[^:]*:.*(streamModel|receiveModelStream|finalizeModelStreamAttempt)|composition/[^:]*:.*(newPlanSelection|planSelection.*components|projectTool)"
target_issues=$(.bin/golangci-lint run --no-config --default=none --enable=gocognit --enable=funlen --issues-exit-code=0 ./composition/... ./runtime/... ./extension/... 2>&1 | rg "$target_pattern" || true)
test -z "$target_issues"
```

Before implementation, run the same analyzer/filter and prove the negative control is non-empty for current `composition.Registry.acquire` and `runtime.streamModel`. After implementation, the first two searches must return no stale production symbols or renamed lifecycle flags, and the target output must be empty. The targeted assertion records the existing unrelated `funlen`/`gocognit` baseline without turning it into scope expansion. `make check` remains the repository-wide passing lint gate.

## Full gates

Run from the repository root:

```bash
make check
git diff --check
```

Do not update `docs/` or redesign `examples/`. Production API decisions must ignore example compatibility. If `make check` exposes an example compile break, make only the smallest compile-only example repair and do not review or redesign example behavior.

## Regression matrix

- Fresh and resumed plan acquisition.
- Global and matching/mismatched session capability selection.
- Session-over-global prompt precedence.
- Resume exclusion of newly mounted instances and capabilities.
- Deterministic fingerprints across mount order.
- Mount-preparation rollback on invalid/uncloneable tools, with no mount publication or later acquisition output.
- Snapshot dispatch release on runtime plan compilation/sealing failures.
- Request preparation, dispatch start, stream success, stream failure, and terminal ledger failure.
- Retryable pre-delta failure and non-retryable post-delta failure.
- Invocation panic, post-delta receive panic, close panic, cancellation, nil reader, nil chunk, malformed concatenation, and empty stream.
- Usage accumulation across failures, first-token/chunk observability, live delta order, observer completion, and model lifecycle notification cardinality.

## Final review gate

Re-run the thermo-nuclear code-quality review against `main...HEAD`, excluding `docs/` and `examples/` and treating backward compatibility as dead code. Completion requires an explicit result with no Critical, Important, or P1 findings. A new blocker finding starts another plan-review-implementation iteration; a green test suite alone is not completion evidence.

## Acceptance criteria

- Every focused and full gate passes without new skips.
- Race tests report no data races.
- No production file crosses 1,000 lines.
- No compatibility alias, wrapper, migration, flag, or dual implementation is added.
- Target static-complexity checks pass.
- The final thermonuclear re-review reports no blocker-grade findings.
- The worktree contains only the scoped plan, production, and test changes.
- The final commit tree contains `.agents/plans/final-thermo-structural-simplification/00-overview.md` and the complete reviewed plan directory despite the default ignore rule.
