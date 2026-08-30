# Execution Handoff

## Dependency-ordered work packages

1. **Canonicalize scope applicability and capability compilation**
   - Files: `extension/registry.go`, `extension/*_test.go`, `composition/registry.go`, and focused `composition/*_test.go`.
   - Result: one scope predicate, snapshot-only instance filtering, mount-frozen tool identities, focused typed projectors, and one dispatch ownership handoff.
   - Gate: `go test` and `go test -race` for `extension`, `composition`, and `runtime`; target structural checks.
2. **Make model-stream attempts explicit**
   - Files: `runtime/model_stream.go`, `runtime/orchestrator.go:streamModelAttempts`, optional proposed `runtime/model_stream_receive.go`, and focused runtime tests.
   - Result: one explicit caller-owned attempt result, a callback-bounded receiver, and a durable-state-driven finalizer with preserved failure ordering and panic-safe partial state.
   - Gate: `go test` and `go test -race` for `runtime`, `model`, and `store/sqlite`; target structural checks.
3. **Integrate and re-audit**
   - Run `make check` and `git diff --check`.
   - Inspect `git diff main...HEAD` for compatibility code and unrelated changes.
   - Re-run the thermo-nuclear review outside `docs/` and `examples/`.
   - If any Critical, Important, or P1 finding remains, keep `eino-agent-5m4` open and begin another reviewed remediation iteration.
4. **Deliver**
   - Record final gate and review evidence in `eino-agent-5m4`.
   - Close the Beads issue only after the no-findings re-review.
   - Force-stage the ignored reviewed plan with `git add -f .agents/plans/final-thermo-structural-simplification` and stage scoped production/tests normally.
   - Commit the scoped changes.
   - Verify `git ls-tree -r --name-only HEAD -- .agents/plans/final-thermo-structural-simplification` lists all five plan files.
   - Run `git pull --rebase`, `bd dolt push`, and `git push`.
   - Verify `git status` reports a clean branch up to date with origin.

## Parallelization constraints

- Work packages 1 and 2 touch separate production files and can be implemented independently, but the calling agent should integrate them sequentially in this shared worktree.
- Do not allow independent agents to edit overlapping test files.
- The final thermonuclear review begins only after both packages and all full gates pass.

## Verification commands by package

```bash
go test ./extension ./composition ./runtime
go test -race ./extension ./composition ./runtime
go test ./runtime ./model ./store/sqlite
go test -race ./runtime ./model ./store/sqlite
target_pattern="composition/registry.go:.*(Function 'acquire'|func .?\\(\\*Registry\\)\\.acquire)|runtime/model_stream[^:]*:.*(streamModel|receiveModelStream|finalizeModelStreamAttempt)|composition/[^:]*:.*(newPlanSelection|planSelection.*components|projectTool)"
target_issues=$(.bin/golangci-lint run --no-config --default=none --enable=gocognit --enable=funlen --issues-exit-code=0 ./composition/... ./runtime/... ./extension/... 2>&1 | rg "$target_pattern" || true)
test -z "$target_issues"
make check
git diff --check
```

## Final definition of done

- Two independent plan reviews and one adversarial plan review complete.
- The calling agent applies every accepted finding directly to the plan before implementation.
- Both blocker-grade structures are simplified without behavior drift.
- Focused tests, race tests, structural checks, `make check`, and `git diff --check` pass.
- The final thermonuclear review reports no Critical, Important, or P1 findings.
- No required follow-up remains untracked in Beads.
- `eino-agent-5m4` is closed.
- All scoped changes are committed and pushed.
- The commit tree contains the five reviewed plan files.
- `git status` reports a clean branch up to date with origin.

## Deferred work

- Documentation under `docs/` remains intentionally unchanged.
- Example design remains intentionally unchanged; only compile-only repairs are permitted if unavoidable.
- Tool execution parallelism and tool-result ordering remain unchanged.
- Non-blocker test-file length findings remain outside this review iteration.
