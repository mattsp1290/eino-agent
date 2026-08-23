# Execution Handoff

## Dependency-ordered work packages

1. Runtime protection: update `afterToolOutcome` and disposition-based output encoding; add fresh-result transform regressions for initial and resumed orchestration; run `go test ./runtime`.
2. Strict resume: update `acquireResumePlan`; add stale-copied and canonical fingerprint tests; run `go test ./runtime`.
3. Protected schema clones: update runtime tool cloning/comparison; add schema-preservation, replacement, and malformed-schema tests; run `go test ./runtime`.
4. Settlement builder: add and populate reserved IDs on `runtime.ToolCall`, reject missing reservations, populate the reserved records and returned part, and add unit/SQLite acceptance coverage; run `go test ./runtime ./tools ./store/sqlite`.
5. Slice cloning: extend slice visit shape and add both-visitation-order regression coverage; run `go test ./model`.
6. Integration gate: run `go test -race ./runtime ./tools ./model ./store/sqlite`, `make check`, and `git diff --check`.
7. Delivery: inspect status/diff, close Beads issue `eino-agent-b3x`, run push preflight, commit only the plan and implementation files, `git pull --rebase`, `bd dolt push`, `git push`, prune stale remote refs, and verify the branch is clean and up to date.

## Dependencies and parallelization

- Packages are largely independent, but the calling agent owns all edits because the reviewed plan and final diff must remain coherent.
- Runtime protection and schema-clone tests share `runtime/extensions_test.go`; implement them sequentially to avoid edit conflicts.
- Run the SQLite integration test after the builder field contract is stable.

## Integration and regression gates

- A failure in any focused test stops delivery and is fixed before broader gates.
- A failure in `make check` stops commit unless it is proven unrelated and recorded as a follow-up Beads issue; no such exception is currently expected.
- A failed push is resolved and retried. The task is not complete until `git status` reports the branch up to date with its upstream.

## Definition of done

- All five supplied review findings have code fixes and direct regression coverage.
- Two independent plan reviews and one adversarial review are complete, and accepted findings are incorporated into these documents before coding.
- Focused, race, and repository gates pass.
- No remaining scoped work requires a follow-up issue.
- The implementation and plan are committed and pushed on `feat/deeper-extensibility`.

## Deferred work

None. Any newly discovered out-of-scope defect must be recorded in Beads before session completion.
