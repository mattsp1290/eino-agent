# Execution handoff

## Planning state

Status: Ready. Implementation has not occurred.

Application context and scope in [00-overview.md](00-overview.md) are authoritative.

## Dependency-ordered work packages

### WP1: Pin the completed dependency

Result: the agent module compiles against `catalog` commit `63a3c99`.

- Update `go.mod` and `go.sum` to the exact completed commit pseudo-version.
- Run `go mod tidy -diff` and a catalog import compile check.
- Stop if resolution selects a different commit.

Prerequisites: clean worktree and claimed bead `eino-agent-9f7`. Parallelization: none.

### WP2: Compose durable source identity

Result: source and host hashes both affect the frozen plan.

- Implement [01-durable-identity.md](01-durable-identity.md).
- Add focused validation, sensitivity, and resume-drift tests.
- Run `go test ./composition ./session`.

Prerequisites: WP1. Parallelization: implementation and tests share the same core file and should have one owner.

### WP3: Replace the adapter

Result: `MountStandard` atomically mounts catalog tools through composition and the old API no longer exists.

- Implement [02-catalog-composition-adapter.md](02-catalog-composition-adapter.md).
- Rewrite `tools/einotools/einotools_test.go` around production run plans.
- Add the process-wide ref-counted locker, lexical operation-pattern resolvers, cross-mount concurrency tests, bounded-pattern tests, MCP pending-envelope test, and unsupported-platform error seam.
- Run `go test -race ./tools/einotools ./composition`.

Prerequisites: WP2. Parallelization: adapter implementation must precede final integration tests.

### WP4: Seal workspace and input admission

Result: durable runs keep one workspace authority root and preserve leaf duplicate-key rejection.

- Canonicalize non-empty workspace metadata before admission persists the run.
- Add fresh/resume tests that retarget an input symlink and prove the persisted canonical root remains unchanged.
- Reject duplicate top-level tool-call arguments before canonical object conversion and add no-persistence/no-execution tests.
- Run `go test -race ./runtime`.

Prerequisites: WP3. Parallelization: workspace admission and argument-boundary work touch different runtime files but integrate through the WP5 production test.

### WP5: Prove the orchestration path

Result: provider order, permissions, durable settlement, and resume use the translated bundle correctly.

- Remove the alphabetical tool re-sorts from existing `runtime/orchestrator.go` and `runtime/interrupt.go`.
- Preserve existing snapshot order followed by composition-plan order.
- Add a runtime integration test that mounts standard tools, exposes catalog order to a fake model, invokes file read, applies an operation-specific permission rule, and asserts the persisted pattern and settled leaf output.
- Run `go test -race ./runtime ./tools/einotools ./composition`.

Prerequisites: WP4. Parallelization: the runtime test depends on the final adapter and admission behavior.

### WP6: Update supported documentation

Result: dependency evidence and consumer setup match the implemented path.

- Implement the documentation changes in [03-verification-and-documentation.md](03-verification-and-documentation.md).
- Run the stale-reference search gate.

Prerequisites: WP5 behavior and API names are final. Parallelization: documents may be edited independently after the API compiles.

### WP7: Integrate and ship

Result: quality gates pass, Beads is closed, and the branch is pushed.

1. Run `make fmt`.
2. Run the complete verification matrix in [03-verification-and-documentation.md](03-verification-and-documentation.md).
3. Inspect `git diff`, `git diff --check`, and `git status --short` for unrelated drift.
4. Update and close `eino-agent-9f7` only after every acceptance criterion passes.
5. Commit only the plan, dependency, code, test, and documentation files for this task.
6. Run `git pull --rebase`.
7. Run `bd dolt push`.
8. Run `git push`.
9. Verify `git status --short --branch` reports the branch up to date with origin.

Prerequisites: WP1 through WP6. Parallelization: none for final gates and publication.

## Integration and regression gates

- No active code calls `tools.Registry` to install standard `eino-tools` definitions.
- No compatibility alias for `RegisterDefaults` remains.
- Catalog and host identity inputs both reach `session.ToolPlanIdentity`.
- Strict resume rejects catalog identity drift.
- Provider-visible tool order matches catalog order, and order-only drift rejects resume.
- One mount failure publishes zero standard tools.
- Same-root and shared-static non-concurrent calls serialize across separate mounts through the adapter's process-wide locker; idle keys are reclaimed.
- Path, command, URL, and tracker operation patterns reach policy and durable calls from the final normalized input.
- Admission persists a canonical workspace root, resume cannot follow a retargeted input symlink, and duplicate top-level tool keys never persist or execute.
- Scope filtering, mount lease lifetime, enabled/disabled tool selection, and existing AG-UI/Wasm/native registrations remain green.
- The exact completed dependency commit is recorded in code and current documentation.

## Final definition of done

1. Every success criterion in [00-overview.md](00-overview.md) has automated or documentation evidence.
2. Two independent plan reviews and one fresh adversarial review completed, and accepted findings are incorporated before implementation.
3. Focused tests, race tests, module tidiness, formatting, lint, WIT regeneration checks, and `make check` pass.
4. The obsolete helper and its dead path are deleted.
5. No follow-up issue is needed for work inside the requested scope. Any newly discovered out-of-scope defect has a Beads issue.
6. The issue is closed, the commit is pushed, and the worktree is clean and synchronized with origin.

## Deferred work

- None currently identified inside this request.
