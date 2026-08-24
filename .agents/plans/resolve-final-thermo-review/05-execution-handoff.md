# Execution Handoff

## Dependency-ordered work packages

1. **Composition atomicity (`eino-agent-o85`).** Change `composition.Registry.Mount`, `mountToolDefinition`, and focused tests. This is independent and establishes the frozen-definition publication pattern.
2. **Store-owned transactions (`eino-agent-5w8`).** Change `session.Store`, SQLite, store contracts, runtime admission, fakes, and examples. This must precede orchestrator migration.
3. **Wasm lifetime (`eino-agent-dqs`).** Change `wasmext.module` and `wasmext.Loader` lifecycle plus adversarial timeout/close tests.
4. **Wasm public API (`eino-agent-0zm`).** Delete free resource-losing loaders, expose the closeable permissions handle, and update references. This follows the lifecycle change.
5. **Orchestrator construction (`eino-agent-88n`).** Privatize state, centralize defaults/validation, delete alternate admission/transactor injection, and migrate all call sites.
6. **Dead semantics/docs (`eino-agent-17d`).** Simplify tool selection, strengthen collision coverage, and remove stale current-schema wording.
7. **Integration verification.** Run focused/race/full gates, inspect the diff, and close all six issues only after acceptance passes.

Packages 1 and 3 are logically independent, but one implementer should keep the working tree sequential and run focused tests after each. Packages 2 and 5 must remain ordered. Packages 3 and 4 must remain ordered.

## Per-package completion checks

- Each package has focused behavioral tests and a structural `rg` search for the removed path.
- Compilation failures from breaking interfaces are resolved by migrating callers, not by restoring compatibility aliases.
- No package changes persisted schema or introduces a feature flag.
- Any newly discovered follow-up outside these six findings becomes a Beads issue before session completion.

## Integration sequence

1. Run `gofmt` on changed Go files and `git diff --check`.
2. Run focused non-race tests, then focused race tests.
3. Run `make check`.
4. Inspect `git diff --stat`, `git diff`, and `git status --short` for scope and unrelated changes.
5. Update/close the six Beads issues and create issues for legitimate deferred work.
6. Commit only the plan and implementation changes with a descriptive message.
7. Run `git pull --rebase`, `bd dolt push`, and `git push`.
8. Verify `git status` reports a clean branch up to date with origin.

## Definition of done

- All six issue acceptance criteria pass.
- Both independent plan reviews have been reconciled into the plan before implementation.
- All focused, race, and repository-wide quality gates pass.
- The six Beads issues are closed with completion context.
- The implementation and plan are committed and pushed to the current remote branch.
- The final working tree is clean and up to date with origin.

## Deferred work

No deferred work is planned. File a new Beads issue if implementation uncovers a distinct architectural problem that cannot be resolved safely within these boundaries.
