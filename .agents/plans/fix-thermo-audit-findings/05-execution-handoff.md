# Execution Handoff

## Delivery order

1. Implement canonical resolved-model validation and remove fallback reconstruction (`eino-agent-9rk`).
2. Consolidate extension identity types, validators, and duplicate keys (`eino-agent-e76`).
3. Simplify notifications to contained reporting (`eino-agent-30b`).
4. Replace Wasm opaque handles with direct loader registration (`eino-agent-7hv`).
5. Split giant tests by behavioral seam (`eino-agent-7o1`).
6. Update current docs, run full verification, close issues, commit, and push.

Notification simplification precedes the Wasm API rewrite so registration changes build on the final registrar/observer contract. Test decomposition is last so semantic edits remain easy to review.

## Per-slice gate

- Run focused tests for touched packages.
- Check the diff for compatibility aliases, duplicated validators, or unrelated edits.
- Update the corresponding Beads issue notes/status when needed.

## Final gate

```bash
make check
git diff --check
git status --short --branch
bd lint
```

Run the repository preflight checks before remote operations: confirm repository root, `origin`, current branch, GitHub authentication/scopes, and push permission.

## Commit and push

Use a selective commit that includes the implementation, tests, maintained docs, plan, and Beads metadata while excluding unrelated drift. Suggested commit subject:

```text
refactor: eliminate residual extension identity debt
```

Then complete the repository-mandated sequence, resolving any rebase conflicts or test regressions before retrying:

```bash
git pull --rebase
bd dolt push
git push
git status --short --branch
```

Record the actual commit SHA and verify the branch is up to date with `origin`.

## Definition of done

- Two independent plan reviewers and one fresh adversarial reviewer have reported findings.
- Accepted findings are reflected in this plan; rejected findings have a documented rationale in the session record.
- All success criteria in `00-overview.md` are satisfied.
- Focused tests, `make check`, and diff/structural checks pass.
- All five Beads issues are closed; any genuinely discovered follow-up is filed in Beads.
- Changes and Beads data are committed/pushed as applicable.
- The worktree is clean and the branch is synchronized with `origin`.

## Deferred work

None planned. Newly discovered work should be fixed in scope when safe; otherwise file a Beads issue before ending the session.

## Verification record

- Two independent reviews and one fresh adversarial review completed; all actionable findings were accepted into the plan before implementation.
- Focused package tests and the full `make check` gate, including the repository-wide race suite, passed on 2026-08-27.
- Structural searches found no live fallback helpers, duplicate session identity types, notification policy/result API, opaque exported Wasm loaded handles, or old two-step Wasm load methods.
- All decomposed composition and orchestrator test files are below 1,000 lines.
