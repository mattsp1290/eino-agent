# Execution Handoff

## Dependency-ordered work packages

1. **Protected extension boundaries and explicit run state**
   - Change: proposed `runtime/extension_execution.go`, `extension_model.go`, `extension_tool.go`; `runtime/orchestrator.go`, `interrupt.go`, `admission.go`, `ledger.go`; focused tests.
   - Result: no unsafe interface inspection and no context-hidden plan.
   - Gate: focused runtime tests plus the structural `rg` checks in `01-extension-boundaries.md`.

2. **First-class scoped leases**
   - Change: `extension/types.go`, `extension/registry.go`, `composition/registry.go`, and their tests.
   - Result: applicable capabilities retain mounts without fake notifications; unrelated sessions do not.
   - Parallelization: can be implemented after work package 1 compiles and before or alongside package 3, but edits to shared tests should remain serialized.
   - Gate: `go test -race ./extension ./composition`.

3. **Canonical tool execution and settlement**
   - Change: proposed `runtime/tool_execution.go`, `runtime/tool_settlement.go`; `runtime/orchestrator.go`, `interrupt.go`, `types.go`; `tools/output.go`; runtime/tools/store tests.
   - Result: one post-claim state machine, one lower-level envelope builder, and explicit adapters for executed outcomes and resumed-running interruption.
   - Prerequisite: work package 1's explicit execution state.
   - Gate: focused runtime/tools/session/SQLite tests and fresh/resume equivalence coverage.

4. **Responsibility-based decomposition and documentation**
   - Change: delete `runtime/extensions.go`; create the focused runtime files listed in `04-decomposition-and-verification.md`; update architecture docs.
   - Result: every modified production file, including `runtime/orchestrator.go`, remains below the non-waivable 1,000-line gate and ownership is legible.
   - Prerequisite: behavior changes from packages 1-3 are stable.
   - Gate: focused tests, file-size inspection, dependency tests, and `git diff --check`.

5. **Repository verification and delivery**
   - Run `make fmt`, then `make check`.
   - Review the complete diff for scope, generated drift, debug code, and unrelated changes.
   - Close `eino-agent-8fd` only after every acceptance criterion passes.
   - Commit the plan, implementation, tests, and documentation with a focused message.
   - Run the repository preflight workflow, `git pull --rebase`, `bd dolt push`, and `git push`.
   - Inspect `git stash list` and preserve unrelated user stashes; resolve only task-owned stale state.
   - Run `git remote prune origin` after the successful push.
   - Verify `git status` reports a clean branch up to date with `origin/feat/deeper-extensibility`.

## Stop/go gates

- Stop before implementation if either independent reviewer finds a durable schema change, documented callable invocation contract, or output-exposure change not covered by this plan.
- Stop before settlement integration if the canonical builder cannot preserve claim fencing, reserved IDs, one completion timestamp, and current restrictive model output.
- Stop before push if `make check` changes generated files or any quality gate fails.
- Go when both requested reviews complete, accepted findings are incorporated, and the plan remains internally consistent.

## Definition of done

- All success criteria in `00-overview.md` are observed.
- Both independent plan reviews complete with valid findings or `No material findings`.
- Accepted review findings are incorporated before implementation.
- Focused tests and `make check` pass.
- No remaining in-scope work requires a follow-up Beads issue.
- `eino-agent-8fd` is closed and Beads data is pushed.
- Git changes are committed and pushed; the worktree is clean and up to date with origin.

## Deferred work

- Removing legacy extension construction fields is a separate product/API migration.
- Parallel tool execution and concurrency-key scheduling remain separate work.
- Any expansion of model-visible tool metadata or attachments requires a separate privacy review.
