# Execution Handoff

## Dependency-ordered work packages

1. **Declare point authority.** Change point and registry construction APIs, add the runtime catalog, update composition construction, and repair compile errors in tests.
2. **Make assistant tool turns atomic.** Extend creation envelopes, restrict lifecycle-owned writes, persist complete turns transactionally, terminalize skipped calls in fresh and resume paths, and enforce zero unfinished calls during run settlement.
3. **Make notification delivery nonblocking.** Add the plan-owned dispatcher with per-task leases and the one run-owned infrastructure dispatcher, then rerun atomic event-order tests against the final asynchronous contract.
4. **Simplify replay pagination.** Delete `AfterPartID`, bind cursors to sessions, and use a page-bounded parts query that excludes lookahead.
5. **Integrate and verify.** Run focused tests after each package, then the repository gates.

Point-catalog work precedes dispatcher work because both change `extension.Plan` and registry tests. Atomic persistence lands before dispatch so its post-commit boundary is explicit; its event assertions run again after asynchronous dispatch lands. Replay pagination can be developed independently but lands before the final gate.

## Verification by package

### Point authority

```bash
go test ./extension ./composition ./runtime ./wasmext
go test -race ./extension ./composition
```

Acceptance: no mount order can select point semantics, copied canonical handles dispatch, and unknown handles fail before publication.

### Notification dispatch

```bash
go test ./extension ./runtime
go test -race ./extension ./runtime
```

Acceptance: blocked observers, reporters, and infrastructure sinks do not block admission, run execution, settlement, plan release, or `Handle.Done`; only the callback's own mount lease remains retained.

### Atomic tool turns

```bash
go test ./session ./store/sqlite ./runtime
go test -race ./store/sqlite ./runtime
```

Acceptance: each committed tool request has its call, request part, and event; lifecycle-owned parts cannot bypass transition methods; the store refuses terminal run settlement with unfinished calls; transaction rollback leaves none of the turn records.

### Replay pagination

```bash
go test ./session/history ./store/sqlite ./store/storetest
```

Acceptance: page contents and ordering are stable, cross-session cursors fail, off-page malformed parts are not decoded, and dead cursor state is gone.

## Integration gates

Run from the repository root:

```bash
make fmt
make check
git diff --check
```

If design-scope exclusions make a full gate fail only because `examples/` uses a deleted API, apply the smallest compile-only update and rerun the full gate. Do not review or refactor example behavior.

## Issue, commit, and push protocol

1. Record any genuinely deferred work as Beads issues.
2. Close `eino-agent-igd` only after every acceptance criterion passes.
3. Review the final diff for unrelated changes and secret-bearing files.
4. Commit the related implementation and plan with a message that explains the removed invalid states.
5. Run push preflight.
6. Run `git pull --rebase`, `bd dolt push`, and `git push` as separate checked commands.
7. Verify `git status -sb` reports the branch up to date with its upstream.

## Definition of done

- All four review findings are structurally removed.
- No compatibility alias, migration, flag, or parallel implementation remains.
- Every focused and repository-wide gate passes.
- `eino-agent-igd` is closed with the implemented outcome.
- The implementation and plan are committed and pushed.
- The worktree is clean and synchronized with `origin/feat/deeper-extensibility`.

## Deferred work

None planned. File a Beads issue if implementation exposes a separate defect that cannot be fixed without expanding this request.
