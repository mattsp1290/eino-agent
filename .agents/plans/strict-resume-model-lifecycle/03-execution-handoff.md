# Execution Handoff

## Dependency-ordered work packages

1. Implement [Work Package 1](01-strict-tool-fingerprint.md) in `composition/registry.go` and `composition/registry_test.go`.
2. Implement [Work Package 2](02-model-lifecycle-pairing.md) in `runtime/orchestrator.go` and the nearest runtime lifecycle tests.
3. Run targeted tests, formatting, and full repository gates.
4. Close Beads issue `eino-agent-791`, commit only related files, and complete the repository close protocol in the exact order below.

Work Packages 1 and 2 have no code dependency and may be developed in either order. Their tests must both pass before integration gates.

## Work Package 1 result

- Changed symbol: existing `composition.toolSchemaHash`.
- Test surface: existing `composition/registry_test.go`.
- Verification: `go test ./composition`.
- Acceptance: separate mutations to `Retention.MaxInlineBytes`, `Retention.StoreExternal`, `Retention.Redact`, `Concurrency`, and `Metadata` cause strict resume mismatch; equivalent metadata map order remains stable.

## Work Package 2 result

- Changed symbol: existing `(*runtime.StreamingOrchestrator).streamModel`.
- Test surface: existing `runtime/ledger_test.go` or another existing runtime test file with the same orchestration helpers.
- Verification: `go test ./runtime`.
- Acceptance: unsafe audit rejection and a forced `ModelRequestDispatchStarted` transition failure both emit neither lifecycle event with a live dispatch installed; completion never occurs without request; every notified/dispatched attempt emits exactly one completion unless final ledger transition fails, in which case the expected pair is one request and zero completions.

## Integration and regression gates

Run focused gates first:

```bash
gofmt -w composition/registry.go composition/registry_test.go runtime/orchestrator.go runtime/ledger_test.go
go test ./composition ./runtime
```

Adjust the `gofmt` file list if the runtime test lands in a different existing file. Then run the repository gate:

```bash
make check
```

If `make check` fails because a documented external tool is unavailable, run every available component target and record the exact missing dependency. Do not treat a targeted-test pass as equivalent to `make check`.

After tests pass, close and publish in this order:

```bash
bd close eino-agent-791
git add <related-plan-code-and-test-files>
git commit -m "fix: tighten extension resume and lifecycle events"
git pull --rebase
bd dolt push
git push
git status
git stash list
git remote prune origin
git status
```

Do not remove a user stash. Inspect `git stash list` and report any entries. The final `git status` must show a clean tree and an upstream-synchronized branch.

## Final definition of done

- Both review findings have regression tests that fail against the pre-change behavior.
- Strict tool fingerprints cover all deterministic serialized definition policy fields.
- Model request and completion extension notifications are paired by attempt.
- Lifecycle regressions use a non-nil dispatch and cover dispatch-start transition failure.
- No public API or durable descriptor schema changed.
- Formatting and repository quality gates pass.
- Two independent plan reviews and one adversarial plan review complete, and accepted findings are incorporated before implementation.
- Beads issue `eino-agent-791` is closed.
- Related plan, code, and test changes are committed and pushed.
- `git status` reports a clean branch up to date with origin.
- Stashes were inspected without removing user data, and stale remote-tracking branches were pruned.

## Deferred work

No follow-up work is expected. Create a Beads issue before handoff if implementation reveals an out-of-scope durable-schema, callback-identity, or event-contract problem.
