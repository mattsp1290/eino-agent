# Execution Handoff

## Dependency-ordered work packages

### 1. Remove duplicate handler ownership

Change `extension/registry.go`, `composition/registry.go`, `runtime/extension_plan.go`, and direct tests.

Gate:

```text
go test -race ./extension ./composition ./runtime ./session
```

Acceptance: dispatch is the only handler authority, handler-only components remain durable, combined components merge once, and failed plan construction releases its lease.

### 2. Simplify event publication

Change `runtime/event_sink.go`, `runtime/extension_execution.go`, `runtime/admission.go`, `runtime/orchestrator.go`, `runtime/interrupt.go`, `session/types.go`, SQLite and fake store implementations, `stream/tail.go`, their focused tests, event-kind adapters/tests/docs, and focused architecture documentation.

Gate:

```text
go test -race ./runtime ./store/sqlite
```

Acceptance: event fanout performs no inferred persistence; admission, settlement, and tool transition tests prove one atomic durable record followed by publication of the store-returned canonical record; admission event publication still precedes `RunAdmittedPoint`; every remaining non-live event kind has an identified persistence owner; tail overflow is explicitly live-only.

### 3. Delete point reflection identity

Change `extension/types.go` and focused identity tests.

Gate:

```text
go test -race ./extension
```

Acceptance: exact point-definition identity behavior is unchanged and no reflection signature remains.

### 4. Change and regenerate the Wasm contract

Change `wit/eino-agent-extensions.wit`, `wasmext/engine.go`, `wasmext/wrappers.go`, `wasmext/wasmtime_worlds.go`, Wasm examples, fake components, tests, generated bindings, fixtures, and focused documentation.

Commands:

```text
make wit
make wasm-fixtures
go test -race ./wasmext ./runtime
```

Acceptance: tool permission patterns come from the required guest export using final normalized input; context-source bindings expose only system/user roles; all rebuilt fixtures validate.

Boundary acceptance: 4,096 bytes succeeds, 4,097 bytes is `ErrorSize`, tighter configured output limits win, failure precedes permission evaluation and persistence, and resume reuses the persisted pattern without another guest resolver call.

### 5. Repository integration

Run formatting and the complete repository gates:

```text
make fmt
make check
make wasm-fixtures
git diff --check
```

Re-run `git status --short` after fixture generation. Any generated diff must be intentional and included.

## Parallelization constraints

- Work packages 1, 2, and 3 are logically independent but touch shared extension/runtime tests; apply them sequentially to keep compile failures attributable.
- Work package 4 changes shared generated types and must regenerate before its Go callers can compile.
- Do not run `make wit` concurrently with Go edits under `wasmext/gen`.
- Full integration begins only after all focused gates pass.

## Integration and regression gates

- Fresh and resume acquisition produce the same descriptor fingerprint for equivalent live plans.
- Handler-only mounts still block `Close` until the frozen dispatch plan releases.
- Event sink failure cannot roll back admission, run settlement, or tool transitions.
- Tool pending/running/terminal events remain unique and atomically committed with state.
- Permission-pattern derivation sees middleware-transformed input and executes before permission policy evaluation.
- Wasm module timeout, close, memory, input, and output bounds still apply to the new operation.
- The native context contribution validator still rejects assistant and rich-message injection even though Wasm can no longer represent assistant context.
- No production file crosses 1,000 lines.

## Commit and push protocol

1. Run the `preflight --push` checks before the first push attempt.
2. Close `eino-agent-llr` only after all gates pass.
3. Stage only implementation, generated, fixture, test, documentation, and Beads changes belonging to this task; use `git add -f .agents/plans/thermo-review-final-remediation/` for the reviewed plan because `/.agents/plans/` is ignored.
4. Commit with a message describing structural simplification.
5. Run `git pull --rebase`.
6. Run `bd dolt push`.
7. Run `git push`.
8. File Beads issues for any remaining work discovered during implementation.
9. Inspect `git stash list`; remove only stashes created by this task (none are expected), never user-owned stashes.
10. Run `git fetch --prune` to prune stale remote-tracking state.
11. Verify `git status --short --branch` reports a clean branch up to date with its upstream.

## Definition of done

- Two independent plan reviews and one adversarial review completed before implementation.
- The calling agent incorporated every accepted material finding into the plan.
- All five original thermo-nuclear findings meet their acceptance criteria.
- Direct API/WIT changes contain no compatibility code.
- Focused tests, `make check`, `make wasm-fixtures`, and `git diff --check` pass.
- `eino-agent-llr` is closed.
- All task changes are committed and pushed to `origin/feat/deeper-extensibility`.
- Beads data is pushed and the final worktree is clean.

## Deferred work

None. Record any newly discovered out-of-scope work in Beads before closing the session.
