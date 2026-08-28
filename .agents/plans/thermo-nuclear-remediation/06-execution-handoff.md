# Execution Handoff

## Dependency-ordered work packages

Implementation may begin only after the two independent reviews, the adversarial review, and the accepted-finding disposition in [07-review-disposition.md](07-review-disposition.md) are complete.

### 1. Semantic extension points

Implement [01-semantic-extension-points.md](01-semantic-extension-points.md).

Primary files: `extension/types.go`, `extension/dispatch.go`, `extension/registry.go`, `runtime/extension_lifecycle.go`, runtime invocation files, `wasmext/points.go`, and native examples.

Verification:

```text
go test ./extension ./runtime ./composition ./wasmext ./examples/native-extension
go test -race ./extension ./runtime
```

Gate: no transform or hook callback uses `extension.Next`; true around points still prove exactly-once delegation.

### 2. Component-owned run plans

Implement [02-component-owned-run-plans.md](02-component-owned-run-plans.md).

Primary files: `session/extensions.go`, `runtime/extension_plan.go`, `composition/registry.go`, and strict plan/resume tests.

Prerequisite: work package 1 fixes semantic handler kinds recorded by diagnostics.

Verification:

```text
go test ./session ./runtime ./composition
go test -race ./runtime ./composition
```

Gate: descriptor fingerprint and resume mismatch tests cover every nested capability kind.

### 3. Atomic tool transitions and events

Implement [03-atomic-tool-events.md](03-atomic-tool-events.md).

Primary files: `session/types.go`, SQLite execution/tool files, store contract tests, runtime tool execution/preparation/resume, and event publishing helpers.

This package is conceptually independent but follows work packages 1 and 2 in one working tree to avoid conflicting runtime test edits.

Verification:

```text
go test ./session ./store/sqlite ./runtime
go test -race ./store/sqlite ./runtime
```

Gate: injected event-write failures leave no state transition, while infrastructure-sink failures occur after commit.

### 4. Wasm and dead-surface reduction

Implement [04-wasm-and-dead-surfaces.md](04-wasm-and-dead-surfaces.md).

Primary files: source WIT, generated bindings, Wasm engine/wrappers/points, guest fixture source/binary, `wasmext.ModuleConfig`, and `tools/session`.

Prerequisite: work package 1 provides the retained semantic run-hook registration API.

Verification:

```text
make wit
go test ./wasmext ./tools/session
make wasm-fixtures
```

Gate: regenerated bindings and fixtures contain only the retained run-hook operations.

### 5. Integration, documentation, and final gates

Implement [05-verification-and-docs.md](05-verification-and-docs.md).

Verification:

```text
make fmt
make check
git diff --check
```

## Parallelization constraints

- Initial plan reviews may run concurrently; implementation work packages run sequentially in the shared worktree.
- Work package 2 depends on handler-kind decisions from work package 1.
- Work package 4 depends on the retained hook registration API from work package 1.
- Work package 3 is logically independent but overlaps runtime and fake-store tests, so integrate it after work package 2.
- Regenerate WIT and Wasm fixtures only after source interfaces settle.

## Final definition of done

- Two independent reviews and one adversarial review completed before implementation, and accepted findings were incorporated.
- All five tracked Beads issues are closed with acceptance evidence.
- No compatibility layer, old descriptor decoder, old WIT version, ignored parameter, or inert configuration field remains.
- `make check`, `make wasm-fixtures`, and `git diff --check` pass.
- The plan and implementation are committed in a focused commit or small ordered series.
- The preflight skill confirms remote/auth prerequisites.
- `git pull --rebase`, `bd dolt push`, and `git push` succeed.
- Final `git status` reports a clean branch up to date with origin.

Implementation status: work packages 1-5 and all local verification gates are complete. Remote preflight, commit, Beads closure/push, Git push, and final clean-status verification remain part of the session close sequence.

## Deferred work

- A real per-turn lifecycle contract is deferred. Create a new Beads issue only if a concrete consumer requires it.
- Guest configuration is deferred until a WIT import and end-to-end guest behavior are specified.
