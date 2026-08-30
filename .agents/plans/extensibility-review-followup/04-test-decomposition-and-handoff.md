# Test Decomposition and Execution Handoff

## Test decomposition

Split existing `runtime/extensions_test.go` after functional changes stabilize. Implemented files under existing `runtime/`:

- `extension_prompt_guard_test.go`: prompt materialization and mounted guard behavior.
- `extensions_test.go`: tool middleware, protected execution views, and stream delegation safety.
- `extension_plan_lifecycle_test.go`: plan catalog/acquisition, resume, release, and settled-notice lifecycle.

Move each test without changing assertions except where the new grouped `RunPlanSpec` requires direct construction updates. Keep helpers in the narrowest file that uses them; put a helper in an existing shared runtime test support file only when at least two focused suites require it. Delete `runtime/extensions_test.go` when empty.

Acceptance:

- each file has one coherent production concern;
- no runtime test file is 1,000 lines or more;
- `rg -n '^func Test' runtime/extension_*_test.go` makes ownership obvious from names;
- test count and behavior remain present after moves.

## Dependency-ordered work packages

### 1. Canonical point identity

Change `extension/types.go`, `extension/registry.go`, `extension/dispatch.go`, `session/extensions.go`, and direct tests.

Gate:

```text
go test -race ./extension ./session
```

### 2. Component-owned run plans

Change `extension` typed identities, `composition/registry.go`, `runtime/extension_plan.go`, and affected composition/runtime/session tests.

Prerequisite: work package 1 supplies the shared typed handler kind and identities.

Gate:

```text
go test -race ./extension ./composition ./runtime ./session
```

### 3. Atomic tool transitions

Change `session.ExecutionStore`, SQLite, store contracts, runtime transitions, and every fake implementation.

This package is logically independent but touches runtime tests, so apply it after grouped plan construction to reduce simultaneous compile failures.

Gate:

```text
go test -race ./session ./store/storetest ./store/sqlite ./runtime
```

### 4. Around lifecycle enforcement

Change `InvokeAround`, add concurrency-focused tests, and verify downstream tool execution behavior.

Gate:

```text
go test -race ./extension ./runtime
```

### 5. Test split and repository integration

Move the mixed runtime tests, run formatting, inspect the diff for accidental behavior changes, and execute the complete gate.

```text
make check
git diff --check
```

## Integration and regression gates

- Fresh and resume extension descriptors retain identical ownership and fingerprints for semantically equivalent plans.
- Mount close still waits on plan leases and callback self-close detection still applies.
- Tool transition persistence remains fenced, atomic, idempotent, and published only after commit.
- Around-handler errors remain bounded and delegated terminal failures remain distinguishable.
- All examples and Wasm/native integration tests compile with the direct greenfield APIs.

## Definition of done

- All five review findings meet their acceptance criteria.
- Two independent subagents reviewed the plan and every accepted finding was applied before implementation.
- No compatibility aliases, adapters, dual paths, migrations, or feature flags were added.
- `make check` and `git diff --check` pass.
- `eino-agent-clh` is closed.
- Changes and plan documents are committed.
- `git add -f .agents/plans/extensibility-review-followup` is used, and `git ls-files --error-unmatch` succeeds for all five expected plan files before commit.
- `git pull --rebase`, `bd dolt push`, and `git push` succeed.
- Final `git status` is clean and up to date with origin.

## Deferred work

None. Record newly discovered out-of-scope work in Beads before closing this session.
