# Verification and Documentation Gates

## Goal

Prove the six findings are removed structurally and behaviorally, then align public guidance with the breaking current-only API.

## Focused verification matrix

| Invariant | Behavioral gate | Structural gate |
|---|---|---|
| Mount publication is atomic | failed mount leaves no state; later mount succeeds | no fallible return after `CommitMount` |
| Admission is atomic | injected failures roll back all durable rows/events | no `Transactor`, optional transaction, or direct `admitDurable` path |
| Wasm lifetime is safe | stubborn worker blocks destruction but not caller return | worker owns `Done` and gate release |
| Load ownership is visible | close/idempotence suites pass | no resource-losing free `Load*` functions |
| Orchestrator is sealed | constructor/default/error tests pass | no public dependency fields, `Admit`, or `WithTransactor` |
| Tool semantics are coherent | collisions rejected; prompts still shadow | no tool overwrite map or stale version/migration wording |

## Required commands

Run focused package tests after each package, then run:

```bash
gofmt -w <changed-go-files>
go test ./composition ./extension ./session ./store/... ./runtime ./wasmext
go test -race ./composition ./store/... ./runtime ./wasmext
git diff --check
make check
```

Use `rg` searches tailored to the final symbol names. At minimum prove deletion of:

```text
session.Transactor
session.Tx
WithTransactor
Admitter.Transactor
wasmext.LoadTool (free function)
wasmext.LoadPermissionsPolicy (free function)
wasmext.LoadEventSink (free function)
tool shadowing claims
migration 002 current-schema claims
descriptor Version 2 current comments
```

Search for direct `StreamingOrchestrator` literals and exported field mutation separately because Go symbol searches cannot distinguish free functions from methods without context.
Search every embedded `session.Store` declaration and verify that admission-reachable decorators forward `WithinTx` safely rather than inheriting a nil embedded interface method.

## Documentation surface

- Update architecture, integration, prompt, and design documents that describe store requirements, runtime construction, extension tool collisions, current descriptor version, or Wasm loader ownership.
- Update examples to use only constructor/options and ownership-visible Wasm loading.
- Do not add compatibility/migration sections for APIs that never had users.

## Review and regression gate

- Inspect the final diff for unrelated user changes before staging.
- Run the thermo-nuclear rubric once more against the changed boundaries.
- Treat any race, goroutine leak, unlocked return, optional admission write path, or hidden Wasm owner as release-blocking.
- Treat documentation wording that promises deleted behavior as release-blocking.
