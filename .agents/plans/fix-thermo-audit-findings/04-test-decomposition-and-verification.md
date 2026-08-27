# Test Decomposition and Verification

Issue: `eino-agent-7o1`

## Objective

Split the two giant test files along behavioral seams after production changes are stable. Keep test names and coverage intact while making future changes locally understandable.

## Composition test layout

Current source: `composition/registry_test.go` (about 1,157 lines).

- `composition/registry_mount_test.go`: install, commit, rollback, deactivate, and mount lifecycle behavior.
- `composition/registry_scope_test.go`: scope validation, conflicts, and capability restrictions.
- `composition/registry_identity_test.go`: canonical identity validation, structured duplicate detection, and fingerprint behavior.
- `composition/registry_resume_test.go`: durable/resume behavior and related fixtures.
- Keep small registry smoke tests in `registry_test.go`, or delete the original if every test has a clearer home.
- Keep shared fixtures only when used across multiple files; otherwise colocate them with their tests.

## Runtime test layout

Current source: `runtime/orchestrator_test.go` (about 1,553 lines).

- `runtime/orchestrator_fresh_test.go`: admission and fresh-execution lifecycle behavior.
- `runtime/orchestrator_provider_test.go`: model resolution, stream handling, observer events, and provider terminal outcomes.
- `runtime/orchestrator_tool_test.go`: tool dispatch, middleware, approvals, and tool-result behavior.
- `runtime/orchestrator_resume_test.go`: interruption, unfinished-tool settlement, snapshots, and resume behavior.
- `runtime/orchestrator_test_support_test.go`: fixtures genuinely shared by multiple behavioral files.
- Keep small orchestrator smoke tests in `orchestrator_test.go`, or delete the original if no cohesive smoke group remains.

No resulting test file should exceed 1,000 lines. Prefer substantially smaller files where natural seams permit it; do not create an indiscriminate helper dump.

## Documentation verification

Review and update current documentation affected by behavior changes:

- `docs/architecture/context.md`
- `docs/architecture/providers.md`
- `docs/architecture/runtime.md`
- `docs/architecture/extension-points.md`
- `docs/architecture/extensibility.md`
- `docs/consumer-guide.md`
- `examples/wasm-extensions/README.md`

Historical files under `docs/prompts/` and completed plan directories are evidence, not maintained API documentation, and should not be rewritten.

## Verification sequence

1. Run focused tests after each semantic slice.
2. After moving tests, run all affected packages to prove moves did not change behavior.
3. Run `make check`.
4. Run `git diff --check`.
5. Verify the schema-v1 descriptor JSON and fingerprint golden remain exact; otherwise intentionally bump the schema.
6. Verify oversized-test thresholds with `wc -l`.
7. Search for deleted compatibility/fallback symbols and stale public APIs.
8. Inspect the final diff for accidental edits to historical material or unrelated user work.

Suggested structural searches:

```bash
rg 'admissionProviderID|admissionModelID|snapshotModelIdentity' .
rg 'ArtifactIdentity|ExtensionScope' --glob '*.go' .
rg 'Notification(ReturnFailures|Contained|Policy)|Failures' --glob '*.go' .
rg 'Loaded(ContextSource|Hook|ToolMiddleware)' .
wc -l composition/*_test.go runtime/*_test.go
```

Search results in this plan directory or historical docs do not represent live compatibility code; evaluate results by location.
