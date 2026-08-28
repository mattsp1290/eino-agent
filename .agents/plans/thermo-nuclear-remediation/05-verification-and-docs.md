# Verification and Documentation

## Goal

Prove the redesigned boundaries through public behavior, generated artifacts, race coverage, and updated architecture documentation.

## Integration test matrix

| Area | Required observable proof |
| --- | --- |
| Semantic points | ordering, clone isolation, contained notifications, fail-fast hooks, gate rejection, transform waterfalls, required around delegation |
| Run plans | one component identity, deterministic fingerprint, strict fresh/resume parity, foreign-session rejection, no leaked plan lease |
| Tool durability | atomic state/event create; atomic run-lease/tool-lease/event claim; atomic call/result-envelope/event settlement; complete rollback on event failure; idempotent replay; stale-fence rejection |
| Runtime publication | committed durable event precedes best-effort external publication; external failure does not undo or fail the transition |
| Wasm lifecycle | retained run hooks execute exactly once; removed turn hooks are absent from source, bindings, fake engines, and fixtures |
| Dead surfaces | no inert guest config or ignored scope parameter remains |

Tests must exercise SQLite and the reusable store contract, not only runtime mocks.

## Documentation surface

Update existing documents where the changed contracts appear:

- `docs/architecture/extension-points.md`: semantic point modes, handler kinds, failure behavior, and exact pipelines.
- `docs/architecture/extensibility.md`: native and Wasm capability matrix.
- `docs/architecture/runtime.md`: component-owned plan acquisition and hook lifecycle.
- `docs/architecture/storage.md`: atomic tool transition plus event writes.
- `docs/architecture/agui-events.md`: durable versus live publication ordering.
- `docs/architecture/tools.md` and `docs/consumer-guide.md`: tool store signatures and transform authoring examples.
- `examples/wasm-extensions/README.md` and `wit/README.md`: retained WIT worlds and regeneration commands.

Do not document old names, descriptor JSON, or migration steps.

## Verification sequence

Run focused gates after each work package, then the complete repository gate:

```text
go test ./extension ./runtime ./composition
go test ./session ./store/sqlite ./store/storetest
go test ./wasmext
go -C examples/wasm-extensions test ./...
go test -race ./extension ./runtime ./composition ./store/sqlite
make wasm-fixtures
make check
git diff --check
```

Inspect the final diff for generated-code drift, accidental compatibility wrappers, and files crossing the 1,000-line threshold.

## Acceptance criteria

- Every command above passes.
- `git status` contains only intended plan, source, generated, fixture, test, and documentation changes before commit.
- All five Beads issues meet their acceptance criteria and close only after the complete gate passes.
- Any genuinely deferred discovery becomes a new Beads issue before push.
