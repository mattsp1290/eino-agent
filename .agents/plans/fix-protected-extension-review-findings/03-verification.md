# Verification Strategy

## Regression matrix

| Finding | Test layer | Required assertion |
|---|---|---|
| Permission metadata after transform | Runtime start and strict-resume orchestration/extension dispatch | Fresh transformed result retains protected permission status; actual atomic settlement, model payload, and notification use expected-failure status in both paths |
| Resume fingerprint recomputation | Runtime unit | Copied stale fingerprint cannot admit changed descriptor; release runs |
| Atomic settlement builder | Tools unit plus SQLite integration | Missing reservations fail early; reserved records are complete and `SettleToolCall` accepts them |
| Tool schema clone | Runtime unit | Schema survives cloning independently; replacement is rejected; malformed/nil schema wrappers fail closed without panic |
| Aliased slice shape | Model unit | Full/prefix views retain distinct deterministic lengths and contents |

## Focused commands

Run after each relevant work package:

```bash
go test ./runtime
go test ./tools ./store/sqlite
go test ./model
```

Run the changed-package race gate:

```bash
go test -race ./runtime ./tools ./model ./store/sqlite
```

## Repository gate

Run the documented complete gate:

```bash
make check
```

If formatting changes files, use `make fmt`, inspect the resulting diff, and rerun focused tests plus `make check`.

## Acceptance review

- Inspect `git diff --check` for whitespace defects.
- Inspect the complete diff for unrelated worktree changes.
- Confirm all five review scenarios fail on the old behavior and pass with the implementation, either from newly added tests or direct pre-fix evidence.
- Confirm the only public API change is the accepted `runtime.ToolCall` reserved-ID field addition; document that downstream unkeyed literals must migrate. Confirm no durable schema, migration, dependency, or generated-file change occurred.
