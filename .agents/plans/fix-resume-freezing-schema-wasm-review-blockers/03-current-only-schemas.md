# Current-Only Schemas

## Goal and prerequisite

Represent the only supported SQLite and extension descriptor formats as version `1`, with no code, data, fixtures, or duplicate files for undeployed predecessors.

## Existing evidence

- `store/sqlite/store.go` embeds `migrations/001_sqlite_store.sql` and `migrations/002_model_requests.sql` and upgrades earlier databases.
- Root `migrations/` duplicates the embedded store migrations but is not the runtime embed source.
- `session.ExtensionPlanSchemaVersion` is `2`; runtime rejects version `1`, so retaining the numbering supplies no compatibility.
- Tests assert upgrade and obsolete-version behavior that the confirmed operating context does not need.

## Change surface

- `store/sqlite/migrations/001_sqlite_store.sql`: become the complete current bootstrap schema, including model requests and the run fencing columns from [01-fenced-run-ownership.md](01-fenced-run-ownership.md).
- `store/sqlite/migrations/002_model_requests.sql`: delete.
- Root `migrations/001_sqlite_store.sql` and `migrations/002_model_requests.sql`: delete the unused duplicate directory contents; if repository tooling proves the root path authoritative, instead make it the sole generated source and document generation. Current evidence favors deletion.
- `store/sqlite/store.go`: embed only migration `001`; inspect `sqlite_master` without DDL, bootstrap only a truly empty database inside one explicit transaction, and otherwise validate exact current metadata and structure without writes; delete sequential upgrade loops and version-2 branches.
- `store/sqlite/store_test.go`: delete upgrade success fixtures; retain/rewrite tests for fresh creation and explicit rejection of non-current or malformed databases.
- `session/extensions.go`: set `ExtensionPlanSchemaVersion = 1` and describe it as the first/current undeployed schema.
- Descriptor tests across `session`, `runtime`, and `composition`: replace old version-1 compatibility/rejection cases with version `0` and `2` invalid cases.
- Architecture documentation that claims sequential migrations or descriptor version `2`: describe fresh current-only schema expectations.
- `store/sqlite/doc.go`: replace its forward-only migration promise with fresh-schema bootstrap plus read-only exact-schema validation.
- `docs/consumer-guide.md`: remove migration-order preservation guidance and state the pre-release recreate-on-schema-change workflow.

## Intended behavior

- A missing/truly empty SQLite database initializes the complete version-1 schema in one transaction and commits only after structural validation.
- `Open` first inspects `sqlite_master` read-only. A nonempty database with any non-current marker or incomplete structure fails clearly before any DDL/DML; it is not repaired, upgraded, or partially modified.
- The current run table includes claim token and lease columns on creation; no `ALTER TABLE` exists.
- Extension plan fingerprints use schema version `1`; every other value is rejected.
- There is one authoritative SQL copy embedded by the SQLite package.

## Tests and acceptance

- Fresh in-memory and file-backed stores contain all tables and run-fencing columns and report version `1`.
- Opening version `0`, version `2`, or structurally incomplete databases fails without mutating them. Tests compare before/after `sqlite_master` snapshots and include a partial-database fixture; file-backed tests compare bytes when SQLite journaling permits a stable closed-file comparison.
- Descriptor validation accepts version `1` and rejects `0` and `2`.
- `rg -n "002_model_requests|ExtensionPlanSchemaVersion = 2|schema version 2" . --glob '!\.git/**'` returns no live compatibility reference.
- `find migrations store/sqlite/migrations -type f` shows exactly the selected authoritative current schema set.
- `go test ./session ./store/sqlite ./runtime ./composition` passes.

## Dependencies and exclusions

- Land the run record columns and schema collapse together so no intermediate schema is asserted runnable.
- Do not write an upgrade, export/import utility, fallback reader, or compatibility documentation.
- Reject old local developer databases; deleting/recreating them is the intended pre-release workflow.
