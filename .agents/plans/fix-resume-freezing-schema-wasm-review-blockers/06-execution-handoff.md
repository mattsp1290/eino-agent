# Execution Handoff

## Ordered work packages

1. **Current schema foundation.** Edit `store/sqlite/migrations/001_sqlite_store.sql`, remove obsolete SQL, reset descriptor version, and simplify open/version logic. Include the proposed run columns but do not yet expose an unfenced runtime path. Verify fresh/rejected schema and descriptor tests.
2. **Fenced run lifecycle.** Change `session.Store` to expose one `ExecutionStore` scoped by `RunFence`, move model-request and all other execution writes behind it, make lease duration/store-time semantics authoritative, add atomic `SettleRun`, and update SQLite, test stores, admission, resume, execution, compaction, events, and heartbeat together. Verify store contract and runtime concurrency tests before proceeding.
3. **Checked tool freezing.** Change clone/snapshot/plan/Wasm definition APIs and all callers. Verify mutation isolation and fail-closed tests.
4. **Typed Wasm interfaces.** Introduce narrow typed component interfaces per world and closure-based shared module execution, then migrate one world at a time. Keep tests green between worlds.
5. **File decomposition.** Move Wasmtime codecs and host wrappers into cohesive files only after typed behavior is green; delete old giant files rather than leaving forwarding compatibility shells.
6. **Documentation and full gates.** Update architecture docs, run all focused/full commands, and perform the final strict maintainability audit.

## Parallelization constraints

- Packages 1 and 2 are one atomic design sequence because the new store contract depends on the fresh schema.
- Package 3 can be implemented independently of run fencing after shared interfaces compile, but must precede moving the Wasm tool wrapper.
- Package 4 must precede package 5 so file moves do not obscure semantic changes.
- Only one implementer should edit `wasmext` interface/wrapper files at a time.

## Verification by package

| Package | Primary files/symbols | Required gate |
|---|---|---|
| 1 | SQLite migration/open logic; `ExtensionPlanSchemaVersion` | `go test ./session ./store/sqlite` and obsolete-token searches |
| 2 | `Run`, `Store`, `ClassifyResume`, admission/resume, proposed heartbeat | `go test -race ./session ./store/sqlite ./runtime` or documented non-race fallback |
| 3 | `tools.Definition`, snapshots, plan tool freezing, `LoadedTool` | `go test ./tools ./composition ./runtime ./wasmext` |
| 4 | `compiledComponent`, `module`, Wasmtime typed methods | normal and `CGO_ENABLED=0 go test ./wasmext` |
| 5 | proposed world/wrapper files; deleted giant aggregators | Wasm tests plus dispatcher/file-size searches |
| 6 | architecture docs and repository | `go test ./...`, `go vet ./...`, `make check`, `git diff --check` |

## Definition of done

- All four thermo-nuclear findings have observable regression coverage.
- Resume ownership is atomic, renewable, and token-fenced from admission through terminal settlement.
- Tool schemas are never silently aliased or zeroed after clone failure.
- Only fresh current schema version `1` exists for SQLite and extension descriptors.
- Wasm dispatch is typed and decomposed without wire-contract changes.
- All required quality gates pass.
- `eino-agent-7yj` is closed with the completed acceptance evidence.
- Plan, implementation, tests, docs, and Beads changes are committed on the current branch.
- `git pull --rebase`, `bd dolt push`, and `git push` succeed; final status is clean and up to date with origin.

## Deferred work

No compatibility, migration, feature-flag, or legacy cleanup work is deferred. File a Beads issue only for a newly discovered concern that is outside these four findings and is not required for correctness.
