# Fix Resume, Freezing, Schema, and Wasm Review Blockers

Status: Implemented and verified. Required independent plan reviews and the final thermo-nuclear maintainability audit are complete.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "68aa361c90552355dcc05706bd84e4871ba34610ac7101f740eb08598067d7f4",
    "confirmed_at": "2026-08-23T23:27:31Z"
  }
}
```

The user explicitly confirmed that the project has no users and that backward compatibility is dead code. Delete undeployed compatibility paths instead of preserving, migrating, deprecating, or feature-flagging them.

## Change classification

- Change type: breaking correctness hardening and architectural simplification.
- Affected areas: `session`, `runtime`, `store/sqlite`, `tools`, `composition`, `wasmext`, tests, embedded SQL, root migration artifacts, and architecture documentation.
- Tracking issue: `eino-agent-7yj`.

## Requested outcome

Resolve all four blockers from the thermo-nuclear maintainability review:

1. Make resume ownership atomic and fenced so one live execution cannot be interrupted or overwritten by another.
2. Make tool-definition and run-plan freezing fail closed on every clone/serialization error.
3. Collapse undeployed schema history to one current schema and delete upgrade-only code and tests.
4. Replace stringly typed Wasmtime dispatch and giant mixed-responsibility files with typed world boundaries and cohesive files.

## Success criteria

- A run execution has a unique claim token; every execution-owned durable mutation is conditional on that token.
- Lease admission, expiry comparison, claim, and renewal use the store's authoritative clock; injected runtime clocks cannot steal or prematurely expire work.
- Resume never bypasses a live lease because an owner label matches, and concurrent resume attempts have exactly one winner.
- A live execution renews its run lease while blocked in model or tool work; stale execution writes fail with `session.ErrConflict`.
- Tool registry construction, snapshotting, plan acquisition, and Wasm tool loading return an error rather than retaining or returning an unfrozen schema.
- SQLite opens only the current schema created from one embedded migration; no `002` migration or upgrade path remains.
- Extension plan descriptors use current schema version `1`; no test or branch preserves the undeployed version `1` to version `2` transition.
- Wasm wrappers call typed component methods. No production dispatcher accepts `(operation string, input any, output any)` or contains operation/type switch matrices.
- `wasmext/wasmtime_abi.go` and the current multi-world `wasmext/wrappers.go` are decomposed along stable responsibilities.
- Targeted race/regression tests, `go test ./...`, `make check`, and `git diff --check` pass.

## Repository findings

- `session.ClassifyResume` currently returns `ResumeOwned` whenever owner strings match, even with a live lease. `runtime.StreamingOrchestrator.ownerID` defaults every instance to `"runtime"`.
- `session.Store` has no atomic run-claim operation or claim token. SQLite lease renewal reads and rewrites the run, while `FinishRun` fences only on status.
- Runtime creates leases but does not run a heartbeat. A long model or tool call can therefore appear stale while it is alive.
- `runtime.cloneTool` returns a zero-value tool when checked cloning fails. `tools.cloneParamsOneOf` returns the original mutable pointer when checked cloning fails.
- `store/sqlite/store.go` embeds both `001_sqlite_store.sql` and `002_model_requests.sql`; equivalent root-level SQL files duplicate the schema history.
- `session.ExtensionPlanSchemaVersion` is `2` although the earlier descriptor format was never deployed and is deliberately rejected.
- `wasmext` routes every world through `compiledComponent.Call(context.Context, string, any, any)`. `wasmtimeComponent.callABI` lowers, invokes, and lifts through three operation switches.
- `wasmext/wasmtime_abi.go` is 949 lines and `wasmext/wrappers.go` is 701 lines, each combining multiple independent world contracts.

## Key decisions

1. **Claim tokens are authority; owner IDs are diagnostics.** Add a unique token for each admitted or resumed execution. Every post-admission run-scoped mutation compares the current token atomically in the same statement or transaction as its write. Do not treat matching owner labels as permission.
2. **The store arbitrates resume and time.** Add an atomic claim operation that changes owner, token, and lease only when the persisted nonterminal lease is expired according to the store clock. Runtime passes durations, never authoritative timestamps. A preliminary read may optimize plan lookup but cannot grant authority.
3. **One scoped capability owns execution writes.** `Store.Execution(RunFence)` returns an `ExecutionStore` used by `runExecution` for every message, part, event, model-request, tool-call, context-epoch, lease, and settlement mutation. Admission bootstrap uses the capability only after `AdmitRun` returns the persisted token/deadline.
4. **Lease liveness is maintained centrally.** Start a run-scoped heartbeat after admission or claim and stop it before terminal settlement. Renewal failures cancel execution and prevent further scoped writes.
5. **Terminal state and its durable event settle atomically.** `ExecutionStore.SettleRun` token-fences the status transition and final durable event in one transaction. Transport notification occurs only after commit.
6. **Frozen means successfully cloned.** Public/internal clone boundaries that can serialize Eino schemas return errors. Delete unchecked fallbacks rather than adding panic or alias semantics.
7. **Only current schemas exist.** Fold model request tables into the sole SQLite bootstrap migration, remove root duplicates and upgrade machinery, and reset the undeployed extension descriptor schema to version `1`.
8. **Narrow typed world methods replace the ABI switchboard.** Each compiled component implements only its WIT world's typed interface plus lifecycle methods. A shared module core retains timeout/admission accounting around closures, while world-specific modules and Wasmtime codecs expose only their own operations over a small raw-call primitive.

Rejected alternatives:

- Making the default owner ID random without a token leaves stale writers able to settle after lease theft.
- Checking lease state in runtime and then writing ownership creates a check-then-act race.
- Extending lease duration without renewal only changes the race window.
- Keeping infallible `Clone` APIs with an original-pointer fallback violates immutability.
- Retaining migration 002 or descriptor version 2 for hypothetical compatibility contradicts the confirmed application context.
- Replacing operation strings with an enum while keeping `any` inputs preserves the same central dispatch and type-assertion growth.

## Target control flow

```text
Start
  -> allocate owner label + unique claim token in AdmissionIDs + lease duration
  -> Store.AdmitRun(current run record, duration)
       -> store stamps persisted lease deadline from its clock
  -> Store.Execution(RunFence) returns the only execution writer
  -> heartbeat renews through the scoped writer using a duration
  -> execute plan through the scoped writer
       -> every part/event/model/tool/context write verifies the run token
  -> stop heartbeat
  -> ExecutionStore.SettleRun(run, final durable event) atomically
  -> emit transport-only completion notification after commit

Resume
  -> read descriptor and acquire matching immutable plan
  -> Store.ClaimRun(duration) WHERE nonterminal + store-clock lease expired
       -> atomically install new owner + new token + lease
  -> heartbeat renews lease with new token
  -> recover only work whose lease expired with the run
  -> stop heartbeat and atomic scoped SettleRun
```

## Scope, constraints, and non-goals

- Delete source, stored-data, and test compatibility that has no users.
- Preserve model-visible results, extension ordering, WIT names, resource limits, and permission policy semantics.
- Do not add flags, migration adapters, dual reads, deprecation wrappers, or legacy fixtures.
- Keep owner IDs for observability but never authorization.
- Keep the SQLite store as the durable atomicity boundary.
- Keep generated WIT bindings and public component contracts unchanged.
- Do not redesign session message/tool settlement beyond the run fencing needed here.

## Risks and gates

- Stop if a non-test production store exists that cannot provide an atomic conditional claim; current repository evidence identifies SQLite plus test doubles only.
- A heartbeat error must cancel execution before any further durable mutation; silently continuing would recreate split-brain execution.
- A tool-call lease must never extend beyond a shorter run lease. Claiming a tool call must renew the run lease to at least the call lease, and the heartbeat must keep the run lease current during long calls.
- Plan acquisition must release exactly once when a claim loses or execution exits.
- Normalized SQLite run columns are authoritative for status, owner, token, and lease; JSON payload reads overlay those columns and heartbeat renewal never rewrites the whole record.
- `WithClock` controls domain/observability timestamps only; it cannot decide lease expiry.
- Cgo file decomposition must preserve Wasmtime ownership/free behavior and pass both normal and `CGO_ENABLED=0` build paths.
- No blocking decisions remain.

## Document map

- [01-fenced-run-ownership.md](01-fenced-run-ownership.md): atomic claims, token-fenced writes, heartbeats, and resume recovery.
- [02-checked-tool-freezing.md](02-checked-tool-freezing.md): checked schema cloning from registration through execution.
- [03-current-only-schemas.md](03-current-only-schemas.md): collapse SQLite and extension descriptor history.
- [04-typed-wasm-boundaries.md](04-typed-wasm-boundaries.md): typed component operations and file decomposition.
- [05-verification-and-docs.md](05-verification-and-docs.md): cross-cutting tests, documentation, and quality gates.
- [06-execution-handoff.md](06-execution-handoff.md): dependency order, commands, and definition of done.
