# Sealed Runtime API and Dead Semantics

## Goal and prerequisites

Make validated construction the sole supported orchestrator path, then delete unreachable tool-shadowing logic and stale descriptions. Complete the mandatory store-contract change first.

## Seal `StreamingOrchestrator`

### Evidence

- `runtime/orchestrator.go`, `StreamingOrchestrator`, exposes every dependency and tuning field.
- `runtime/options.go` says direct struct literals remain supported and duplicates default behavior in getters/validation.
- `StreamingOrchestrator.admitter` accepts an `Admit` override and independently fills its dependencies.
- Production examples and many runtime tests construct or mutate the struct directly.

### Exact change surface

- Change `runtime/orchestrator.go`:
  - Privatize dependency, tuning, observer, and ledger fields.
  - Remove the `Admit` override and transactor field.
  - Add an unexported initialized/configuration invariant so a zero-value direct construction fails immediately and predictably.
  - Derive `Admitter` only from the orchestrator's canonical private store, events, extensions, and clock.
  - Replace fallback getters with constructor-populated values where practical.
- Change `runtime/options.go`:
  - Make options write private fields.
  - Delete `WithTransactor`.
  - Apply defaults once in `NewStreamingOrchestrator`: clock `time.Now`, owner ID `"runtime"`, attempts `1`, tool turns `8`, queue size `1`, lease `time.Minute`, model-request cap `4 MiB`, ledger disabled, and optional plan/events/permissions/observer nil.
  - Reject explicit nil clock, empty owner ID, non-positive attempts/tool turns/queue size/lease/model-request cap, and nil values for required interface options. Omission uses the defaults above.
  - Defensively clone every mutable option input, including trace attributes, history epoch data, and model-request safe-option slices.
- Migrate all production/examples/tests to `NewStreamingOrchestrator` and options. Test-only internal construction is permitted only for a unit that directly exercises an otherwise unreachable private helper; behavior tests must use the constructor.
- Delete `examples/datadog.AttachRuntimeObserver`; construct with `runtime.WithObserver` instead.
- In `examples/minimal-server.NewServer`, close the live-tail resource and SQLite store if the now-fallible orchestrator construction fails.

### Intended invariants

- Dependencies and limits cannot change after construction.
- Admission cannot be replaced independently of the orchestrator store/event/extension configuration.
- A single configured/initialized guard is shared by `Start`, `Resume`, and `Status`; request-specific ID validation remains at those operation boundaries.
- Start/resume and downstream helpers use constructor-populated defaults directly and contain no fallback branches.
- A zero-value orchestrator reports a clear construction error rather than panicking.

### Tests and acceptance

- Constructor tests cover missing required dependencies and invalid option bounds.
- Mutation-after-construction tests prove caller changes to trace attributes, history epoch data, and safe-option slices do not alter runtime state.
- Existing orchestration behavior tests use the constructor and retain their assertions.
- Reflection or AST coverage proves the concrete type has no exported configuration fields; external examples compile using only constructor options.
- Structural searches find no `StreamingOrchestrator{` outside a narrowly documented internal unit fixture, `.Admit`, `WithTransactor`, or exported dependency field access.
- Run `go test ./runtime ./examples/...` and `go test -race ./runtime`.

## Remove unreachable tool shadowing

### Evidence

- `composition.Registry` collision validation rejects duplicate tool names across applicable global and session registrations.
- `composition.selectTools` still constructs global and session maps and overwrites global entries with session entries.
- `docs/architecture/tools.md` documents the collision prohibition.
- `docs/architecture/extension-points.md` claims both session tools and prompts shadow globals.

### Exact change surface

- Simplify `composition.selectTools` to filter applicable registrations and deterministically sort them, without precedence maps or overwrite behavior.
- Keep prompt selection and prompt shadowing unchanged.
- Add/retain tests that reject global/session tool collisions, including collisions contributed by separate mounts.
- Correct `docs/architecture/extension-points.md` to distinguish forbidden tool collisions from supported prompt shadowing.
- Correct the descriptor version comment in `session/extensions.go` and remove migration-002 claims from current architecture docs.
- Update field-based construction guidance in `docs/integrations/datadog.md`, `docs/integrations/ag-ui-go-server-example.md`, and `docs/architecture/security.md`.
- Search all docs/comments for `migration 002`, descriptor `Version 2`, and tool-shadow wording; update only references that describe current behavior.

### Acceptance and exclusions

- Tool selection has one path for every applicable registration and keeps stable sorting.
- Collision errors continue to name the conflicting tool and scopes/mounts as existing tests require.
- Prompt shadow tests remain unchanged and passing.
- Do not introduce a precedence option or feature flag.
- Do not rewrite historical Git content or add a migration.
