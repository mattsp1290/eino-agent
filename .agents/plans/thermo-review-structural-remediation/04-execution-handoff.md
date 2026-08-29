# Execution Handoff

Status: Implemented and verified after two independent plan reviews.

## Dependency-ordered work packages

1. Run-plan sealing
   - Replace identity-bearing capability inputs with behavior-owned registration fields.
   - Derive durable identities and reject static conflicts in `runtime.NewRunPlan`.
   - Update composition acquisition and focused tests.
   - Gate: focused `runtime` and `composition` tests plus `git diff --check`.
2. Tool source identity
   - Remove `tools.Provenance`.
   - Replace paired optional source hashes with one optional source identity.
   - Preserve schema, executor, artifact, and resume fingerprint coverage.
   - Gate: `go test ./tools ./tools/einotools ./composition`.
3. WASM registration boundary
   - Route all public loader registration through `*composition.Registrar`.
   - Preserve private extension adapters and module cleanup.
   - Gate: `go test ./wasmext`.
4. Canonical event contract
   - Delete runtime event types and use `session.EventRecord` directly.
   - Make runtime sinks void and best-effort.
   - Remove converters and preserve AG-UI synchronous transport error checks.
   - Gate: focused `runtime`, `agui`, and `stream` tests.
5. Integration, issue closure, commit, and push
   - Run stale-symbol searches and all repository quality gates.
   - Close the four claimed Beads issues only after their acceptance criteria pass.
   - Commit the plan and implementation together.
   - Run preflight, rebase on the remote branch, push Beads state, push Git, prune stale remote refs, and verify a clean branch up to date with origin.

## Files and symbols by package

| Package | Existing symbols changed |
|---|---|
| `runtime` | `RunPlanSpec`, `PlanTool`, `PlanPrompt`, `PlanGuard`, `PlanRestriction`, `NewRunPlan`, `sealedPlanTools.ResolveTools`, `Event` (removed), `EventKind` (removed), `EventSink`, `EventSinkFunc`, `EventPublishedPoint`, `runEventSink`, `eventQueue` |
| `composition` | `ToolRegistration`, proposed `ToolSourceIdentity`, `Registrar.Tool`, `Registry.acquire`, `composedToolSchemaHash`, `composedToolExecutorHash`, `validateToolSourceIdentity` |
| `tools` | `Definition`, `Provenance` (removed) |
| `tools/einotools` | source identity registration literals |
| `wasmext` | `Loader.RegisterEventSink`, `RegisterContextSource`, `RegisterHook`, `RegisterToolMiddleware`, `loadedEventSink.Emit`, `registerEventSink`, bounded event projections |
| `agui` | `Bridge.Emit`, `replay`, `Reconnect`, `runtimeEvent` (removed) |
| `stream` | `Tail.Emit` |

## Prerequisites and parallelization constraints

- Work package 1 precedes work package 2 because the derived hashes move into the new capability input.
- Work package 3 can execute after work package 2 and shares composition registrar fixtures.
- Work package 4 is architecturally independent but touches many runtime tests. Apply it after the plan refactor to avoid rewriting the same fixtures twice.
- The implementer owns all worktree edits. Plan reviewers inspect only and return findings.
- Do not change `docs/`. Keep `examples/` outside the review and allow only mechanical compilation updates required by removed APIs.

## Verification commands

Run focused gates after each package as listed above. Run these integration gates from the repository root after all packages compile:

```bash
rg -n 'runtimeEventRecord|func runtimeEvent|runtime\.Event\b|runtime\.EventKind\b|runtime\.Usage|runtime\.EventError|runtime\.RedactionClass|tools\.Provenance|Definition\.Provenance|SourceSchemaHash|SourceExecutorHash' --glob '!docs/**'
rg -n 'func \(l \*Loader\) Register.*extension\.Registrar' wasmext
go test ./...
go test -race ./runtime ./composition ./agui ./stream ./tools/... ./wasmext
CGO_ENABLED=0 go test ./runtime ./composition ./agui ./stream ./tools/... ./wasmext
make fmt-check vet mod-tidy-check lint wit-check
git diff --check
```

The first two `rg` commands must return no matches. Generated WIT output must remain unchanged. If `make fmt-check` fails, run `make fmt`, verify that it did not modify `docs/` or `examples/`, and rerun the gate.

## Integration and regression gates

- Fresh and resumed runs produce deterministic matching extension-plan fingerprints.
- Invalid static plan structure fails before admission and releases its dispatch snapshot once.
- Dynamic tool resolver failures remain execution errors and cannot publish invalid tools.
- Live and replay events use one canonical record without losing `ToolTransition`, usage, error, redaction, payload, ID, or creation time.
- Runtime observation sink failures cannot become run failures.
- AG-UI replay still returns transport errors and reconnect still reports tail overflow.
- Tool definition clones do not gain ownership or fingerprint state.
- Tool schema, source executor, and component artifact changes alter resume fingerprints as intended.
- WASM mount rollback and close finalize modules exactly once.

## Definition of done

- Beads issues `eino-agent-cdk`, `eino-agent-tn9`, `eino-agent-nwx`, and `eino-agent-frw` meet their acceptance criteria and are closed.
- Exactly two requested plan reviews completed and accepted findings were incorporated.
- No unresolved blocking decision remains.
- No compatibility shim, alias for a removed contract, feature flag, or migration was added.
- No file under `docs/` changed. Any changed file under `examples/` is a mechanical removed-API compilation update.
- Focused tests, full tests, race tests, non-CGO tests, static gates, WIT generation checks, and `git diff --check` pass.
- The plan and implementation are committed on the current feature branch.
- Beads and Git state are pushed.
- `git status` reports a clean branch up to date with origin.

## Deferred work

No deferred work is planned. Create a Beads issue for any newly discovered out-of-scope defect before session completion.
