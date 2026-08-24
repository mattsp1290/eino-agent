# Resolve Final Thermo-Nuclear Review Findings

Status: Complete. Two independent reviews were reconciled, implementation is complete, and all repository quality gates pass.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "6e4799c5b6edf50f81fda2073452ce4f2c102c3708b6cf7c075b9a78ce9ea2a4",
    "confirmed_at": "2026-08-24T12:48:57Z"
  }
}
```

The user explicitly confirmed that this code has no users and backward compatibility is dead code. Delete obsolete APIs and alternate paths instead of adding adapters, deprecations, dual behavior, stored-data migrations, or feature flags.

## Change classification

- Change type: breaking API simplification, transaction-boundary hardening, and lifecycle correction.
- Affected areas: `composition`, `session`, `store/sqlite`, `store/storetest`, `runtime`, `wasmext`, examples, tests, and architecture documentation.
- Tracking issues: `eino-agent-o85`, `eino-agent-5w8`, `eino-agent-dqs`, `eino-agent-0zm`, `eino-agent-88n`, and `eino-agent-17d`.

## Requested outcome

Resolve all six remaining thermo-nuclear review findings, validate the result, commit the related changes, and push the current branch.

## Success criteria

- Composition mount publication performs all fallible work before committing extension and capability state.
- Every supported `session.Store` provides one store-owned transaction boundary; admission has no non-transactional or separately injected transactor path.
- A timed-out Wasm worker retains its serialization gate and in-flight ownership until it actually exits; component and engine destruction never overlaps it.
- Public Wasm loading APIs either return an explicit close handle or make a `Loader` the visible owner.
- Runtime orchestrators have one validated construction path and no public mutable dependency fields or admission override.
- Tool selection contains no impossible shadowing branch, and current architecture/schema documentation matches the implemented invariants.
- Focused tests, `make check`, `git diff --check`, structural searches, and final clean/up-to-date Git status pass.

## Repository findings

- `composition.Registry.Mount` calls `CommitMount` before a second definition clone that can fail while `Registry.mu` is held.
- `runtime.Admitter` conditionally discovers or accepts a separate `session.Transactor`, then falls back to multiple non-atomic writes.
- `wasmext.module.call` owns the wait-group reference and gate in the caller even though invocation runs in another goroutine.
- Free `wasmext.LoadTool`, `LoadPermissionsPolicy`, and `LoadEventSink` functions return callable values without preserving a visible close owner.
- `runtime.StreamingOrchestrator` exposes mutable dependencies, permits direct struct construction, and accepts a parallel `Admit` configuration object.
- Composition rejects global/session tool-name collisions, but `selectTools` and one architecture document still describe unreachable session shadowing.
- Current-only descriptor/schema comments still mention removed version-2 and migration-002 history.
- The branch was clean and `make check` passed before this planning pass.

## Key decisions

1. **Freeze once, publish once.** Treat the clone performed by `extension.Registrar.Tool` as the only fallible definition freeze. Mount wrapping becomes an infallible value transformation before an atomic append/commit section.
2. **Put transactions on `session.Store`.** Add `WithinTx(context.Context, func(context.Context, Store) error) error` directly to `Store`; delete `Tx`, `Transactor`, `Admitter.Transactor`, and `WithTransactor`.
3. **Let the Wasm worker own its lifetime.** The invocation goroutine releases the gate and wait-group reference. A drain-expired timeout quarantines the module and starts deferred finalization that closes resources only after all workers exit.
4. **Expose ownership in Wasm API shape.** Delete free resource-losing loaders. Add an explicit closeable permissions-policy handle; keep receiver-based `Loader` methods because the receiver is the documented owner.
5. **Seal, do not emulate, construction.** Privatize orchestrator dependencies, set defaults and validate options in `NewStreamingOrchestrator`, remove alternate injection paths, and migrate repository call sites.
6. **Keep the security-preserving no-shadow rule.** Simplify tool selection around the collision invariant already enforced by composition. Prompt shadowing remains unchanged.

Rejected alternatives:

- Rolling back a partially committed mount leaves extension lifecycle and capability slices with multiple publication authorities.
- Preserving an optional `Transactor` keeps atomicity dependent on runtime wiring and permits Store/Transactor mismatch.
- Releasing a Wasm gate after a timer expires permits use-after-close and concurrent access to a non-thread-safe component.
- Keeping deprecated free loaders preserves the ownership bug for an undeployed API.
- Retaining exported orchestrator fields with comments cannot prevent post-construction invariant changes.
- Implementing tool shadow precedence weakens the existing collision defense and adds behavior that no reachable state uses.

## Target control flow

```text
extension registration -> frozen definitions -> validate complete mount
  -> infallible wrapped values -> atomic extension/capability publication

runtime admission -> Store.WithinTx -> all durable admission writes -> commit/rollback

Wasm call -> acquire gate -> worker owns gate + in-flight ref
  -> normal exit: release both
  -> drain-expired timeout: quarantine -> worker exits -> deferred resource close

NewStreamingOrchestrator(options) -> defaults + validation -> private immutable dependencies
```

## Scope, non-goals, and constraints

- Preserve durable admission contents, event ordering inside the transaction, permission behavior, extension ordering, prompt shadowing, and public orchestration behavior.
- Preserve bounded timeout returns to callers; resource cleanup may finish asynchronously after an uncooperative worker exits.
- Do not preserve source compatibility for removed interfaces, fields, functions, or options.
- Do not add database migrations or compatibility tests for unreleased contracts.
- Do not broaden this work into scheduler, extension protocol, or Wasm ABI redesign.
- Keep production files below the review rubric's giant-file threshold; split lifecycle helpers if needed.

## Risks, assumptions, and gates

- SQLite nested transactions must reuse the current transaction-backed store rather than opening a second transaction.
- Deferred Wasm finalization must be exactly once, race-safe, and observable by later `Close` calls.
- Constructor migration is mechanically broad; structural searches gate against missed exported-field literals and removed symbols.
- Stop if a second production `session.Store` implementation exists and cannot provide atomic transactions. Current searches found SQLite as the only production implementation.
- Stop if any free resource-losing Wasm loader has an external consumer in this repository. Current searches found tests and internal documentation only.
- No blocking decisions remain.

## Review reconciliation

Two independent reviewers recommended revision before implementation. The accepted corrections now require: one mutex-linearized Wasm call/shutdown admission protocol plus a closing signal for queued calls; persistent completion at both module and Loader levels; explicit orchestrator defaults, validation, defensive copies, and entry-point guards; exact nested-transaction semantics; audits of embedded stores; and fallible-constructor cleanup in examples. No feedback was rejected as incompatible with the requested outcome. The two reviewers proposed different safe reference-registration points for queued Wasm calls; this plan chooses registration under `mu` before gate waiting, with caller-to-worker ownership transfer, because it both prevents `WaitGroup.Add`/`Wait` races and accounts for queued callers until closure wakes them.

## Document map

- [01-atomic-composition-and-admission.md](01-atomic-composition-and-admission.md): make mount and durable admission publication atomic.
- [02-wasm-lifetime-and-api.md](02-wasm-lifetime-and-api.md): correct timeout ownership and delete resource-losing load paths.
- [03-sealed-runtime-api-and-dead-semantics.md](03-sealed-runtime-api-and-dead-semantics.md): seal orchestrator construction and remove unreachable tool shadowing.
- [04-verification-and-documentation.md](04-verification-and-documentation.md): define regression, documentation, and structural gates.
- [05-execution-handoff.md](05-execution-handoff.md): order the implementation and define completion.
