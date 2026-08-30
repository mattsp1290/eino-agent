# Extensibility Review Follow-up

Status: Implemented and verified on 2026-08-28. `make check` and `git diff --check` pass.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "af89b3f101b774b87232b8aa5c871e8e67d3c759c48a23b24f4b998c5f6273ae",
    "confirmed_at": "2026-08-28T23:36:43Z"
  }
}
```

The user explicitly stated that the project has no users and backward compatibility is dead code. Change public Go interfaces and durable in-repository fixtures directly. Do not add aliases, adapters, dual paths, migrations, or feature flags.

Tracked work: `eino-agent-clh`.

## Change type and affected areas

This is a greenfield correctness and maintainability refactor.

- `extension`: enforce synchronous around delegation and replace pointer/string point identity with canonical typed identity.
- `composition` and `runtime`: preserve component ownership from the mounted snapshot through executable run-plan sealing.
- `session`, `store/sqlite`, and `runtime`: return and publish the exact tool state/event pair committed atomically.
- `runtime` tests: split the 1,012-line mixed-concern suite along production boundaries.

## Requested outcome

Fix all five findings from the thermo-nuclear review without compatibility scaffolding:

1. `InvokeAround` never returns while an admitted `next` call is still executing.
2. Tool-transition mutations return the canonical committed call and event; runtime never reconstructs a persisted event.
3. A component remains the ownership unit from extension snapshot to durable descriptor.
4. Extension points use one canonical contract-plus-kind identity and one shared typed handler-kind enum.
5. No runtime extension test file exceeds the review threshold or mixes unrelated subsystems.

## Measurable success criteria

- A race-enabled test proves an early-returning around callback cannot let terminal work outlive `InvokeAround`.
- `session.ExecutionStore` returns one `ToolTransitionResult` from create, claim, and settle.
- SQLite returns the exact `EventRecord` yielded by `appendEvent`, including idempotent replay.
- `rg -n "publishToolTransition|settlementCall" runtime` has no production match.
- `runtime.RunPlanSpec` accepts component-owned records rather than four owner-repeating capability slices.
- `runtime.NewRunPlan` does not build a `byInstance` ownership map or regroup flattened capabilities.
- Independent point values with the same contract, kind, and callback signature interoperate; conflicting signatures fail during mount publication.
- `session.RegistrationIdentity.Kind` uses `extension.HandlerKind`; no session-local handler enum remains.
- Every `runtime/*_test.go` file remains below 1,000 lines and has one coherent concern.
- `make check` and `git diff --check` pass.

## Repository findings

- `extension.InvokeAround` records `activeCalls` and returns `ErrNextNotCalled` immediately when a callback returns early, while the delegated goroutine can continue mutating durable state.
- `runtime/tool_preparation.go` reconstructs pending/running events after persistence and silently drops reconstruction errors.
- `runtime/tool_execution.go` reconstructs terminal events and `settlementCall` discards `ToolSettlement.Apply` errors.
- `store/sqlite/execution.go` already receives the canonical record from `appendEvent` but discards it.
- `composition.Registry.acquire` produces flat `PlanTool`, `PlanPrompt`, `PlanGuard`, and `PlanRestriction` slices with repeated component owners.
- `runtime.NewRunPlan` rebuilds component ownership with a map and five loops.
- Extension dispatch uses private pointer identity and a private enum, diagnostics stringifies that enum, and session defines a second semantic enum.
- `runtime/extensions_test.go` is 1,012 lines and combines prompt rendering, tool guards, middleware, stream validation, plan acquisition, resume, and settlement lifecycle tests.

## Key decisions

1. **Drain admitted delegation.** Mark an around callback that returns while `next` is active, wait for the admitted call to finish, then return a dedicated lifecycle error. A blocked terminal therefore keeps the outer invocation blocked, matching the documented synchronous contract.
2. **Return an atomic transition result.** Introduce `session.ToolTransitionResult { Call, Event }` and use it as the result of all three tool-transition mutations. The store owns derivation and runtime publishes `result.Event` only after success.
3. **Seal component-owned plan input without changing execution order.** Introduce `runtime.PlanComponent` containing the component plus its handler identities and owner-free executable capabilities. Composition creates one record per mounted component and assigns non-durable global sequence tokens using the existing selectors; runtime validates ownership directly and globally merges executable collections by those tokens.
4. **Canonicalize point identity for the registry lifetime.** Export `extension.HandlerKind`, key points by `Contract`, kind, and callback signature, and establish one registry-lifetime signature per durable contract-plus-kind during atomic mount publication. Independently constructed equivalent points then dispatch identically, including after earlier mounts close.
5. **Split tests by behavior.** Keep helpers near their narrowest consumers and use focused runtime test files; do not create a generic dumping-ground helper file.

Rejected alternatives:

- Do not return immediately with an error while an admitted `next` still runs; that preserves the correctness bug.
- Do not add a second event read after commit; the mutation already has the canonical record inside the transaction.
- Do not keep flat capability inputs with a helper that merely hides regrouping.
- Do not canonicalize points by contract alone; point kind and callback signature are required to reject unsafe type collisions.
- Do not preserve session enum aliases or old store signatures.

## Target flows

```text
Around callback starts next -> callback returns early -> close callback admission
  -> wait for admitted next -> classify lifecycle violation -> InvokeAround returns

SQLite fenced transaction -> derive state/event -> persist state -> append event
  -> return {canonical call, canonical event} -> runtime publishes returned event

extension snapshot mounted component
  -> {component, typed handler identities, selected executable capabilities}
  -> runtime validates/seals same component record -> durable ComponentPlan
```

## Scope, non-goals, and constraints

In scope: all five review findings, direct API changes, all affected fakes/contracts/tests, and architecture comments required to describe the new invariants.

Out of scope: new extension kinds, new storage backends, database schema changes, event delivery retries, and redesign of tool execution semantics beyond publishing canonical committed transitions.

Constraints: preserve atomic SQLite writes, idempotent replay, mount lease ownership, deterministic descriptor fingerprints, bounded public callback errors, and current selection semantics.

## Risks and gates

- Point signatures use runtime type identity only for in-process safety; durable fingerprints continue to encode contract, version, kind, registration, scope, and owner. A registry-lifetime signature catalog is authoritative and is not cleared by deactivation or close.
- Stop if canonical point validation would require reflection values in durable records. Reflection belongs only in private dispatch keys.
- Stop if any tool transition can commit successfully without returning both records from the same fenced transaction.
- Stop if component grouping changes ordering, prompt shadowing, resume filtering, or descriptor fingerprints for semantically identical plans.
- Test splitting is mechanical and follows behavior changes so merge conflicts do not obscure functional review.

There are no unresolved blocking decisions.

## Plan review disposition

Two independent subagents completed review. Their substantive findings were accepted: preserve cross-component execution order independently of descriptor sorting, retain point-signature authority across unmounts, define one exact grouped handler-identity API, remove residual owner fields from nested plan input, specify event/error precedence, and force-add this ignored plan directory. No compatibility recommendation was introduced.

## Document map

- [01-extension-dispatch-contract.md](01-extension-dispatch-contract.md): synchronous around lifecycle and canonical point identity.
- [02-atomic-tool-transitions.md](02-atomic-tool-transitions.md): store-returned canonical transition records and runtime publication.
- [03-component-owned-run-plans.md](03-component-owned-run-plans.md): preserve component ownership through composition and runtime.
- [04-test-decomposition-and-handoff.md](04-test-decomposition-and-handoff.md): split tests, execute work in dependency order, and run final gates.
