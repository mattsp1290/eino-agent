# Thermo-Nuclear Architecture Fixes

Status: Implemented and locally verified. Two independent requested reviews completed, evidence-backed corrections were incorporated before implementation, and the repository quality gates passed.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "7e50dea042572ea0163a85e951a6f15a534cf798d1f859aeab1fb77c9f756054",
    "confirmed_at": "2026-08-23T14:12:56Z"
  }
}
```

This reuses the user-confirmed operating context already recorded on this branch in `.agents/plans/strict-resume-model-lifecycle/00-overview.md`. No rollout flag or compatibility shim is required. Preserve durable descriptor and SQLite data compatibility unless a work package explicitly proves a schema change necessary; none is currently planned.

## Change classification

- Change type: structural maintainability and lifecycle-correctness refactor.
- Affected areas: `extension`, `composition`, `runtime`, `tools`, and their tests and architecture documentation.
- Tracking issue: `eino-agent-8fd`.

## Requested outcome

Resolve the four thermo-nuclear review blockers:

1. Remove `unsafe` inspection of Go interface representation from protected extension inputs.
2. Give fresh and resumed tool calls one canonical execution and settlement operation, and make the public tools settlement helper delegate to the same durable encoder.
3. Replace synthetic composition notification leases with explicit scoped leases so unrelated plans cannot delay mount close.
4. Replace context-hidden run plans with explicit non-nil execution state and decompose `runtime/extensions.go` by responsibility.

## Success criteria

- `runtime` contains no `unsafe` import or interface-word identity comparison for extension validation.
- Model and tool callbacks always receive nil host-callable fields. Unchanged delegation invokes the captured host callable; any nonnil callable injection fails closed with `extension.ErrProtectedMutation` before an inner callback or terminal runs.
- Fresh and pending-resume tool execution call the same runtime function for execute, transform, output encoding, durable settlement, notification, and observation.
- `tools.BuildToolSettlement` delegates to the runtime-owned canonical builder; `rg` finds one durable output encoder and one settlement-envelope builder.
- A session-scoped callback-only mount is not leased by an unrelated session plan.
- Composition capabilities still hold their owning mount until every applicable frozen plan releases.
- Runtime code has no `withRunPlan` or `runPlanFromContext` helper.
- Every new or modified production Go file, including `runtime/orchestrator.go`, is below 1,000 lines; `runtime/extensions.go` no longer exists as a monolith.
- `make check` passes and the branch is committed and pushed.

## Scope

- Narrow callback-visible model and tool values to data-only views while preserving existing exported point types where practical.
- Add first-class lease-scope staging and plan leasing to `extension.Registry`.
- Move canonical tool output and settlement construction into `runtime`, below the existing `tools -> runtime` dependency direction.
- Share fresh and resume execution after a call is durably claimed.
- Pass a proposed unexported `runExecution` value explicitly through extension-sensitive runtime methods.
- Split extension code into focused files in the existing `runtime` package.
- Update tests and architecture documentation for the new ownership boundaries.

## Non-goals

- Change extension contract IDs or durable plan fingerprint semantics.
- Change SQLite schemas or `session.ToolSettlementStore` atomicity rules.
- Remove legacy `Tools`, `Context`, `Hooks`, or `Middleware` construction fields in this refactor.
- Change tool permission, retry, retention, or interruption behavior except where canonicalization removes existing divergence.
- Introduce feature flags or a new public extension bus.
- Rework the Wasmtime C ABI.

## Repository findings

- `runtime/extensions.go` is 1,086 lines and mixes plan acquisition, public point declarations, prompt and guard orchestration, event fanout, cloning, and protected-input validation.
- `runtime.sameInterfaceIdentity` reads private interface words with `unsafe`; the protected clients and executors are not behaviorally replaceable and need not cross the callback boundary.
- `runtime.executePreparedTools` and `runtime.resumeRunWithSettlement` independently implement the terminal tool state machine.
- `tools.BuildToolSettlement` has no production caller and builds a different envelope from runtime's `encodeToolOutput` path because `tools` imports `runtime` and runtime cannot call back into `tools`.
- `composition.Registry.Mount` registers `eino-agent/composition/lease` notifications solely to obtain extension-plan references. Its empty-scope fallback is global, including callback-only mounts whose real callbacks can be session-scoped.
- A nonterminal run always acquires a `*runtime.RunPlan`, but runtime hides it in `context.Context` and performs more than twenty plan lookups and nil checks.
- `extension.Notify` already treats a nil plan as a no-op and `extension.Invoke` already delegates directly for a nil plan, so an explicit execution value can centralize the empty-plan behavior without special branches at every call site.

## Key decisions

1. **Keep host callables out of callback-visible values.** Sanitize `model.Resolved.Client`, `model.Resolved.Streamer`, `model.Request.Observer`, `runtime.Tool.Executor`, `runtime.Tool.InputDecoder`, and every callback-visible `runtime.ToolCall.Approval`, including protected outcomes. Terminals close over the authoritative originals. Any injected callable fails validation before delegation.
2. **Make runtime the canonical settlement owner.** `tools` already depends on `runtime`; placing data-only output and settlement builders in runtime avoids a cycle and lets `tools` retain source-level wrappers where useful.
3. **Lease scopes are registry metadata, not callbacks.** Extend staged mount state with validated scopes that participate in snapshots but never diagnostics, ordering, dispatch, or fingerprints.
4. **Share the post-claim state machine.** Fresh admission and resume may differ before claim and after the returned model message, but execution through durable settlement must have one implementation.
5. **Carry plan state explicitly.** A proposed unexported `runExecution` owns the non-nil `RunPlan`, release, dispatch, event sink, and settlement predicate for one fresh or resumed run.

Rejected alternatives:

- Retain unsafe identity checks behind helper wrappers: this preserves dependence on unspecified runtime layout.
- Compare only reflect-comparable callables: functions and non-comparable implementations still require special cases.
- Import `tools` from `runtime`: this creates a package cycle.
- Keep synthetic notifications but remove only the global fallback: this fixes one symptom but preserves a fake contract in dispatch state.
- Split `runtime/extensions.go` without changing ownership: this moves complexity without deleting it.

## Target control flow

```text
Start/Resume
  -> acquire non-nil frozen RunPlan
  -> create runExecution{orchestrator, plan}
  -> explicit extension dispatch with data-only values
  -> durable tool claim (fresh or resume-specific)
  -> runExecution.executeAndSettleClaimedTool
       -> guards / permissions
       -> protected tool execute
       -> legacy after middleware
       -> protected result transform
       -> canonical output + settlement envelope
       -> atomic or legacy settlement
       -> settled notification + observation
  -> release plan exactly once
```

Composition mount ownership:

```text
installer registrations
  -> callback entries + explicit capability lease scopes
  -> one mountState
  -> Snapshot(target) leases mountState when either kind applies
  -> diagnostics/fingerprint include real callbacks and capabilities only
```

## Risks and gates

- Stop if sanitizing a callable breaks a documented callback contract that explicitly permits invoking or replacing it. No such contract is documented; current validators reject replacement.
- Stop if a canonical output format would newly expose attachment or metadata content. Preserve runtime's current restrictive JSON fields: `tool_call_id`, `status`, `content`, `structured`, `truncated`, `original_size`, `inline_size`, `external`, and `redacted`. Deprecate the richer tools wrapper fields rather than exposing them through runtime.
- Use one `settlementCtx := context.WithoutCancel(ctx)` for every terminal strict or partial-legacy write once execution has produced an outcome.
- Preserve claim owner/token fencing and reserved result IDs in every strict settlement and reconciliation path.
- Keep callback-only mount descriptors and fingerprints unchanged after synthetic lease removal.
- The plan requires exactly two independent reviews because the user requested two; it intentionally omits the implementation-plan skill's optional third adversarial pass.

## Assumptions and unresolved decisions

- Assumption: existing exported extension input structs may retain callable fields for source compatibility, but dispatched copies can set those fields to nil because callbacks are not allowed to replace them.
- Assumption: runtime's current model-visible tool output is the conservative canonical format. `tools.ModelOutput` wrappers will align without expanding exposure, and canonical durable metadata is limited to host-owned call metadata plus documented `output_status`, truncation, external-storage, redaction, and size keys.
- No blocking decisions remain.

## Document map

- [01-extension-boundaries.md](01-extension-boundaries.md): remove unsafe callable identity and define explicit run execution state.
- [02-scoped-mount-leases.md](02-scoped-mount-leases.md): replace synthetic callbacks with first-class scoped leases.
- [03-canonical-tool-settlement.md](03-canonical-tool-settlement.md): unify output encoding and fresh/resume settlement.
- [04-decomposition-and-verification.md](04-decomposition-and-verification.md): split the monolith, update documentation, and run structural checks.
- [05-execution-handoff.md](05-execution-handoff.md): dependency order, gates, and definition of done.
