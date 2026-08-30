# Final Thermo-Nuclear Remediation

Status: Ready for implementation. Implementation had not occurred when this plan was finalized.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "d57d6b12029403b9a32aa8f556ef719e8bd0ab586b8017e5f732e176712c6898",
    "confirmed_at": "2026-08-28T16:36:34Z"
  }
}
```

The repository is pre-release. Change public Go and WIT contracts directly. Do not add aliases, compatibility shims, legacy decoding, dual schemas, migrations, or feature flags.

## Change type and affected areas

This is a greenfield structural simplification across four ownership boundaries:

- `extension`, `composition`, and `runtime`: remove the second copy of frozen handler ownership.
- `runtime`: make event publication a transport/observation concern instead of an event-kind-driven persistence switchboard.
- `wit`, `wasmext`, generated bindings, and Wasm examples: represent permission-pattern derivation and context roles explicitly.
- `extension`: remove reflection metadata made obsolete by canonical point-definition identity.

Tracked work: `eino-agent-llr`.

## Requested outcome

Implement the five accepted thermo-nuclear findings after two independent plan reviews and one adversarial review. Commit and push the verified result.

## Success criteria

- Frozen handler identities have one in-memory authority: `extension.Plan.HandlerComponents()`.
- `extension.MountedValue` and `runtime.PlanComponent` do not carry duplicate handler slices.
- Wasm tools derive permission patterns through an explicit WIT function; the host never probes a reserved JSON property.
- The context-source WIT role enum exposes only roles accepted by `runtime.ContextAssembly`.
- Runtime event fanout never infers persistence from `Event.LiveOnly` or `Event.Kind`; every durable publication is based on the canonical store-returned record.
- Point definitions contain no unused `reflect.Type` signature.
- No compatibility surface is retained for the replaced Go or WIT contracts.
- `make check`, `make wasm-fixtures`, `git diff --check`, and focused race tests pass.
- The Beads issue is closed and both Git and Dolt state are pushed.

## Repository findings

- `extension.Registry.snapshot` copies the same selected handlers into `Plan.handlerComponents` and `MountedValue.handlers`.
- `composition.Registry.acquire` copies `MountedValue.handlers` into `runtime.PlanComponent.Handlers`.
- `runtime.NewRunPlan` compares `PlanComponent.Handlers` against `Dispatch.HandlerComponents()` and persists the duplicate copy.
- `wasmext.toolDefinition` probes `permission_pattern` from generic tool input even though native tools expose an explicit `tools.PermissionPattern` callback.
- The `tool` WIT world exports only `metadata` and `execute`, so the guest cannot own its permission-pattern logic explicitly.
- `runEventSink.Emit` conditionally persists events based on `LiveOnly` and two event-kind exceptions. Admission currently discards the store-returned start record, and start/finish publication reconstructs events instead of publishing those canonical records.
- `EventModelFallbackEngaged` and `EventContextEpochChanged` promise durable semantics despite having no canonical persistence site; only examples and adapter/observability tests consume them.
- `text-role.assistant` is decoded by the Wasm context adapter and then rejected by `validateContextContributionMessage`.
- `pointDefinition.signature` is populated by every point constructor but is only checked for non-nil. Exact point-definition pointer identity is the actual registry and dispatch authority.

## Key decisions

1. **Use the dispatch plan as the sole handler authority.** `runtime.NewRunPlan` will seed its component accumulator from `Dispatch.HandlerComponents()` and merge capability-owned `PlanComponent` records into it.
2. **Keep capability ownership explicit.** `runtime.PlanComponent` remains for tools, prompts, guards, and restrictions, but it loses `Handlers`.
3. **Make permission-pattern derivation a required Wasm tool operation.** Add `permission-pattern(input-json)` to `tool-api`; do not retain the reserved-field fallback.
4. **Keep persistence at canonical mutation sites.** `runEventSink` will only fan out events to infrastructure and `EventPublishedPoint`; tool/run/admission mutations continue publishing their already-persisted records explicitly.
5. **Encode context role validity in WIT.** Remove `assistant` from `text-role`; retain the unrelated assistant count in `role-counts`.
6. **Delete orphan durable event contracts.** Remove the unused model-fallback and context-epoch event kinds and their helper/payload/adapter projections rather than retaining durability promises with no owner. Mark tail overflow explicitly live-only at construction.

Rejected alternatives:

- Do not keep both handler slices and optimize their comparison.
- Do not add a metadata flag, JSON pointer, or fallback convention for Wasm permission patterns; a function matches the native semantic contract and validates the final normalized input.
- Do not preserve `permission_pattern` probing for old guests.
- Do not retain implicit event persistence for hypothetical future producers.
- Do not keep reflection identity as defensive documentation; pointer identity already enforces the invariant.
- Do not preserve orphan model-fallback or context-epoch event surfaces; the confirmed no-user/no-compatibility context permits deleting them directly.

## Target architecture

```text
extension snapshot
  -> dispatch plan owns selected handlers + leases
  -> mounted values own component payload + callback context
  -> composition emits capability-only PlanComponent records
  -> runtime seeds descriptor components from dispatch handlers
  -> runtime merges capabilities by component owner

Wasm tool input
  -> normalize final JSON object
  -> guest permission-pattern(input-json)
  -> validate bounded non-empty pattern
  -> persist pattern with tool call

runtime event
  -> canonical state mutation persists durable record
  -> fanout publishes Event or persisted EventRecord
  -> infrastructure sink, then contained EventPublishedPoint
```

## Scope and non-goals

In scope:

- All five review findings.
- Direct API and WIT changes plus generated artifacts and fixtures.
- Focused tests, architecture docs, and consumer documentation required to describe the resulting contracts.

Out of scope:

- New extension points or Wasm worlds.
- Changes to durable database schemas. The unused model-fallback and context-epoch event wire surfaces are intentionally deleted.
- A compatibility path for existing Wasm components or `RunPlanSpec` callers.
- General event retry, transport backpressure, or parallel tool execution redesign.
- Changes to external repositories.

## Risks, assumptions, and gates

- **Stop/go:** handler-only plans must retain their component identity and lease after `PlanComponent.Handlers` is removed.
- **Stop/go:** fresh and resumed descriptor fingerprints must remain equal for the same live plan after ownership deduplication.
- **Stop/go:** the explicit Wasm permission operation must run on the final normalized input and obey module time/input/output bounds.
- **Stop/go:** every remaining non-live `EventKind` must have an identified canonical persistence owner, while live-only kinds must never reach persistence implicitly.
- **Stop/go:** all checked-in Wasm fixtures must be rebuilt after WIT regeneration; generated Go alone is insufficient.
- **Assumption:** the pinned TinyGo and `wasm-tools` commands used by `make wasm-fixtures` remain available. Toolchain absence is an environment blocker, not permission to keep stale binaries.
- No unresolved blocking or non-blocking design decisions remain.

## Document map

- [01-plan-and-event-ownership.md](01-plan-and-event-ownership.md): remove duplicate handler state and implicit event persistence.
- [02-wasm-contract-cleanup.md](02-wasm-contract-cleanup.md): add explicit permission derivation and remove the impossible context role.
- [03-point-identity-cleanup.md](03-point-identity-cleanup.md): delete unused reflection identity.
- [04-execution-handoff.md](04-execution-handoff.md): dependency order, verification gates, commit, and push protocol.
