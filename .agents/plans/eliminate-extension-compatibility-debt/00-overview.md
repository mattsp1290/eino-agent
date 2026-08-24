# Eliminate Extension Compatibility Debt

Status: Implemented. Reviewed by two independent reviewers; accepted corrections applied.

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

The user explicitly confirmed that no external users exist and backward compatibility is dead code. Delete undeployed API, descriptor, persistence, and orchestration compatibility paths instead of migrating or flagging them.

## Change classification

- Change type: breaking architectural simplification and correctness hardening.
- Affected areas: `runtime`, `composition`, `session`, `store/sqlite`, `model`, `providers/fake`, `wasmext`, tests, examples, and architecture/consumer documentation.
- Tracking issue: `eino-agent-26t`.

## Requested outcome

Implement the accepted thermo-nuclear review findings:

1. Seal executable run plans to runtime-computed durable descriptors.
2. Remove the undeployed legacy extension pipeline and compatibility modes.
3. Make tool settlement uniformly atomic.
4. Give final normalized tool input sole authority over permission patterns.
5. Replace reflective cloning and silent serialization fallback with explicit error-returning request boundaries.

## Success criteria

- A `RunPlanProvider` cannot return an independently fingerprinted descriptor; runtime computes the descriptor from validated plan evidence.
- `StreamingOrchestrator` has no direct `Tools`, `Context`, `Hooks`, or `Middleware` execution fields and no compatibility-only system-prompt flag.
- `session` has no legacy or partial-legacy plan mode and accepts only the current descriptor schema.
- Every claimed tool call reserves result IDs and commits through `session.Store.SettleToolCall`; runtime contains no ordered multi-write fallback or reconciliation scan.
- `ToolPreparePoint` can change normalized JSON input but cannot independently change `ToolCall.Pattern`; permission evaluation derives the pattern once from final input.
- `model.Request` cloning and request hashing return errors for unsupported values; production code contains no generic reflective graph cloner and no ignored request-serialization error.
- `make check` and `git diff --check` pass, the Beads issue closes, and the branch is committed and pushed.

## Repository findings

- `runtime.RunPlan` exposes executable fields and `Descriptor` independently. `acquireRunPlan` verifies only the descriptor's self-hash.
- `runtime.RequiresToolSettlement` is populated by `composition.Registry` but runtime derives behavior from the descriptor instead, so the field does not enforce its stated invariant.
- `StreamingOrchestrator` runs direct tools/context/hooks/middleware before or after plan-backed extension points.
- `session.PlanPartialLegacy`, `session.PlanLegacy`, schema-v1 fingerprint rules, and repair tests exist only for undeployed compatibility.
- `runtime.commitToolSettlement` retains a non-atomic `FinishToolCall`/`AppendMessage`/`AppendPart` fallback. Resume compensates through `ListUnreconciledToolSettlements`.
- `runtime.prepareToolCalls` copies `PreparedToolCall.Call.Pattern` and immediately overwrites it from JSON input.
- `model.Request.Clone` uses a generic reflection graph walker. Runtime also uses JSON clones that return `nil` on failure, while `modelRequestContentHash` ignores `json.Marshal` errors.

## Key decisions

1. **Runtime seals plans.** Introduce proposed `runtime.RunPlanSpec` and `runtime.NewRunPlan`. Each capability record binds identity and behavior in one value; the constructor freezes tools and derives the descriptor from those records and `extension.Plan.Diagnostics`. Providers cannot supply an arbitrary registry or parallel descriptor evidence. `RunPlan.Descriptor()` returns a defensive clone for persistence/resume comparison while executable state stays private.
2. **One extension pipeline.** Keep infrastructure `EventSink` and host `permissions.Policy` separate. Route tools, context contributions, lifecycle hooks, and tool transforms only through `composition.Registry` and typed extension points.
3. **One descriptor format.** Retain `SchemaVersion` for future evolution, but remove `PlanMode` and all schema-v1 behavior. Reject every descriptor whose version is not `session.ExtensionPlanSchemaVersion`.
4. **Atomic settlement is a store invariant.** Add `SettleToolCall` to `session.Store` and delete the optional settlement interface and reconciliation method. Every option-built or direct-struct orchestrator therefore has the same atomic contract.
5. **Canonical request data is error-returning.** Change `model.Request.Clone` and `extension.CloneFunc` to return errors. Use explicit typed copies for Eino messages, reject nonzero non-serializable fields and all nonempty `Extra` maps, and reuse one canonical audited projection for hashes and extension views.

Rejected alternatives:

- Keeping public `RunPlan` fields and adding more post-hoc checks preserves two sources of truth.
- Keeping partial-legacy mode but making it unreachable preserves branches and tests with no consumer.
- Retaining non-atomic settlement for stores without `ToolSettlementStore` contradicts the runtime's durable tool invariants.
- Honoring a separately supplied `Pattern` lets permission authority diverge from executed input.
- Keeping `Clone() Request` with silent zero-value fallback prevents callers from reacting to invalid provider metadata.

## Target architecture

```text
composition.Registry.acquire
  -> select callbacks + tools + prompts + guards + restrictions
  -> runtime.NewRunPlan(RunPlanSpec{typed behavior + identity evidence})
       -> validate current schema evidence
       -> derive descriptor entries
       -> fingerprint descriptor
       -> return opaque RunPlan

StreamingOrchestrator.Start/Resume
  -> acquire opaque RunPlan
  -> use the plan's single tools/context/hook/transform pipeline
  -> reserve tool result IDs before claim
  -> execute claimed call
  -> session.Store.SettleToolCall exactly once
  -> release plan
```

## Scope and constraints

- Delete source-level APIs and stored-shape compatibility that exist only for undeployed behavior.
- Preserve the current extension contract IDs, callback ordering, Wasm WIT ABI, permission semantics, and model-visible tool output shape unless this plan explicitly changes authority.
- Do not add feature flags, migrations, adapter shims, or deprecation wrappers.
- Preserve `EventSink` as infrastructure delivery and `permissions.Policy` as host authority.
- Keep every modified production Go file below 1,000 lines.

## Risks and gates

- Stop if removing a direct extension option reveals a production call site outside tests, examples, or documentation; none exists in current repository evidence.
- Stop if a store used by production packages cannot implement atomic settlement. Current SQLite already does.
- Treat any request-clone error after durable model dispatch begins as a correctness failure; canonicalization must finish before the ledger enters `dispatch_started`.
- Plan release must remain exactly once on every fresh/resume error and panic path.
- No blocking decisions remain.

## Document map

- [01-sealed-plan-and-single-pipeline.md](01-sealed-plan-and-single-pipeline.md): opaque plan construction, current-only descriptors, and legacy pipeline deletion.
- [02-atomic-tool-lifecycle.md](02-atomic-tool-lifecycle.md): one atomic settlement and one permission-pattern authority.
- [03-explicit-model-request-boundary.md](03-explicit-model-request-boundary.md): checked cloning, canonical hashing, and extension request projections.
- [04-verification-and-documentation.md](04-verification-and-documentation.md): repository-wide cleanup and gates.
- [05-execution-handoff.md](05-execution-handoff.md): dependency order and definition of done.
