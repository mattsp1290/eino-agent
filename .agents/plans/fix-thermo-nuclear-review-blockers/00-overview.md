# Fix Thermo-Nuclear Review Blockers

Status: Complete. Planning, three independent reviews, implementation, and repository verification are complete.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "0325964984c7b5060211e863f6ba829bc8a9d2c59424046aa5f127ffde626b6a",
    "confirmed_at": "2026-08-24T01:04:44Z"
  }
}
```

The user explicitly confirmed that the project has no users and backward compatibility is dead code. Delete superseded APIs, test-only bypasses, and stranded integration paths. Do not add adapters, aliases, migrations, deprecations, feature flags, or dual behavior.

## Change classification

- Change type: breaking architectural simplification and invariant hardening.
- Affected areas: `session`, `store/sqlite`, `store/storetest`, `runtime`, `composition`, `tools`, `tools/agui`, `agui`, `wasmext`, examples, tests, and architecture/consumer documentation.
- Tracking issue: `eino-agent-fcf`.

## Requested outcome

Apply the accepted thermo-nuclear review fixes and deliver the result on the current branch:

1. Make atomic tool settlement the only terminal store contract.
2. Route AG-UI client tools through `composition.Registry` and sealed run plans.
3. Delete the unenforced runtime tool-concurrency contract.
4. Remove tests that bypass sealed run-plan construction.
5. Stop exposing the mutable `TurnSnapshot` object graph to tool callbacks.
6. Run all quality gates, commit the related changes, and push them.

## Success criteria

- `session.Store` exposes `SettleToolCall` but not `FinishToolCall`; SQLite has no exported terminal-call-only write.
- AG-UI client tools mount as session-scoped `composition.ToolRegistration` values and enter runs through `runtime.RunPlanProvider`.
- `runtime.ToolConcurrency`, `Tool.Concurrency`, `ToolScope.ConcurrencyKey`, and `tools.Definition.Concurrency` no longer exist.
- Runtime tests construct executable plans through `runtime.NewRunPlan` with valid identity-bound capabilities or through `composition.Registry`; no test mutates private `RunPlan` fields after construction.
- Every plan-tool resolver, tool scope resolver, and executor receives a bounded data-only context with no Eino messages, provider clients, tool executors, or configuration object graph.
- Admission deep-clones model messages through an explicit error-returning boundary before retaining them.
- `make check`, `git diff --check`, focused structural searches, and the final clean/up-to-date Git status pass.

## Repository findings

- `session.Store` still includes both `FinishToolCall` and `SettleToolCall`; only SQLite and tests use the former.
- `tools/agui.Registry` still implements the removed aggregate `runtime.ToolRegistry` integration, while `StreamingOrchestrator` now accepts tools only through `RunPlanProvider`.
- `runtime/tool_preparation.go` executes every prepared tool in one serial loop and never reads the declared concurrency mode or key. Built-in mutable tools already enforce safety inside their own implementations.
- `runtime/run_plan_test.go` explicitly preserves older orchestration behavior by overwriting `RunPlan.tools`, and many tests construct private plan fields directly.
- `runtime.TurnSnapshot.Clone` copies message pointers, while `runtime.cloneMessages` copies only the first container layer. `tools.ScopeResolver` and `tools.Execution` receive that graph.
- The current branch is clean and `make check` passed before implementation planning.

## Key decisions

1. **One terminal store operation.** Remove `FinishToolCall` from `session.Store`. Keep the conditional SQLite row update as an unexported helper called only inside `SettleToolCall`'s transaction.
2. **Mount client tools; do not aggregate registries.** Replace the stateful `tools/agui.Registry` with a direct mount adapter that converts one `agui.ClientToolSnapshot` into typed `tools.Definition` values and mounts them in `composition.Registry` for that session.
3. **Use explicit client-tool JSON results.** Replace the undeployed rich `runtime.ToolResult` dispatcher contract with validated `json.RawMessage`. This is an intentional breaking simplification: client tools produce exactly one canonical JSON result value, with no attachment or per-call metadata channel.
4. **Delete runtime concurrency metadata.** Existing tools keep their internal mutex/locker behavior. No replacement scheduler is added because the declared runtime modes have never controlled execution.
5. **Bound tool-visible run data end to end.** Add data-only scope and execution contexts derived from `TurnSnapshot`; `PlanTool.Resolve`, internal plan materialization, typed-tool scope resolution, and execution callbacks receive only the appropriate bounded value. Remove the public `runtime.ToolRegistry` callback abstraction rather than preserving another full-snapshot escape hatch.

Rejected alternatives:

- Keeping `FinishToolCall` as a deprecated alias preserves the invariant bypass.
- Restoring `StreamingOrchestrator.Tools` recreates the deleted dual pipeline.
- Teaching runtime to schedule by unused concurrency metadata creates a second locking authority alongside existing tool-owned locks.
- Deep-copying arbitrary `TurnSnapshot` graphs before every tool callback preserves excessive authority and clone complexity.
- Retaining private-field test fixtures leaves the sealed-plan contract untested.

## Target architecture

```text
AG-UI request tool definitions
  -> tools/agui.MountClientTools
  -> composition.Registry.Mount(session-scoped typed definitions)
  -> runtime.NewRunPlan(identity-bound PlanTool values)
  -> runtime derives ToolContext from internal TurnSnapshot
  -> tool scope/decoder/executor receives bounded context
  -> runtime.BuildToolSettlement
  -> session.Store.SettleToolCall (only terminal API)
```

## Scope and constraints

- Preserve permission evaluation, extension ordering, and durable resume rejection on plan mismatch. AG-UI output is intentionally narrowed to one validated JSON result value because there are no deployed consumers.
- Preserve internal locking in `tools/einotools` and `tools/session` while deleting only unenforced runtime concurrency metadata.
- Preserve AG-UI client tool names, JSON schemas, permissions, and metadata. Bind a host-supplied, restart-stable dispatcher artifact identity into each mount so executable behavior participates in the plan fingerprint.
- Do not preserve source compatibility for removed APIs.
- Do not add stored-data migrations; no released database requires them.
- Keep modified production files below 1,000 lines.

## Risks, assumptions, and gates

- Client-tool remount lifecycle remains host-owned: close the previous session mount before mounting a new generation. Active plans retain their existing lease until release.
- An interrupted run resumes only when the same client-tool generation is mounted and its descriptor fingerprint matches; this is the existing strict plan rule.
- Stop if a production package outside the documented AG-UI integration uses `runtime.ToolRegistry` as an orchestrator input; current searches found none.
- Stop if bounded tool context lacks data used by a production tool; repository searches currently show only session identity, workspace identity/root, and bounded turn metadata.
- No blocking decisions remain.

## Document map

- [01-atomic-settlement-and-dead-concurrency.md](01-atomic-settlement-and-dead-concurrency.md): remove the terminal write bypass and unused concurrency contract.
- [02-canonical-agui-client-tools.md](02-canonical-agui-client-tools.md): mount client tools through composition and update integration guidance.
- [03-bounded-tool-context.md](03-bounded-tool-context.md): replace mutable snapshot exposure with an explicit data boundary.
- [04-sealed-plan-tests-and-verification.md](04-sealed-plan-tests-and-verification.md): rebuild fixtures through production constructors and define structural gates.
- [05-execution-handoff.md](05-execution-handoff.md): dependency order, commands, and definition of done.
