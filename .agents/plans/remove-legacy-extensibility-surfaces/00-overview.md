# Remove Legacy Extensibility Surfaces

Status: Ready. This document is an implementation plan; no implementation work had occurred when the plan was published.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "f3c242d7f05196a7c557a16ce3c9b48bc347075e716ce9e5989306754c2c6ee1",
    "confirmed_at": "2026-08-26T01:40:22Z"
  }
}
```

The user explicitly stated that the project has no users and that backward compatibility is dead code. Delete obsolete APIs directly. Do not add aliases, forwarding methods, migrations, feature flags, or deprecation periods.

## Change classification

- Change type: breaking internal/public API cleanup and capability-boundary correction.
- Affected areas: `session` store contracts, `store/sqlite`, model-request ledger orchestration, tool definition materialization, composition mounts, session tools, settlement tests, and architecture/consumer documentation.
- Tracking issue: `eino-agent-0go`.

## Requested outcome

The repository has one enforceable write boundary, one model-request ledger boundary, and one tool-composition path. Legacy registries and thin output adapters no longer exist.

Success requires all of the following:

1. Run-owned SQLite writes are unreachable from the top-level `*sqlite.Store` API and execute only through a validated `session.RunFence`.
2. Model-request reads remain available at the top-level store while creates and updates require `session.ExecutionStore`.
3. Enabling the model-request ledger uses the execution-scoped writer without a contradictory top-level capability assertion; disabled ledgers retain the current non-persistence behavior.
4. `composition.Registry` materializes `tools.Definition` directly without `tools.Registry`, `tools.Snapshot`, registration generations, or a second enable/disable pass.
5. Session tools mount through `composition.Registry` and participate in immutable run plans, strict identity, deactivation, and resume.
6. The `tools` output facade and facade-only tests are removed; canonical settlement behavior remains tested in `runtime`.
7. Documentation names the final APIs and all repository quality gates pass.

## Repository findings

Verified facts:

- `session.ExecutionStore` is documented as the run-fenced mutation capability, but `store/sqlite/store.go` exports the same mutations directly on `*Store`.
- `session.ExecutionStore` embeds the combined `session.ModelRequestStore`, while `runtime.NewStreamingOrchestrator` incorrectly asserts that the top-level store implements that combined interface.
- `runtime.streamModel` already audits every provider request even when ledger persistence is disabled.
- Only tests construct `tools.Registry`; `tools/session.Register` is the sole production helper that accepts it.
- `composition.Registry.acquire` creates a one-element `tools.Snapshot` only to materialize one already-selected definition.
- `tools/output.go` forwards to `runtime` and has no production callers.
- The SQLite schema already contains model-request records. No stored-data migration is required.
- The worktree was clean and synchronized with `origin/feat/deeper-extensibility` before planning.

## Target architecture

```text
session.Store
  -> admission, claims, read APIs, ModelRequestReader
  -> Execution(run fence)
       -> all run-owned mutations, ModelRequestWriter

composition.Registry
  -> validates and freezes tools.Definition at mount
  -> seals selected definitions into runtime.RunPlan
  -> tools.Materialize creates one runtime.Tool per bounded scope

tools/session.Mount
  -> composition.Registry.Mount
  -> immutable component identity and scoped tool registrations

runtime
  -> optionally persists the canonical model-request ledger through ExecutionStore
  -> owns ToolOutput, EncodeToolOutput, and BuildToolSettlement
```

## Key decisions

1. Split model-request reads from writes. `session.Store` embeds `ModelRequestReader`; `session.ExecutionStore` embeds `ModelRequestWriter`.
2. Keep ledger persistence opt-in, but make its only write boundary `ExecutionStore`. No-users status removes compatibility obligations but does not decide future prompt-retention policy.
3. Export one definition-level materializer from `tools`; do not preserve registry or snapshot types.
4. Mount session tools through composition with caller-supplied component identity and scope, matching `tools/einotools` and `tools/agui`.
5. Move the SQLite settlement integration test to `runtime`; delete facade-specific unit tests that duplicate runtime behavior.

Rejected alternatives:

- Making ledger persistence mandatory would change privacy, retention, storage-growth, and contention policy beyond the user's compatibility decision.
- Keeping `tools.Registry` as a deprecated facade would preserve two sources of truth and generation semantics that composition already owns.
- Allowing raw SQLite writers for fixtures would leave the production fence bypass intact.
- Feature flags and migrations are unnecessary because the user confirmed there are no users or compatibility obligations.

## Scope and constraints

In scope:

- Direct API deletion and test migration.
- Interface and implementation changes needed to enforce the boundaries.
- Documentation corrections caused by those changes.

Out of scope:

- SQLite schema changes or data migration.
- New tool behavior, permission semantics, or extension lifecycle features.
- Changes to upstream `eino-tools`.
- Unrelated review candidates such as event-sink delivery policy.

## Risks and gates

- Gate: every test fixture that writes run-owned data must first admit a run and derive an execution store from its returned claim token.
- Gate: when enabled, ledger failures must remain fail-closed before provider dispatch and on terminal ledger transitions.
- Risk: removing the snapshot layer can accidentally reapply or omit tool selection. Selection stays in `composition.Registry`; `tools.Materialize` materializes exactly one selected definition.
- Risk: moving session tools to composition changes identity requirements. Tests must prove stable component identity, scope routing, deactivation, and strict plan materialization.
- Risk: broad test edits can hide fencing regressions. Retain stale-fence and atomic settlement assertions at the store contract and SQLite layers.

There are no unresolved blocking or non-blocking decisions.

## Document map

- [01-storage-and-ledger.md](01-storage-and-ledger.md): seal writes and establish the optional ledger's execution-scoped contract.
- [02-canonical-tool-composition.md](02-canonical-tool-composition.md): remove the legacy registry and mount session tools through composition.
- [03-settlement-docs-and-verification.md](03-settlement-docs-and-verification.md): delete thin adapters, migrate coverage, update docs, and run gates.
- [04-execution-handoff.md](04-execution-handoff.md): dependency order, file map, acceptance criteria, and completion protocol.
