# Thermo-Nuclear Quality Fix Plan

Status: implemented and verified on 2026-08-27

Tracking issues: `eino-agent-9rk`, `eino-agent-e76`, `eino-agent-30b`, `eino-agent-7hv`, `eino-agent-7o1`

## Confirmed application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "5a103722a881d753dad5dc19a7477507a63908b97e76558af273f811356ef23a",
    "confirmed_at": "2026-08-25T00:33:30Z"
  }
}
```

This is a breaking architectural cleanup. There are no users to migrate, so compatibility aliases, deprecated wrappers, dual code paths, and feature flags are explicitly out of scope. Durable schema version 1 remains unchanged because this work tightens validation and internal identity representation without changing the persisted envelope.

## Goal

Remove the remaining maintainability debt identified by the thermo-nuclear review: fragmented model identity, duplicated extension identity systems, unused notification policy machinery, opaque Wasm handles, and giant test files.

## Success criteria

- A fresh run has exactly one complete resolved model identity and a non-nil streamer before admission side effects.
- Admission, invocation, and durable snapshots use resolved model identity directly; configuration fallback logic is gone.
- Live and durable extension paths share the canonical `extension.Artifact` and `extension.Scope` types and validation rules.
- Duplicate detection uses structured keys rather than delimiter-concatenated strings.
- Extension notifications have one contained-failure behavior and no unused return-policy API.
- Wasm context sources, hooks, and tool middleware register directly through `Loader`; no public opaque `Loaded*` handles remain.
- `composition/registry_test.go` and `runtime/orchestrator_test.go` are decomposed into cohesive files, each below 1,000 lines.
- Focused tests and `make check` pass; the final diff contains no compatibility scaffolding or stale documentation.
- All five Beads issues are closed, changes are committed, Beads data is pushed, the Git branch is pushed, and the worktree is clean and synchronized.

## Scope

- Model resolution/admission: `model`, `runtime`, related architecture docs and tests.
- Extension identity: `extension`, `session`, `composition`, `runtime`, related tests.
- Notification containment: `extension`, `runtime`, related docs and tests.
- Wasm registration and ownership: `wasmext`, consumer docs/examples and tests.
- Test decomposition: the two oversized test files and shared test helpers.

Historical material under `docs/prompts/` and completed plan artifacts is not rewritten. No durable schema migration, feature flag, or compatibility layer will be added.

## Evidence behind the plan

- `runtime/admission.go` reconstructs provider/model identity from configuration when resolved fields are absent.
- `runtime/context.go` falls back to configured identity when the resolved pair is incomplete.
- `session/extensions.go` duplicates extension artifact/scope concepts and forms duplicate keys with NUL-delimited strings.
- `extension/notify.go` exposes a return-failures mode while runtime callers discard the result.
- `wasmext` returns exported concrete `Loaded*` wrappers that consumers must pass back into registration functions.
- `composition/registry_test.go` and `runtime/orchestrator_test.go` exceed the review threshold.

## Architecture decisions

### Resolved model identity is authoritative

Add one model-owned validator, tentatively `model.ValidateResolved(selection, resolved)`, that checks requested provider/model constraints, `resolved.Provider.ID`, `resolved.Model.ID`, `resolved.Model.ProviderID`, and non-nil streamer. Call it immediately after a custom resolver returns and again at direct admission boundaries. Delete `admissionProviderID`, `admissionModelID`, and `snapshotModelIdentity`; fresh-run admission and snapshots use the existing nested resolved fields directly. Do not add flat identity fields to `model.Resolved`.

Resume-only snapshots may omit a live streamer because resume currently settles recorded unfinished tool calls without dispatching a new model request. Any future resume path that invokes a model must resolve and validate afresh first.

### One extension identity vocabulary

Use `extension.Artifact` and `extension.Scope` directly in `session` and delete `session.ArtifactIdentity` and `session.ExtensionScope`. Export only the minimal validation helpers required by downstream packages. Scope keys remain opaque strings. Replace concatenated duplicate keys with private comparable structs whose fields express the identity dimensions.

If importing `extension` from `session` creates a cycle, stop and move the shared value types into a dependency-neutral leaf package; do not preserve parallel type systems.

### Notifications are always contained

Make `extension.Notify` return nothing. Delete `NotificationPolicy`, `NotificationContained`, `NotificationReturnFailures`, `Failures`, accumulation state, and registry policy fields. A failing observer is reported through the existing failure reporter and iteration continues.

### Loader registers Wasm extensions directly

Make all concrete loaded wrappers private. Add direct `Loader.RegisterContextSource`, `Loader.RegisterHook`, and `Loader.RegisterToolMiddleware` methods accepting context, registrar, registration metadata, and module config. A method loads the module, stages callbacks through the registrar, and attaches rollback-aware ownership before mount preparation can succeed.

`Loader` remains the sole long-lived owner of successfully mounted modules and must outlive active mounts. A failed mount preparation must publish nothing and retain no module ownership: registration installs a rollback effect or equivalent ownership token that atomically untracks and finalizes the module. Define and test races between rollback and `Loader.Close`. Mount deactivation does not independently close a successfully mounted module.

### Split tests by behavior, not arbitrary line ranges

Move cohesive test families and their local helpers into named files. Preserve assertions and test names while eliminating duplicated fixtures. Perform this after behavior-changing work so semantic diffs stay reviewable.

## Target flows

```text
fresh request
  -> Resolver.Resolve
  -> model.ValidateResolved
  -> Admitter.Admit (validates direct callers)
  -> snapshot resolved provider/model IDs
  -> invoke resolved streamer
```

```text
extension artifact + scope
  -> extension validation
  -> session structured identity keys
  -> composition registry
  -> runtime plan and durable fingerprint
```

```text
Wasm ModuleConfig
  -> Loader.Register*
  -> instantiate private wrapper
  -> stage callbacks in registrar
  -> loader tracks module ownership
  -> installer commits mount
```

## Risks and controls

- Stronger admission validation may expose incomplete test fixtures: update them with explicit resolved identity and a dummy streamer rather than restoring fallbacks.
- Direct Wasm registration couples module, mount, and loader lifetimes: document and test close behavior, staging/commit failures, rollback, and close races.
- Shared identity types may reveal an import cycle: use a leaf package only if the cycle is real.
- Mechanical test moves can hide behavior changes: make them last and verify with focused tests plus `git diff --check`.

## Work documents

1. [Canonical model and extension identity](./01-canonical-model-and-extension-identity.md)
2. [Contained notifications](./02-contained-notifications.md)
3. [Direct Wasm registration](./03-direct-wasm-registration.md)
4. [Test decomposition and verification](./04-test-decomposition-and-verification.md)
5. [Execution handoff](./05-execution-handoff.md)
