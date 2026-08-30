# Thermo Review Structural Remediation

Status: Implemented and verified after two independent plan reviews.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "b105070409a3af52b816d2f433b70c2ce3947ab672dc4b349c23fd9e3a6cf9fb",
    "confirmed_at": "2026-08-29T11:15:12-04:00"
  }
}
```

The user confirmed that this code has no users and that backward compatibility is dead code. Replace weak public contracts directly. Do not add aliases, deprecated fields, adapters, feature flags, migrations, or dual implementations.

## Change type and affected areas

This is a greenfield structural refactor across package boundaries. It removes parallel identity and behavior models, duplicated event envelopes, write-only provenance, and an inconsistent WASM registration API.

Affected production areas, excluding `docs/` and excluding `examples/` from design review:

- `runtime`: run-plan sealing, capability validation, event types, event queues, and publication.
- `composition`: canonical plan input, tool source identity, resume fingerprints, and registrar ownership.
- `session`: the canonical event record already used by durable storage.
- `agui` and `stream`: event consumption, replay, live-tail behavior, and transport error checks.
- `tools` and `tools/einotools`: definition state and source identity construction.
- `wasmext`: mount-level registration methods and lifecycle tests.

## Requested outcome

Resolve Beads issues `eino-agent-cdk`, `eino-agent-tn9`, `eino-agent-nwx`, and `eino-agent-frw` without preserving unused compatibility surfaces.

Success means:

- One run-plan sealing boundary derives each `session.*PlanIdentity` from component-owned capability inputs and rejects structural conflicts before run admission.
- `session.EventRecord` is used directly by runtime, AG-UI, WASM observers, and live tail. Runtime no longer exposes a duplicate event, event-kind, usage, error, or redaction type or handwritten durable-to-runtime converter.
- Runtime event publication is explicitly best-effort. Queue behavior, persisted-event publication, extension notification, and AG-UI replay apply that contract consistently.
- `tools.Definition` contains executable tool behavior only. It contains no component provenance written by composition.
- `composition.ToolRegistration` carries one optional source-identity value rather than two independently optional hash fields.
- Resume fingerprints still include both source-aware schema identity and source-aware executor identity.
- Every public `wasmext.Loader.Register*` method accepts `*composition.Registrar`. Installers do not call `Registrar.Extensions()` to mount WASM capabilities.
- Focused package tests, `make check`, and `git diff --check` pass.

## Scope and constraints

In scope:

- Direct breaking changes to the current Go API.
- Deleting public construction fields and types that only serve the current repository.
- Updating production code and tests outside `docs/` and `examples/`.
- Mechanical compile-only updates under `examples/` when a removed API otherwise prevents the full repository gate from compiling. Examples remain excluded from design and quality review.
- Preserving current durable event storage and replay behavior.
- Preserving mount rollback, cleanup, and run-plan release behavior.

Non-goals:

- Documentation updates or behavioral redesign of examples.
- New event durability guarantees.
- Reliable delivery, retries, buffering, or backpressure for observation sinks.
- A generic capability type that combines tools, prompts, guards, and restrictions into one tagged union.
- Moving `composition` into `runtime` or introducing a new cross-package framework.
- Compatibility aliases for removed runtime event or tool provenance types.

## Repository-grounded findings

- `composition.Registry.acquire` constructs `runtime.PlanTool`, `runtime.PlanPrompt`, `runtime.PlanGuard`, and `runtime.PlanRestriction` with caller-supplied `session.*PlanIdentity` values. `runtime.NewRunPlan` then verifies and copies those parallel identity and behavior graphs.
- `runtime.NewRunPlan` derives handler identities from `extension.Plan` but accepts all other capability identities from its caller. Prompt-name conflict detection also occurs later in `runtime.renderSystemPrompt`.
- A tool resolver remains dynamic because it materializes a tool from `runtime.ToolScopeContext`. Its returned name and cloneability cannot be proven at plan-seal time and require a narrow execution-time guard.
- `runtime.Event` duplicates every durable `session.EventRecord` field except `ToolTransition`, while renaming `ID` to `EventID` and `CreatedAt` to `Time`. `runtime.EventKind`, `runtime.Usage`, `runtime.EventError`, and `runtime.RedactionClass` duplicate string or session contracts.
- `runtime.runtimeEventRecord` and `agui.runtimeEvent` manually copy the same event fields. Both conversions can drift from `session.EventRecord`.
- `runtime.eventQueue` discards `EventSink.Emit` errors. Persisted event publication also discards them by design. The return value therefore promises propagation that runtime cannot provide, while an unrecovered panic can still terminate publication.
- `agui.Bridge` records encoder and transport errors internally. Replay and reconnect can inspect `Bridge.Err()` after synchronous emission without requiring runtime observation sinks to return errors.
- `composition.Registrar.Tool` writes component and artifact provenance into `tools.Definition`. Only `Provenance.ExecutorHash` is later read, even though the component already owns artifact identity.
- `composition.ToolRegistration.SourceSchemaHash` and `SourceExecutorHash` permit an invalid half-present state that requires paired-string validation.
- `wasmext.Loader.RegisterTool` accepts `*composition.Registrar`, while the four other public registration methods accept `extension.Registrar`. Composition installers therefore cross the abstraction through `Registrar.Extensions()`.

## Key decisions

1. Keep `runtime.RunPlan` opaque but replace identity-bearing plan inputs with capability inputs whose registration fields and behavior live in one value. `runtime.NewRunPlan` remains the cross-package sealing entry point because `composition` cannot initialize unexported runtime state.
2. Derive all `session.*PlanIdentity` values and restriction hashes inside `runtime.NewRunPlan`. Validate duplicate prompt names, capability IDs, owner conflicts, scopes, and required behavior before returning a plan.
3. Retain only dynamic execution checks that construction cannot prove: context cancellation, resolver failure, resolved tool name equality, and defensive cloning.
4. Delete `runtime.Event` and `runtime.EventKind`. Use `session.EventRecord` directly and keep untyped runtime event-kind constants for readable construction and switching.
5. Change `EventSink.Emit` to return no error. Runtime sinks are observation outputs after durable mutation or live best-effort outputs, not transaction participants.
6. Route infrastructure publication through one panic-isolating helper. Recovered infrastructure panics are intentionally dropped because the sink is best-effort; extension notification still runs.
7. Make AG-UI replay and reconnect call `Bridge.Emit` and then inspect `Bridge.Err()`. This preserves synchronous transport failure reporting at the transport boundary.
8. Replace the two source hash fields with an immutable value-form `composition.ToolSourceIdentity`. Its zero value means absent; its only nonzero construction path validates two lowercase SHA-256 hashes.
9. Remove `tools.Provenance`. Compute schema and executor identities during plan acquisition from the staged source identity, frozen definition, and mounted component artifact.
10. Route all public WASM loading methods through `*composition.Registrar`; keep raw `extension.Registrar` use inside private adapter functions.

Rejected alternatives:

- Keeping runtime event aliases for individual duplicated types leaves two event envelopes and conversion code.
- Returning event sink errors through a new asynchronous error channel changes orchestration semantics and creates a second run-failure authority for observation infrastructure.
- Removing the resolved-tool name check assumes arbitrary resolver behavior can be validated before it runs.
- A single combined source hash loses the independent schema and executor identities required by resume fingerprints.
- Exposing `extension.Registrar` from WASM loader methods preserves the exact boundary leak under review.

## Target architecture

```text
composition mount payload
  -> component-owned capability inputs
  -> runtime.NewRunPlan sealing boundary
       -> validate owners, registrations, scopes, and static conflicts
       -> derive session plan identities and restriction hashes
       -> seal descriptor fingerprint
       -> retain executable behavior in opaque RunPlan

session.EventRecord
  -> runtime best-effort EventSink
       -> stream.Tail live distribution
       -> agui.Bridge transport projection
       -> extension EventPublishedPoint notification
  -> AG-UI durable replay without envelope conversion

ToolRegistration{SourceIdentity?} + owning Component.Artifact
  -> schema fingerprint + executor fingerprint
  -> run-plan descriptor
```

## Risks, assumptions, and gates

- Blocking decisions: none. The user resolved users, compatibility, and feature flags.
- Stop if any production package outside `composition` constructs a run plan. Repository inspection currently finds only `composition`; runtime test helpers are not compatibility surfaces.
- Stop if a sink error currently changes a production run outcome. Repository inspection shows event queues and persisted publication discard sink errors; focused tests must prove the explicit best-effort contract.
- Preserve payload defensive copies at every fan-out boundary. Clone payload bytes when the event queue assumes asynchronous ownership and once for every live-tail subscriber.
- Isolate infrastructure sink panics in live and persisted publication. A panic must not skip extension notification, abort an admitted run, strand queue closure, or suppress later queued events.
- Preserve `ToolTransition` on durable events. The canonical event envelope must not silently zero a field during replay or live publication.
- Preserve snapshot release on every plan-sealing failure and exactly once on normal release.
- Preserve deterministic ordering and fingerprints across mount order and resume acquisition.
- No rollback or migration path is required because there are no users or deployed compatibility promises. Git revert is the recovery mechanism before release.

## Document map

- `01-run-plan-sealing.md`: collapse capability identity and behavior construction into one sealing boundary.
- `02-canonical-events.md`: use the durable event record everywhere and make sink delivery best-effort.
- `03-tool-identity-and-wasm-registration.md`: remove provenance state and expose one mount-level WASM API.
- `04-execution-handoff.md`: order implementation, verification, issue closure, commit, and push.
