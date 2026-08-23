# Strict Resume and Model Lifecycle Fixes

Status: Ready. Planning completed before implementation; this document specifies planned work and does not report delivery status.

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

No compatibility migration, rollout flag, or legacy fingerprint acceptance is required. The fixes must still preserve deterministic descriptors for unchanged definitions and paired lifecycle events for dispatched attempts.

## Change classification

- Change type: correctness and privacy hardening.
- Affected areas: composed-tool durable plan identity in `composition`, model-attempt extension notifications in `runtime`, and targeted regression tests.
- Tracking issue: `eino-agent-791`.

## Requested outcome

Fix both latest review findings:

1. Strict resume rejects a remounted tool when a serialized behavior-affecting definition field changes, including `Retention`, `Concurrency`, or `Metadata`.
2. `ModelCompletedPoint` is emitted only for an attempt that emitted `ModelRequestedPoint`.

Success requires targeted tests to prove each failure mode, unchanged-definition stability, and the existing repository quality gates to pass.

## Scope

- Expand `toolSchemaHash` in `composition/registry.go` to include every deterministic, serialized definition field that affects model exposure or runtime policy: name, description, converted parameter schema, permissions, retry safety, concurrency, retention, and metadata.
- Preserve the existing `session.ExtensionPlanEntry.SchemaHash` and descriptor schema instead of adding a new durable field or version.
- Track whether the model-request notification was emitted during one `streamModel` call and gate the completion notification on that state.
- Add focused tests in `composition/registry_test.go` and `runtime/ledger_test.go` or the nearest existing runtime lifecycle test file.

## Non-goals

- Do not hash callback function identities for `Decode`, `Normalize`, `Encode`, `Execute`, or `Scope`. The component artifact and executor hashes remain the restart-stable identity for executable behavior.
- Do not change public tool or extension event types.
- Do not change ledger state transitions, retry rules, or provider dispatch ordering.
- Do not add feature flags or migration logic.

## Repository findings

- `composition.buildDescriptor` places `toolSchemaHash` in each tool entry's `SchemaHash`, then `session.FingerprintExtensionPlan` includes that value in the durable descriptor fingerprint.
- `toolSchemaHash` currently includes only `Name`, `Description`, converted `Parameters`, `Permissions`, and `RetrySafe`.
- `tools.Definition` also contains deterministic policy/configuration fields `Concurrency`, `Retention`, and `Metadata`; these flow into the materialized `runtime.Tool` and affect execution, retention/privacy, and exposed tool metadata.
- `composition.Registrar.Tool` overwrites provenance from the mounted component. The descriptor separately carries component artifact identity and `ExecutorHash`, so executable callback fields are not suitable JSON hash inputs.
- `runtime.StreamingOrchestrator.streamModel` defers `ModelCompletedPoint` before prompt rendering and ledger preparation. Its current `ledgerTransitionOK` guard starts true, which allows completion notification on failures before `ModelRequestedPoint`.
- `ModelRequestedPoint` is emitted after prompt rendering, request auditing/creation, and an enabled ledger's transition to `ModelRequestDispatchStarted`, but before opening the provider stream.
- Existing composition tests remount the same component around a persisted descriptor and assert `runtime.ErrExtensionPlanMismatch`. Existing ledger tests exercise pre-dispatch audit rejection and dispatched provider panic.
- The worktree was clean before planning on branch `feat/deeper-extensibility`.

## Key decisions

1. Keep the durable descriptor schema unchanged. Expanding the hash input makes new descriptors strict without adding a storage migration.
2. Encode the hash input as a named struct and use `encoding/json`. Go's JSON encoder sorts string-keyed map keys, so equivalent metadata maps remain deterministic.
3. Hash the declared `Concurrency` value, including the empty value. Empty and explicit `parallel` materialize identically today, but the fingerprint must identify the mounted definition as declared and detect remount changes.
4. Set a local `modelRequested` flag at the `ModelRequestedPoint` emission site. Gate `ModelCompletedPoint` on both `modelRequested` and successful ledger finalization.
5. Keep completion for provider-open errors, nil streams, receive errors, cancellation, and panics because those paths occur after the request notification.

## Change model

```text
Tool remount
  -> serialize schema + runtime policy + metadata
  -> SHA-256 SchemaHash
  -> extension plan fingerprint
  -> strict resume match or ErrExtensionPlanMismatch

Model attempt
  -> render prompt
  -> audit/create request record
  -> mark dispatch started when ledger-enabled
  -> emit ModelRequested and set modelRequested
  -> open/read provider stream
  -> finalize ledger
  -> emit ModelCompleted only if modelRequested && ledgerTransitionOK
```

## Risks and assumptions

- Assumption: component `Artifact.Hash` and the derived `ExecutorHash` remain the contract for executable callbacks and scope resolution.
- Risk: using a broad struct such as `tools.Definition` would try to encode functions and fail. The implementation must enumerate only deterministic data fields.
- Risk: setting `modelRequested` too early would preserve the unmatched completion bug for notification-free failures. Set it only at the notification call site.
- Risk: setting `modelRequested` after provider open would suppress completion for attempts whose dispatch notification was emitted but whose provider open failed.
- Test requirement: cover failure of the ledger transition to `ModelRequestDispatchStarted`, which is the latest failure boundary before `ModelRequestedPoint`, with an active notification dispatch.

## Stop/go gates

- Stop if expanding the hash requires a durable descriptor field or schema-version change; reassess compatibility before implementation.
- Stop if existing event semantics define completion independently of request notification; no such contract was found during planning.
- Go when targeted tests demonstrate hash sensitivity and exact request/completion pairing, including a live-dispatch failure at the dispatch-start ledger transition.

## Document map

- [01-strict-tool-fingerprint.md](01-strict-tool-fingerprint.md): expand and verify the deterministic tool definition hash.
- [02-model-lifecycle-pairing.md](02-model-lifecycle-pairing.md): pair request and completion notifications around dispatch.
- [03-execution-handoff.md](03-execution-handoff.md): dependency order, commands, acceptance gates, and completion protocol.
