# Component-Owned Run Plans

## Goal and prerequisite

Persist each component identity and artifact once, with typed nested capability identities. Start after semantic handler kinds from work package 1 are fixed.

## Existing evidence

- `session/extensions.go` defines five top-level identity collections that each repeat component ownership.
- `ValidateExtensionPlan` maintains an artifact-consistency map because the same component can appear in every collection.
- `FingerprintExtensionPlan` sorts five collections through five comparator families.
- `runtime.NewRunPlan` first groups handler diagnostics by instance but appends every other capability directly.
- `composition.Registry.AcquireResumePlan` walks all collections to rebuild instance and tool-selection maps.

## Proposed durable model

Implement the following proposed shape in existing `session/extensions.go`; exact private helper names may change.

```text
ExtensionPlanDescriptor
  SchemaVersion
  Fingerprint
  Components []ComponentPlan

ComponentPlan
  InstanceID
  Artifact
  Handlers []RegistrationIdentity
  Tools []ToolPlanIdentity
  Prompts []PromptPlanIdentity
  Guards []GuardPlanIdentity
  Restrictions []RestrictionPlanIdentity
```

Nested capability identities must remove `InstanceID` and `Artifact`. They retain the names, registration IDs, scopes, ordering, schema/executor hashes, and rules hashes that define behavior.

## Validation and fingerprint invariants

- A descriptor contains at most one `ComponentPlan` per instance ID.
- `extension.ValidateComponent` validates the component once.
- Each nested identity validates its own stable IDs, scope, hashes, semantic handler kind, and duplicate key.
- Validation rejects an empty `ComponentPlan`. Fresh construction omits components that contribute no executable behavior, so there is exactly one canonical descriptor for a behavior set.
- All session-scoped identities in the complete descriptor use one session key.
- Fingerprinting clones the descriptor, clears `Fingerprint`, sorts each nested collection, sorts components by component identity, normalizes empty slices, and hashes canonical JSON.
- Handler ordering remains part of identity. Tool order remains in the existing schema hash. Prompt and guard order remain explicit.
- Schema version may remain `1` because no persisted consumer exists. Do not add old-shape decoding or migration.

## Runtime and composition changes

- Change the executable input to preserve ownership after nested durable identities drop it. Prefer `ComponentRunPlanSpec{Component extension.Component, Handlers, Tools, Prompts, Guards, Restrictions}`; an equivalent owner-bearing wrapper is acceptable only if every capability is impossible to construct without an `extension.Component`.
- `runtime/extension_plan.go`: build a private component accumulator keyed by the explicit component owner from dispatch diagnostics and every behavior-bound capability.
- Validate conflicting artifacts while accumulating, then emit one descriptor component per key.
- Keep executable `RunPlan` slices separate from the durable nested representation.
- `composition/registry.go`: construct nested identity values without repeated owner fields.
- `AcquireResumePlan`: validate once, loop over `persisted.Components`, populate the selected instance set, validate scopes, and populate exact tool identity keys using the enclosing component ID.
- Preserve exact current fingerprint comparison before and after live plan acquisition.
- Update run/admission/storage JSON tests directly; do not retain fixtures for the old shape.

## Exact change surface

- `session/extensions.go` and `session/extensions_test.go`.
- `runtime/extension_plan.go`, `runtime/run_plan_test.go`, `runtime/extensions_test.go`, and resume/provider tests.
- `composition/registry.go`, `composition/registry_resume_test.go`, and identity/scope tests.
- Any store or transport fixture that constructs `ExtensionPlanDescriptor` literals.
- `docs/architecture/extension-points.md`, `docs/architecture/runtime.md`, `docs/architecture/storage.md`, and `docs/consumer-guide.md` where they describe descriptor shape.

## Tests and acceptance criteria

- Descriptor validation rejects duplicate components, conflicting nested identities, invalid scopes, and invalid semantic handler kinds.
- Descriptor validation rejects empty components; injecting one changes neither the accepted fingerprint domain nor resume selection because it is invalid before either operation.
- Fingerprints are independent of component and nested-slice input order.
- Fingerprints change for every behavior-relevant nested field.
- Fresh plan construction emits one component record even when that component contributes all capability kinds.
- Tool-only, prompt-only, guard-only, and restriction-only component specs retain their owner and artifact and emit the correct component record without relying on dispatch diagnostics.
- Every executable behavior instance ID is validated against its enclosing component owner.
- Resume selects only persisted components and exact persisted tools.
- Foreign session scope, missing component, changed artifact, added behavior, removed behavior, and changed behavior all fail before resumed execution.
- The repeated artifact-consistency map and five top-level instance reconstruction loops are deleted.

## Risks and exclusions

- Preserve typed nested lists; do not replace them with a tagged `any` payload.
- Do not add migration code or incrementally support both descriptor shapes.
- Keep component selection stable under map iteration by canonical sorting before fingerprinting.
