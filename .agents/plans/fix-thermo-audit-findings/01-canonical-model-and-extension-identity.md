# Canonical Model and Extension Identity

Issues: `eino-agent-9rk`, `eino-agent-e76`

## Objective

Make model and extension identity complete, validated, and authoritative at the boundary where each object enters the runtime. Remove downstream reconstruction and duplicate representations.

## Model resolution changes

### `model/provider.go`

- Add `ValidateResolved(selection Selection, resolved Resolved) error`.
- Require non-empty `resolved.Provider.ID` and `resolved.Model.ID`.
- Require `resolved.Model.ProviderID` to equal `resolved.Provider.ID`.
- Require a non-nil streamer.
- When the request selects a provider or model explicitly, require the resolved values to match.
- Return field-specific errors so resolver/admission failures remain diagnosable.

Keep resolver construction and adapter lookup separate from validation. The validator is the single definition of a usable fresh-run resolution.

### `runtime/orchestrator.go`

- Validate every custom resolver result immediately after `Resolve` and before admission, observer notification, snapshot writes, or model invocation.
- Pass the validated `model.Resolved` value through without supplementing it from runtime configuration or adding flat identity fields.

### `runtime/admission.go`

- Validate the supplied resolution at the direct `Admitter.Admit` boundary so callers that bypass the orchestrator receive the same contract.
- Use `resolved.Provider.ID` and `resolved.Model.ID` directly for policy checks, events, and the returned admission record.
- Delete `admissionProviderID` and `admissionModelID`.
- Update test fixtures to provide complete resolved identity and a dummy streamer.

Validation must precede rate-limit, budget, or other admission side effects.

### `runtime/context.go`

- Snapshot `resolved.Provider.ID` and `resolved.Model.ID` directly.
- Delete `snapshotModelIdentity` and the all-or-nothing configuration fallback.
- Update `docs/architecture/context.md`, `docs/architecture/providers.md`, and `docs/architecture/runtime.md` to describe resolved identity as authoritative.

Resume-only snapshots remain exempt from requiring a live streamer while they settle already-recorded unfinished tool calls. If resume later grows a model-dispatch path, that path must resolve and validate before dispatch.

## Extension identity changes

### `extension/types.go`

- Keep `Artifact` and `Scope` as the canonical public value types.
- Export minimal validators, tentatively `ValidateIdentifier`, `ValidateScope`, and `ValidateComponent`, using the existing identifier grammar.
- Treat scope keys as opaque strings; do not encode identity by joining fields with a delimiter.

### `session/extensions.go`

- Replace `ArtifactIdentity` with `extension.Artifact` and `ExtensionScope` with `extension.Scope` throughout live/durable state.
- Delete the duplicate session types and conversion helpers.
- Introduce private comparable key structs such as `handlerIdentityKey`, `toolIdentityKey`, `promptIdentityKey`, `guardIdentityKey`, and `restrictionIdentityKey`.
- Preserve the existing logical duplicate dimensions for every extension point.
- Preserve deterministic fingerprint sorting, but serialize explicit fields rather than using a delimiter as identity storage.
- Add cases containing delimiter-like characters in otherwise opaque fields, especially scope keys.

If this import creates a real `session`/`extension` cycle, stop and extract the value types and validators into a dependency-neutral leaf package. Do not retain duplicate public identity types to avoid the cycle.

### `composition` and `runtime`

- Delete `artifactIdentity`, `scopeIdentity`, and `validateCapabilityIdentity` from composition.
- Validate registrations through the canonical extension validators.
- Store and transport `extension.Artifact` and `extension.Scope` without conversion.
- Update runtime plan construction and session snapshot code to consume those exact values.

## Verification

- Unit tests for every incomplete/mismatched model resolution and a valid complete resolution.
- A direct admission test proving invalid resolution fails before limiter/budget side effects.
- A custom-resolver test proving invalid output fails before provider-input history/store reads, notifications, and admission effects.
- A direct-admission test proving invalid output fails before any store call.
- Snapshot tests proving only resolved provider/model IDs are persisted.
- Extension validator tests for invalid IDs and opaque scope keys.
- Duplicate-detection tests for values that would collide under delimiter concatenation.
- Fingerprint stability tests after the structured-key rewrite.
- A golden schema-v1 descriptor JSON and fingerprint test proving the canonical typed fields serialize identically; if the golden changes, deliberately bump the schema because no migration compatibility is required.
- Focused package tests for `model`, `session`, `composition`, and `runtime` before the full quality gate.

## Completion signal

Searches find no model identity fallback helpers, no session-owned artifact/scope types, and no delimiter-concatenated identity maps.
