# Durable source identity composition

## Goal and prerequisites

Carry catalog schema and executor identities through composition without weakening the host-owned definition, component artifact, or configuration identities. This work requires the current `composition.ToolRegistration`, `tools.Definition.Provenance`, and `session.ToolPlanIdentity` flow.

## Repository evidence

- Existing `composition/registry.go` symbol `Registrar.Tool` discards any prior executor provenance and writes `component.Artifact.Hash` into `tools.Provenance.ExecutorHash`.
- Existing `composition/registry.go` symbol `toolSchemaHash` hashes model metadata plus host permissions, retry safety, retention, and metadata.
- Existing `Registry.acquire` copies those results into `session.ToolPlanIdentity`; strict resume compares the complete descriptor fingerprint.
- The completed sibling catalog explicitly requires its two leaf hashes to contribute to the final persisted identities.

## Exact change surface

- `composition/registry.go`
  - Extend existing `ToolRegistration` with proposed `SourceSchemaHash string` and `SourceExecutorHash string`.
  - Add private proposed identity record constants and helpers beside `toolSchemaHash`.
  - Change `Registrar.Tool` to validate the source-hash pair and set composed executor provenance instead of overwriting it with the artifact hash alone.
  - Change `Registry.acquire` to compute schema identity from the full registration rather than only `tools.Definition`.
- `composition/registry_test.go`
  - Add source hash validation, identity sensitivity, deterministic composition, and strict-resume drift tests.
- `tools/registry.go`
  - No new public leaf identity fields. `tools.Provenance.ExecutorHash` continues to carry the final host-composed executor identity into frozen plans.
- `session/extensions.go`
  - No schema change. Existing `ToolPlanIdentity.SchemaHash` and `ExecutorHash` remain the durable storage surface.

## Intended behavior and invariants

Use fixed-field JSON records so identity boundaries cannot collide:

```text
schema version:   eino-agent-tool-schema-v2
source hash:      ToolRegistration.SourceSchemaHash
definition hash:  current complete tools.Definition schema/policy hash
registration order: ToolRegistration.Order

executor version: eino-agent-tool-executor-v2
source hash:      ToolRegistration.SourceExecutorHash
artifact hash:    extension.Component.Artifact.Hash
```

- Hash each marshaled record with SHA-256 and encode lowercase hex.
- Accept both source hashes empty for sources that have no independent catalog identity.
- Reject only-one-present, wrong-length, uppercase, or non-hex source hashes before leasing or publication.
- Include the empty source field in composed records. This makes the algorithm explicit and changes all development fingerprints consistently without a compatibility branch.
- Keep opaque policy in `Artifact.ConfigHash`, which is already part of the same durable plan entry.
- Treat order as executable model behavior. An order-only change must change the composed schema identity even though `session.ToolPlanIdentity` has no separate order field.
- Never allow a source hash to replace the host definition hash or component artifact hash.
- Preserve rollback atomicity when identity validation fails.

## Tests and acceptance criteria

1. Register a tool with two valid source hashes and assert the persisted schema and executor hashes are valid SHA-256 hex and differ from each raw source hash.
2. Change only `SourceSchemaHash`; assert only the persisted schema hash changes.
3. Change only `SourceExecutorHash`; assert only the persisted executor hash changes.
4. Change a host definition policy field; assert the persisted schema hash changes while the raw source hash is unchanged.
5. Change `component.Artifact.Hash`; assert the persisted executor hash and artifact identity change.
6. Change `component.Artifact.ConfigHash`; assert the descriptor fingerprint changes through artifact identity.
7. Acquire a descriptor, remount with only one changed source hash, and assert `AcquireResumePlan` returns `runtime.ErrExtensionPlanMismatch`.
8. Change only `ToolRegistration.Order`; assert the schema identity and descriptor fingerprint change and resume rejects the remount.
9. Table-test invalid partial and malformed source-hash pairs; assert no tool or component is mounted.
10. Keep existing native, Wasm, example, and AG-UI registrations green with the deliberate empty-source identity form.

## Dependencies, risks, and exclusions

- Complete this before translating the catalog so the adapter cannot silently discard identity.
- Do not add a second descriptor field or bump `session.ExtensionPlanSchemaVersion`.
- Do not hash callback pointers, Go type names, raw environment data, or opaque interface values.
- Do not export the identity composition helpers unless another package requires them after implementation evidence.
