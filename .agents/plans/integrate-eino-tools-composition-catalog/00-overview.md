# Integrate the eino-tools composition catalog

Status: Ready

Planning only. No implementation described by this plan has occurred.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "bf7e0c4c07e70174f4454fd36ff3b34fac312eac3b640b8874b67b03284f373f",
    "confirmed_at": "2026-08-25T16:15:41Z"
  }
}
```

The user confirmed that the application has no users and that backward compatibility is dead code. Delete the disconnected registration API. Do not add aliases, deprecation shims, feature flags, dual paths, or stored-plan migrations.

## Change classification

- Change type: dependency adoption, composition-boundary replacement, durable identity change, adapter rewrite, tests, and documentation.
- Affected areas: `go.mod`, `go.sum`, `composition`, `tools/einotools`, consumer documentation, architecture documentation, and dependency status.
- External source: `github.com/mattsp1290/eino-tools/catalog` at completed commit `63a3c99272c2359e24484698f2bd62e6fac849b6`.

## Requested outcome

Make the standard `eino-tools` bundle executable through the only production path, `composition.Registry`. Translate all catalog definitions into one atomic composition mount, preserve leaf schema and executor identity in the durable run-plan descriptor, enforce the catalog concurrency declarations, and delete `tools/einotools.RegisterDefaults` and its low-level registry path.

## Success criteria

1. A caller can mount the 10 default catalog tools, or 11 with tracker write configured, into a global or exact-session composition scope with one call.
2. Acquired run plans and the model provider receive the catalog tools in catalog order and execute against the admitted canonical workspace.
3. Changing only a catalog `SchemaHash` changes `session.ToolPlanIdentity.SchemaHash`.
4. Changing only a catalog `ExecutorHash` changes `session.ToolPlanIdentity.ExecutorHash`.
5. Resume acquisition rejects either identity drift through `runtime.ErrExtensionPlanMismatch`.
6. Every `Concurrent=false` workspace definition shares one process-wide canonical-root lock across mounts. Every `Concurrent=false` static definition shares one process-wide catalog-ID lock across mounts. Idle lock keys are reclaimed. `Concurrent=true` definitions do not take either lock.
7. Catalog construction, translation, or composition validation failure publishes no partial mount.
8. The obsolete `RegisterDefaults` symbol and direct leaf-package assembly disappear from `tools/einotools`.
9. Focused tests and `make check` pass.
10. Stable permission-pattern resolvers derive filesystem paths, shell commands, URLs, and tracker targets from the same normalized input that is persisted and executed.
11. Admission persists one canonical workspace root, duplicate JSON keys are rejected before canonicalization, and resume cannot retarget a run through a changed workspace symlink.

## Scope

- Advance the `eino-tools` dependency from `e6ee664be93b` to completed commit `63a3c99272c2359e24484698f2bd62e6fac849b6` (`v0.1.1-0.20260825160656-63a3c99272c2`).
- Add source schema and executor identity inputs to `composition.ToolRegistration`.
- Compose source identity with host-owned definition and artifact identity using explicit versioned JSON records and SHA-256.
- Rewrite `tools/einotools` around `catalog.Standard` and `composition.Registry.Mount`.
- Preserve catalog order through runtime model preparation and make registration order part of durable tool identity.
- Add adapter-owned permission-pattern resolvers for operation-specific policy matching.
- Canonicalize workspace authority at admission and reject duplicate tool-call keys before runtime object canonicalization.
- Keep host-owned component, scope, configuration identity, permissions, retention, metadata, and locking policy explicit at the adapter boundary.
- Replace the old adapter tests with composition, identity, execution, atomicity, and concurrency tests.
- Replace documentation that describes the adapter as disconnected.

## Non-goals

- Change any leaf schema, leaf result envelope, catalog identity algorithm, or executable-provenance behavior in `eino-tools`.
- Move session-native tools into `eino-tools`.
- Add a second registry, a compatibility wrapper, registration generations, or a feature flag.
- Invent a configuration digest for opaque URL, user-interaction, or tracker policy. The mounting caller supplies `extension.Artifact.ConfigHash` and must cover those inputs.
- Change the durable extension-plan schema. Existing schema fields can carry the composed hashes, and no stored descriptors require migration.
- Modify or commit the sibling `eino-tools` repository.
- Implement an MCP ask/answer transport or durable question-correlation protocol. This change preserves the catalog leaf's `pending` envelope; the hosting application must supply the later interaction flow.
- Treat lexical filesystem permission patterns as physical-path or symlink-target policy. Workspace admission owns the contents and symlink policy inside the canonical root.

## Repository findings

### Verified facts

- `eino-tools` commit `63a3c99` adds `catalog.Standard(Options) ([]Definition, error)` on Unix and a non-Unix `ErrUnsupportedPlatform` result.
- Each catalog definition has an explicit ID, model name, `Binding`, `RetrySafe`, `Concurrent`, `SchemaHash`, `ExecutorHash`, metadata accessor, and fresh factory.
- The default order is file read, write, edit, list, glob, search, apply patch, shell, URL fetch, and user interaction. Tracker write is optional and last.
- Every workspace definition declares `Concurrent=false`. URL fetch alone declares `Concurrent=true`; user interaction and tracker write are non-concurrent static definitions.
- Catalog factories require an already canonical absolute workspace directory. Search and shell capture and recheck executable provenance.
- `composition.Registrar.Tool` currently overwrites executor provenance with only `component.Artifact.Hash`.
- `composition.Registry.acquire` recomputes a host definition hash but has no catalog schema input.
- `tools/einotools.RegisterDefaults` constructs metadata with `/`, assembles leaf packages itself, and writes only to `tools.Registry`; the production run-plan provider never reads that registry.
- `composition.Registry.Mount` already stages all registrations and rolls back the component when any registration fails.
- `internal/workspace.Locker` accepts arbitrary string keys, is context-aware, and allows independent keys to proceed concurrently.
- The focused baseline `go test ./tools/einotools ./composition ./session` passes before this change.

### Decisions

1. Keep leaf hashes on `composition.ToolRegistration` as `SourceSchemaHash` and `SourceExecutorHash`. These fields identify the executable source that supplied the host definition; they are not mutable runtime provenance.
2. Require source hashes as a pair when either is present and validate lowercase 64-character SHA-256 hex. Native and Wasm registrations without a separate source catalog remain valid because their host definition and component artifact still provide complete identity inputs.
3. Always produce versioned composed hashes. The schema hash combines the optional source schema hash with the existing host definition hash. The executor hash combines the optional source executor hash with the component artifact hash.
4. Replace the adapter entry point with proposed `MountStandard`. The caller supplies `extension.Component` and adapter options; the helper owns catalog translation and calls the atomic composition mount.
5. Keep the adapter lock coordinator private and process-wide. This makes the catalog concurrency contract invariant across all standard mounts without trusting caller-supplied behavior or sharing topology. Use ref-counted entries so process-wide scope does not retain idle workspace keys.
6. Use the catalog order as consecutive tool order values. Preserve existing snapshot order followed by composition-plan order when runtime merges tool sources; remove later alphabetical re-sorts. Include registration order in the host schema-identity input so order-only drift rejects resume.
7. Keep raw JSON decode, canonical normalization, structured result encoding, and permission-pattern derivation at the adapter boundary. Leaf tools remain ordinary Eino `InvokableTool` instances.
8. Canonicalize a configured workspace root once before durable run admission, store the resolved path, and reuse it unchanged on resume.
9. Reject duplicate top-level JSON keys before the runtime converts arguments to a canonical object, preserving the leaf contract instead of accepting last-value-wins input.

## Target flow

```text
catalog.Standard(captured leaf options)
  -> ordered catalog.Definition values
  -> tools/einotools translation
       - Info() without a workspace sentinel
       - tools.Definition with host retention/permissions/metadata
       - source schema/executor hashes
       - factory invocation with canonical workspace instance
       - shared lock when Concurrent=false
       - pattern from normalized operation identity
  -> composition.Registry.Mount(component, complete installer)
  -> frozen RunPlan
       - composed schema identity
       - composed executor identity
       - artifact and config identity
  -> durable descriptor / strict resume comparison
```

## Compatibility, rollout, migration, and rollback

- Compatibility: intentionally not preserved. Delete `RegisterDefaults`; do not retain an alternate low-level setup.
- Rollout: one dependency and consumer commit. No feature flag applies because there are no users.
- Stored data: no migration. There are no user descriptors to retain, and the descriptor shape is unchanged.
- Configuration: callers must compute `extension.Artifact.ConfigHash` from opaque catalog policy inputs before mounting.
- Rollback before persistence: revert this commit and dependency pin.
- Rollback after persistence: no existing users require support. A development descriptor created with the new hashes must be abandoned or recreated after rollback.

## Risks and gates

- Stop if either catalog hash is discarded before `session.ToolPlanIdentity` is created.
- Stop if `MountStandard` can publish a valid prefix after a later definition fails.
- Stop if a symlink alias or relative workspace reaches a catalog factory without host canonicalization.
- Stop if separate locks allow file read, mutation, search, patch, and shell calls for one canonical root to overlap.
- Stop if runtime reorders catalog tools before the provider request or accepts an order-only resume drift.
- Stop if a path, shell command, URL, or tracker target cannot reach permission policy as the persisted call pattern.
- Stop if the persisted workspace root can change between initial execution and resume.
- Stop if duplicate top-level keys reach a standard leaf after runtime canonicalization.
- Treat any dependency pin other than completed commit `63a3c99` or its exact pseudo-version as unexpected drift.

## Assumptions and unresolved decisions

- Assumption: the existing default retention policy of 64 KiB inline plus external storage remains the standard adapter default. The new options can replace it explicitly.
- Assumption: source hashes are optional for non-catalog composition registrations because those sources use their component artifact as executor identity and the complete host definition as schema identity.
- Blocking decisions: none.
- Non-blocking decisions: exact exported adapter option names may change during implementation if tests reveal a clearer Go API without changing the ownership boundary above.

## Document map

- [01-durable-identity.md](01-durable-identity.md): compose catalog and host identity into strict run-plan fingerprints.
- [02-catalog-composition-adapter.md](02-catalog-composition-adapter.md): replace the disconnected helper with an atomic catalog mount and correct locking.
- [03-verification-and-documentation.md](03-verification-and-documentation.md): dependency, regression, documentation, and quality gates.
- [04-execution-handoff.md](04-execution-handoff.md): dependency-ordered implementation sequence and definition of done.
