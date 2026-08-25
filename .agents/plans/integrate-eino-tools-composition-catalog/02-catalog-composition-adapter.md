# Catalog composition adapter

## Goal and prerequisites

Replace the isolated `tools.Registry` helper with one atomic standard-catalog mount into `composition.Registry`. Durable identity composition from [01-durable-identity.md](01-durable-identity.md) must exist first.

## Repository evidence

- Existing `tools/einotools/einotools.go` imports each leaf package, uses `/` for metadata construction, and exposes `RegisterDefaults` over a low-level registry.
- Existing `composition.Registry.Mount` stages all tools under one component and rolls back every staged lease and cleanup on error.
- `catalog.Standard` returns metadata without construction, fresh factories, deterministic order, binding kind, retry safety, concurrency policy, and source hashes.
- `catalog.Definition.New` rejects workspace roots that are not absolute, resolved, existing canonical directories.
- `internal/workspace.Locker.Do` can serialize any stable string key.

## Proposed public adapter

Rewrite existing `tools/einotools/einotools.go` around this conceptual surface:

```go
type Options struct {
    Catalog     catalog.Options
    Scope       extension.Scope
    Retention   *runtime.RetentionPolicy
    Permissions map[string][]string
    Metadata    map[string]string
}

func MountStandard(
    context.Context,
    *composition.Registry,
    extension.Component,
    Options,
) (*composition.Mount, error)
```

These names are proposed. The ownership and behavior are required.

- `extension.Component` remains caller-owned so artifact and opaque configuration identity are explicit.
- `Options.Scope` must be a valid global or exact-session scope.
- `Options.Catalog` passes every supported injected leaf dependency through unchanged to `catalog.Standard`.
- The adapter uses one private process-wide keyed locker for every standard mount. Callers cannot replace or accidentally partition this safety policy.
- Nil `Retention` selects 64 KiB inline plus external storage. A non-nil value is copied exactly.
- `Permissions` is keyed by explicit catalog registration ID, not model name. Reject unknown IDs so misspellings cannot silently remove policy.
- `Metadata` is copied and applied to each definition. Add `source=eino-tools` when absent; reject a conflicting source value.

## Translation behavior

For every ordered catalog definition:

1. Call `Info()` without constructing a workspace tool.
2. Verify the returned name matches the catalog name and schema containers exist.
3. Build a `tools.Definition` with description, cloned parameters, raw JSON decoder, canonical raw normalizer, structured raw encoder, deterministic permission-pattern resolver, retry declaration, retention, permissions, and metadata.
4. Set `composition.ToolRegistration.ID` to the explicit catalog ID.
5. Set consecutive order values so run-plan tool order matches catalog order.
6. Set the caller-selected mount scope and component instance ID.
7. Set `SourceSchemaHash` and `SourceExecutorHash` from the catalog definition.
8. Build execution as a fresh factory call followed by `InvokableRun`.

Workspace definitions require the already admitted canonical `execution.Context.WorkspaceRoot`, verify it still resolves to itself through existing `workspace.CanonicalRoot`, pass that exact root through `catalog.Instance`, and use `workspace:<canonical-root>` as the non-concurrent lock key.

Static definitions pass an empty `catalog.Instance`. A non-concurrent static definition uses `registration:<catalog-id>` as its lock key so the same user or tracker dependency serializes across mounts. A concurrent definition invokes without the locker.

The lock covers both factory construction and invocation. Return context cancellation, factory errors, invocation errors, and invalid JSON output without publishing substitute output.

## Keyed locker lifecycle

- Add a private package-level lock coordinator in existing package `tools/einotools`. It is process-wide because canonical workspace and static-dependency concurrency must not depend on component or session mount boundaries.
- Track holders and waiters per string key under one mutex.
- Remove an entry only after the active holder exits and all waiters, including canceled waiters, release their references.
- Never create two live lock entries for the same key during handoff or cancellation.
- Add focused same-key exclusion, different-key parallelism, canceled-waiter, and high-cardinality churn tests. The churn test must observe zero idle entries through a same-package test seam.

## Permission-pattern contract

Derive patterns after raw input validation and canonical normalization. The resolver must decode only the bounded fields it needs and return an error for a missing or wrong-type required identity field. Filesystem patterns use a cleaned, slash-separated workspace-relative request namespace. Normalize that value into the persisted input before both permission resolution and leaf execution. Reject absolute paths and lexical escapes above the workspace. This is lexical request authorization; admitted workspace policy remains responsible for symlinks inside the root.

| Catalog definitions | Pattern |
|---|---|
| file read/write/edit | cleaned workspace-relative `path` |
| file list | cleaned workspace-relative `path`; omitted or empty becomes `.` |
| glob | cleaned workspace-relative `path` when present, otherwise the glob `pattern` |
| search | cleaned workspace-relative `path` when present, otherwise the search `pattern` |
| shell | normalized `cmd` |
| URL fetch | normalized `url` |
| tracker write | normalized `id` |
| apply patch | stable generic `apply_patch`; per-file patch authorization is deferred because one call can touch multiple paths |
| user interaction | stable generic `user_interact`; never persist the user question as permission metadata |

Document the two generic cases and lexical path semantics so callers cannot mistake them for physical-path or content-granular authorization. Runtime permits at most 4096 bytes in a persisted pattern; reject a longer derived identity rather than truncating it, document that this is stricter than some leaf schemas, and test 4096/4097-byte boundaries. Tests must prove path, command, URL, and tracker patterns reach `permissions.StaticPolicy` and approval requests unchanged.

## Lifecycle and error paths

- Build and validate the complete catalog before calling `composition.Registry.Mount`.
- Translate every definition during the installer call. Any metadata, policy, or registration error aborts the mount.
- `composition.Registry.Mount` remains the single publication transaction and mount lifetime owner.
- `Mount.Deactivate` stops future plan acquisition. Existing acquired plans retain their component lease until `Release` and `Mount.Close` drains it.
- The adapter holds no process-global registry and owns no background resources.
- The caller must keep the workspace and fingerprinted search/shell executables valid for the acquired plan lifetime.
- Default MCP user interaction returns the catalog leaf's `pending` result and relies on a hosting application to correlate and deliver a later answer. The adapter serializes the invocation but does not invent an interaction transport.

## Deletions

- Delete existing `RegisterDefaults`.
- Delete private `workspaceSpec`, `staticSpec`, direct leaf constructors, fake-root metadata lookup, `registerWorkspace`, and `registerStatic`.
- Delete tests whose only subject is low-level `tools.Registry` generations.
- Do not leave an alias from `RegisterDefaults` to `MountStandard`.

## Tests and observable acceptance criteria

Rewrite `tools/einotools/einotools_test.go` to cover:

1. Default global mount produces the exact 10 catalog IDs and names in catalog order through `AcquireRunPlan` and `ResolveTools`.
2. A tracker writer produces the 11th tool.
3. File read executes against an admitted temporary workspace and preserves the leaf result contract.
4. A session mount is invisible to another session.
5. Deactivate, acquired-plan lease retention, and close behavior match the composition lifecycle.
6. Catalog construction error, unknown permission ID, invalid scope, duplicate name, and invalid source identity leave diagnostics and acquired plans unchanged.
7. The descriptor contains source-sensitive composed hashes and the exact caller component/config identity.
8. Same-root non-concurrent definitions serialize across tool names. Different workspace roots can run concurrently.
9. Two separately mounted session bundles targeting one canonical workspace serialize across mounts. Different workspace roots can run concurrently.
10. Non-concurrent static definitions serialize by catalog-ID key across mounts. URL fetch, the catalog's concurrent static definition, bypasses the locker.
11. A symlink alias resolves to the admitted canonical root before factory invocation and therefore shares the same workspace lock.
12. URL-fetch client, shell/search options, user interaction options, and tracker writer reach catalog factories through `Options.Catalog`.
13. Runtime permission and approval tests observe the documented path, command, URL, and tracker patterns from the final persisted input. Cover `a/../secret`, absolute and escaping paths, omitted file-list path, and 4096/4097-byte patterns. Assert lexical normalization is identical in the persisted call and leaf input.
14. On non-Unix builds, `MountStandard` preserves `errors.Is(err, catalog.ErrUnsupportedPlatform)` and publishes no component or tools.
15. A platform-independent injected catalog-acquisition error proves `errors.Is(err, catalog.ErrUnsupportedPlatform)` and unchanged composition diagnostics; Windows cross-compilation remains a separate build check.
16. MCP user interaction settles with the exact leaf `pending` envelope and documentation states the external completion requirement.
17. No test imports or invokes the removed `RegisterDefaults` path.

Use small same-package test seams for synthetic catalog definitions when concurrency and identity mutation cannot be proven safely with live leaf tools. Do not expose a production API only for tests.

## Dependencies, risks, and exclusions

- Depends on the exact completed catalog API and identity work package 1.
- A caller-supplied `ConfigHash` must cover opaque injected behavior. Documentation must state this because the adapter cannot hash interface semantics. Locking policy is fixed by the adapter executor and therefore covered by its source/host executor identity rather than caller configuration.
- Do not use `BindingStatic` as evidence that a tool needs no permissions. URL fetch can access HTTPS and absolute `file://` paths.
- Do not retain the low-level registry for tests or examples.
