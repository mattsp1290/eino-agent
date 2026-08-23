# First-Class Scoped Mount Leases

## Goal

Make mount lifetime an explicit registry concern and prevent unrelated plans from retaining session-scoped or callback-only mounts.

## Repository evidence

- `composition/registry.go:leasePoint` is a fake notification used only for plan references.
- `composition.Registry.Mount` derives scopes from tools, prompts, guards, and restrictions, then creates a global scope when that set is empty.
- `extension.Registry.snapshot` leases a mount only when at least one registration entry applies.
- `buildDescriptor` explicitly filters `leasePoint`, proving it is lifecycle metadata rather than a real extension contract.

## Exact change surface

- `extension/types.go`: extend `Registrar` with a proposed lifecycle-only `Lease(Scope) error` method, or an equivalently named unexported-capability method exposed through the public registrar interface.
- `extension/registry.go`: stage, validate, deduplicate, publish, snapshot, and diagnose explicit lease scopes as mount metadata.
- `extension/extension_test.go`: add scope applicability, deduplication, rollback, deactivation, drain, and callback-only coverage.
- `composition/registry.go`: delete `leasePoint` and register each composition capability's scope through the lifecycle method.
- `composition/registry_test.go`: add unrelated-session close regression and preserve existing drain tests.

## Intended design

`stagingRegistrar` records a deduplicated set of validated lease scopes. `Registrar.Lease` is permitted only while the mount is open and returns `ErrMountClosed` or the existing invalid-registration error family on invalid input.

`mountState` stores the committed lease scopes. `Registry.snapshot(target, allowed)` leases a mount when either:

- at least one real callback entry applies to `target`; or
- at least one explicit lease scope applies to `target`.

The resulting `Plan.entries` contains only real callbacks. Plan release functions retain every applicable mount once, regardless of how many callbacks or capability scopes matched.

Composition registers a lease scope when a tool, prompt, guard, or restriction is staged. Callback-only mounts need no synthetic lease because their real callback entries already retain the mount. Cleanup-only mounts are not retained by unrelated plans.

## Invariants and error paths

- Lease scopes never appear in dispatch ordering, diagnostics, descriptors, or fingerprints.
- Mount publication remains atomic across callbacks, capabilities, cleanup effects, and lease scopes.
- Rollback runs cleanup without publishing lease scopes.
- Deactivation immediately blocks both callbacks and capabilities from new plans.
- Existing plans keep the mount alive until release.
- Session scope matching remains exact and accepts the same opaque keys as callback/capability scopes.

## Tests and acceptance criteria

- Mount a session-A callback only; acquire a session-B plan; deactivate and close the mount while session-B's plan remains open; close must return immediately.
- Repeat with a session-A composition tool; session-B must not retain it.
- Acquire a session-A plan for that tool; close must block until plan release.
- A mount with a real global callback remains retained by every applicable plan without an explicit composition lease.
- A capability-only global mount retains correctly.
- Direct `SnapshotInstances` tests prove an included explicit lease is retained and an excluded mount is not retained.
- Persist a plan for session-scoped mount A, add same-session mount B, then resume A. B must deactivate and close while the resumed A plan remains open.
- Diagnostics and persisted descriptors contain no lifecycle-only lease entry.
- `rg -n 'leasePoint|composition/lease|composition-lease' composition extension` returns no production matches.

## Dependencies and exclusions

- This work is independent of tool settlement after the extension-boundary work compiles.
- Do not change descriptor schema versions or registration ordering.
- Do not expose plan reference counts as public diagnostics.
