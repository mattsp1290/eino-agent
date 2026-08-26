# Canonical Tool Composition

## Goal and prerequisites

Remove the disconnected mutable tool registry. Keep `tools.Definition` and one definition-level materialization function, then publish session tools through `composition.Registry`.

Prerequisite: `composition.Registry` remains the sole owner of mount atomicity, scope collision checks, component leases, diagnostics, run-plan selection, and strict resume identity.

## Definition materialization

Change `tools/registry.go` by replacing it with `tools/definition.go` (proposed new file under existing `tools/`):

- Retain `Definition`, callback types, `Provenance`, `Execution`, `Clone`, `ValidateDefinition`, and the decoder/executor/materialization helpers.
- Add proposed exported function `Materialize(ctx context.Context, definition Definition, scope runtime.ToolScopeContext) (runtime.Tool, error)`.
- `Materialize` checks context cancellation, validates and defensively clones the definition, clones the bounded scope, and returns exactly one `runtime.Tool`.
- `Materialize` does not evaluate enabled/disabled tool lists. `composition.Registry` owns selection before the run plan is sealed.
- Delete `Registry`, `Registration`, `registered`, `Snapshot`, `SnapshotEntry`, `NewRegistry`, `Register`, `Replace`, `Unregister`, `NewSnapshot`, generation ordering, and registry-level `ResolveTools`.
- Delete `ErrStaleRegistration`; retain errors still used by definition validation and composition collision checks.

Replace `tools/registry_test.go` with `tools/definition_test.go` (proposed new file under existing `tools/`):

- Keep tests for validation, defensive schema/container cloning, typed decode/normalize/pattern/execute behavior, scope defaults, malformed input, and context cancellation.
- Delete tests whose only subject is mutable registry generations, replacement, unregistration, snapshot order, or callbacks mutating the live registry.
- Test that `Materialize` returns independent runtime containers across calls.

Change `composition/registry.go`:

- In `Registry.acquire`, clone the selected definition once for the sealed resolver.
- Resolve each plan tool with `tools.Materialize` directly.
- Remove the one-entry snapshot, fake registration generation, length check, and second enable/disable filter.
- Preserve schema/executor fingerprint calculation and dispatch release on acquisition errors.

Migrate omitted live consumers:

- `runtime/extensions_registry_test.go`: materialize standalone definitions with `tools.Materialize` or construct the canonical composition-backed plan required by the test's subject.
- `wasmext/wasmext_test.go`: mount Wasm definitions into `composition.Registry`, use that registry as `runtime.RunPlanProvider`, and delete the test-only `wasmTestPlanProvider` wrapper around `*tools.Registry`.
- Do not introduce another test-only registry abstraction.

Acceptance criteria:

- `rg` finds no `tools.Registry`, `tools.Snapshot`, `tools.Registration`, `NewSnapshot`, or `ErrStaleRegistration` references.
- Enabled/disabled selection tests continue to pass at the composition/run-plan boundary.
- Strict resume continues to reject schema, executor, artifact, scope, registration ID, and order drift.

## Session tool mount

Change `tools/session/session.go`:

- Replace `Register` with proposed `Mount(ctx, registry, component, scope, options) (*composition.Mount, error)`.
- Accept caller-supplied `extension.Component` and `extension.Scope`; do not invent restart identity inside the package.
- Build the same mandatory and optional definitions.
- Mount them atomically with `composition.Registry.Mount` and `composition.InstallerFunc`.
- Register each definition with a stable ID based on its existing name, the supplied component instance ID, application-order offsets, and the supplied scope.
- Return no partial mount if any definition or registration is invalid.
- Preserve `State`, `SubagentRunner`, `SkillLoader`, permission patterns, retention limits, and per-runtime-session data isolation.

Change `tools/session/session_test.go`:

- Use `composition.NewRegistry`, a stable native test component, and `Mount`.
- Acquire a run plan for each test session and resolve tools from the plan.
- Prove state isolation, bounded output, optional callbacks, permission metadata, deterministic registration order, deactivation, and scope routing.
- Prove restart-stable resume: acquire and persist a descriptor, rebuild a fresh composition registry, remount equivalent session tools with the same component/scope/options, and require `AcquireResumePlan` plus tool resolution to succeed.
- Prove meaningful session-tool identity drift is rejected before execution by varying optional tool presence, registration order, component artifact/config identity, and scope.
- Close/release acquired plans and mounts in test cleanup so lifecycle tests cannot leak leases.

Add documentation examples only where the repository already discusses session tools. Do not add a new host framework or automatic default mount.

Acceptance criteria:

- Session tools appear in a composition-acquired run plan.
- Deactivating the mount removes its definitions from future plans while an already-acquired plan remains usable until released; closing the mount succeeds after that plan is released.
- Session-scoped mounts do not appear in other sessions.
- Duplicate applicable tool names fail the entire mount without partial publication.
- Equivalent remounts resume successfully after registry reconstruction; meaningful component, scope, order, or definition drift returns `runtime.ErrExtensionPlanMismatch`.

Verification:

- `go test ./tools/... ./composition/... ./runtime/... ./wasmext/...`
- `go test -race ./tools/... ./composition/...`
