# Tool Identity and WASM Registration

## Goal and prerequisite state

Remove write-only provenance from tool definitions, make optional source identity structurally explicit, and expose one composition-owned registration boundary for all WASM capabilities.

Prerequisites:

- The application context in `00-overview.md` remains valid.
- Beads issues `eino-agent-nwx` and `eino-agent-frw` are claimed.
- Run-plan capability inputs have a defined place for derived schema and executor hashes.

## Repository evidence

- `tools.Definition.Provenance` is assigned only by `composition.Registrar.Tool`.
- Only `Definition.Provenance.ExecutorHash` is read later, in `composition.Registry.acquire`.
- The other provenance fields duplicate `extension.Component` and `Artifact` ownership already retained in the plan descriptor.
- `ToolRegistration.SourceSchemaHash` and `SourceExecutorHash` require `validateToolSourceIdentity` to reject a half-present state.
- `tools/einotools` is the production source that supplies both source hashes.
- `wasmext.Loader.RegisterTool` already accepts `*composition.Registrar`; the other loader registration methods accept `extension.Registrar`.
- Private WASM adapter functions already isolate registration of event sinks, context sources, hooks, and middleware.

## Exact change surface

### Tool identity

- `tools/definition.go`
  - Remove `Definition.Provenance` and the `Provenance` type.
  - Keep `Definition.Clone` focused on executable definition containers.
- `composition/registry.go`
  - Add immutable `ToolSourceIdentity` with unexported schema and executor hash fields.
  - Add `NewToolSourceIdentity(schemaHash, executorHash string) (ToolSourceIdentity, error)` as the only nonzero constructor.
  - Replace `ToolRegistration.SourceSchemaHash` and `SourceExecutorHash` with value-form `SourceIdentity ToolSourceIdentity`.
  - Treat the zero value as no adapter-supplied identity.
  - Reject empty or malformed hashes in the constructor before registration.
  - Freeze the definition without mutating it with component state.
  - During acquisition, derive the final executor hash from `SourceIdentity` and `mounted.component.Artifact.Hash`.
  - Derive the schema hash from the same source identity and frozen definition.
- `composition/tool_identity.go`
  - Change hash helpers to accept the optional source identity value.
  - Preserve the versioned hash domains and deterministic JSON inputs.
- `tools/einotools/einotools.go` and related tests
  - Construct one validated `ToolSourceIdentity` value from the existing source schema and executor hashes.
- Composition and tool tests
  - Delete provenance assertions.
  - Add nil, complete, malformed, and resume-fingerprint identity cases.

### WASM registration

- `wasmext/loader.go`
  - Change `RegisterEventSink`, `RegisterContextSource`, `RegisterHook`, and `RegisterToolMiddleware` to accept `*composition.Registrar`.
  - Reject nil through the same mount-level contract used by `RegisterTool`.
  - Call `registrar.Extensions()` only inside the loader when invoking private extension adapter functions.
  - Continue to register cleanup on the composition registrar before committing the loaded capability.
- WASM tests outside `examples/`
  - Exercise every public loader method through `composition.Registry.Mount`.
  - Keep lower-level private adapter tests on `extension.Registry` where they test adapter behavior rather than public loader ownership.
  - Preserve rollback, loader-close, mount-close, and in-flight shutdown assertions.

## Intended behavior and invariants

- `tools.Definition` has no component, artifact, config, or fingerprint state.
- Tool source identity is either absent or complete. No half-present public state exists.
- Registration copies source identity by value; caller mutation cannot change mounted fingerprints.
- The final schema identity changes when the frozen definition schema or source schema hash changes.
- The final executor identity changes when the source executor hash or owning artifact hash changes.
- Resume acquisition rejects changed schema or executor identity through the existing sealed-plan comparison.
- All public WASM capability registration participates in composition mount atomicity and cleanup.
- Installers never need the raw extension registrar for a public WASM loader call.

## Tests and acceptance criteria

- `go test ./tools ./tools/einotools ./composition`
- `go test ./wasmext`
- Add or adapt tests proving:
  - zero source identity is valid for host and WASM definitions;
  - the constructor rejects either empty or malformed hash;
  - caller state cannot mutate an identity after registration or mount;
  - schema and executor source changes alter the sealed fingerprint;
  - artifact changes alter executor identity without tool definition mutation;
  - a failed WASM registration releases the module and mount cleanup exactly once;
  - all public loader registration methods accept the same registrar type.
- Acceptance is observable when searches find no `tools.Provenance`, `Definition.Provenance`, `SourceSchemaHash`, or `SourceExecutorHash`, and no public `Loader.Register*` method accepts `extension.Registrar`.

## Dependencies, risks, and exclusions

- Apply the source identity change after the run-plan input shape is stable.
- The WASM signature change is independent but should share the same composition test pass.
- Do not merge component artifact identity into `tools.Definition` under another field name.
- Do not expose private WASM adapter registration as a second public path.
- Do not edit `docs/`. Keep any `examples/` edit mechanical and limited to removed-API compilation.
