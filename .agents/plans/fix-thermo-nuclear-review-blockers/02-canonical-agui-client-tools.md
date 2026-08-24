# Canonical AG-UI Client Tools

## Goal and prerequisite state

Replace the stranded aggregate client-tool registry with a direct composition mount. Complete the dead-concurrency type deletion first so the new API does not reproduce removed fields.

## Repository evidence

- `tools/agui/registry.go` combines a `runtime.ToolRegistry` with per-session client state but no longer connects to `StreamingOrchestrator`.
- `examples/ag-ui-go-server-example/sketch.go` calls `SetClientTools` but never supplies the registry through `RunPlanProvider`.
- `composition.Registrar.Tool` is the canonical typed-tool registration path and stamps provenance into sealed plans.
- `agui.ClientToolSnapshot.RuntimeTools` currently builds `runtime.Tool` directly, bypassing typed definitions.

## Exact change surface

- `agui/client_tools.go`:
  - change `ClientToolDispatcher` to return validated `json.RawMessage` rather than `runtime.ToolResult`;
  - add `DispatcherArtifactID string` to `ClientToolSnapshot`; require it to be nonempty and restart-stable, and require hosts to change it whenever dispatcher behavior changes;
  - replace `ClientToolSnapshot.RuntimeTools` with proposed `Definitions`, returning cloned `tools.Definition` values;
  - make JSON parameter cloning error-returning; reject unsupported parameter graphs instead of returning aliases;
  - preserve client-tool permission and metadata defaults in each definition.
- `tools/agui/registry.go`:
  - replace `Registry`, `NewRegistry`, `SetClientTools`, `ClearClientTools`, and `ResolveTools` with `MountClientTools(ctx, *composition.Registry, agui.ClientToolSnapshot, agui.ClientToolDispatcher) (*composition.Mount, error)`; the snapshot is the explicit source of dispatcher identity;
  - validate nonempty session ID, nonzero generation, nonempty/unique tool names, dispatcher presence, and registry presence;
  - map the dispatcher artifact ID into the component artifact hash together with session ID hash, generation, canonical tool schemas, permissions, and metadata; use a fixed adapter artifact name/version/source kind and a canonical config hash so every required `extension.Component` field is explicit and restart-stable;
  - register every definition through `composition.ToolRegistration` with `extension.SessionScope` and stable IDs.
- `composition/registry.go`: enforce an explicit trust-boundary collision policy during selection: a session client tool must not shadow an applicable server/global tool. Reject the conflicting mount or plan acquisition atomically, including when the global tool is mounted after the client tool.
- `tools/agui/registry_test.go` and `agui/client_tools_test.go`: replace aggregate-resolution tests with mount, descriptor, scope, schema, execution, generation-identity, invalid-boundary, and resume-mismatch tests.
- `examples/ag-ui-go-server-example/sketch.go`: replace `SetClientTools` with a mount helper that returns the host-owned `*composition.Mount`; remove the in-memory generation registry if it no longer serves a runtime invariant.
- `docs/consumer-guide.md` and `docs/integrations/ag-ui-go-server-example.md`: show composition registry construction, client-tool mount ownership, `WithRunPlanProvider`, remount order, and strict resume behavior.

## Intended behavior and lifecycle

1. The host converts one AG-UI request tool set into `ClientToolSnapshot`.
2. `MountClientTools` validates and freezes the data before publishing the mount.
3. `composition.Registry.AcquireRunPlan` selects that session-scoped tool set.
4. The run retains the mount lease through completion.
5. The host closes the prior mount before publishing a replacement generation. A close waits for active plans or returns the supplied context error.
6. Resume succeeds only when the persisted generation and canonical definitions reproduce the same descriptor.
7. Client results are one valid JSON value. Dispatcher errors propagate; invalid JSON is rejected; attachments and per-call result metadata are not part of the new contract.
8. A client definition cannot shadow a server/global tool with the same name.

## Tests and acceptance

- `go test ./agui ./tools/agui ./composition ./runtime`
- An integration test mounts one client tool, starts a runtime through `WithRunPlanProvider(plans)`, observes the model call it, dispatches it, and verifies atomic durable settlement.
- A different session cannot see the mounted client tool.
- Changing generation or schema changes the plan fingerprint and rejects resume against the old descriptor.
- Changing dispatcher artifact identity with otherwise identical definitions changes the fingerprint and rejects resume.
- Global/server versus client name collisions are rejected regardless of mount order.
- Tests cover valid JSON, invalid JSON, and dispatcher errors and assert the exact durable result envelope.
- `rg -n 'type Registry struct|runtime.ToolRegistry|RuntimeTools|SetClientTools|ClearClientTools' tools/agui agui examples/ag-ui-go-server-example docs/consumer-guide.md docs/integrations/ag-ui-go-server-example.md` finds no obsolete integration API.

## Risks and exclusions

- Do not add a second plan provider or aggregate registry abstraction.
- Do not retain automatic in-memory replacement state; mount ownership is explicit.
- Do not promise resume after the matching client-tool mount is removed.
