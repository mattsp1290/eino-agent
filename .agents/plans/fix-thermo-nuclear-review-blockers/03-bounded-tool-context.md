# Bounded Tool Context

## Goal and prerequisite state

Prevent tool definitions, scope resolvers, and executors from receiving the mutable full turn graph. This work can proceed after concurrency fields are removed and before AG-UI definitions are finalized.

## Repository evidence

- `runtime.TurnSnapshot` contains Eino message pointers, full configuration, model clients, and callable tools.
- `TurnSnapshot.Clone` copies only message and tool slices.
- `runtime.cloneMessages` leaves nested multimodal pointers and maps shared.
- Production tool callbacks use only session ID, workspace ID/root, and bounded turn metadata.

## Exact change surface

- `runtime/types.go`: add proposed `ToolScopeContext` containing only session/workspace identity needed during definition selection, and `ToolContext`, a data-only execution value containing:
  - `Turn BoundedTurnMetadata`;
  - `WorkspaceID string`;
  - `WorkspaceRoot string`.
- `runtime/extension_context.go`: add proposed `toolContext(TurnSnapshot, []Tool) ToolContext`, a defensive clone for `Turn.ToolNames`, and one shared helper that sorts the final effective tool list then derives the execution context for both fresh and resume execution.
- `runtime/extension_plan.go`:
  - change `PlanTool.Resolve` and `sealedPlanTools` to accept bounded scope/materialization data, never `TurnSnapshot`;
  - remove the public `runtime.ToolRegistry`/`ToolRegistryFunc` callback boundary and keep the sealed capability slice inside `RunPlan`;
  - materialize selected definitions using `ToolScopeContext`, then derive final execution `ToolContext` only after the effective tool list is assembled and sorted.
- `tools/registry.go`:
  - change `ScopeResolver` exactly to `func(runtime.ToolScopeContext) runtime.ToolScope`; no `Definition` or other callable graph is passed to scope callbacks;
  - change public registry materialization and snapshot materialization to accept `runtime.ToolScopeContext`;
  - change `Execution.Snapshot` to `Execution.Context runtime.ToolContext`;
  - do not capture execution context during one-definition materialization; pass the finalized execution context when runtime invokes the resolved tool;
  - keep enable/disable selection inside the registry before discarding the full snapshot.
- `tools/einotools`, `tools/session`, `wasmext/wrappers.go`, and tests: read session/workspace/turn metadata from the bounded context.
- `runtime/orchestrator.go`, `runtime/tool_permissions.go`, `runtime/tool_execution.go`, and `runtime/interrupt.go`:
  - add `ToolContext` to the `ToolExecutor.Execute` boundary and thread it through guards, permission checks, native/typed execution, and resumed tool calls;
  - use the same finalization helper for fresh and resumed execution so both receive the complete sorted effective tool-name set.
- `runtime/config_snapshot.go` and `runtime/admission.go`:
  - make proposed admission snapshot freezing error-returning;
  - deep-clone Eino messages through the checked `model.Request.Clone` boundary before storing them in `TurnSnapshot`;
  - pre-clone only fallible mutable request payload before `existingAdmission`, `admitDurable`, event emission, or extension notification; construct the final snapshot afterward from authoritative persisted run/session/epoch identities;
  - strengthen idempotent admission validation so requested session, epoch, assistant message, parent, config/model identity, and input cannot disagree with the persisted admission, or fail the retry before returning an inconsistent snapshot;
  - propagate unsupported message-shape errors with zero durable or observable mutation.
- `runtime/types.go`: narrow or remove the misleading `TurnSnapshot.Clone` comment; retain the method only for trusted internal container copies.
- `docs/architecture/tools.md`, `docs/consumer-guide.md`, and `docs/architecture/config-lifecycle.md`: replace full-snapshot callback and container-only clone guidance with the bounded scope/execution and checked admission-clone contracts.

## Intended invariants

- Tool callbacks cannot observe or mutate message content, provider clients, system prompts, tool executors, or arbitrary configuration maps.
- Tool-visible slices and maps are defensively copied.
- Scope materialization sees only identity/workspace data; execution receives the exact final sorted effective tool-name set after plan resolution.
- Admission rejects unsupported model message graphs before creating durable run/message records.
- Wasm tools receive the same bounded turn metadata they received before, derived without access to the full snapshot.

## Tests and acceptance

- `go test ./runtime ./tools/... ./wasmext`
- A nested multimodal input mutation after `Start` cannot change the admitted snapshot or provider request.
- A tool callback cannot compile against `TurnSnapshot` through `PlanTool.Resolve`, plan registry/materialization, `ScopeResolver`, or `tools.Execution`; a custom `RunPlanSpec` resolver proves messages, configuration, provider clients, and executable tools are unobservable.
- Tool context mutation does not affect sibling tools or runtime state.
- Execution tests assert exact final tool names, session/run IDs, workspace fields, and the corresponding Wasm projection.
- Resume execution receives the same complete sorted tool-name set as fresh execution.
- Idempotent admission tests reject mismatched session, epoch, assistant-message, parent, config/model, and input identities rather than mixing requested and persisted state.
- A clone-failure spy-store test proves zero store write calls, no event, and no extension notification.
- A compile-time signature assertion proves `ScopeResolver` cannot receive `tools.Definition`.
- Repository-wide structural search shows no `execution.Snapshot` and no tool/materialization callback parameter of type `runtime.TurnSnapshot`.

## Risks and exclusions

- Stop if a production tool requires raw message content; redesign that requirement as a separate explicit capability rather than widening `ToolContext`.
- Do not put secrets or arbitrary `config.Snapshot.Metadata` into `ToolContext`.
- Do not introduce a reflective graph cloner.
- Do not transport tool context through `context.Context` values or capture a partial context before final tool selection.
