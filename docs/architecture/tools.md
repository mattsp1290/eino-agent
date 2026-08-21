# Tool Registry Boundary

Date: 2026-06-27

The typed tool registry turns host or adapter tool definitions into
`runtime.Tool` values for a specific `runtime.TurnSnapshot`.

## Responsibilities

The `tools` package owns:

- registration-time validation for tool definitions;
- stale-registration protection using monotonically increasing generations;
- typed decoding of model-provided JSON input;
- typed execution context carrying the durable runtime tool call and turn
  snapshot;
- structured output encoding;
- per-session scope and concurrency metadata;
- model-facing `schema.ToolInfo` assembly without reusing mutable containers.

Concrete leaf behavior remains outside this package. Future integration beads
can wrap `eino-tools` implementations as `tools.Definition` values.

`wasmext.LoadTool` also returns an ordinary `tools.Definition`. Its decode and
encode functions validate bounded JSON and its executor invokes the versioned
`tool` WIT world. Register it through the same `Registry.Register` method as a
native definition. Prefer `wasmext.Loader.LoadTool` so the embedding host has a
single bounded `Close(ctx)` lifecycle owner.

## Materialization

Materialization happens per turn snapshot. Enabled and disabled tool names from
`config.ToolConfig` are applied at resolve time, so a config reload affects
future turn snapshots without mutating tools already retained by an in-flight
run.

Default scopes derive from snapshot metadata:

- `workspace_id` becomes `runtime.ToolScope.WorkspaceID`;
- `workspace_root` becomes `runtime.ToolScope.Root`;
- the default concurrency key is `session_id:tool_name`.

Definitions can override this with a `ScopeResolver` when a tool needs a
different serialization domain.

## Input And Output

Every tool definition provides:

- a `Decoder` from raw model JSON into a typed host value;
- an `Executor` over that typed value and runtime call context;
- an `Encoder` from typed output into structured JSON.

Malformed model input is returned as `tools.ErrMalformedInput`, allowing runtime
settlement to distinguish bad tool-call arguments from host execution failures.

## Mutability

The registry defensively copies definition slices/maps and returns fresh
`runtime.Tool` containers for every materialization. Model-facing tool info
contains a fresh `Extra` map so one session cannot mutate another session's tool
metadata through shared `schema.ToolInfo` state.

## Reversible and strict-plan tools

`tools.Registry.Snapshot` freezes definitions in deterministic generation
order. `Unregister` removes only the exact active generation, preventing stale
reload handles from deleting replacements. `composition.Registry` adds global
and exact-session layers: a session definition shadows a same-name global
definition, while restrictions only intersect.

Strict registry-backed calls reserve result message and part IDs at admission
and require `session.ToolSettlementStore`. SQLite commits terminal tool state
and the model-visible result atomically and idempotently. Each settlement must
present the owner and token of the durable tool claim whose work it commits;
missing or stale claim identities conflict before terminal state is applied.
The protected stage order is documented in
[`extension-points.md`](extension-points.md#exact-pipelines).
