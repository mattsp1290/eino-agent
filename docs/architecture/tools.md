# Tool Registry Boundary

Date: 2026-06-27

The typed tool registry turns host or adapter tool definitions into
`runtime.Tool` values from a bounded `runtime.ToolScopeContext`.

## Responsibilities

The `tools` package owns:

- registration-time validation for tool definitions;
- stale-registration protection using monotonically increasing generations;
- typed decoding of model-provided JSON input;
- typed execution context carrying the durable runtime tool call and bounded,
  content-free turn metadata;
- structured output encoding;
- per-session authority scope;
- model-facing `schema.ToolInfo` assembly without reusing mutable containers.

Concrete leaf behavior remains outside this package. Future integration beads
can wrap `eino-tools` implementations as `tools.Definition` values.

`wasmext.LoadTool` also returns an ordinary `tools.Definition`. Its decode and
encode functions validate bounded JSON and its executor invokes the versioned
`tool` WIT world. Register it through the same `Registry.Register` method as a
native definition. Prefer `wasmext.Loader.LoadTool` so the embedding host has a
single bounded `Close(ctx)` lifecycle owner.

## Materialization

Materialization happens per bounded scope context. Enabled and disabled tool names from
`config.ToolConfig` are applied at resolve time, so a config reload affects
future runs without mutating tools already retained by an in-flight
run.

Default scopes derive from bounded workspace metadata:

- `workspace_id` becomes `runtime.ToolScope.WorkspaceID`;
- `workspace_root` becomes `runtime.ToolScope.Root`;

Definitions can override this with a `ScopeResolver` when a tool needs a
different authority root. The resolver receives no messages, configuration
graph, provider clients, or executable definition.

## Input And Output

Every tool definition provides:

- a `Decoder` from raw model JSON into a typed host value;
- an `Executor` over that typed value and runtime call context;
- an `Encoder` from typed output into structured JSON.

Malformed model input is returned as `tools.ErrMalformedInput`, allowing runtime
settlement to distinguish bad tool-call arguments from host execution failures.

## Mutability

The registry defensively copies definition slices/maps and returns fresh
`runtime.Tool` containers for every materialization. Host metadata stays on
`runtime.Tool`; provider-facing `schema.ToolInfo.Extra` is empty. Tool schemas
and all remaining containers are copied so one session cannot mutate another
session's model request state.

## Reversible and strict-plan tools

`tools.Registry.Snapshot` freezes definitions in deterministic generation
order. `Unregister` removes only the exact active generation, preventing stale
reload handles from deleting replacements. `composition.Registry` adds global
and exact-session layers. Applicable global and session tools may not share a
name, preventing request-scoped behavior from shadowing a trusted server tool;
restrictions only intersect.

Registry-backed calls reserve result message and part IDs at admission.
`session.Store.SettleToolCall` commits terminal tool state
and the model-visible result atomically and idempotently. Each settlement must
present the owner and token of the durable tool claim whose work it commits;
missing or stale claim identities conflict before terminal state is applied.
The protected stage order is documented in
[`extension-points.md`](extension-points.md#exact-pipelines).

## Canonical result encoding

The runtime owns the single tool-output encoder and settlement builder.
`runtime.ToolOutput` contains only bounded model-visible fields: call ID,
status, inline content/structured data, truncation sizes, and external/redacted
flags. Tool-controlled attachment locations and arbitrary metadata are never
copied into provider history.

`tools.ModelOutput` aliases that runtime type, while
`tools.EncodeModelOutput` and `tools.BuildToolSettlement` are thin adapters over
the runtime implementation. Callers building a settlement supply the
authoritative claimed `session.ToolCall` and one explicit completion time; the
builder does not read a clock or store. Runtime persists host-owned output
classification and size metadata alongside the fenced terminal call.
