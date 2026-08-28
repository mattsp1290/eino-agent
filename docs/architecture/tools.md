# Tool Definition Boundary

Date: 2026-06-27

The `tools` package turns one host or adapter definition into a `runtime.Tool`
from a bounded `runtime.ToolScopeContext`. `composition.Registry` is the only
registry and selects and seals definitions into immutable run plans.

## Responsibilities

The `tools` package owns:

- registration-time validation for tool definitions;
- validation and canonicalization of model-provided JSON-object input;
- deterministic normalization to a non-null JSON object;
- explicit permission-pattern derivation from canonical normalized input;
- JSON-native execution context carrying the durable runtime tool call and bounded,
  content-free turn metadata;
- JSON output validation and runtime-owned result encoding;
- per-session authority scope;
- model-facing `schema.ToolInfo` assembly without reusing mutable containers.

Concrete leaf behavior remains outside this package.
`tools/einotools.MountStandard` translates deterministic
`eino-tools/catalog` definitions into `tools.Definition` values and mounts the
complete set atomically through `composition.Registry`.

`wasmext.Loader.LoadTool` also returns an ordinary `tools.Definition`. Its
normalizer and executor validate bounded JSON, and the executor invokes the
versioned `tool` WIT world. Mount it through `composition.Registrar.Tool` like
a native definition; the Loader remains the single `Close(ctx)` lifecycle
owner.

## Materialization

Materialization happens per bounded scope context. `composition.Registry`
applies enabled and disabled names from `config.ToolConfig` when it acquires a
run plan, so a config reload affects future runs without mutating tools already
retained by an in-flight run.

Default scopes derive from bounded workspace metadata:

- `workspace_id` becomes `runtime.ToolScope.WorkspaceID`;
- `workspace_root` becomes `runtime.ToolScope.Root`;

Definitions can override this with a `ScopeResolver` when a tool needs a
different authority root. The resolver receives no messages, configuration
graph, provider clients, or executable definition.

## Input And Output

Every tool definition provides one JSON-native `Executor`. It receives
canonical object input plus the durable runtime call context and returns valid
JSON. A definition may provide `Normalize` and `Pattern` callbacks over that
same JSON boundary. Hosts that prefer typed functions use `TypedNormalizer`,
`TypedPermissionPattern`, and `TypedExecutor`; those adapters do not create a
second runtime contract.

Runtime invokes `Pattern` after the final prepare transform and persists the
result with the canonical object input. Runtime never probes generic JSON for
permission field names. Definitions without a callback use the tool name.

The standard adapter cleans filesystem operation patterns into a lexical,
workspace-relative namespace and persists that normalized input before
permission evaluation and execution. It derives shell commands, URLs, and
tracker IDs directly, and uses bounded generic patterns for apply-patch and
user interaction. Runtime rejects duplicate top-level argument keys before
canonical object encoding.

Malformed model input is returned as `tools.ErrMalformedInput`, allowing runtime
settlement to distinguish bad tool-call arguments from host execution failures.

## Mutability

`composition.Registry` defensively freezes definitions at mount and run-plan
acquisition. `tools.Materialize` returns fresh `runtime.Tool` containers for
one already-selected definition. Host metadata stays on `runtime.Tool`;
provider-facing `schema.ToolInfo.Extra` is empty. Tool schemas and all remaining
containers are copied so one session cannot mutate another session's model
request state.

## Reversible and strict-plan tools

`composition.Registry` freezes deterministic global and exact-session layers.
Applicable global and session tools may not share a name, preventing
request-scoped behavior from shadowing a trusted server tool; restrictions only
intersect. Mount deactivation removes capabilities from future plans while
leased plans remain valid until release. `tools/session.Mount` publishes the
built-in session tools through the same component identity, scope, and resume
path as native, catalog, AG-UI, and Wasm tools.

Catalog source schema/executor hashes are composed with host definition order
and component artifact identity before they enter `session.ToolPlanIdentity`.
Changing the leaf source, host policy, tool order, or adapter artifact therefore
rejects strict resume. Non-concurrent standard definitions share one
process-wide, ref-counted lock coordinator; workspace keys are canonical roots
and static keys are catalog IDs.

Run-plan-backed calls reserve result message and part IDs at admission.
`session.ExecutionStore.SettleToolCall(session.SettleToolCallRequest)` commits
terminal tool state, the reserved model-visible result envelope, and the
canonical terminal event atomically and idempotently. Each settlement must
present the owner and token of the durable tool claim whose work it commits;
missing or stale claim identities conflict before terminal state is applied.
The protected stage order is documented in
[`extension-points.md`](extension-points.md#exact-pipelines).

## Canonical result encoding

The runtime exclusively owns the tool-output encoder and settlement builder.
`runtime.ToolOutput` contains only bounded model-visible fields: call ID,
status, inline content/structured data, truncation sizes, and external/redacted
flags. Tool-controlled attachment locations and arbitrary metadata are never
copied into provider history.

Callers building a settlement through `runtime.BuildToolSettlement` supply the
authoritative claimed `session.ToolCall` and one explicit completion time; the
builder does not read a clock or store. Runtime persists host-owned output
classification and size metadata alongside the fenced terminal call.
