# Permission And Approval Hooks

Date: 2026-06-27

Tool execution is gated by a permission policy before the runtime calls the
materialized tool executor.

Tool-call middleware runs first. Policy receives the final rewritten input's
computed `runtime.ToolCall.Pattern`, so it always decides on the operation that
will actually execute. A policy cannot rewrite arguments or bypass middleware.

Hosts can supply native policies through `runtime.WithPermissions`, including
plain functions adapted by `permissions.PolicyFunc`. The versioned
`permissions-policy` WIT world and `wasmext.LoadPermissionsPolicy` provide the
same Go interface for Wasm-backed policies; model execution and credentials
remain native.

## Decisions

`permissions.Policy` returns one of three decisions:

- `allow`: execute the tool;
- `deny`: skip execution and return model-visible denial output;
- `ask`: call the runtime approval hook before execution.

`permissions.StaticPolicy` evaluates `config.PermissionRule` values in order.
Rules can match a permission name and a simple pattern. Unknown rule actions are
treated as operational policy failures, not model-visible denials.
Malformed rule patterns are also operational policy failures so invalid policy
configuration cannot silently grant access.

## Runtime Behavior

`runtime.ExecuteToolWithPermissions` applies policy and approval hooks around a
`runtime.ToolExecutor`.

Model-visible outcomes:

- denied by policy;
- approval required but no approval requester is available;
- approval rejected with a permission denial;
- user interruption while waiting for approval.

Operational outcomes:

- runtime context cancellation or deadline expiration;
- policy backend failure;
- approval backend failure;
- unknown policy action;
- actual tool execution failure.

Operational failures are returned as errors so durable settlement can classify
them separately from ordinary tool denial output.

## Approval Requests

Approval requests include session ID, run ID, tool call ID, permission name,
matched operation pattern, and tool metadata. Runtime passes
`runtime.ToolCall.Pattern` when set; otherwise it falls back to the tool call
name and then the materialized tool name. Approval hooks stay host-defined so UI,
CLI, or service-policy implementations can decide how to ask the user or an
external policy engine.
