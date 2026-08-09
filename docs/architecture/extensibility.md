# Runtime Extensibility

Date: 2026-08-08

`eino-agent` exposes Go interfaces as its extension contract. Native functions,
native structs, and Wasm-backed wrappers all enter the orchestrator through the
same functional options; the orchestrator does not branch on implementation
kind.

## Construction and seams

`runtime.NewStreamingOrchestrator` is the preferred constructor. It applies
options in order, reports `Store`, `Model`, and `IDs` together when required
dependencies are missing, and preserves the existing zero-value fallbacks for
optional scalar, struct, function, and observer fields. Later scalar options
replace earlier values. `WithContextSource`, `WithHook`, and
`WithToolMiddleware` append in registration order.

`Admit` has no option. It is a derived aggregate synthesized from `Store`,
`Transactor`, `Events`, `Hooks`, and `Clock`. Existing struct-literal
construction remains supported.

| Seam | Native path | Wasm contract | Wrapper status |
| --- | --- | --- | --- |
| Tool | `tools.Definition` via `tools.Registry` and `runtime.WithToolRegistry` | `tool` | Phase A wrapper and fixture |
| Permission policy | `permissions.Policy` / `PolicyFunc` via `runtime.WithPermissions` | `permissions-policy` | Phase A wrapper and fixture |
| Context source | `runtime.ContextSource` / `ContextSourceFunc` | `context-source` | Phase B WIT authored |
| Event sink | `runtime.EventSink` / `EventSinkFunc` | `event-sink` | Phase B WIT authored |
| Hook | `runtime.Hook` / `HookFuncs` | `hook` | Phase B WIT authored |
| Tool middleware | `runtime.ToolMiddleware` / `ToolMiddlewareFuncs` | `tool-middleware` | Phase B WIT authored |
| Persistence | `session.Store` and `session.Transactor` | none | Native only by design |
| Models/providers | `model.Resolver`, normally `model.AdapterResolver` | none | Native only by design |
| Durable IDs | `runtime.IDGenerator` | none | Native only by design |

The WIT package is `eino-agent:extensions@0.1.0`. Published packages are
immutable; see `wit/README.md` for evolution rules. Generated bindings are
committed under `wasmext/gen` and reproduced with `make wit`.

## Tool middleware

`BeforeToolCall` runs after typed input decoding and before any durable
tool-call record is created. Middleware sees the output of the preceding
middleware. The final JSON input determines `ToolCall.Pattern` and is the only
input copied into the assistant tool-call part, pending/running/settled events,
the durable tool-call record, permission and approval requests, and execution.
Permissions therefore evaluate what will execute.

`AfterToolCall` runs after execution and before encoding or settlement. It runs
in reverse registration order, producing an onion lifecycle. Its final result
is used by durable output, the settled event, `PartToolResult`, and the next
model-visible tool message. The executor error is informational and cannot be
removed; a middleware error can turn a success into failure but cannot turn an
executor failure into success.

Pending calls resumed from storage already contain rewritten input, so resume
does not run `BeforeToolCall` again. `AfterToolCall` runs when a pending call is
actually re-executed. A call found running is settled interrupted without
execution and skips both middleware phases.

## Wasm trust and lifecycle

`wasmext` is a leaf package. Core packages do not import Wasmtime or generated
bindings. The host supplies an explicit local path, allowed root, configured
module name, expected SHA-256, and limits. Loading resolves symlinks, rejects
URLs and root escapes, checks regular-file size, reads once, verifies those
exact bytes, and compiles the verified bytes. No directory discovery, network
fetch, hot reload, or marketplace is involved.

Every call has nonzero time, memory, input, and output defaults. Timeout uses
Wasmtime epoch interruption through the engine boundary, not context
cancellation alone. Components receive no filesystem, sockets, environment,
process, credential, endpoint, full config snapshot, resolved model object, or
conversation content. The only capability contract is the bounded
`eino-agent:host/log@0.1.0` import; module identity is attached by the host.

The embedding host owns shutdown. Prefer `wasmext.NewLoader`, load wrappers
through it, and call `Loader.Close(ctx)`. Close stops admission, interrupts
in-flight work, drains within bounds, and releases compiled state once. Calls
after close return a classified error. Wasmtime uses CGO, which is a deployment
and cross-compilation trade-off.

Wasm event sinks add a synchronous bounded guest call at each `Emit`. Tool
lifecycle emits can add up to three such calls per tool call on the run-loop
critical path. The Phase B wrapper must therefore keep its per-call timeout
small; current sink errors are discarded by runtime call sites.

### Current engine limitation

Wasmtime-go v47 compiles, reflects, links, and instantiates components, but its
published Go API does not yet expose component-function invocation or canonical
value lifting/lowering. The engine boundary validates the checked-in Phase A
components today, while live calls fail closed as `wasmext.ErrorTrap`. Replacing
that isolated call implementation when upstream exposes `ComponentFunc` does
not change wrapper or orchestrator APIs.

## pi parity map

| pi extension point | eino-agent seam | Native path | Wasm path / status |
| --- | --- | --- | --- |
| `customTools` | Tool registry | `tools.Definition`, `tools.Registry.Register`, `runtime.WithToolRegistry` | `wasmext.LoadTool`, `tool` world; engine call gap noted above |
| `tool_call` veto | Permission policy | `permissions.Policy`, `permissions.PolicyFunc`, `runtime.WithPermissions` | `wasmext.LoadPermissionsPolicy`, `permissions-policy` world; engine call gap noted above |
| `tool_call` argument rewrite | Tool middleware | `runtime.ToolMiddleware`, `runtime.WithToolMiddleware` | `tool-middleware` world, Phase B wrapper gap |
| `tool_result` patch | Tool middleware | `runtime.ToolMiddleware`, `runtime.WithToolMiddleware` | `tool-middleware` world, Phase B wrapper gap |
| `before_agent_start` / context | Context source and hook | `runtime.ContextSource`, `runtime.WithContextSource`, `runtime.Hook` | `context-source` and `hook` worlds, Phase B wrapper gap |
| `subscribe(listener)` | Event sink | `runtime.EventSink`, `runtime.WithEventSink` | `event-sink` world, Phase B wrapper gap |
| `sessionManager` | Session persistence | `session.Store`, `session.Transactor`, `runtime.WithStore`, `runtime.WithTransactor` | No Wasm path by design |
| `registerProvider` | Model resolver | `model.AdapterResolver`, `runtime.WithModelResolver` | No Wasm path by design; models and credentials stay native |
| Provider request/header interception | Adapter transport | Wrap `model.Adapter` | Gap by design; adapters own transport and credentials |
| Model selection | Admission snapshot/resolver | `config.Snapshot.Model`, `model.Resolver` | No Wasm path by design |
| Commands, shortcuts, renderers, themes | Presentation layer | Out of scope | Out of scope |
