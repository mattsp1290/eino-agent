# Runtime Extensibility

Date: 2026-08-09

`eino-agent` exposes typed Go contracts as its extension boundary. Native
functions, native structs, and Wasm-backed wrappers are mounted into a
`composition.Registry`, which supplies one immutable `runtime.RunPlan` per run.
The orchestrator does not branch on implementation kind.

Typed interception, frozen capability plans, lifecycle, the producer/consumer
catalog, exact pipelines, and request-ledger privacy are documented in
[`extension-points.md`](extension-points.md).

## Construction and seams

`runtime.NewStreamingOrchestrator` requires infrastructure dependencies and one
`runtime.RunPlanProvider`, including for a zero-capability application.
`composition.Registry` is the standard provider: a
mount atomically stages typed callback registrations, tools, prompts, guards,
restrictions, and cleanup. Each executable behavior carries its identity in
the same plan record, so descriptor state cannot drift from execution state.

Admission has no option or public service object. The orchestrator constructs
its private one-shot admission pipeline from the store, event sink, extension
plan, and clock.

| Seam | Native path | Wasm contract | Wrapper status |
| --- | --- | --- | --- |
| Tool | `composition.Registrar.Tool` with a `tools.Definition` | `tool` | Phase A wrapper and fixture |
| Permission policy | `permissions.Policy` / `PolicyFunc` via `runtime.WithPermissions` | `permissions-policy` | Phase A wrapper and fixture |
| Context source | `extension.Use` with `runtime.ContextAssemblePoint` | `context-source` | Implemented |
| Event sink | `runtime.EventSink` / `EventSinkFunc` | `event-sink` | Implemented |
| Hook | Typed lifecycle points in `runtime` | `hook` | Implemented |
| Tool middleware | `runtime.ToolPreparePoint` and `runtime.ToolResultTransformPoint` | `tool-middleware` | Implemented |
| Persistence | transactional `session.Store` | none | Native only by design |
| Models/providers | `model.Resolver`, normally `model.AdapterResolver` | none | Native only by design |
| Durable IDs | `runtime.IDGenerator` | none | Native only by design |

The WIT package is `eino-agent:extensions@0.1.0`. Published packages are
immutable; see `wit/README.md` for evolution rules. Generated bindings are
committed under `wasmext/gen` and reproduced with `make wit`.

## Tool interception

`runtime.ToolPreparePoint` runs after typed input decoding and before any
durable tool-call record is created. Each interceptor sees the output of the
preceding interceptor. Runtime normalizes the final value to a deterministic,
non-null JSON object, then the tool definition's explicit pattern resolver
derives `ToolCall.Pattern` from that same input. Both input and pattern are
persisted; permissions, approval, execution, settlement, and resume reuse them
without generic JSON-key probing. A failed prepare keeps the validated tool-name
fallback so its durable failure can settle without masking the original error.

`runtime.ToolResultTransformPoint` runs after execution and before encoding or
settlement. Around interceptors unwind in reverse registration order, producing
an onion lifecycle. The final result
is used by durable output, the settled event, `PartToolResult`, and the next
model-visible tool message. The executor error is informational and cannot be
removed; a middleware error can turn a success into failure but cannot turn an
executor failure into success.

Pending calls resumed from storage already contain rewritten input, so resume
does not run `ToolPreparePoint` again. `ToolResultTransformPoint` runs when a
pending call is actually re-executed. A call found running is settled
interrupted without execution and skips both interception phases.

## Wasm trust and lifecycle

`wasmext` is a leaf package. Core packages do not import Wasmtime or generated
bindings. The host supplies an explicit local path, allowed root, configured
module name, expected SHA-256, and limits. Loading resolves symlinks, rejects
URLs and root escapes, checks regular-file size, reads once, verifies those
exact bytes, and compiles the verified bytes. No directory discovery, network
fetch, hot reload, or marketplace is involved.

Every call has nonzero time, memory, input, and output defaults. Timeout uses
Wasmtime epoch interruption through the engine boundary, not context
cancellation alone. Calls for one compiled module are serialized and each call
gets a fresh store and component instance; this keeps an epoch interrupt from
collaterally trapping a sibling call. Components receive no filesystem, sockets, environment,
process, credential, endpoint, full config snapshot, resolved model object, or
conversation content. The only capability contract is the bounded
`eino-agent:host/log@0.1.0` import. When `ModuleConfig.Observer` is set, bounded
guest log observations flow through its configured exporter with the
host-configured module name and verified digest attached; otherwise logs are
dropped.

The embedding host owns shutdown. Prefer `wasmext.NewLoader`, load wrappers
through it, and call `Loader.Close(ctx)`. Close stops admission, interrupts
in-flight work, drains within bounds, and releases compiled state once. Calls
after close return a classified error. Wasmtime uses CGO, which is a deployment
and cross-compilation trade-off.

Wasm event sinks add a synchronous bounded guest call at each `Emit`. Tool
lifecycle emits can add up to three such calls per tool call on the run-loop
critical path. The Phase B wrapper must therefore keep its per-call timeout
small; current sink errors are discarded by runtime call sites.

### Engine implementation note

Wasmtime-go v47 supplies compilation, reflection, linking, stores, limits, and
epoch interruption, but its published Go surface does not yet expose dynamic
component-function calls or nested host-interface definitions. `wasmext`
therefore keeps a small lifting/lowering and host-linking layer over Wasmtime's
official v47 C component API. Host wrappers depend on narrow typed interfaces
for the tool, permissions, context, event, hook, and middleware worlds; export
names and codecs are selected by those typed methods rather than an
`operation string`/`any` switchboard. The layer is internal and round-tripped
against the checked-in guests.

## pi parity map

| pi extension point | eino-agent seam | Native path | Wasm path / status |
| --- | --- | --- | --- |
| `customTools` | Tool plan | `tools.Definition`, `composition.Registrar.Tool` | `wasmext.Loader.LoadTool`, `tool` world |
| `tool_call` veto | Permission policy | `permissions.Policy`, `permissions.PolicyFunc`, `runtime.WithPermissions` | `wasmext.Loader.LoadPermissionsPolicy`, `permissions-policy` world |
| `tool_call` argument rewrite | Tool prepare point | `extension.Use` with `runtime.ToolPreparePoint` | `wasmext.Loader.RegisterToolMiddleware` |
| `tool_result` patch | Tool result point | `extension.Use` with `runtime.ToolResultTransformPoint` | `wasmext.Loader.RegisterToolMiddleware` |
| `before_agent_start` / context | Typed lifecycle points | `runtime.RunBeforeExecutePoint`, `runtime.ContextAssemblePoint` | `wasmext.Loader.RegisterContextSource`, `wasmext.Loader.RegisterHook` |
| `subscribe(listener)` | Event sink | `runtime.EventSink`, `runtime.WithEventSink` | `event-sink` world and wrapper |
| `sessionManager` | Session persistence | `session.Store`, `runtime.WithStore` | No Wasm path by design |
| `registerProvider` | Model resolver | `model.AdapterResolver`, `runtime.WithModelResolver` | No Wasm path by design; models and credentials stay native |
| Provider request/header interception | Adapter transport | Wrap `model.Adapter` | Gap by design; adapters own transport and credentials |
| Model selection | Admission snapshot/resolver | `config.Snapshot.Model`, `model.Resolver` | No Wasm path by design |
| Commands, shortcuts, renderers, themes | Presentation layer | Out of scope | Out of scope |
