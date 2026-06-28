# ag-ui-go-server-example Integration Sketch

This sketch maps the current `ag-ui-go-server-example` shape to stable `eino-agent` APIs without importing or copying `internal/agent` code.

## Adapter Points

| Current example responsibility | eino-agent API |
| --- | --- |
| Fiber route parses `RunAgentInput` and owns path/auth policy | Keep in the app; call `runtime.Orchestrator.Start` with a `runtime.Request` |
| AG-UI messages to Eino messages | Use `eino-agui/convert.ToEinoMessages`, as shown by `examples/ag-ui-go-server-example.StartRequest` |
| SSE stream/reconnect boilerplate | `transport.SSEHandler` with `session.Store`, `stream.Tail`, route-owned session lookup, and `after` cursor parsing |
| Local paused/history storage | `store/sqlite.Open` for durable sessions, runs, messages, parts, tool calls, epochs, and replayable events |
| Client-defined AG-UI tools | `tools/agui.Registry.SetClientTools` plus `agui.ClientToolSnapshot` |
| Interrupt route | `transport.InterruptHandler` and an app-owned active-handle lookup |

## Sketch

The buildable package in `examples/ag-ui-go-server-example` intentionally uses `net/http` adapter signatures because `eino-agent` does not own the consuming app's Fiber router. A Fiber route can either adapt the request into `http.Request` for these helpers or mirror the same config fields in its `SendStreamWriter` closure.

Core flow:

1. Open local storage with `sqlitestore.Open`.
2. Build a `stream.Tail` and pass it as the runtime `EventSink` and SSE live tail.
3. Build `tools/agui.Registry` with any server tool registry and an AG-UI client-tool dispatcher.
4. For each AG-UI run request, convert messages and install client tools for that session.
5. Call `runtime.StreamingOrchestrator.Start`.
6. Serve replay/reconnect through `transport.SSEHandler`.
7. Serve interrupts through `transport.InterruptHandler`.

## Stable APIs Used

- `runtime.StreamingOrchestrator`, `runtime.Request`, `runtime.Handle`
- `session.Store` and `store/sqlite.Open`
- `stream.Tail`
- `transport.SSEHandler`, `transport.InterruptHandler`
- `agui.ClientToolSnapshot`, `agui.ClientToolDispatcher`
- `tools/agui.Registry`

## Consumer-Owned Work

The integration should keep these in `ag-ui-go-server-example`:

- Fiber route names and request body validation;
- provider/model construction;
- product prompts and route-specific posture;
- auth and tenant/session lookup;
- UI state payloads;
- any compatibility route that still needs the original in-memory resume semantics.

The runtime can replace the reusable session/run/tool/replay layers without changing those application-owned concerns.
