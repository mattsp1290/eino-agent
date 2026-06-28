# ag-ui-go-server-example Integration Sketch

This sketch maps the current `ag-ui-go-server-example` shape to stable `eino-agent` APIs without importing or copying `internal/agent` code.

## Adapter Points

| Current example responsibility | eino-agent API |
| --- | --- |
| Fiber route parses `RunAgentInput` and owns path/auth policy | Keep in the app; call `runtime.StreamingOrchestrator.Start` with a `runtime.Request` |
| AG-UI messages to Eino messages | Use `eino-agui/convert.ToEinoMessages`, as shown by `examples/ag-ui-go-server-example/sketch.go` |
| SSE stream/reconnect boilerplate | `transport.SSEHandler` with `session.Store`, `stream.Tail`, route-owned session lookup, and `after` cursor parsing |
| Local paused/history storage | `store/sqlite.Open` for durable sessions, runs, messages, parts, tool calls, epochs, and replayable events |
| Client-defined AG-UI tools | `tools/agui.Registry.SetClientTools` plus `agui.ClientToolSnapshot` |
| Interrupt route | `transport.InterruptHandler` and an app-owned active-handle lookup |

## Sketch

The buildable package in `examples/ag-ui-go-server-example` intentionally uses `net/http` adapter signatures because `eino-agent` does not own the consuming app's Fiber router. A Fiber route can either adapt the request into `http.Request` for these helpers or mirror the same config fields in its `SendStreamWriter` closure.

Core flow:

1. Open local storage with `store/sqlite.Open`.
2. Build a `stream.Tail` for live fanout.
3. Build a composite `runtime.EventSink` that appends non-live events to `session.Store.AppendEvent` and then emits every event to the `stream.Tail`; use that composite sink for `StreamingOrchestrator.Events`, and pass only the tail to `transport.SSEHandler`.
4. Build `tools/agui.Registry` with any server tool registry and an AG-UI client-tool dispatcher.
5. For each AG-UI run request, convert messages and install client tools for that session.
6. Call `runtime.StreamingOrchestrator.Start`.
7. Serve replay/reconnect through `transport.SSEHandler`.
8. Serve interrupts through `transport.InterruptHandler`.

Do not pass `stream.Tail` alone as `StreamingOrchestrator.Events`: it is a live fanout, not durable event storage. The minimal server example includes a concrete durable-plus-live sink pattern.

## POST-SSE Route Shape

The current app's `/agentic`, `/agentic_chat`, `/tool_based_generative_ui`, and `/human_in_the_loop` routes are POST endpoints that return an SSE stream from the same request. To preserve that contract, the Fiber route should:

1. parse `RunAgentInput` before `SendStreamWriter`;
2. default AG-UI `threadId` and `runId` the same way the current app does;
3. map `threadId` to `session.ID`;
4. maintain the AG-UI `runId` as response identity and runtime metadata;
5. assign a server-owned client-tool generation for that session;
6. start the runtime outside the request body's parsing lifetime;
7. stream replay plus live tail events for that session into the current `SendStreamWriter` until the admitted run emits a terminal event.

Moving to a separate `POST -> 202 Accepted` start route plus `GET /events` reconnect route is possible, but it is a client contract change from the current AG-UI example.

## Identity

- Durable session ID: use AG-UI `threadId`, via `session.ID(threadID)`.
- AG-UI thread ID in SSE: render the same `threadId`.
- AG-UI run ID in SSE: keep the request/defaulted AG-UI `runId`, not the generated durable `session.RunID`.
- Runtime run ID: keep `runtime.Handle.RunID()` in an app-owned active-handle map for interrupt/control-plane lookup.
- Reconnect: clients need the durable session/thread ID and AG-UI run ID; replay cursors come from the `after` query value or the app's equivalent cursor persistence.

## Client Tools

AG-UI `RunAgentInput.tools` has no revision field. `tools/agui.Registry` uses generations to reject stale delayed updates, so the consuming server must maintain a monotonic per-session counter and start at `1`. If a request has an empty tool list, clear that session's registered client tools so old frontend tools are not exposed to later runs.

## AG-UI Resume

`RunAgentInput.resume` in the current app is a streamed AG-UI resume path for human-in-the-loop approvals. `transport.ResumeHandler` is different: it is a control-plane helper around `runtime.Resume` and returns `202 Accepted`, not a replacement for the POST-SSE AG-UI resume response.

Until a dedicated AG-UI resume adapter maps resume entries to durable runtime tool-call settlement inside the same SSE response, keep the app-owned resume branch for compatibility. The sketch's `StartRequest` rejects resume payloads to prevent accidentally admitting them as fresh runtime starts.

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
