# Consumer Guide

This guide is the public embedding contract for a server that wants to use
`eino-agent` as a Go runtime for Eino agents with AG-UI streaming and Datadog
observability. The project is a library, not a hosted service: applications own
routes, auth, tenant mapping, provider credentials, deployment config, and UI
policy.

For a runnable starting point, see `examples/minimal-server` and
`docs/examples/minimal-server.md`.

## Package Surface

| Package | Use it for | You still provide |
| --- | --- | --- |
| `runtime` | Run admission, active run handles, interruption, resume, turn snapshots, tool execution, hooks, and runtime events. | Store, provider/model resolver, tool registry, config snapshot, auth, HTTP routes. |
| `session` | Durable sessions, runs, messages, parts, tool calls, context epochs, replay cursors, and recovery records. | A concrete store backend and tenancy-specific session IDs. |
| `store/sqlite` | Embedded SQLite `session.Store` and `session.Transactor` implementation. | Database path, lifecycle, backups, migrations policy, production HA choice. |
| `store/storetest` | Contract tests for custom stores. | Backend-specific persistence and isolation tests. |
| `transport` | HTTP adapters for AG-UI SSE replay/live tail, interrupt, resume, and message decoding. | Route layout, middleware, auth, request validation, cursor persistence. |
| `agui` | Durability/replay policy for AG-UI event families and client-tool classification. | Product decisions for conditional reasoning/state/custom-event replay. |
| `stream` | Bounded live event tails for active sessions. | Capacity choice and reconnect/resync UX. |
| `model` | Provider/model catalog and resolver contracts. | Concrete provider clients and credentials. |
| `config` | Immutable run configuration snapshots and validation lifecycle. | Config loading, plugin ordering, secrets source, reload trigger. |
| `tools` | Typed tool registry and materialization helpers. | Concrete tool definitions and approval policy. |
| `permissions` | Tool permission policy primitives. | Product-specific approval UI and enforcement defaults. |
| `obs` | Redaction/correlation policy definitions for Datadog/eino-obs. | Exporter configuration and any opt-in content summaries. |

## Minimal Embed

A typical server wires these pieces once at startup through
`runtime.NewStreamingOrchestrator`. This snippet is schematic:
`newIDGenerator`, `providerResolver`, `toolRegistry`, and `eventSink` are
application-owned implementations. The required fields for a successful minimal
start are `Store`, `Model`, `IDs`, and a non-empty request `SessionID`.
`Transactor`, `Events`, `Tools`, `Permissions`, `OwnerID`, queue sizing, leases,
hooks, and context sources add production behavior.

```go
store, err := sqlite.Open(ctx, "agent.db")
if err != nil {
    return err
}
tail := stream.NewTail(128)
ids := newIDGenerator()

orchestrator, err := runtime.NewStreamingOrchestrator(
    runtime.WithStore(store),
    runtime.WithTransactor(store),
    runtime.WithModelResolver(providerResolver),
    runtime.WithToolRegistry(toolRegistry),
    runtime.WithEventSink(eventSink{Store: store, Tail: tail, IDs: ids}),
    runtime.WithIDGenerator(ids),
    runtime.WithOwnerID("api-server-1"),
)
if err != nil {
    return err
}
```

The event sink is replay-critical. `stream.Tail` alone is live-only; a server
that wants reconnect/replay audit records must append durable non-live events
to `session.Store` before or alongside tail emission:

```go
func (s eventSink) Emit(ctx context.Context, event runtime.Event) error {
    if !event.LiveOnly {
        if event.EventID == "" {
            event.EventID = s.ids.NewEventID()
        }
        if _, err := s.store.AppendEvent(ctx, eventRecord(event)); err != nil {
            return err
        }
    }
    return s.tail.Emit(ctx, event)
}
```

`runtime.EventRunStarted` is already persisted during admission by the current
orchestrator implementation, so sinks may skip duplicating that event if they
share the same store.

When admitting a run, the host supplies durable session identity, user input,
and an immutable `config.Snapshot`:

```go
handle, err := orchestrator.Start(ctx, runtime.Request{
    SessionID: session.ID("tenant-123/thread-456"),
    Input:     messages,
    Config:    snapshot,
    Metadata:  map[string]string{"workspace_id": "workspace-1"},
})
```

The returned `runtime.Handle` is the live control surface for that admitted
run. Use `Done()` for terminal status, `Interrupt()` for cancellation, and
`FollowUp()` only when the runtime and product workflow allow it.

## HTTP and AG-UI

`transport.SSEHandler` combines durable replay from `session.Store` with live
events from `stream.Tail`:

```go
sseHandler := transport.SSEHandler(transport.SSEConfig{
    Store:   store,
    Tail:    tail,
    Auth:    authenticate,
    Session: sessionFromRoute,
    Cursor:  cursorFromRequest,
    ThreadID: func(_ *http.Request, id session.ID) string {
        return string(id)
    },
})
```

The handler does not create sessions, admit runs, or authorize callers. Your
server should put it behind existing middleware and should expose run admission,
interrupt, and resume endpoints with product-specific validation.

`transport.InterruptHandler` and `transport.ResumeHandler` adapt those control
endpoints to runtime handles, but the application decides how to locate handles
and who may operate on them.

The minimal server example shows interrupt wiring and omits resume routing to
keep the example small; production servers that expose resume should wrap
`transport.ResumeHandler` with the same auth and handle lookup policy used for
run control.

A simple cursor contract is `after=<event_id>&limit=<n>`, where `after` maps to
`session.EventCursor.AfterEventID` and `limit` bounds replay. `SSEConfig.OnComplete`
receives the next durable cursor and any replay/live-tail error; use it to
persist client cursor state or log that the client needs a fresh snapshot.
Live-tail overflow means the subscriber fell behind a bounded queue, so the
client should reconnect and resync from durable replay rather than assuming it
received every live event.

## Durable Versus Live-Only

The durable source of truth is:

- `session.Session`
- `session.Run`
- `session.Message`
- ordered `session.Part`
- `session.ToolCall`
- `session.ContextEpoch`
- selected `session.EventRecord` audit/status records

Live-only data is:

- model text deltas before they settle into assistant message parts;
- transient reasoning or activity deltas;
- AG-UI custom events that are not promoted to a durable contract;
- live-tail overflow notices;
- transport write attempts and old SSE frames.

Replay must reconstruct `MESSAGES_SNAPSHOT` from durable messages and parts. It
must not infer conversation content from arbitrary event payloads or replay old
SSE frames. Event records are useful for audit, recovery, observability, and
cursor boundaries; they are not a substitute for durable message/part history.

## Storage Requirements

Custom stores must implement `session.Store`; transactional stores should also
implement `session.Transactor`. Every backend should run `store/storetest.Run`
from its tests. Backends that expose `session.Transactor` should also run
`store/storetest.RunTransactional`.

Required semantics:

- `AdmitRun` atomically creates a run and acquires per-session ownership.
- A second nonterminal run for the same session returns `session.ErrSessionBusy`.
- `FinishRun` records one terminal state: completed, failed, or interrupted.
- Replay ordering is deterministic and cursor-based.
- Duplicate writes with identical caller-supplied IDs are idempotent.
- Duplicate writes with incompatible payloads return `session.ErrConflict`.
- Tool calls are created pending, claimed with owner/token fencing, and settled
  exactly once.
- Startup recovery can list unfinished runs and unfinished tool calls.

SQLite is available as an embedded implementation. Hosted or multi-region
backends must preserve the same observable behavior at the `session.Store`
boundary. SQLite migrations are backend-owned durable state changes: take
backups before production migrations, test rollback from a copied database, and
do not change migration order after release.

## Tool Lifecycle

Register native and Wasm-backed tools through the same `tools.Registry` path.
The embedding host owns Wasm shutdown:

```go
registry := tools.NewRegistry()
_, err := registry.Register(tools.Definition{
    Name:        "lookup_ticket",
    Description: "Look up a support ticket by ID.",
    Parameters:  params,
    Decode:      decodeTicketInput,
    Encode:      encodeTicketOutput,
    Execute:     executeTicketLookup,
    RetrySafe:   true,
})
if err != nil {
    return err
}

loader := wasmext.NewLoader()
defer loader.Close(ctx)
wasmDefinition, err := loader.LoadTool(ctx, wasmext.ModuleConfig{
    Name:           "review_tool",
    Path:           "extensions/review-tool.wasm",
    AllowedRoot:    "extensions",
    ExpectedSHA256: expectedDigest,
})
if err != nil {
    return err
}
if _, err := registry.Register(wasmDefinition); err != nil {
    return err
}

orchestrator, err := runtime.NewStreamingOrchestrator(
    runtime.WithStore(store),
    runtime.WithModelResolver(resolver),
    runtime.WithIDGenerator(ids),
    runtime.WithToolRegistry(registry),
)
```

`tools.Definition` requires `Decode`, `Encode`, and `Execute`. `Register`
creates a generation; `Replace` updates only the active generation so delayed
plugin reloads cannot overwrite newer definitions. `config.Snapshot.Tools`
controls per-run enable/disable filtering during tool materialization:

```go
snapshot.Tools.Enabled = []string{"lookup_ticket"}
snapshot.Tools.Disabled = []string{"shell"}
```

To register the standard coding-agent leaf tools from `eino-tools`, call
`tools/einotools.RegisterDefaults(ctx, registry, options)`. To compose
server-side tools with AG-UI client tools, wrap the server registry with
`tools/agui.NewRegistry(serverRegistry, dispatcher)` and update per-session
client definitions through `SetClientTools`. The server still owns client-tool
dispatch authorization and any product-specific approval flow.

Runtime-controlled tools use this lifecycle:

1. Materialize tools for one immutable `runtime.TurnSnapshot`.
2. Persist a pending `session.ToolCall` before execution.
3. Claim the call with owner, claim token, and lease.
4. Execute with `context.Context`, durable IDs, scope, and approval requester.
5. Settle output, structured output, attachments, metadata, or error.
6. Append tool-result message/part records for the next provider turn.
7. Emit observability and audit events.

Tool-call state transitions can also emit runtime/AG-UI events around pending,
running, and terminal states when the bridge policy enables them.

Non-idempotent tools must not be retried automatically after interruption or
restart. Retry requires both `runtime.Tool.RetrySafe` and store evidence that
the prior call did not settle.

Tools that touch shared workspace state should use sequential concurrency with
a canonical workspace-root key. Independent workspaces may run concurrently.

## Configuration

`config.Snapshot` is frozen at admission. Config reloads, plugin changes,
permission changes, and provider/model changes affect later runs only. They do
not mutate an in-flight turn snapshot.

Applications should validate:

- agent identity and selected model;
- provider credentials and environment policy;
- enabled/disabled tool sets;
- permission rules;
- observability redaction options;
- plugin identity, order, and provenance.

Secrets should be resolved at the host/provider boundary and should not be
stored in durable run metadata.

## Observability

Datadog/LLM observability is opt-in through `eino-obs`. Use no-network or fake
exporters in tests and examples. Safe defaults forbid raw prompts, raw model
outputs, raw tool input/output, attachments, file paths, reasoning, encrypted
reasoning, state snapshots, custom event payloads, secrets, headers, cookies,
and API keys.

High-cardinality IDs such as session, run, message, tool call, thread, and
trace IDs belong on spans/observations/log correlation, not metric labels. Use
low-cardinality labels for service, env, version, operation, provider, model
family, tool kind, status, and error classification.

Bounded input/output summaries require explicit host opt-in and must be
scrubbed before export.

## Migration Notes

When adapting an existing agent backend:

- Pick a stable `session.ID` first; do not use per-turn IDs as the AG-UI
  conversation key.
- Project durable messages and ordered parts on the hot path before claiming
  AG-UI replay support.
- Treat best-effort event logs as forensic input unless they have lossless
  write guarantees.
- Preserve tool-call identity; do not collapse repeated same-name tool calls
  into one ID.
- Emit `runtime.EventRunFinished` only for terminal run outcomes.
- Separate live progress text from replayable assistant content.
- Start with the adapter-service pattern if the existing backend cannot yet own
  AG-UI reconnect cursors, replay store semantics, and redaction policy.

See `docs/integrations/ag-ui-go-server-example.md`,
`docs/integrations/datadog.md`, and `docs/integrations/ensemble.md` for
integration-specific sketches.

## Out Of Scope

`eino-agent` does not own:

- product route layout, cookies, bearer tokens, or tenant authorization;
- hosted provider credentials or secret storage;
- AG-UI protocol implementation internals owned by `eino-agui`;
- Datadog exporter internals owned by `eino-obs`;
- leaf coding-tool behavior owned by `eino-tools`;
- browser/client UI state beyond replayable AG-UI projections;
- raw SSE frame persistence as a replay mechanism;
- completed AG-UI parity for ensemble or any other consumer until that adapter
  exists.
