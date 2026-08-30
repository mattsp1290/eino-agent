# Consumer Guide

This guide is the public embedding contract for a server that wants to use
`eino-agent` as a Go runtime for Eino agents with AG-UI streaming and Datadog
observability. The project is a library, not a hosted service: applications own
routes, auth, tenant mapping, provider credentials, deployment config, and UI
policy.

For a runnable starting point, see `examples/minimal-server` and
`docs/examples/minimal-server.md`.

## Installation Release Candidate

The planned root pin `github.com/mattsp1290/eino-agent@v0.1.3` is a release
candidate, not a supported or usable pin yet. It requires Go 1.26.3 and will
be promoted to the supported pin only after the tag is published and an
unrelated module verifies the complete graph through the standard public Go
proxy and checksum database with no replacement, workspace, vendor tree, or
checkout access.

The release candidate depends on the separately published generated-bindings
module `github.com/mattsp1290/eino-agent/wasmext/gen@v0.1.0`, whose repository
tag is `wasmext/gen/v0.1.0`. Consumers must not add a workaround for that
internal dependency.

## Package Surface

| Package | Use it for | You still provide |
| --- | --- | --- |
| `runtime` | Run admission, active run handles, interruption, resume, turn snapshots, tool execution, typed extension dispatch, and runtime events. | Store, provider/model resolver, run-plan provider, config snapshot, auth, HTTP routes. |
| `session` | Durable sessions, runs, messages, parts, tool calls, context epochs, replay cursors, and recovery records. | A concrete store backend and tenancy-specific session IDs. |
| `store/sqlite` | Embedded transactional `session.Store` implementation. | Database path, lifecycle, backups, migrations policy, production HA choice. |
| `store/storetest` | Contract tests for custom stores. | Backend-specific persistence and isolation tests. |
| `transport` | HTTP adapters for AG-UI SSE replay/live tail, interrupt, resume, and message decoding. | Route layout, middleware, auth, request validation, cursor persistence. |
| `agui` | Durability/replay policy for AG-UI event families and client-tool classification. | Product decisions for conditional reasoning/state/custom-event replay. |
| `stream` | Bounded live event tails for active sessions. | Capacity choice and reconnect/resync UX. |
| `model` | Provider/model catalog and resolver contracts. | Concrete provider clients and credentials. |
| `config` | Immutable run configuration snapshots and validation lifecycle. | Config loading, plugin ordering, secrets source, reload trigger. |
| `tools` | Typed tool definitions and per-run materialization. | Concrete tool definitions and approval policy. |
| `permissions` | Tool permission policy primitives. | Product-specific approval UI and enforcement defaults. |
| `obs` | Redaction/correlation policy definitions for Datadog/eino-obs. | Exporter configuration and any opt-in content summaries. |

## Minimal Embed

A typical server wires these pieces once at startup through
`runtime.NewStreamingOrchestrator`. This snippet is schematic:
`newIDGenerator`, `providerResolver`, and `planProvider` are application-owned
implementations. A successful construction requires a Store, ModelResolver,
RunPlanProvider, and IDGenerator. A successful start also requires a non-empty
request `SessionID`. EventSink, permissions policy, owner ID override, queue
sizing, and lease tuning are optional.

```go
store, err := sqlite.Open(ctx, "agent.db")
if err != nil {
    return err
}
tail := stream.NewTail(128)
ids := newIDGenerator()

orchestrator, err := runtime.NewStreamingOrchestrator(
    runtime.WithStore(store),
    runtime.WithModelResolver(providerResolver),
    runtime.WithRunPlanProvider(planProvider),
    runtime.WithEventSink(tail),
    runtime.WithIDGenerator(ids),
    runtime.WithOwnerID("api-server-1"),
)
if err != nil {
    return err
}
```

The runtime persists eligible non-live events through the run-fenced execution
store before forwarding copies to the configured sink. External sinks are for
live transport and observability delivery; they do not mutate session state.
`stream.Tail` remains live-only, while reconnect replay reads the runtime's
committed messages, parts, epochs, and durable event records.

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
run. Use `Done()` for terminal status and `Interrupt()` for cancellation.

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

Custom stores must implement the complete transactional `session.Store`
contract. Every backend should run `store/storetest.Run` from its tests.

Required semantics:

- `AdmitRun` atomically creates a run and acquires per-session ownership.
- Every existing run ID is rejected with `session.ErrConflict`; starting a run
  is one-shot and never resumes or replays prior admission side effects.
- A second nonterminal run for the same session returns `session.ErrSessionBusy`.
- `SettleRun` atomically records one terminal state and its canonical
  `run_finished` event: completed, failed, or interrupted.
- Replay ordering is deterministic and cursor-based.
- Duplicate ordinary records with identical caller-supplied IDs are
  idempotent; `AdmitRun` is the explicit exception above.
- Duplicate writes with incompatible payloads return `session.ErrConflict`.
- `CreateToolCall`, `ClaimToolCall`, and `SettleToolCall` accept typed request
  envelopes and atomically commit each tool phase with its canonical event;
  claims use owner/token fencing and terminal settlement happens exactly once.
- Startup recovery can list unfinished runs and unfinished tool calls.

SQLite is available as an embedded implementation. Hosted or multi-region
backends must preserve the same observable behavior at the `session.Store`
boundary. The pre-release SQLite backend accepts only its current schema. After
a schema change, recreate local databases rather than relying on upgrade or
rollback compatibility.

## Tool Lifecycle

Mount native and Wasm-backed tool definitions through the same
`composition.Registry`. The embedding host owns the mount and Wasm shutdown:

```go
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
plans := composition.NewRegistry(nil)
component := extension.Component{
    InstanceID: "review-tool-v1",
    Artifact: extension.Artifact{
        Name: "review-tool", Version: "1", Hash: expectedDigest,
        ConfigHash: configDigest, SourceKind: extension.SourceWasm,
    },
}
mount, err := plans.Mount(ctx, component, composition.InstallerFunc(
    func(_ context.Context, registrar *composition.Registrar) error {
        return registrar.Tool(composition.ToolRegistration{
            ID: "review-tool",
            Scope: extension.GlobalScope(), Definition: wasmDefinition,
        })
    },
))
if err != nil {
    return err
}
defer mount.Close(ctx)

orchestrator, err := runtime.NewStreamingOrchestrator(
    runtime.WithStore(store),
    runtime.WithModelResolver(resolver),
    runtime.WithIDGenerator(ids),
    runtime.WithRunPlanProvider(plans),
)
```

Every executable extension enters through a `runtime.RunPlanProvider` and is
bound to one explicit component owner plus stable scope and capability identity
before its descriptor is fingerprinted. The durable descriptor records the
component instance and artifact once around its nested typed handler, tool,
prompt, guard, and restriction identities. Native `tools.Definition` values use the same
`composition.Registrar.Tool` path with a `SourceNative` component. Runs with no
extensions use a fingerprinted empty plan from an explicitly configured
provider; omitting `runtime.WithRunPlanProvider` is a constructor error.

Tool restriction `Allowed` and `Denied` values are sets. Registration rejects
blank names, a completely empty policy, or a name present in both sets, and
canonicalizes duplicates and ordering before the plan is fingerprinted.

Set `ModuleConfig.Observer` when guest log lines should be exported through an
`einoobs.Observer`; `wasmext` attaches the configured module name and verified
digest and enforces a 4 KiB-or-tighter message bound.

`tools.Definition` requires `Decode`, `Encode`, and `Execute`.
`composition.Registry.Mount` validates and freezes definitions under a stable
component identity; deactivation stops future plan acquisition while acquired
plans retain their leases. `config.Snapshot.Tools` controls per-run
enable/disable filtering during tool materialization:

```go
snapshot.Tools.Enabled = []string{"lookup_ticket"}
snapshot.Tools.Disabled = []string{"shell"}
```

Mount the standard `eino-tools` catalog through the same composition registry
used by every other executable component:

```go
plans := composition.NewRegistry(reporter)
component := extension.Component{
    InstanceID: "standard-coding-tools",
    Artifact: extension.Artifact{
        Name: "eino-tools-standard", Version: "63a3c99",
        Hash: adapterArtifactDigest, ConfigHash: catalogPolicyDigest,
        SourceKind: extension.SourceNative,
    },
}
standardMount, err := einotools.MountStandard(ctx, plans, component, einotools.Options{
    Scope: extension.GlobalScope(),
    Catalog: einotoolcatalog.Options{
        URLFetchOptions: urlPolicy,
        TrackerWriter: trackerWriter,
    },
    Permissions: map[string][]string{
        einotoolcatalog.IDFileRead: {"workspace.read"},
        einotoolcatalog.IDShell: {"shell"},
        einotoolcatalog.IDURLFetch: {"network"},
    },
})
if err != nil {
    return err
}
defer standardMount.Close(context.Background())

orchestrator, err := runtime.NewStreamingOrchestrator(
    runtime.WithRunPlanProvider(plans),
    // store, model resolver, IDs, permissions, events, and other options...
)
```

`catalogPolicyDigest` must cover opaque URL client policy, user-interaction
surface and I/O policy, tracker-writer configuration, and the deployment rule
that keeps fingerprinted search/shell executables stable. The adapter owns one
process-wide lock domain for every catalog definition marked non-concurrent.
Admission resolves an existing workspace symlink once and persists that
canonical root for resume.

Filesystem permission patterns are cleaned workspace-relative request paths;
they are lexical, so workspace admission still owns symlink policy inside the
root. Shell commands, URLs, and tracker IDs become operation patterns. Patterns
are bounded to 4096 bytes. `apply_patch` and `user_interact` use stable generic
patterns because one patch can touch several files and questions must not enter
permission metadata. The default MCP `user_interact` leaf returns a `pending`
envelope; the hosting application supplies question correlation and the later
answer flow.

Mount AG-UI client tools into `plans` with `tools/agui.MountClientTools`. Close
the prior session mount before publishing a replacement, and supply a
restart-stable dispatcher artifact ID that changes whenever dispatch behavior
changes.

Runtime-controlled tools use this lifecycle:

1. Select and scope tools from a data-only `runtime.ToolScopeContext`.
2. Atomically persist a pending `session.ToolCall` and its canonical pending event.
3. Atomically claim the call with owner, claim token, lease renewal, and its
   canonical running event.
4. Execute with `context.Context`, durable IDs, scope, approval requester, and
   a bounded `runtime.ToolContext` containing content-free turn metadata.
5. Atomically settle output or error with the reserved tool-result message,
   part, and canonical terminal event.
6. Best-effort publish the exact already-committed phase events to live and
   extension sinks; publication failure never reverses durable state.
7. Emit observability from the committed result.

Tool-call state transitions publish runtime/AG-UI events only after the matching
pending, running, or terminal event is durably committed and when the bridge
policy enables live delivery.

Register native tool transforms through their semantic point API while mounting
the component. Each transform returns the value consumed by the next
registration:

```go
err := extension.OnTransform(
    registrar.Extensions(),
    runtime.ToolResultTransformPoint,
    extension.Registration{
        ID: "tool/result-metadata", Order: runtime.OrderApplication, Scope: scope,
    },
    func(_ context.Context, value runtime.ToolResultTransform) (runtime.ToolResultTransform, error) {
        if value.Result.Metadata == nil {
            value.Result.Metadata = map[string]string{}
        }
        value.Result.Metadata["reviewed"] = "true"
        return value, nil
    },
)
```

Non-idempotent tools must not be retried automatically after interruption or
restart. Retry requires both `runtime.Tool.RetrySafe` and store evidence that
the prior call did not settle.

Tools that touch shared workspace state own synchronization at the resource
boundary. The built-in workspace tools lock canonical roots internally; the
runtime does not advertise a separate scheduling contract.

## Configuration

`config.Snapshot` and the complete mutable Eino message graph are checked and
deep-cloned before any durable admission write. Config reloads, plugin changes,
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

## Consuming extension-aware runs

Use `composition.NewRegistry` when one mount must atomically own typed handlers
and scoped capabilities. Mount instances need stable artifact and effective
configuration hashes; these become durable resume identity. Global scope and
exact session scope route trusted code but do not provide tenant isolation.

Pass the registry via `runtime.WithRunPlanProvider`. The configured agent
prompt is always materialized at `runtime.OrderRuntime`; named mounted prompt
sections are evaluated per provider step around it. `session.Store` exposes
model-request reads, and its run-fenced `ExecutionStore` owns model-request
writes for every provider attempt. Set a retention policy for those records,
and allowlist only non-secret option keys. See the
[`extension point catalog`](architecture/extension-points.md) and the
[`native extension example`](../examples/native-extension).
