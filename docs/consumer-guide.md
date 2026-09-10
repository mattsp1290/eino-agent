# Consumer Guide

This guide is the public embedding contract for a server that wants to use
`eino-agent` as a Go runtime for Eino agents with AG-UI streaming and Datadog
observability. The project is a library, not a hosted service: applications own
routes, auth, tenant mapping, provider credentials, deployment config, and UI
policy.

For a runnable starting point, see `examples/minimal-server` and
`docs/examples/minimal-server.md`.

## Installation

Use the verified SQL-store implementation `v0.3.4-0.20260910012408-cec27e5eb734` at
commit `cec27e5eb734b78a8e6dbe49c07bb8dd1cbac12e` with Go 1.26.3:

```sh
go get github.com/mattsp1290/eino-agent@cec27e5eb734b78a8e6dbe49c07bb8dd1cbac12e
```

This pin includes the host-owned SQLite/PostgreSQL APIs, discovery and session
watch. On 2026-09-10 (UTC), its local gates and an unrelated PostgreSQL consumer
passed through the public Go proxy and checksum database with an empty module
cache, `GOWORK=off`, no replacement, workspace, vendor tree or sibling checkout.
See [the exact evidence](dependency-status.md#sql-store-consumer-publication).
CloudWeGo Eino is `v0.8.13`; PostgreSQL 17 is the supported server baseline.

The separately published generated-bindings dependency remains
`github.com/mattsp1290/eino-agent/wasmext/gen@v0.1.0`, through repository tag
`wasmext/gen/v0.1.0`. Consumers need no workaround for that dependency. Earlier
release/discovery pins use older store APIs or schemas; their evidence is
historical. Existing SQLite files are unsupported and remain untouched.

## Package Surface

| Package | Use it for | You still provide |
| --- | --- | --- |
| `runtime` | Run admission, active run handles, interruption, resume, turn snapshots, tool execution, typed extension dispatch, and runtime events. | Store, provider/model resolver, run-plan provider, config snapshot, auth, HTTP routes. |
| `session` | Durable sessions, runs, messages, parts, tool calls, context epochs, replay cursors, and recovery records. | A concrete store backend and tenancy-specific session IDs. |
| `store/sqlite` | Embedded transactional `session.Store` implementation. | Database path, lifecycle, backups, migrations policy, production HA choice. |
| `store/postgres` | Transactional `session.Store` over a pgx-backed `*sql.DB` for a dedicated PostgreSQL 17 database. | Pool configuration and shutdown, credentials, auth, migration timing, backups and retention. |
| `store/storetest` | Contract tests for custom stores. | Backend-specific persistence and isolation tests. |
| `transport` | HTTP adapters for AG-UI SSE replay/live tail, interrupt, resume, and message decoding. | Route layout, middleware, auth, request validation, cursor persistence. |
| `agui` | Durability/replay policy for AG-UI event families and client-tool classification. | Product decisions for conditional reasoning/state/custom-event replay. |
| `watch` | Coherent bounded durable session windows and same-process transient text. | Exact authorized session, finite capacities, polling/read deadlines, reconnect policy. |
| `stream` | Bounded live event tails for active sessions. | Capacity choice and reconnect/resync UX. |
| `model` | Provider/model catalog and resolver contracts. | Concrete provider clients and credentials. |
| `config` | Immutable run configuration snapshots and validation lifecycle. | Config loading, plugin ordering, secrets source, reload trigger. |
| `tools` | Typed tool definitions and per-run materialization. | Concrete tool definitions and approval policy. |
| `permissions` | Tool permission policy primitives. | Product-specific approval UI and enforcement defaults. |
| `obs` | Redaction/correlation policy definitions for Datadog/eino-obs. | Exporter configuration and any opt-in content summaries. |

SQLite storage uses a host-owned modernc `*sql.DB`. Call `sqlite.Migrate`
explicitly with writers stopped, then `sqlite.New` on the same pool. New only
validates; neither operation closes or configures the pool. Set foreign-key and
busy-timeout options in the URI so they apply to every connection. Construct file
URIs with `net/url` to preserve special characters in paths. Reopen an initialized
file with a fresh host pool and New; migration is unnecessary on that path.

Private `:memory:` databases require one retained connection from migration
through use. For `file:name?mode=memory&cache=shared`, retain a keeper connection
until all stores finish. Do not set connection lifetimes or idle timeouts that
close the final memory connection. Closing the last connection loses that database.
Only the fresh Goose baseline is supported; legacy schemas are rejected without
repair or import. The host owns shutdown and any disposable database cleanup.

### PostgreSQL pool and schema ownership

Both SQL stores share the internal GORM implementation; Goose is their sole
schema authority. PostgreSQL uses fixed `public` tables in a dedicated database.
Shared application schemas, namespace options, other SQL backends and legacy
imports are unsupported. Provision the database in the host, stop writers for
setup, and explicitly call `postgres.Migrate(ctx, pool)`. Normal startup calls
only `postgres.New(ctx, pool)`, which verifies the current schema without DDL.
See [the runnable host example](../examples/postgres-store/README.md).

```go
// Import database/sql, store/postgres and pgx/v5/stdlib.
pool, err := sql.Open("pgx", dsn) // host-supplied secret; do not log it
if err != nil { return err }
defer pool.Close()
if err := pool.PingContext(ctx); err != nil { return err }
// Host setup only, with writers quiesced: postgres.Migrate(ctx, pool).
st, err := postgres.New(ctx, pool)
if err != nil { return err }
_ = st // pass to runtime.WithStore and the committed read adapters
```

Use a finite caller deadline for opening, migration and requests. Hosts using
`pgxpool` can borrow it through `stdlib.OpenDBFromPool(nativePool)`. The host
closes the SQL wrapper first and then the native pool; the store closes neither
and does not change connection settings. Abandoning a store instance releases
no host resource, and store errors do not transfer pool ownership.

Nil pools and unsupported drivers match `session.ErrConflict`; an uninitialized
or unsupported schema also conflicts. Closed pools and canceled contexts return
their underlying errors. Use `errors.Is`/`errors.As`, not formatted error text.
Initialization errors suppress connection text but hosts must still avoid
logging credentials or unwrapped driver details.

Both backends expose the optional `session.ObservationReader` and
`session.SessionDiscoveryReader` capabilities. These read committed snapshots
outside caller transactions and reject transaction-bound store handles.
PostgreSQL writes use read-committed transactions and row locks; its observation
and discovery reads use repeatable-read snapshots. Lease decisions use the
database clock. [Storage architecture](architecture/storage.md#transactions)
describes public nesting and internal mutation savepoints.

Hosts own backups, authorization, retention/deletion scheduling and PostgreSQL
vacuum/health. There is no retention API or automatic Down/reset. Ad hoc row
deletion can break durable relationships and observation invariants. Dropping
a dedicated disposable test database is cleanup, not a production retention
strategy. Rollback requires a database compatible with the chosen implementation
or explicitly disposable fresh data; old SQLite files remain untouched and
cannot be opened by this baseline. Ensemble and Birbparty adoption are separate
projects; existing Ensemble schemas are not migration inputs.

## Minimal Embed

A typical server wires these pieces once at startup through
`runtime.NewStreamingOrchestrator`. This snippet is schematic:
`newIDGenerator`, `providerResolver`, and `planProvider` are application-owned
implementations. A successful construction requires a Store, ModelResolver,
RunPlanProvider, and IDGenerator. A successful start also requires a non-empty
request `SessionID`. EventSink, permissions policy, owner ID override, queue
sizing, and lease tuning are optional.

```go
// sql is database/sql; url is net/url. The SQLite package registers modernc.
uri := url.URL{Scheme: "file", Path: "agent.db", OmitHost: true,
    RawQuery: "_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"}
pool, err := sql.Open("sqlite", uri.String())
if err != nil { return err }
pool.SetMaxOpenConns(1)
pool.SetMaxIdleConns(1)
defer pool.Close() // the host retains this pool until shutdown
if err := sqlite.Migrate(ctx, pool); err != nil { return err }
store, err := sqlite.New(ctx, pool)
if err != nil { return err }
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
admission, err := orchestrator.Start(ctx, runtime.Request{
    SessionID: session.ID("tenant-123/thread-456"),
    Message:   runtime.UserMessage{Content: submittedText},
    Config:    snapshot,
    Metadata:  map[string]string{"workspace_id": "workspace-1"},
})
handle := admission.Handle // non-nil for an unkeyed/new admission
```

The returned `runtime.Handle` is the live control surface for that admitted
run. Use `Done()` for terminal status and `Interrupt()` for cancellation.

For retried ingress, set `AdmissionKey` from the host's frozen event identity.
The first caller receives `AdmissionNew` and its handle. A matching retry
receives `AdmissionExisting`, the same immutable receipt, and no handle; use
`LookupAdmission` or the store/watch APIs to observe the original run. A retry
with different message or included configuration receives
`session.ErrAdmissionConflict`. Keep credentials out of included metadata.
Receipts remain available after completion, failure, interruption, and later
runs. A duplicate never resumes an unfinished run; recovery is an explicit
`Resume` decision by the host.

## Durable Provider-Private State

Providers that require opaque assistant continuation objects can opt into the
state-aware Eino boundary. Register one strict key and one immutable contract;
the core runtime never parses the objects:

```go
codec, err := model.NewEinoJSONExtraStateCodec(model.EinoJSONExtraStateConfig{
    ExtraKey: "openaicodex:reasoning_items",
    Contract: model.ProviderStateContract{
        CodecID: "github.com/mattsp1290/eino-providers/openaicodex/reasoning-items",
        Version: 1,
        CompatibilityKey: "openaicodex-responses-reasoning-v1",
        Limits: model.ProviderStateLimits{
            MaxItems: 32,
            MaxItemBytes: 10 * 1024 * 1024,
            MaxMessageBytes: 16 * 1024 * 1024,
            MaxEnvelopeBytes: 13_985_112,
            MaxStoredMessageBytes: 22_632_024,
        },
    },
})
if err != nil {
    return err
}
streamer, err := model.NewEinoStreamerWithProviderState(einoModel, codec)
if err != nil {
    return err
}
```

The registered `Extra` value must have dynamic type `[]json.RawMessage`, and
each item must be exactly one non-null JSON object. Capture preserves its exact
bytes; durable storage uses standard padded base64 so whitespace and property
order survive SQLite close/reopen. Codec limits must be positive and cannot
exceed the core ceilings shown above (with at most 64 items).

Codecs are trusted in-process adapter code. Provider output and stored bytes
are untrusted and fail closed on malformed JSON/envelopes, limits, ordering,
ownership, provider, codec/version, or compatibility mismatch. A different
model may consume earlier state only when the provider, codec/version, and
compatibility key still match. Otherwise start a new session or create an
explicit state-free active epoch; runtime never silently strips incompatible
active state.

Provider state follows the owning message's database and backup retention. It
is retained but inactive when compaction summarizes that message, is never
copied into summaries, and remains active for retained-tail messages. Raw
`session.Store` access is an operator boundary. Hosts remain responsible for
database encryption, access control, backups, expiry, and deletion because
opaque or encrypted provider data is still sensitive.

Consumers submit only the new user message as usual. They do not parse
Responses items, restore `Extra`, merge history, or write a second history
store. Runtime captures and atomically persists the assistant state, while the
state-aware adapter privately restores it after ledger and extension handling.
Ordinary history, AG-UI replay/live events, request ledgers, extensions, logs,
traces, errors, and snapshots contain no provider-state bytes, base64, or
content-derived digest.

`Message` is exactly one new user submission. Do not copy a client transcript
into the request, preload provider messages, or append the submitted user or
assistant records yourself. During admission, runtime loads the session's
committed history, adds the current user text to the provider snapshot, and
atomically persists the new user message/part and assistant placeholder. A
second run for the same settled session therefore receives the first durable
user/assistant pair followed by only its new user message. `LoadHistory` and
AG-UI replay read that same durable transcript.

The persistence outcome depends on where a run stops:

- A synchronous admission failure commits none of that attempted run's
  transcript records. If this was a later admission, the existing session and
  earlier transcript remain unchanged.
- A provider or pre-execution failure after successful admission retains the
  admitted user text and an empty assistant placeholder, and finishes the run
  as failed.
- An interruption after admission retains the same pair and finishes the run
  as interrupted.
- Successful settlement retains the user text and the completed assistant
  content.

These records are runtime-owned. Event sinks, HTTP adapters, and application
stores must not dual-write them; doing so creates duplicate replay history and
bypasses the run fence.

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

SQLite and PostgreSQL implement this boundary through shared persistence code.
Their constructors accept only the current explicit baseline. This does not
promise compatibility with older SQLite files, existing application-owned
PostgreSQL schemas, or other hosted/multi-region database implementations.

## Tool Lifecycle

Mount native and Wasm-backed tool definitions through the same
`composition.Registry`. The embedding host owns the mount and Wasm shutdown:

```go
loader := wasmext.NewLoader()
wasmDefinition, err := loader.LoadTool(ctx, wasmext.ModuleConfig{
    Name:           "review_tool",
    Path:           "extensions/review-tool.wasm",
    AllowedRoot:    "extensions",
    ExpectedSHA256: expectedDigest,
})
if err != nil {
    return err
}
plans, err := composition.NewRegistry(nil)
if err != nil {
    return err
}
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
defer func() {
    mount.Deactivate()
    closeWithin := func(closeFn func(context.Context) error) error {
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        return closeFn(shutdownCtx)
    }
    _ = closeWithin(mount.Close)
    _ = closeWithin(loader.Close)
}()

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

`tools.Definition` requires `Name` and `Execute`. `Normalize` and `Pattern` are
optional callbacks; typed adapters can supply them without changing the
JSON-native runtime contract.
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
plans, err := composition.NewRegistry(reporter)
if err != nil {
    return err
}
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
defer func() {
    standardMount.Deactivate()
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _ = standardMount.Close(shutdownCtx)
}()

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

### Delegated web-search extension

The canonical `web_search` contract is deliberately not defined in this
repository. It belongs to `eino-agent-extensions`; see the
[`delegated web-search ownership decision`](architecture/web-search-extension-ownership.md)
and its
[`fresh-module runtime proof`](../testdata/external-consumer/delegated_web_search_fixture_test.go).
The integration uses these public target seams:

- Define the JSON-native boundary with `tools.Definition`, a raw
  `tools.InputNormalizer`, `tools.TypedPermissionPattern`, and
  `tools.TypedExecutor`. Ordinary `tools.TypedNormalizer` is suitable only if
  the input type itself implements equivalently strict unmarshalling.
- Publish it with `composition.Registry.Mount` and
  `composition.ToolRegistration`; freeze execution with `AcquireRunPlan`, and
  recover with `AcquireResumePlan`.
- Identify native ownership through `extension.Component`,
  `extension.Artifact`, `extension.SourceNative`, and a global or exact-session
  scope.
- Pass the registry through `runtime.WithRunPlanProvider`. Execution receives
  `runtime.ToolCall`, bounded `runtime.ToolContext`, and the executor's
  `context.Context`; `runtime.RetentionPolicy` enforces the extension's
  calculated output budget.
- For strict resume, capture the fingerprinted `runtime.RunPlan.Descriptor`,
  call `runtime.RunPlan.Release`, verify the persisted
  `session.ExtensionPlanDescriptor` with
  `session.VerifyExtensionPlanForSession`, and pass the resulting
  `session.SealedExtensionPlan` in `runtime.ResumePlanRequest`. Mismatch is
  `runtime.ErrExtensionPlanMismatch`, and every returned plan must be
  released. `session.SealExtensionPlanForSession` is separately reserved for
  newly reconstructed fingerprintless descriptors, not plan descriptors.
- Apply host policy through `permissions.Policy` or
  `permissions.StaticPolicy`.
- Shut down by calling `composition.Mount.Deactivate`, then `Close` with a new
  finite context. Deactivation removes the tool from future plans while
  already acquired plans retain their lease until `runtime.RunPlan.Release`.

The extension fixes the tool name and registration ID at `web_search`, the
permission at `network.web.search`, and the non-sensitive constant permission
pattern at `web_search`. The host supplies and rotates honest component
artifact identity, owns credentials and backend lifecycle, and never places
secrets in configuration hashes or model input.

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

`config.Snapshot` is checked and deep-cloned before admission. Runtime builds
the provider message graph from committed durable history plus the one current
`UserMessage`, then freezes that graph for the admitted turn. Config reloads,
plugin changes, permission changes, and provider/model changes affect later
runs only. They do not mutate an in-flight turn snapshot.

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


## Session state observation

Session observation is included in the verified SQL-store pin under Installation.
Construct one observation service for the store and share it with all observed
orchestrators in this process:

```go
observer, err := watch.NewService(store, watch.Options{
    Snapshot: session.ObservationLimits{
        MaxMessages: 50, MaxTools: 100, MaxParts: 200,
        MaxSnapshotBytes: 1 << 20, MaxTextBytes: 128 << 10,
    },
    PollInterval: 50 * time.Millisecond, ReadTimeout: time.Second,
    MaxSubscriptions: 64, MaxWatchedSessions: 32, MaxLiveRuns: 32,
    MaxLiveTextBytes: 1 << 20, PendingUpdates: 64,
})
if err != nil { return err }
// Include runtime.WithSessionObserver(observer) in orchestrator construction.
sub, err := observer.Watch(ctx, authorizedSessionID)
if err != nil { return err }
defer sub.Close()
initial := sub.Initial() // detached committed snapshot, including Exists=false
_ = initial
for {
    update, err := sub.Next(ctx)
    if err != nil { return err }
    // Durable replaces the whole message/run/tool window.
    // Live replaces transient text for its exact attempt identity.
    // LiveUnavailable removes the qualified transient overlay.
    _ = update
}
```

The values above illustrate finite host choices. Every option must be positive.
MaxMessages selects the most recent eligible user/assistant messages;
OmittedOlderMessages marks older omitted history. MaxTools and MaxParts bound
the whole snapshot, including empty text parts. MaxTextBytes bounds cumulative
display text. MaxSnapshotBytes charges a conservative encoded budget: 256
bytes per record plus six times each included string's UTF-8 length. Oversized
included content returns `session.ErrObservationTooLarge` with no partial view.
SQL selects at most each configured row limit plus one and does not load
excluded provider, reasoning, system, or tool-result payloads.

A durable watermark combines a persistent random store incarnation, exact
SessionID, and nonnegative signed 64-bit revision. Only committed mutations
advance it. Timestamps, opaque IDs, and model-attempt sequences do not order
revisions. Durable replacements may skip intermediate transactions; this is a
state watch, not an audit changefeed. A store incarnation change or revision
regression requires a fresh watch.

Live updates carry service incarnation, session/run/message/model-request
identity, attempt/step, attempt sequence, and publication version. They never
advance a durable watermark. Replace a matching unfinalized visible message's
overlay; purge overlays when their message or run leaves the snapshot, the
message finalizes, or the run becomes terminal. Do not concatenate transient
text onto finalized text. The AG-UI WatchBridge implements these rules.

The live cache retains current text without subscribers. MaxLiveRuns bounds
entries and MaxLiveTextBytes bounds total retained text. Missing capacity or
text overflow produces explicit unavailability instead of a false complete
prefix. An unavailable notice with an empty MessageID qualifies the active run
when no eligible placeholder is visible; it never creates a message. Attempt
completion and run release clear retained text. Service
recreation cannot restore lost transient prefixes. This is same-process live
observation; separate SQLite connections provide committed read isolation,
not distributed token delivery.

One poll worker is shared per watched session and stops after its last
subscription detaches. PollInterval checks durable revisions even without
runtime hints or EventSink delivery. ReadTimeout bounds each store operation;
custom readers must honor context cancellation and return committed detached
snapshots. A read failure terminates observation with a content-free error.

PendingUpdates bounds coalesced updates. Overflow discards pending data,
detaches that subscription, and exposes `watch.ErrResyncRequired` outside the
queue even if no later publication occurs. Call Watch again for recovery.
`Resnapshot` is only for a live subscription: it discards obsolete queued work,
reads fresh durable state and reseeds current live text even while a provider
is paused. A failed resnapshot ends the subscription. Concurrent Next calls
return ErrBusy; canceling a Next context cancels that wait only. Canceling the
Watch lifetime or calling Close detaches. Initial and returned updates own
their slices.

The allowlist includes user/assistant display text, durable IDs, finalized
flags, safe run status/provider/model/active-lease fields, and tool identity,
name and status. It excludes raw errors, claim tokens, system instructions,
configuration, metadata, reasoning, private provider state, tool arguments and
tool results. Display text itself can contain sensitive user content. Hosts
must authorize the exact session and escape text appropriately.

Host shutdown remains explicit: stop admitting requests, interrupt and await
owned handles, deactivate and close mounts with bounded contexts, close any
Wasm loaders, close subscriptions/service and any legacy tail, then close the
store after all users drain. Service.Close(ctx) owns only observation and can
be called again after a timeout. StreamingOrchestrator has no public Close.

The external-consumer fixture exercises SQLite, mounted native tools, real
scripted streaming, blocked sinks, detach, overflow recovery, interruption,
strict fenced tool resume, reopen and cleanup without credentials. The local
gate checks the current checkout with independently resolved dependencies.
The verified discovery/watch pin also passed the published-mode fixture;
see [publication evidence](dependency-status.md#workspace-discovery-publication).
`make windows-compile` checks pure-Go session/watch and tools/einotools;
transitive Wasm dependencies still limit the broader runtime platform surface.

## Workspace conversation discovery

Allocate conversation IDs independently of workspace IDs. Built-in SQLite
implements the optional `session.SessionDiscoveryReader`; another Store must
opt in explicitly. If unavailable, return a host-level unavailable-discovery
error rather than querying backend internals. SQLite and the reusable
`storetest.RunDiscovery` contract cover empty and completed conversations.

After authorizing the workspace, the public create/list/select flow is:

```go
// st is a session.Store. Allocate a new conversation ID in host code.
now := time.Now().UTC()
_, err := st.CreateSession(ctx, session.Session{
    ID: conversationID, WorkspaceID: authorizedWorkspace,
    Title: string(conversationID), CreatedAt: now, UpdatedAt: now,
})
if err != nil { return err }
reader, ok := st.(session.SessionDiscoveryReader)
if !ok { return errors.New("session discovery unavailable") }
page, err := reader.ListSessions(ctx, session.SessionDiscoveryQuery{
    WorkspaceID: authorizedWorkspace, Limit: 50,
})
if err != nil { return err }
// Show page.Sessions; select and authorize one returned ID for history/Start.
// For another page, supply the same workspace and page.NextCursor.
// Stop when NextCursor is empty. An empty cursor starts a refresh.
```

Continue the chosen ID with the existing `orchestrator.Start` flow, supplying
matching `Config.Metadata["workspace_id"]`. To pre-create sessions compatible
with current runtime admission, initially use Title=ID, empty ParentID and
Directory, nil session/request Metadata, and omit `workspace_root`. Reopening
the same database and rediscovering either ID preserves independent histories.
The executable public journey is
`testdata/external-consumer/session_discovery_fixture_test.go`.

Discovery reads the current stored title, including empty or edited titles. Safe
arbitrary title mutation and admission after renaming remain the separate request
at `~/.agents/projects/eino-agent/requests/2026-09-08-durable-conversation-renaming.md`.
This feature does not resolve that request or the full TUI milestone.

Workspace selectors are exact UTF-8 strings (1–1024 bytes), with no wildcard or
normalization. They carry no authorization. Hosts authorize every page and every
selected conversation. Limit defaults to 50, maximum 100; cursors are bounded to
8192 bytes and bind database/workspace. Summaries expose only ID, workspace,
current title and creation/update timestamps. ID/workspace have 1024-byte ceilings,
title 16384; an oversized included record fails the entire page. Titles can contain
user text requiring host sanitization.

Ordering is creation time descending, then bytewise ID descending, with zero
times last. UpdatedAt is metadata time, not message activity. Refresh to see new
rows ahead of the cursor or changed titles already displayed. Inserts behind may
appear later; changing workspace/creation keys during traversal requires restart.
Each page is a committed view with no retained snapshot. List outside store
transactions and handle context cancellation and the stable `ErrDiscovery*`
classes described in the [storage contract](architecture/storage.md#workspace-session-discovery).

Hosts own canonicalization, authorization, numbering, excerpts, title fallback,
selection/preferences, launch and active-turn switching policy. SQLite's new
projection schema intentionally rejects older databases without modification.
Preserve desired data and explicitly choose a current-schema database before
switching; rollback pairs the prior binary with its prior database. There is no
automatic deletion or migration.
