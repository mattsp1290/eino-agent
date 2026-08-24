# Runtime Architecture

Date: 2026-06-27

This document defines the first public architecture boundary for
`eino-agent`. It builds on:

- `docs/architecture/reference-integration-research.md`
- `docs/architecture/reference-runtime-research.md`
- `docs/dependency-status.md`

The goal is a reusable Go runtime for Eino-based agents that provides durable
session/run orchestration, AG-UI streaming/replay boundaries, typed tool
settlement, Datadog-observable lifecycle events, provider/model catalog
resolution, and host-application embedding hooks.

## Design Principles

`eino-agent` is an orchestration layer, not a replacement for the pinned
libraries.

- `eino-agui` remains the AG-UI protocol bridge. `eino-agent` uses it for
  message conversion, SSE emission, stream tapping, and AG-UI client-tool
  classification.
- `eino-tools` remains the reusable coding-tool implementation set.
  `eino-agent` materializes those tools into runtime-controlled tool calls,
  permissions, durable settlement, and observability.
- `eino-obs` remains the observability helper library. `eino-agent` emits
  runtime lifecycle data into it rather than defining a second telemetry stack.
- Eino remains the model/tool execution framework. `eino-agent` admits, locks,
  snapshots, observes, and recovers runs around Eino calls.

## Package Map

| Package | Responsibility | Deliberately not responsible for |
| --- | --- | --- |
| `session` | Durable sessions, runs, messages, parts, tool-call records, context epochs, replay cursors, and transaction boundary contracts. | Provider clients, tool implementations, AG-UI SSE delivery, Datadog export. |
| `runtime` | Run admission, per-session execution ownership, turn snapshots, interruption, tool-call settlement orchestration, hooks, context-source timing, and internal events. | Durable store implementation, concrete provider SDKs, concrete tools, protocol-specific transport. |
| `model` | Provider/model catalog, model selection, request-time auth/env resolution, and Eino chat-model construction contracts. | Prompt assembly, session storage, tool execution, provider-specific UI. |
| `config` | Immutable runtime configuration snapshots, agent profiles, provider config, tool permission config, context sources, hooks, plugins, validation. | File format policy, plugin installer, provider client construction. |

Future implementation beads may add adapter packages, but they should stay
thin:

- `agui` or `transport/agui`: HTTP/SSE admission and replay/tail APIs that use
  `eino-agui`.
- `tools`: adapters that wrap `eino-tools` packages in `runtime.Tool`.
- `observability`: adapters that map `runtime.Event` and durable records into
  `eino-obs`.
- `store/*`: concrete `session.Store` implementations.

Import direction is part of the public contract:

- `session` is independent of `runtime`, `model`, and `config`.
- `config` may refer to `model` selection types, but not to runtime execution
  or session storage.
- `model` is independent of `runtime`, `session`, and `config`.
- `runtime` coordinates `session`, `config`, and `model`.
- AG-UI, tool, observability, and store adapters depend on these contracts; the
  core packages do not depend on concrete adapters.

## Session and Run Lifecycle

The runtime treats a run as durable before it is executable.

1. Host code loads and validates a `config.Snapshot`.
2. Runtime creates or locates a `session.Session`.
3. Runtime admits a `session.Run` with selected agent, provider, model,
   config/plugin/component snapshot metadata, parent message/run IDs, and
   context epoch.
4. Runtime acquires the implementation-defined per-session execution lock.
5. Runtime emits internal `runtime.EventRunStarted`.
6. Runtime builds a `runtime.TurnSnapshot`.
7. Runtime resolves tools and provider model for that snapshot.
8. Runtime streams the Eino provider call, persists replayable messages/parts,
   emits live-only deltas, and settles tool calls.
9. Runtime flushes save-point work: pending messages, tool results, context
   epoch transitions, observability data, and hook results.
10. Runtime finishes the run as completed, failed, or interrupted before
    releasing the session lock.

This avoids conflating a live Eino stream with the durable conversation. A
client can disconnect from AG-UI SSE without changing the authoritative run
state.

## Durable Store Boundary

The `session.Store` interface is intentionally centered on durable facts:

- sessions and session metadata;
- admitted runs and terminal run state;
- ordered replayable messages and parts;
- tool-call claim and settlement records;
- context epoch start/finish records.

The `session.Transactor` interface is separate so concrete stores can provide
SQL, embedded log, or in-memory test transactions without shaping every runtime
method around one database.

Store implementations must provide these invariants:

- an admitted run is visible before provider streaming begins;
- `session.Store.AdmitRun` is the atomic per-session ownership operation;
- only one nonterminal run owns a session unless a future branch design
  explicitly allows parallel ownership;
- `ErrSessionBusy` is returned when admission conflicts with an active owner;
- leases and owner IDs support stale-owner detection without relying on process
  IDs alone;
- replay reads come from messages and parts, not from captured SSE frames;
- unfinished runs and tool calls can be detected after restart;
- non-idempotent unfinished tool calls are not automatically rerun.

Recovery implementations use `ListUnfinishedRuns`, `ActiveRun`, and
`ListUnfinishedToolCalls` to conservatively mark unfinished work interrupted,
or to retry only when an explicit retry-safe contract allows it.

## Turn Snapshots and Context Epochs

`runtime.TurnSnapshot` is internal state for one provider request. Runtime
admission checks and deep-clones `config.Snapshot` and the complete supported
Eino message graph before any durable write. It
captures:

- durable run/session/context epoch IDs;
- the validated config snapshot;
- the resolved provider/model client;
- provider-ready Eino messages;
- materialized runtime tools;
- resolved system prompt;
- creation time.

Config reloads, composition mount changes, and user follow-ups affect future
turn snapshots only. They do not mutate an in-flight model call.

Each fresh or resumed run also owns an explicit internal execution object. It
holds the frozen `RunPlan`, extension dispatch, and composed event sink and is
passed through admission, request-ledger, model, tool, settlement, and resume
boundaries. Run-plan state is never hidden in `context.Context`; contexts carry
cancellation and deadlines only. The execution object releases its frozen plan
exactly once when the run goroutine exits.

Compaction creates a new `session.ContextEpoch`. A context epoch records the
parent epoch, summarized message range, summary message, retained tail start,
provider/model used for the summary, trigger/reason, and next-run policy.
Replay can therefore explain which full history was summarized, which tail
remained active, and whether runtime stopped, auto-continued, or replayed the
triggering prompt.

## Message Replay Versus Live AG-UI

Replayable history is stored as `session.Message` and ordered `session.Part`
records. AG-UI live events are transport output for active runs.

AG-UI adapters should:

- use `eino-agui/convert` to convert replayable history into AG-UI messages;
- use `eino-agui/emitter` and `eino-agui/stream` for live SSE emission and
  model stream tapping;
- mark model text/reasoning deltas as live-only runtime events;
- persist settled message/part snapshots separately from SSE delivery state;
- expose replay and live-tail APIs that are explicit about this distinction.

The durable store should not persist old SSE frames as the replay source of
truth.

## Tool Lifecycle

`runtime.Tool` is the runtime envelope around an Eino-compatible tool. It
contains Eino `ToolInfo`, an executor, retry-safety metadata, and runtime
metadata. The concrete leaf behavior should come from `eino-tools` where
possible.

Every tool call follows this durable lifecycle:

1. model requests a tool call;
2. runtime persists a pending `session.ToolCall`;
3. runtime claims the call with an owner and claim token before executing it;
4. tool receives `context.Context`, session/run/message/call IDs, and an
   approval requester;
5. runtime converts the protected outcome to one bounded canonical output;
6. runtime settles the call and reserved tool-result message/part through one
   shared fresh/resume operation;
7. observability receives tool start/end/error events.

If interruption happens while a call is running, runtime settles the durable
record as interrupted. Automatic retry is allowed only when the tool declares it
retry-safe and the store proves the prior call did not settle.

Tool materialization receives only bounded scope data, and execution receives
only durable call data plus content-free turn metadata. Filesystem-like tools
own synchronization around canonical workspace roots so `file_read`,
`file_write`, `file_edit`, search, shell, and patch operations do not corrupt
shared state. Tool input decoding and output retention are runtime policy, not
hidden metadata conventions.

## Provider and Model Resolution

The `model` package separates metadata from concrete clients:

- `Catalog` lists providers, models, capabilities, and defaults.
- `AuthResolver` resolves credentials at request time.
- `Resolver` builds an Eino `ToolCallingChatModel` for one selected model and
  runtime environment.

Runtime captures the selected provider/model in `session.Run` and captures
component versions in the run metadata. Provider-specific SDK concerns stay
behind model adapters.

## Config and Plugin Lifecycle

`config.Snapshot` is immutable at run admission. It includes:

- agent profile;
- selected model;
- provider options;
- enabled/disabled tool names and permission rules;
- context source declarations;
- hook registrations;
- plugin identity and provenance.

Config loaders may merge files, environment, remote config, or host-supplied
state, but runtime consumes only the validated snapshot. Plugin loading,
ordering, and disposal are explicit lifecycle concerns. Hook execution should be
deterministic, and hook failures after durable commit must not silently roll
back committed session state.

## Observability Boundary

`runtime.Event` is the internal event envelope. It is not a replacement for
`eino-obs`; it is the runtime's stable source for observability adapters. Common
correlation fields, provider/model IDs, message/tool/epoch IDs, usage, error
classification, and redaction class are first-class fields. Adapter-specific
payload JSON is optional and must not be required for common Datadog or AG-UI
correlation.

Observability adapters should map:

- run start/finish/interruption;
- provider request start/finish/error;
- text/reasoning/tool deltas where useful;
- tool call claim/finish/error/interruption;
- context epoch creation and compaction;
- replay and recovery decisions.

Redaction policy is intentionally not finalized here. The follow-up Datadog
bead owns exact redaction defaults and exported attribute names.

## Consumer Embedding

Host applications embed the runtime by providing:

- a `session.Store` and optional `session.Transactor`;
- a `config.Loader` and `config.Validator`;
- a `model.Catalog`, `model.AuthResolver`, and `model.Resolver`;
- a `runtime.RunPlanProvider`, normally `composition.Registry`, for tools,
  prompts, guards, and typed extension handlers;
- one or more `runtime.EventSink` adapters for AG-UI and observability.

The host remains responsible for HTTP routing, authentication, tenancy,
deployment-specific config discovery, concrete database selection, and UI
policy.

### Preferred construction

`runtime.NewStreamingOrchestrator` applies functional options in order and
validates the complete construction before a run can start. `Store`, `Model`,
and `IDs` are required and are all named in one construction error when
missing. Later scalar options override earlier ones, and nil interface
dependencies are errors. The run-plan provider is the sole executable
extension source; an omitted provider yields a sealed empty plan.

`Admit` is deliberately excluded from options. `admitter()` derives it from
`Store`, `Transactor`, `Events`, and `Clock`, which prevents a second
independently configured dependency graph.

### Tool interception save points

`runtime.ToolPreparePoint` runs in registration order after typed input decode.
The runtime derives the permission pattern from final normalized JSON and
persists the assistant
tool-call part, pending/running events, and durable call only from the final
rewritten JSON. Permission checks and the executor see that same input. A
prepare error still admits and settles a failed durable call and does not abort
unrelated sibling calls.

`runtime.ToolResultTransformPoint` unwinds in reverse registration order after
execution and before encoding/settlement. Durable output, the terminal event,
the tool-result part, and model-visible message all use the final patch. The
executor error is immutable and remains authoritative.

Resume never repeats `ToolPreparePoint`: pending records already hold rewritten
input. A pending call that is reclaimed and executed runs
`ToolResultTransformPoint`; a running call settled interrupted without
re-execution skips it. This is the exactly-once boundary for argument rewriting.

## Initial Public Interfaces

This iteration introduces compileable interface packages:

- `session/doc.go`, `session/types.go`
- `runtime/doc.go`, `runtime/types.go`
- `model/doc.go`, `model/types.go`
- `config/doc.go`, `config/types.go`

These interfaces are intentionally narrow enough for implementation beads to
change internals without changing downstream contracts. The expected next beads
can build directly against them:

- durable store interfaces and transaction boundaries;
- minimal agent/model/provider/config implementation;
- runtime context and context-source boundaries;
- typed tool registry and scoped materialization;
- AG-UI event durability and replay rules;
- Datadog observability redaction policy.

## Frozen extension plans

Admission freezes callbacks, tools, prompts, guards, and restrictions into one
leased plan and persists its runtime-derived canonical descriptor. Resume
resolves an exact current-schema fingerprint match before any durable state
change. The authoritative point catalog and model and tool pipeline diagrams are in
[`extension-points.md`](extension-points.md).

The optional model-request ledger assigns one `(attempt, step)` record per
adapter invocation. Here, a step is a provider request plus its resulting tool
batch; it does not add a user-visible turn abstraction.
