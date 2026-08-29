# Extension Points And Capability Plans

Date: 2026-08-20

The extension system has three deliberately separate planes:

- session records and the request ledger are durable facts;
- `runtime.EventSink` carries live transport events and best-effort publications
  of already-committed durable records with its existing backpressure behavior;
- typed extension points are in-process semantic callbacks and contained
  observation. They never replace durable writes or transport delivery.

`extension.Registry[T]` owns typed registration, the host's immutable component
payload, deterministic snapshots, references, and cleanup. `composition.Registry`
atomically commits callbacks with tools, prompts, guards, and restrictions and
implements `runtime.RunPlanProvider`; it has no second component map or mount
lifecycle. A run acquires one immutable snapshot. Its dispatch plan is the sole
release authority for handlers and capability-only payloads. Deactivation blocks
new snapshots immediately; `Close` waits for that one reference set to release
and then runs effects in reverse order.

Non-callback selection scopes are derived from the frozen tool, prompt, guard,
and restriction registrations during commit. There is no public lease-only
registration. Snapshot target and instance filters apply atomically to handler
registrations and those capability scopes, so fresh and resumed plans cannot
disagree about an unpersisted lifecycle-only mount.

## Semantic modes, ordering, and failure

Entries sort by `(order, global-before-session, instance ID, registration ID)`.
The point type fixes callback shape and failure behavior:

- notifications registered with `extension.On` call every observer with a
  defensive copy and contain failures;
- hooks registered with `extension.OnHook` run in order and stop at the first
  failure;
- transforms registered with `extension.OnTransform` form an ordered waterfall
  in which each callback returns the next value;
- gates registered with `extension.OnGate` run in order until a decision
  rejects further execution;
- required-around callbacks registered with `extension.OnAround` form an onion
  and must delegate exactly once.

An around callback's guarded `next` is synchronous: it may be called at most
once, must complete before the callback returns, and must not be retained or
called concurrently. Point-owned validators defend immutable identity and
outcome fields for every semantic mode.

Callback-facing model, tool, and call values are data-only projections.
Provider clients, streamers, observers, tool executors, input decoders, and
approval requesters are always nil at the extension boundary; attempts to
inject callable values fail closed. Runtime keeps the authoritative callables
outside the callback graph and closes over them only in the terminal adapter.
Every mounted executable callback, including tool scope resolution, receives
the canonical callback context so closing its own mount fails with
`extension.ErrSelfClose` instead of waiting on its own plan reference.

Notification handlers receive defensive copies. A handler error or panic is
reported locally and never changes the run result or prevents later handlers.
Hook, transform, gate, and around failures return a bounded
`extension.CallbackError`; its raw cause
is available through `errors.Is`/`errors.As` for trusted diagnostics, but its
text is not persisted.

Order constants reserve broad bands: `runtime.OrderHostPolicy` (`-1000`),
`runtime.OrderRuntime` (`0`), and `runtime.OrderApplication` (`1000`).

## Producer and consumer catalog

| Contract ID | Producer | Mode and consumer | Failure | Durable/resume relationship | Wasm |
| --- | --- | --- | --- | --- | --- |
| `eino-agent/runtime/run-admitted` | admission | contained notice to run observers | contained | after durable admission; fresh runs only | hook adapter |
| `eino-agent/runtime/run-started` | execution start | contained notice | contained | run is already running; fresh runs only | native |
| `eino-agent/runtime/run-settled` | run settlement | contained notice | contained | after `SettleRun`; fresh/resumed nonterminal runs | hook adapter |
| `eino-agent/runtime/model-requested` | model dispatch | contained notice | contained | ledger is `dispatch_started` | native |
| `eino-agent/runtime/model-completed` | stream terminal | contained notice | contained | after ledger terminal commit; every attempt | native |
| `eino-agent/runtime/tool-prepared` | tool preparation | contained notice | contained | before durable tool admission; fresh calls | native |
| `eino-agent/runtime/tool-started` | tool claim | contained notice | contained | call is durably running | native |
| `eino-agent/runtime/tool-settled` | atomic settlement | contained notice | contained | call and result are authoritative after one store commit | native |
| `eino-agent/runtime/event-published` | event publication | contained notice to event observers | contained | after infrastructure sink handoff | event-sink adapter |
| `eino-agent/runtime/run-before-execute` | post-admission execution gate | ordered gate | fail run/reject | after admission; fresh runs only | native |
| `eino-agent/runtime/context-assemble` | snapshot preparation | ordered transform waterfall | fail run | fresh preparation; persisted request sees materialized result | context-source adapter |
| `eino-agent/runtime/turn-prepare` | post-tool snapshot preparation | fail-fast hook | fail run | fresh preparation after frozen tools resolve | native |
| `eino-agent/runtime/model-stream` | provider boundary | required around stream | fail attempt | every adapter attempt; never replayed on tool resume | native |
| `eino-agent/runtime/tool-prepare` | normalized tool input | ordered transform waterfall | fail call | fresh only; final input is persisted | tool-middleware adapter |
| `eino-agent/runtime/tool-execute` | allowed tool body | required around execution | protected failure | fresh or pending-call re-execution | native |
| `eino-agent/runtime/tool-result-transform` | protected tool outcome | ordered transform waterfall | fail call | before atomic settlement; fresh/re-executed calls | tool-middleware adapter |

The catalog is checked against every exported core point by
`runtime.TestPublishedExtensionPointsAppearInCatalog`.

## Exact pipelines

Model preparation and dispatch:

```text
durable history + admitted input
  -> runtime/context-assemble contributions
  -> resolve frozen tools
  -> runtime/turn-prepare bounded metadata
  -> render named prompt sections
  -> derive one AuditedModelInput
  -> persist prepared request
  -> persist dispatch_started
  -> runtime/model-requested
  -> runtime/model-stream -> adapter
  -> persist completed/failed
  -> runtime/model-completed
```

Tool execution and settlement:

```text
decode/normalize
  -> runtime/tool-prepare
  -> persist pending + canonical pending event atomically
  -> best-effort publish the already-persisted pending event
  -> claim running + renew run lease + canonical running event atomically
  -> best-effort publish the already-persisted running event
  -> runtime/tool-started
  -> all deny-only guards
  -> unchanged permission/approval loop
  -> runtime/tool-execute -> body exactly once
  -> runtime/tool-result-transform (protected outcome)
  -> SettleToolCall(call + reserved message + reserved part + canonical terminal event)
  -> best-effort publish the already-persisted terminal event
  -> runtime/tool-settled
```

Running calls found during resume are never re-executed. Pending calls
reuse the persisted normalized input, so prepare transforms do not run twice.
Fresh and pending-resume calls share one post-claim execution and settlement
operation. It builds the bounded result payload, terminal call, result message,
result part, and canonical terminal event at one completion time, then commits
them with a cancellation-free settlement context. Fresh and resumed execution
publish each exact persisted transition event only after commit and before its
phase-specific lifecycle notice. Infrastructure delivery remains best-effort
and never appends the durable event a second time.

## Scope, provenance, and resume

Scope is routing for trusted code, not a sandbox. It is either registry-global
or one exact durable session ID; agent display names are never scope keys.
Tool names must not collide across applicable global and session mounts.
Named session prompts shadow same-name global prompts. Tool restrictions
intersect and guards can only deny or abstain, so session layers cannot
increase authority.

Each admitted run stores one canonical `session.ExtensionPlanDescriptor`.
`runtime.NewRunPlan` derives it from explicitly component-owned dispatch
handlers, tools, prompts, guards, and restrictions; callers cannot supply a
separate descriptor. The descriptor stores one component record containing its
instance and artifact identities, with separate nested typed collections for
handlers, tools, prompts, guards, and restrictions. Empty component records are
invalid. Tool restriction lists are canonical sets: blank, empty, or overlapping
policies are rejected, while duplicates and ordering do not affect identity or
enforcement.
Artifact, configuration, scope, capability, schema, executor, and ordered
registration identity all participate in the fingerprint. Resume requires an
exact current-schema fingerprint match before changing run or tool state.

Resume first verifies that the persisted descriptor matches its own fingerprint,
then passes the durable run's session ID explicitly to plan acquisition. The
provider validates every nested session-scoped identity against that ID, walks
the component records once to acquire only the persisted instances and exact
persisted tools, and independently compares the live plan fingerprint
before changing run or tool state. It never reconstructs the session from scope
records. Unrelated mounts added later are ignored. Local tool generations
prevent in-process ABA but are not durable identity.

Context contributions are text-only system or user messages. They sort by
`(order, source)`, system contributions form a prelude before durable base
history, and user contributions form a suffix after it. Assistant, tool,
multimodal, tool-call-bearing, reasoning, response-metadata, and `Extra` shapes
fail before provider dispatch. Contributions cannot interleave with durable
history in the first release.

## Request ledger and privacy

Every provider attempt is persisted through the current run's
`session.ExecutionStore`; the top-level `session.Store` exposes read-only model
request access. The current SQLite schema stores bounded canonical messages,
rendered system prompt, JSON tool schemas, and an explicit allowlist of string
call options. The default cap is 4 MiB and oversize content fails before
provider dispatch; content is never silently truncated.

Credentials, endpoints, provider runtime objects, opaque options, clients,
callbacks, observers, and trace attributes are excluded. Disallowed message or
tool `Extra` fields fail closed. The dependency's deprecated message
`MultiContent` field is rejected rather than converted. Records move through `prepared`,
`dispatch_started`, and `completed`/`failed`; orphaned nonterminal records are
valid evidence of an uncertain dispatch. A record ID is supplied through
`model.Request.IdempotencyKey`. Adapters may pass it to provider transports
that accept such a key; it is not an exactly-once network claim. Retention and
deletion remain host policy.

## Mount and shutdown example

[`examples/native-extension`](../../examples/native-extension) mounts a
session tool, prompt, context contribution, guard, prepare transform,
settled observer, and cleanup effect. Its test demonstrates concurrent session
visibility and quiescent unmount. Curated Wasm guests live under
[`examples/wasm-extensions`](../../examples/wasm-extensions); their adapters
register the same runtime points without exposing a generic string bus. Inside
a composition installer, direct registration uses the extension registrar, for
example `loader.RegisterHook(ctx, registrar.Extensions(), spec, moduleConfig)`.
Preparation/commit rollback and mount close untrack and finalize the module;
`Loader.Close` safely races those cleanup paths and remains the host-wide
shutdown boundary.
