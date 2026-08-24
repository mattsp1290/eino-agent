# Extension Points And Capability Plans

Date: 2026-08-20

The extension system has three deliberately separate planes:

- session records and the request ledger are durable facts;
- `runtime.EventSink` carries live transport events with its existing
  backpressure behavior;
- typed extension points are in-process interception and contained
  observation. They never replace durable writes or transport delivery.

`extension.Registry` owns generic typed registration, deterministic snapshots,
leases, and cleanup. `composition.Registry` atomically mounts callbacks with
tools, prompts, guards, and restrictions and implements
`runtime.RunPlanProvider`. A run acquires one immutable plan. Deactivation
blocks new snapshots immediately; `Close` waits for frozen plans to release and
then runs effects in reverse order.

Non-callback composition artifacts declare their lease scope directly through
`extension.Registrar.Lease`. Snapshot target and instance filters apply to
those leases exactly as they do to callback registrations, so an unrelated
session or a later same-session mount cannot keep a component alive.

## Ordering and failure

Entries sort by `(order, global-before-session, instance ID, registration ID)`.
Around interceptors form an onion in that order. Their guarded `next` may be
called once; required-delegation points reject a successful short circuit.
Point-owned validators defend immutable identity and outcome fields.

Callback-facing model, tool, and call values are data-only projections.
Provider clients, streamers, observers, tool executors, input decoders, and
approval requesters are always nil at the extension boundary; attempts to
inject callable values fail closed. Runtime keeps the authoritative callables
outside the callback graph and closes over them only in the terminal adapter.

Notification handlers receive defensive copies. A handler error or panic is
reported locally and never changes the run result or prevents later handlers.
Interceptor failures return a bounded `extension.CallbackError`; its raw cause
is available through `errors.Is`/`errors.As` for trusted diagnostics, but its
text is not persisted.

Order constants reserve broad bands: `runtime.OrderHostPolicy` (`-1000`),
`runtime.OrderRuntime` (`0`), and `runtime.OrderApplication` (`1000`).

## Producer and consumer catalog

| Contract ID | Producer | Mode and consumer | Failure | Durable/resume relationship | Wasm |
| --- | --- | --- | --- | --- | --- |
| `eino-agent/runtime/run-admitted` | admission | contained notice to run observers | contained | after durable admission; fresh runs only | hook adapter |
| `eino-agent/runtime/run-started` | execution start | contained notice | contained | run is already running; fresh runs only | native |
| `eino-agent/runtime/run-settled` | run settlement | contained notice | contained | after `FinishRun`; fresh/resumed nonterminal runs | hook adapter |
| `eino-agent/runtime/model-requested` | model dispatch | contained notice | contained | ledger is `dispatch_started` when enabled | native |
| `eino-agent/runtime/model-completed` | stream terminal | contained notice | contained | after ledger terminal commit; every attempt | native |
| `eino-agent/runtime/tool-prepared` | tool preparation | contained notice | contained | before durable tool admission; fresh calls | native |
| `eino-agent/runtime/tool-started` | tool claim | contained notice | contained | call is durably running | native |
| `eino-agent/runtime/tool-settled` | atomic settlement | contained notice | contained | call and result are authoritative after one store commit | native |
| `eino-agent/runtime/event-published` | event publication | contained notice to event observers | contained | after infrastructure sink handoff | event-sink adapter |
| `eino-agent/runtime/run-before-execute` | post-admission execution gate | around decision | fail run/reject | after admission; fresh runs only | native |
| `eino-agent/runtime/context-assemble` | snapshot preparation | around contribution waterfall | fail run | fresh preparation; persisted request sees materialized result | context-source adapter |
| `eino-agent/runtime/turn-prepare` | post-tool snapshot preparation | required around observation | fail run | fresh preparation after frozen tools resolve | hook adapter |
| `eino-agent/runtime/model-stream` | provider boundary | required around stream | fail attempt | every adapter attempt; never replayed on tool resume | native |
| `eino-agent/runtime/tool-prepare` | normalized tool input | around input waterfall | fail call | fresh only; final input is persisted | tool-middleware adapter |
| `eino-agent/runtime/tool-execute` | allowed tool body | required around execution | protected failure | fresh or pending-call re-execution | native |
| `eino-agent/runtime/tool-result-transform` | protected tool outcome | around result waterfall | fail call | before atomic settlement; fresh/re-executed calls | tool-middleware adapter |

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
  -> persist pending + claim running
  -> all deny-only guards
  -> unchanged permission/approval loop
  -> runtime/tool-execute -> body exactly once
  -> runtime/tool-result-transform (protected outcome)
  -> SettleToolCall(call + reserved message + reserved part)
  -> runtime/tool-settled
```

Running calls found during resume are never re-executed. Pending calls
reuse the persisted normalized input, so prepare transforms do not run twice.
Fresh and pending-resume calls share one post-claim execution and settlement
operation. It builds the bounded result payload, terminal call, result message,
and result part at one completion time, commits them with a cancellation-free
settlement context, and only then publishes the settled notice. Fresh execution
alone publishes pending/running/terminal transport updates.

## Scope, provenance, and resume

Scope is routing for trusted code, not a sandbox. It is either registry-global
or one exact durable session ID; agent display names are never scope keys.
Tool names must not collide across applicable global and session mounts.
Named session prompts shadow same-name global prompts. Tool restrictions
intersect and guards can only deny or abstain, so session layers cannot
increase authority.

Each admitted run stores one canonical `session.ExtensionPlanDescriptor`.
`runtime.NewRunPlan` derives it from identity-bound dispatch handlers, tools,
prompts, guards, and restrictions; callers cannot supply a separate descriptor.
Artifact, configuration, scope, capability, schema, executor, and ordered
registration identity all participate in the fingerprint. Resume requires an
exact current-schema fingerprint match before changing run or tool state.

Resume acquires the persisted instances and fingerprint before changing run or
tool state. Unrelated mounts added later are ignored. Local tool generations
prevent in-process ABA but are not durable identity.

## Request ledger and privacy

`runtime.WithModelRequestLedger(true)` requires a
`session.ModelRequestStore`. The current SQLite schema stores bounded canonical
messages, rendered system prompt, JSON tool schemas, and an explicit allowlist
of string call options. The default cap is 4 MiB and oversize content fails
before provider dispatch; content is never silently truncated.

Credentials, endpoints, provider runtime objects, opaque options, clients,
callbacks, observers, and trace attributes are excluded. Disallowed message or
tool `Extra` fields fail closed. Records move through `prepared`,
`dispatch_started`, and `completed`/`failed`; orphaned nonterminal records are
valid evidence of an uncertain dispatch. A record ID is offered only to
adapters implementing `model.IdempotentStreamer` and is not an exactly-once
network claim. Retention and deletion remain host policy.

## Mount and shutdown example

[`examples/native-extension`](../../examples/native-extension) mounts a
session tool, prompt, context contribution, guard, prepare interceptor,
settled observer, and cleanup effect. Its test demonstrates concurrent session
visibility and quiescent unmount. Curated Wasm guests live under
[`examples/wasm-extensions`](../../examples/wasm-extensions); their adapters
register the same runtime points without exposing a generic string bus.
