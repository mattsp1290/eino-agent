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
approval and session-title writers are always nil at the extension boundary;
attempts to inject callable values fail closed. Runtime keeps the authoritative
callables outside the callback graph and closes over them only in the terminal
adapter. The host's `AllowSessionTitle` boolean remains protected plan data.
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

## Typed ADK agent handlers (W6)

`composition.Registrar.Handler(HandlerRegistration{ID, Order, Scope,
Descriptor: HandlerDescriptor{Kind, Version, Config json.RawMessage},
Factory runtime.HandlerFactory})` is a distinct registration category from
the generic hook/transform/gate/around/notification "handler" concept
described above (`extension.HandlerKind`, `session.RegistrationIdentity` on
`ComponentPlan.Handlers`): it registers one factory for a typed ADK
`adk.TypedChatModelAgentMiddleware[*schema.AgenticMessage]`, the interface-based
per-run agent customization point Eino's `adk` package itself defines
(`BeforeAgent`, `BeforeModelRewriteState`/`AfterModelRewriteState`,
`WrapModel`, `WrapInvokableToolCall`/etc., `AfterAgent`). Its sealed identity
lives in its own nested collection, `session.ComponentPlan.AgentHandlers
[]AgentHandlerPlanIdentity{ID, Kind, Version, ConfigHash, Order, Scope}`,
alongside (not merged into) the existing `Handlers`/`Tools`/`Prompts`/
`Guards`/`Restrictions` collections; it participates in the plan fingerprint
the same way every other capability collection does. `Config`'s canonical
hash (decode-then-remarshal, so key order never affects it), not its raw
bytes, is what gets sealed -- the factory closure itself is never
serialized, matching `RunPlanSpec.Agent`/`ToolSearch`'s existing "host
construction identity, not durable capability evidence" treatment.

`discoverHandlerTools` also probes each factory once at plan-compile time
(a bounded, stub-backed `HandlerBuildContext` -- no real session, model, or
workspace content) to enumerate the tools it contributes; the discovered
`{Name, SchemaHash}` set is sealed onto the same `AgentHandlerPlanIdentity`
(`Tools []HandlerToolIdentity`) and, at real per-turn build time,
`adkEngine.sealHandlerTools` synthesizes a durable `runtime.Tool` per sealed
entry and splices it into `TurnSnapshot.Tools` *before* the agent is built
-- so a handler's own tool belongs to the same frozen tool universe as any
composition-registered one, resolved by `prepareToolCalls`/
`resolveToolCall` and dispatched through the full durable claim/permission/
execute/settle pipeline (`handlerToolExecutor`) rather than ADK's own
generic tool-node call. A tool a handler's middleware injects at real
`BeforeAgent` time that was *not* sealed at compile time fails that turn as
a construction error.

`runtime.RunPlan.AgentHandlers()` exposes the sealed, ordered list with live
`Factory` and discovered `Tools` attached; `adkEngine.buildAgent` invokes
every factory fresh for each admitted turn with a bounded
`runtime.HandlerBuildContext` (session/run identity, a ledger-audited model
adapter -- the turn's own mandatory one for every recipe except
summarization, which gets a bounded internal-dispatch adapter instead, see
below -- read-only workspace-scoped filesystem/skill backend views, private
writable scratch backends for plantask/reduction rooted per session, and
this turn's frozen deferred tools). `HandlerBuildContext` carries no
`session.Store`/`session.ExecutionStore` at all -- it is handed identically
to every registered `HandlerFactory`, host-provided ones included, so
either would let an arbitrary host handler fabricate a durable `ToolCall`
settlement or read checkpoint/provider-private state; summarization, the
one recipe that legitimately needs a bounded durable write (mapping a
completed summary into a `session.ContextEpoch`), is given a narrow,
unexported capability instead, reachable only from this package's own
recipe code (the same Go-visibility isolation `authorizeRewrite` already
used). `adkEngine.buildAgent` installs the results into
`AgentBuildContext.Handlers`, ahead of
this runtime's own mandatory tail handlers (`durableGuard`, `settlementSeal`
-- `runtime/adk_middleware.go`), after `durableBaselineHandler`
(`AgentBuildContext.DurableBaseline`), which is installed *first*.
`durableBaselineHandler.BeforeModelRewriteState` rewrites the agent's
in-memory `state.Messages` to a deep clone of the fresh durable projection
of this cycle's committed history -- the durable projection is the
*baseline* every host handler then transforms, not a value discarded and
re-derived afterward; the ledger-audited model adapter dispatches exactly
what the handler chain leaves that baseline as, with no re-projection.
Ordering otherwise follows ADK's own first-registered-is-outermost
handler-wrapping rule for `Wrap*` methods (host handlers, in `Order`/`ID`/
component/scope order, are outermost; this runtime's own tail handlers stay
innermost, directly around the mandatory `Model`/`Tools` adapters, so no
host handler can substitute them) and first-registered-is-first-called for
hook methods (`BeforeAgent`/`BeforeModelRewriteState`/...), which is why
`durableBaselineHandler` -- installed first -- establishes the baseline
before any host handler's own hook runs. The real settlement authority,
though, is `verifySettledToolResults`, called from
`adkModel.prepareDispatchInput` -- the innermost dispatch point every
physical attempt passes through regardless of a host `WrapModel` wrapper
nested around the model after every handler's hooks (including
`settlementSeal`'s own `BeforeModelRewriteState`, kept only as an early,
non-authoritative check calling the identical function) have already run.
It compares every occurrence of a settled tool's model-visible content
(not just the last one a map would remember) against
`durableBaselineHandler`'s own reconstruction for that call ID, hashing the
full canonical content including media fields, and fails the run on
divergence, or on a fabricated result for a call with no durable
settlement, unless the exact post-rewrite content digest was recorded as
an authorized rewrite for that call ID THIS cycle (reset every cycle) by a
sanctioned content-management recipe (patchtoolcalls, reduction --
`wrapAuthorizedContentRewrites`), which also durably records the rewrite
(handler ID, kind, call ID, before/after digest) as an audit event. That
authorization is itself Kind-scoped
(`kindMayRewriteSettledContent`): only reduction may legitimately rewrite
a call ID the baseline already shows real settled content for. A
patchtoolcalls-kind authorization is refused for such a call ID even
though `wrapAuthorizedContentRewrites` recorded it -- patchtoolcalls'
only legitimate purpose is filling in a call with NO durable settlement
at all, never rewriting one that has real settled content (round-two W6
review item 4).

Both integration gaps an earlier pass of this design left open --
host-injected content never reaching the model, and a handler-injected tool
never being callable -- are resolved by `durableBaselineHandler` and the
frozen-tool-universe sealing above, respectively; each fix is proven end to
end through a real turn (`TestAgentsMDHandlerInjectsContentIntoModelRequest`,
`TestFilesystemHandlerToolExecutesThroughDurableWrapper`,
`runtime/adk_middleware_e2e_test.go`).

`examples/agentic-middleware/` proves this same mechanism a second time
from entirely outside the `runtime` package, through only
`composition.Registrar.Handler`/`runtime.StreamingOrchestrator`: a `Mount`
function wires all eight upstream recipes this package ships
(`runtime.HandlerKindAgentsMD`/`Skill`/`Filesystem`/`PlanTask`/
`PatchToolCalls`/`Reduction`/`Summarization`/`ToolSearch`), each mounted
individually and driven through a real turn exercising its own positive
and failure paths, an immutable-input proof via a custom `HandlerFactory`
that tries to rewrite a settled result without authorization, a discovered
deferred tool actually being called (not just found) after toolsearch
surfaces it, and interrupt/resume including a handler `Config` change
being refused on resume, and (round-two W6 review) toolsearch discovery
durably replaying on a fresh turn, after `ResumeRun`, and after a
brand-new orchestrator instance against the same store, plus a resumed run
refusing to proceed when an activated skill's content changed between
pause and resume, scoped to the specific run being resumed rather than the
whole session. Two DIFFERENT handlers rewriting two different results in
one turn ("ordering with two rewrites") is proven in `runtime`'s own test
suite instead
(`TestTwoHandlersRewriteTwoDifferentResultsInOneTurnBothAuthorized` in
`runtime/w6_round2_group_g_test.go`: reduction clears a real settled round
while patchtoolcalls fills a genuinely dangling call in the same turn,
both authorized and audited); this example package's own
`TestReductionClearsOlderRoundAsAuthorizedRewrite` exercises only
reduction's own two-round clearing (one handler, two of its own rewrites),
not two different handlers.
See that package's own doc comment and the W6 section of
`docs/architecture/eino-feature-support.md` for this example's remaining
scope limits. All eight recipes are mounted together in one `RunPlan`, in
both turns of the example's own multi-turn scenario, and each is proven
with its own concrete assertion by
`TestComposedExampleMountsAllEightRecipesInOneRunPlan` (round-two W6
review item 14), which also builds a genuine "dangling call, no durable
settlement" fixture at this black-box level via a custom public
`HandlerFactory`, in addition to the runtime-internal, store-seeded
version. The previously bounded limitation on mounting patchtoolcalls and
summarization together is resolved (eino-agent-0wb): a mid-turn compaction
boundary this runtime commits can no longer land between a
function_tool_call and its function_tool_result -- see the W6
known-limitations history in `eino-feature-support.md`.

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
