# Current State And Reference Research

<!-- markdownlint-disable MD013 -->

## Research Snapshot

This plan was checked against:

- `eino-agent` commit `7149b1eeb8668072107e5e7b57a9a1102c6a907a`.
- DeepSeek Harness commit
  [`141eb6fef83422698aef7a981029e843e8161534`](https://github.com/deepseek-ai/deepseek-harness/commit/141eb6fef83422698aef7a981029e843e8161534),
  dated 2026-08-19.

Harness is explicitly a developer preview, so this plan treats it as a source
of architectural evidence rather than a compatibility target.

## What `eino-agent` Already Has

The previous proposal is not hypothetical. Current main includes:

- `runtime.NewStreamingOrchestrator` and ordered functional options.
- Function adapters for the single-method seams and `HookFuncs` /
  `ToolMiddlewareFuncs` adapters for multi-method seams.
- Tool input rewriting before durable tool admission and reverse-order result
  rewriting before settlement.
- `tools.Registry` with generation-checked `Register` and `Replace`.
- WIT `eino-agent:extensions@0.1.0`, generated bindings, a Wasmtime-backed leaf
  package, and Phase A tool and permissions-policy fixtures.
- Durable runtime events, a live `runtime.EventSink`, reconnect tailing, AG-UI
  projection, OpenTelemetry observation, history projection, context epochs,
  and config-time snapshot plugins.

The deeper plan must compose with those features rather than recreate them.

## Concrete Limitations In Current Main

### One sink and several unrelated listener lists

`StreamingOrchestrator.Events` accepts one `EventSink`; hosts must build their
own fan-out to combine AG-UI, replay tails, audit, or custom observers. Context
sources, hooks, and tool middleware are independent slices with different
failure and ordering rules. There is no common registration identity,
ownership, unregistration, scope, or run-level provenance.

### Fixed broad interfaces

`runtime.Hook` requires four lifecycle methods and lets `BeforeTurn` replace a
whole `TurnSnapshot`, including fields a normal extension should not control.
The adapter makes implementation convenient but does not narrow authority.
The name “turn” also covers the entire current run/tool loop rather than a
formal durable turn abstraction.

### No reversible ownership

Options and public fields configure long-lived dependencies. `tools.Registry`
can replace a generation but cannot unregister it. There is no group handle
whose close removes all contributions and waits for in-flight callbacks to
quiesce. This blocks safe dynamic session composition even without file
watching or automatic hot reload.

### Resume can observe different extension code

`session.Run.Components` exists for component identity, but the runtime does
not freeze an executable extension plan or require the same named/versioned
components on resume. Current resume materializes tools and uses the
orchestrator's current hooks and middleware. Silent code drift is particularly
risky around permissions and recovery of pending tool calls.

### Observation and control are easy to confuse

`runtime.Event` is both the internal transport-neutral shape and the input to
the one live sink. All current sink callback errors are ignored. Model deltas
alone traverse a bounded asynchronous queue whose enqueue can block or return
context cancellation; the sink's returned error still does not reach the run.
Several lifecycle/tool call sites call the sink synchronously and explicitly
discard its error. The interface does not communicate those delivery details.

### Model-visible inputs are not fully auditable

History and admitted input are durable, but `ContextSource`, `BeforeTurn`, tool
materialization, and per-attempt request construction can change messages or
tool schemas in memory. There is no durable record containing the exact
canonical messages, system material, and tool schemas submitted at the
runtime-to-adapter boundary for each attempt.

### Wasm is intentionally curated, but Phase B remains unfinished

The WIT worlds for context source, event sink, hook, and tool middleware are
published at `@0.1.0`; their wrappers are still documented as Phase B gaps.
Replacing those worlds with a generic event ABI would strand published work
and weaken the explicit-data security boundary.

## What DeepSeek Harness Demonstrates

### Three event domains

Harness distinguishes durable `SessionEvent` facts, live `agent/*` control
events, and capability-specific events such as `tools/*`. Its architecture
guide states that choosing the domain is the first design decision. That is a
better model than treating every callback as one undifferentiated event bus.

Source: [Harness architecture](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/docs/architecture.md#events).

### Multiple dispatch modes

Cordis and Harness use different modes for different semantics:

- notifications broadcast a fact;
- serial handlers run in order at a checkpoint;
- waterfalls wrap a default continuation and can transform or stop work;
- parallel dispatch is reserved for suitable fan-out work.

The agent loop exposes `agent/pre-step`, `agent/request`, and request-error
waterfalls plus a serial `agent/turn-stopping` checkpoint. The tool runtime
separates pre-execution policy, around execution, post-execution
transformation, and immutable result observation.

Sources:
[agent event contracts](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/packages/core/agent/src/runtime-types.ts),
[tool event contracts](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/packages/core/tools/src/index.ts),
and [Cordis dispatch implementation](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/vendor/cordis/src/events.ts).

### Reversible effects and scope-aware registration

Harness registrations belong to plugin fibers and unwind when their owner
unloads. Agent scopes route only the relevant registrations. Its scope
documentation is also candid that scope routes trusted same-process code and
is not a security boundary.

Sources: [Cordis fiber lifecycle](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/vendor/cordis/src/fiber.ts)
and [Harness scope contract](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/packages/core/scope/README.md).

### Capability services remain explicit

“Everything is a plugin” does not mean “everything is an event.” Tools,
sessions, prompts, LLMs, and agents remain services with typed APIs; events are
used to observe or intercept their pipelines. Prompt sections and tool schemas
are registered capabilities that are assembled per request.

Sources: [packages map](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/packages/README.md)
and [system prompt registry](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/packages/core/system-prompt/README.md).

### Model-visible means durable

Harness requires model-visible input to be reconstructable from its session
log. It derives messages from durable surface events and records an effective
request header containing call configuration, system material, and tools.
`eino-agent` should adopt the invariant through a different, per-attempt
prepared-request ledger: exact canonical messages, system prompt, safe call
configuration, and tool schemas at the runtime-to-adapter boundary, not
credentials or opaque transport configuration. This ledger is a stronger
per-attempt audit in some respects, but it is not Harness's storage model.

Source: [Harness session-log invariant](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/docs/architecture.md#session-log).

## Adopt, Adapt, Or Reject

| Harness idea | Decision for `eino-agent` | Reason |
| --- | --- | --- |
| Durable, live, and capability event domains | Adopt | Makes durability and failure semantics explicit. |
| Dispatch semantics by purpose | Adapt | Initially implement contained notifications and guarded waterfalls; do not claim Cordis parallel/bail/serial parity. |
| Reversible effect ownership | Adopt | A mount owns arbitrary reverse-order cleanup as well as registrations, without requiring hot reload. |
| Scoped registration | Adapt | Support registry-global and exact-session scope first; there is no durable unique agent ID yet. |
| Immutable final tool-result notification | Adopt | Separates audit from mutation and preserves settlement authority. |
| Pre/around/post tool pipeline | Adapt | Preserve permissions as a non-reorderable core guard and prohibit calling `next` more than once. |
| Request interception | Adapt | Allow bounded option changes and around-stream concerns; message/tool mutation belongs to durable context/tool assembly. |
| Model-visible log invariant | Adapt differently | Add a prepared provider-attempt ledger for the exact runtime-to-adapter projection; do not overclaim final provider-wire visibility. |
| Every runtime service replaceable from config | Reject for this series | `eino-agent` is an embeddable Go library; store, resolver, IDs, and config loading remain explicit dependencies. |
| General Cordis context/service container | Reject | Adds ambient dependency lookup and a second object model without a demonstrated Go need. |
| Automatic plugin hot reload and patch layers | Defer | Lifecycle primitives come first; discovery and reload are deployment policy. |
| Generic event protocol for Wasm | Reject | Curated WIT DTOs provide a safer authority and compatibility boundary. |

## Resulting Scope

The coherent change is a runtime extension kernel plus integration slices. It
is too large for one PR and must be delivered in the ordered packages described
by the roadmap. Commands, jobs, UI nodes, subagent frameworks, marketplaces,
and a general service container are not implied by this architecture.
