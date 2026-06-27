# Reference Runtime Research

Date: 2026-06-27

This note records runtime concepts from `pi` and `opencode` that should inform
`eino-agent` architecture. It is research input only: adopt the patterns where
they fit a Go/Eino/AG-UI runtime, but do not copy TypeScript implementation
details, package layout, or framework choices.

## Source Revisions

| Repository | Local path | Revision inspected |
| --- | --- | --- |
| `pi` | `~/git/_reference/pi` | `f2e9d75388fe17325ebe31372e5287b4acdb67a3` |
| `opencode` | `~/git/_reference/opencode` | `ae53163cad0048b2351e258699e815f4f2110807` |

## Summary

`pi` is most useful as a compact agent-loop and harness design reference. It
shows how to separate live runtime configuration, immutable per-turn snapshots,
tool execution policy, queued steering/follow-up messages, and turn-safe save
points.

`opencode` is most useful as a durable runtime reference. It shows session
admission as database state, message/part replay as durable history, per-session
run locks, cancellation cleanup, provider catalog/auth/config layering, typed
tool envelopes, permission requests, compaction records, plugin lifecycle, and
stale daemon registration protection.

For `eino-agent`, the main architectural implication is that a run is not just
an Eino stream. It needs a durable session/run envelope around the stream, a
replayable message store, a live-only AG-UI tail, explicit context epochs, and a
tool/permission lifecycle that can recover conservatively after interruption.

## Concepts to Adopt From `pi`

### Session admission and phases

Evidence:

- `packages/agent/docs/agent-harness.md`
- `packages/agent/docs/durable-harness.md`

Adopt the explicit separation between structural operations and active turns.
`AgentHarness` has phases such as `idle`, `turn`, `compaction`,
`branch_summary`, and `retry`; structural operations are rejected while the
harness is busy, while steer/follow-up/abort/config setters have documented
turn-safe behavior.

`eino-agent` should model the same boundary with Go types:

- Session admission creates durable session/run state before invoking Eino.
- At most one structural run owns a session at a time.
- Interrupt, follow-up, and config changes are accepted only through explicit
  queues or next-turn snapshots.
- Public APIs should report busy/conflict errors instead of racing the active
  turn.

### Turn snapshots and save points

Evidence:

- `packages/agent/docs/agent-harness.md`
- `packages/agent/src/agent-loop.ts`

`pi` distinguishes latest harness config from the immutable snapshot used by an
in-flight provider request. Save points happen after an assistant turn and tool
results, then pending writes are flushed and a fresh snapshot can affect the
next provider request.

`eino-agent` should adopt this shape. Each run step should capture a
`TurnSnapshot` containing model/provider selection, active tools, resources,
system prompt, stream options, and context epoch. Runtime setters can update
future snapshots, but they must not mutate the in-flight Eino call.

### Replay and durable boundaries

Evidence:

- `packages/agent/docs/durable-harness.md`

`pi` treats session state as the durable source of truth and runtime
dependencies as host-supplied, non-serializable dependencies. It also documents
conservative recovery: unfinished provider requests are marked interrupted,
unfinished tool calls are not retried unless metadata says they are safe, and
queues/pending writes are restored from the session log.

`eino-agent` should adopt that recovery default. Durable history should be
enough to rebuild context and explain what happened, but not enough to resume a
half-open provider stream. Tool calls need stable call IDs and retry-safety
metadata before any automatic retry policy exists.

### Agent loop extension points

Evidence:

- `packages/agent/src/agent-loop.ts`

`pi`'s loop has useful extension seams:

- `transformContext` before converting history to provider messages.
- `prepareNextTurn` after a turn to refresh context/model/reasoning.
- `shouldStopAfterTurn` to stop after a turn even if the model could continue.
- `getSteeringMessages` and `getFollowUpMessages` to drain queued user input at
  safe points.
- sequential or parallel tool-call execution based on global policy or per-tool
  execution mode.

`eino-agent` should provide equivalent hooks as Go interfaces. The names do not
need to match, but the timing does: mutate durable state before the next provider
request, never while converting or streaming the current request.

### Typed events and tool lifecycle

Evidence:

- `packages/agent/src/agent-loop.ts`
- `packages/agent/src/types.ts`

`pi` emits typed lifecycle events such as agent/turn/message start and end,
message updates, and tool execution start/update/end. Tool execution is prepared,
validated, executed, finalized, and then converted into tool-result messages.

`eino-agent` should keep this shape internally, then map it to AG-UI events at
the boundary. Internal events should be richer than SSE transport events so
observability, persistence, and AG-UI emission can share one source of truth.

### Resource cleanup

Evidence:

- `packages/ai/src/session-resources.ts`

`pi` has a tiny registry for session resource cleanup callbacks. The useful
idea is not the global set, but the requirement: session-scoped resources need a
defined cleanup lifecycle.

`eino-agent` should make session/run cleanup explicit for provider clients,
temporary files, tool subprocesses, and observer spans.

## Concepts to Avoid or Defer From `pi`

- Do not copy the TypeScript event stream, package boundaries, or generic
  `AgentMessage` representation. Use Go structs and the pinned `eino-agui`
  conversion/emitter APIs.
- Do not rely on in-memory pending writes as the only source of truth. `pi`
  itself identifies durable pending-write entries as the semi-durable target.
- Do not expose raw harness internals to extensions. `pi` warns that raw
  harness calls from listeners can deadlock; `eino-agent` should expose narrow
  facades with documented allowed phases.
- Defer broad branch-tree navigation until the basic session/run/compaction
  lifecycle is stable.

## Concepts to Adopt From `opencode`

### Session admission as durable state

Evidence:

- `packages/opencode/src/session/session.ts`
- `packages/opencode/src/session/schema.ts`

`opencode` stores sessions as durable rows with IDs, parent IDs, project/workspace
identity, directory/path, title, agent, model, permission rules, metadata,
cost/tokens, share/revert/summary fields, and timestamps. `create`, `fork`,
`patch`, permission updates, title/archive/metadata updates, and message reads
are service methods over that durable state.

`eino-agent` should admit sessions and runs before execution. A run should know
its session ID, parent message/run ID, model/provider, agent profile, permission
snapshot, and created time before the first Eino stream event is emitted.

### History replay with messages and parts

Evidence:

- `packages/opencode/src/session/message-v2.ts`
- `packages/opencode/src/session/processor.ts`

`opencode` persists message rows and ordered part rows. Parts include text,
reasoning, tool, step-start, step-finish, patch, file, compaction, and subtask
content. `toModelMessagesEffect` converts persisted history into provider
messages, including provider-specific media/tool-result handling.

`eino-agent` should persist replayable snapshots independently from live AG-UI
events. AG-UI deltas are live-tail transport; message/part history is what
replay, resume, compaction, audit, and observability should read.

### Run locks and interruption

Evidence:

- `packages/opencode/src/session/run-state.ts`
- `packages/opencode/src/session/processor.ts`

`SessionRunState` keeps one runner per session, exposes `assertNotBusy`,
`ensureRunning`, `startShell`, and `cancel`, and cancels related background jobs.
`SessionProcessor` handles interruption by recording abort errors, waiting
briefly for tool calls, marking unfinished tool calls as interrupted errors,
closing open text/reasoning parts, updating the assistant message completion
time, and setting session status.

`eino-agent` should implement the same invariants:

- one active run per session unless a future design explicitly supports
  parallel branches;
- cancellation propagates to provider streams, tool contexts, subprocesses, and
  background jobs;
- interrupted tool calls settle durably as interrupted rather than disappearing;
- cleanup finalizes open text/reasoning/tool spans before the run is idle.

### Providers, model catalog, auth, and config

Evidence:

- `packages/opencode/src/provider/provider.ts`
- `packages/opencode/src/config/config.ts`

`opencode` separates provider/model catalog data, auth lookup, environment,
config files, plugin-provider extensions, model loaders, default model
selection, and language-model construction. Config loading merges global,
project, remote, managed, environment, and plugin-origin inputs, then exposes a
cached instance config.

`eino-agent` should adopt the layering, not the implementation:

- define provider/model catalog interfaces;
- resolve auth and environment at provider-request time where needed;
- snapshot chosen provider/model/config into each run;
- keep plugin/config loading outside core Eino execution;
- make missing provider/model errors typed and user-actionable.

### Typed tools and permissions

Evidence:

- `packages/opencode/src/tool/tool.ts`
- `packages/opencode/src/session/tools.ts`
- `packages/opencode/src/permission/index.ts`

`opencode` tools have typed parameter decoders, stable IDs, metadata,
attachments, truncation, abort signals, message/session/call IDs, and a
permission `ask` API. Session tool resolution merges agent and session
permissions, wraps execution with before/after plugin hooks, and records tool
state transitions through the processor.

`eino-agent` should adapt this pattern onto `eino-tools`:

- every tool call has a stable call ID, input JSON, status, start/end time,
  output/error, metadata, and retry-safety marker;
- every tool receives `context.Context`, session/run/message/call IDs, and an
  approval facade;
- permissions are decided outside leaf tools when possible, with a tool-local
  ask hook for dynamic cases;
- truncation and attachment handling are runtime policy, not ad hoc tool output
  string manipulation.

### Compaction and context epochs

Evidence:

- `packages/opencode/src/session/compaction.ts`
- `packages/opencode/src/session/processor.ts`

`opencode` detects overflow, creates compaction user parts, selects older head
history while preserving a recent tail budget, includes previous summaries,
allows plugin-provided compaction context/prompt overrides, writes assistant
summary messages, records `tail_start_id`, prunes older completed tool output,
and can auto-continue or replay a prior prompt after compaction.

`eino-agent` should model compaction as a context epoch transition. A compaction
record should say which prior epoch/messages were summarized, which tail message
starts the retained live history, which model/provider produced the summary, and
whether the next run is auto-continued, replayed, or stopped.

### Plugin lifecycle and config hooks

Evidence:

- `packages/opencode/src/plugin/index.ts`
- `packages/opencode/src/config/config.ts`

`opencode` loads internal and configured plugins, preserves plugin origins and
scope, runs plugin `config` hooks after load, executes triggers sequentially for
deterministic order, listens for events, and calls optional `dispose` hooks at
finalization.

`eino-agent` should keep plugin lifecycle explicit:

- discover and validate plugin specs before run admission;
- snapshot plugin/config versions or hashes into run metadata where practical;
- execute hooks in deterministic order;
- expose disposal/cleanup;
- keep plugin failures isolated enough to preserve already-committed session
  state.

### Stale registrations

Evidence:

- `packages/cli/src/services/daemon.ts`

`opencode` avoids signalling a reused PID by authenticating the registered
server and checking registration identity/version/URL/PID before stop/remove
operations. It writes registration and password files atomically with restricted
permissions.

`eino-agent` should apply the same stale-registration principle to any local
run, server, or tool registration: durable records need instance IDs, versions,
addresses, and health/auth checks before cleanup or signal operations.

### System prompt assembly

Evidence:

- `packages/opencode/src/session/system.ts`

`opencode` composes system context from provider/model identity, environment,
project references, skill availability, MCP instructions, and permission-based
tool filtering.

`eino-agent` should use this as a reminder to centralize prompt assembly.
Provider-specific and permission-filtered prompt fragments should be generated
by a runtime component, not scattered across handlers and tools.

## Concepts to Avoid or Defer From `opencode`

- Do not copy the Effect/Layer framework, Bun assumptions, AI SDK abstractions,
  or TypeScript schema stack. Use Go interfaces, `context.Context`, Eino
  components, and the existing pinned Go libraries.
- Do not couple runtime state to a CLI/TUI daemon model. `eino-agent` should be
  reusable for HTTP servers, CLIs, tests, and embedded workflows.
- Do not put provider-specific prompt text or API quirks directly in core run
  control. Keep those behind provider adapters.
- Do not make plugin loading required for the core runtime. Plugins should be an
  extension layer over a minimal session/run/tool core.
- Defer a full marketplace or external plugin installer. Start with local,
  explicit plugin/config registration.
- Avoid automatic retry of unfinished tool calls unless a tool declares
  idempotency and the durable call record proves the prior call did not settle.

## Implications for `eino-agent`

### Runtime packages

Upcoming architecture work should define these boundaries:

- `session`: durable sessions, messages, parts, run admission, history replay,
  and replay cursors.
- `runtime`: run orchestration, run locks, turn snapshots, context epochs,
  interrupt/cleanup, and save points.
- `provider`: provider/model catalog, auth resolution, model selection, and
  Eino model construction.
- `tools`: adaptation of `eino-tools` into runtime tool definitions with
  durable call settlement and permission hooks.
- `compaction`: context epoch summarization and overflow handling.
- `plugins` or `hooks`: deterministic extension points over config, prompts,
  tools, turns, compaction, and observability.
- `transport/agui`: AG-UI admission, live SSE emission, replay snapshot
  conversion, and live-tail behavior using `eino-agui`.

### Durable run model

Minimum durable records should cover:

- session created/updated;
- run admitted with model/provider/config/plugin snapshot metadata;
- user message and parts;
- assistant message and parts;
- tool call start/update/finish/error/interrupted;
- context epoch start/finish and compaction summary/tail start;
- run finish/error/interrupted;
- permission request/reply, if approvals need replay or audit.

### Live versus replayed output

Live AG-UI output should emit typed deltas from the active run only. Replay
should come from persisted messages/parts converted through `eino-agui`, not from
replaying old SSE deltas. This avoids mixing transport delivery state with the
authoritative conversation state.

### Conservative recovery default

After process restart or interrupted transport:

- reopen durable sessions and runs;
- mark unfinished provider requests as interrupted unless a future durable
  journal proves a safe retry point;
- do not rerun non-idempotent tools;
- settle open tool calls as interrupted errors;
- preserve accepted queued messages and pending writes;
- require the next user/run action to create a fresh turn snapshot.

### Acceptance constraints for the next design task

The next architecture design should explicitly answer:

- Where does session admission happen before an Eino stream starts?
- Which records are replayable history versus live-only transport events?
- What is the shape of a turn snapshot and context epoch?
- How are stale session/run/tool registrations detected?
- How are provider/model/config/plugin snapshots captured?
- How do tool calls claim, execute, settle, and recover?
- How does interruption settle open provider, text, reasoning, and tool state?
- How does compaction create a new context epoch without losing replayability?
