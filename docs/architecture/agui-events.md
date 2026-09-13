# AG-UI Event Durability and Replay

Date: 2026-06-27

This document defines how `eino-agent` treats AG-UI-facing events as durable
history, live-tail transport, replay projections, audit records, or omitted
data. It supports AG-UI streaming bridge implementations without replacing
`eino-agui`.

## Boundaries

`eino-agent` delegates protocol mechanics to `eino-agui`:

- `eino-agui/convert` converts between AG-UI protocol messages and Eino
  `schema.Message` values.
- `eino-agui/emitter` emits typed AG-UI SSE events.
- `eino-agui/stream` taps Eino model streams and emits live text, reasoning,
  and optional tool-call events.
- `eino-agui/tools` binds AG-UI client tool definitions to Eino tool metadata.

`eino-agent` owns the durability policy around those helpers:

- what becomes `session.Message` and `session.Part`;
- what becomes `session.ToolCall`;
- what becomes `session.EventRecord`;
- what is emitted only to the live SSE tail;
- what replay reconstructs from durable state;
- what is omitted for privacy or protocol safety.

## Core Rule

Replay never replays old SSE frames as the source of truth. Replay is projected
from durable messages, parts, tool calls, context epochs, and event audit
records, then converted through `eino-agui/convert` and emitted through
`eino-agui/emitter` where needed.

Live tail may emit deltas immediately. Durable storage persists settled facts.
The historical reconnect path combines paged durable replay with a live tail.
That handoff is not a transaction-consistent state snapshot.

Encrypted reasoning is never persisted, never included in message snapshots,
and never replayed.

## Classification Table

| Family | Persisted durable fact | Replay behavior | Live-tail behavior | Omitted |
| --- | --- | --- | --- | --- |
| Run lifecycle | `session.EventRecord` audit with run status metadata. | Not replayed as raw `RUN_STARTED`/`RUN_FINISHED`; replay exposes current run/message state. | Emit live through `eino-agui/emitter`. | None, except transport-only write failures. |
| Text | Settled `session.Part{Kind: PartAssistantGenText}` on assistant message. | Replay as AG-UI assistant message content projected from durable parts. | Emit `TEXT_MESSAGE_*` deltas live. | Empty deltas. |
| Plain reasoning | `session.Part{Kind: PartReasoning}` only when provider and host policy allow storage. | Replay as reasoning content only from durable reasoning parts. | Emit `REASONING_*` live while allowed. | Provider-private or policy-denied reasoning. |
| Encrypted reasoning | Never persisted. | Never replayed. | Not emitted by `eino-agent`; scrub from snapshots. | All encrypted reasoning payloads. |
| Provider-private state | Never persisted as an AG-UI event; runtime may retain a private `PartProviderState`. | Never replayed or decoded by AG-UI. | Never emitted. | All raw bytes, base64, digests, codec diagnostics, and source bindings. |
| Tool calls | `session.ToolCall` plus one canonical `EventRecord` for each pending, running, and terminal phase; state and event commit atomically. | Replay call state from durable tool-call records, parts, and correlated phase events. | After commit, publish the exact persisted event best-effort when the bridge enables `eino-agui/stream.WithLiveToolCallEvents`. | Duplicate post-turn proposals when live tool calls were already emitted. |
| Tool results | `PartFunctionToolResult` plus settled `session.ToolCall` output/error. | Replay bounded model-facing tool result from durable part. | Emit live result through `eino-agui/emitter.ToolResult`. | Oversized raw output beyond retention policy. |
| State snapshots | No durable `PartKind` today (would be host-visible app state, distinct from the W2 model-content block kinds); only when host marks snapshot replay-safe. | Replay latest replay-safe snapshot or host-projected state, once implemented. | Emit live snapshot when state changes. | Sensitive or non-replay-safe host state. |
| State deltas | Optional `EventRecord` audit. | Do not replay raw deltas; replay starts from snapshot. | Emit live deltas. | Deltas superseded by snapshot. |
| Messages snapshots | Not stored as raw AG-UI frames. | Reconstruct from durable messages/parts using `eino-agui/convert`. | May emit live snapshot for UI synchronization. | Raw snapshot frame payload. |
| Activity | Optional `EventRecord` audit metadata. | Not replayed as conversation content. | Emit live activity. | Transient activity with no audit value. |
| Steps | Not persisted today: `Bridge.StepStarted`/`StepFinished` are pure passthroughs to the live emitter and write no durable part or `EventRecord`. (`session.AttemptReplacedEventKind` is a separate, unrelated model-dispatch-retry audit trail, not a record of step boundaries.) | Not replayed; may become annotations/status where UI supports it, once a durable representation is implemented. | Emit live `STEP_*`. | None. |
| Custom events | Optional audit `EventRecord`. | Not replayed unless promoted to a future typed replay contract. | Emit live. | Unknown sensitive payloads by policy. |
| Errors | `EventRecord` plus terminal run/message status. | Replay terminal status/error summary, not necessarily raw `RUN_ERROR`. | Emit live `RUN_ERROR` or related error event. | Provider/internal details redacted by policy. |

## Type Contract

The `agui` package exposes a compileable classification model:

- `agui.EventFamily`: coarse event family names.
- `agui.Disposition`: `persist`, `replay`, `live`, `audit`, or `omit`.
- `agui.Gate`: required host/provider decisions for conditional durability.
- `agui.Rule`: durable part kind, durable audit kind, redaction class,
  snapshot-safety, required gates, and notes for each family.
- `agui.Rules()`: default policy used by bridge implementations.

These types are policy definitions only. They do not emit protocol events and
do not convert messages.

`Rule.AuditKind` is not an AG-UI protocol event name and must not drive
emission. Protocol emission is always implemented through `eino-agui/emitter`
and `eino-agui/stream`. `AuditKind` is only a stable label for optional
`session.EventRecord.Kind` values.

Conditional content remains unsafe until its gate is satisfied:

- `GateProviderReasoningStorage`: required before plain reasoning can be
  persisted, included in snapshots, or replayed. `agui.Gate` values name
  these requirements for documentation and the policy table; they are not
  independently enforced by any code path. The actual mechanism a host
  uses to attest this gate is satisfied is the `includeReasoning`/
  `IncludeReasoning` boolean threaded through `agui.Replay`/
  `agui.Reconnect`/`Bridge`/`transport.SSEConfig` (see the paragraph on it
  under "Replay Projection" below) -- passing `true` there IS the
  attestation; this package trusts it and does not separately verify
  provider/host policy.
- `GateHostReplaySafeState`: required before host state snapshots can be
  persisted, included in snapshots, or replayed. Also a documentation
  label, not an enforced check.

## Replay Projection

Replay uses this order:

1. Read durable messages and parts with `session.Store.ListMessages`.
2. Exclude encrypted reasoning and policy-denied content.
3. Apply rule gates before including reasoning or state snapshot content.
4. Materialize assistant/user/tool messages from durable parts.
5. Convert replayable messages through `eino-agui/convert`.
6. Emit a messages snapshot or replay response through the transport adapter.
7. If a run is active, attach to live tail from the current run cursor.

Replay must preserve durable message/part ordering from `store/storetest`.
Replay must not infer conversation content from `session.EventRecord.Payload`.

As of W7, `agui.Replay` materializes its message snapshot through the
**agentic** pipeline, not the classic one: `emitMessageSnapshot`
(`agui/replay.go`) projects every durable message via
`history.ProjectAgentic`/`convert.ToAgenticProjection` and emits each one
through `emitter.Emitter.EmitCommittedProjection` with
`DeliveryModeReplay`. This removes the `history.ErrClassicUnsupported`
failure the classic (non-agentic) `history.Load`/`history.Project` path hit
on `tool_search_result`, `server_tool_call`/`server_tool_result`,
`mcp_tool_call`/`mcp_tool_result`/`mcp_list_tools_result`,
`mcp_tool_approval_request`/`mcp_tool_approval_response`, and assistant-role
media blocks. **These specific kinds have no native AG-UI representation at
all** (`convert.nativeEventsForBlock` returns none for them): they replay as
an `eino.agentic.v1` custom content-block supplement **only**. Only
`reasoning`, `assistant_gen_text`, `function_tool_call`, and text-only
`function_tool_result` blocks get a native representation alongside their
supplement; every other block kind -- including every `user_input_*` kind --
is custom-supplement-only.

`emitMessageSnapshot` does **not** emit a `MESSAGES_SNAPSHOT`. An earlier W7
fix pass added one built from each projection's `NativeMessage` (populated
by `convert.ToAgenticProjection` for user-role messages only -- see
eino-agui's `nativeUserMessage`), emitted ahead of the per-message
projections, so a native-only client could see user-role history. That
snapshot was reverted: `NativeMessage` is user-role-only upstream, so it
carried user turns alone, ahead of every assistant projection emitted after
it -- reordering a `U1,A1,U2,A2` transcript into `U1,U2,A1,A2` for any
client that treats `MESSAGES_SNAPSHOT` as authoritative -- and it ignored
the cursor entirely, clobbering client state on a cursored reconnect.
eino-agui's own `nativeUserMessage` doc comment states the design intent
directly: hosts assemble snapshots from their own complete committed
transcript, so this bridge must not overwrite unrelated history while
projecting one durable record. **A native-only AG-UI client -- one that
never parses the `eino.agentic.v1` custom envelope -- therefore has no
representation of user-role history on this path at all.** A host that
needs native-only clients to see user turns must assemble its own
`MESSAGES_SNAPSHOT` from its complete committed transcript (as eino-agui's
own doc comment recommends), not rely on `agui.Replay`/`agui.Reconnect` to
supply one.

`TestReplayMessageSnapshotIncludesUserMediaBlock` in `agui/replay_test.go`
proves the media content reaches the stream for `user_input_text`/
`user_input_image` as a `CUSTOM` supplement; it does not cover, and does not
prove anything about, `tool_search_result`/`server_tool_*`/`mcp_*`/assistant
media. See "W7: Agentic committed-projection replay and live emission"
below for the full mechanism. The classic `history.Load`/
`convert.ToAGUIMessages` path still exists and is still used by
`transport.DecodeMessages` for classic JSON ingress; it is `agui.Replay`'s
emission path specifically that no longer uses it.

`agui.Replay`/`agui.Reconnect` also take an explicit `includeReasoning`
parameter (`transport.SSEConfig.IncludeReasoning` at the HTTP boundary),
defaulting to `false`. This is a **host attestation**, not a policy check
this package independently verifies: `agui.GateProviderReasoningStorage`
(`agui/policy.go`) is a documentation/policy label, not a value any code
path reads. `includeReasoning`/`IncludeReasoning` is the actual mechanism,
and it gates durable reasoning content blocks in the message snapshot, live
commit reprojection, **and** the live `EventMessageDelta` path
(`Bridge.emitMessageDelta`) uniformly -- a host that has not set it to
`true` sees no reasoning on any of the three. A host that never sets it
gets the same default the pre-W7 classic pipeline had (no durable reasoning
replayed).

`session.MessageCommittedEventKind` ("message_committed") durable events
are always forwarded to `bridge.Emit`, like any other non-`LiveOnly`
durable event (see `replay()` in `agui/replay.go`); `Bridge` itself is what
skips a redundant one. `Bridge` tracks every message ID it has already
emitted through the committed-projection path (snapshot emission or an
earlier live continuation) for the life of a connection
(`Bridge.projectedMessages`); `emitLiveMessageCommitted` skips a
notification naming a message already in that set instead of re-projecting
it. This is what lets a message that commits **during** the replay window
(never covered by the snapshot, so not in that set yet) reach the client
instead of being dropped alongside the ones the snapshot already covers --
the earlier, broader "skip every `message_committed` during replay" rule
did the latter unconditionally, silently losing that message for the life
of the connection.

## Live Tail

Live tail emits transport events from the active run:

- run lifecycle;
- text deltas;
- reasoning deltas allowed by policy;
- optional live tool-call deltas;
- tool results;
- state snapshots/deltas;
- activity;
- steps;
- errors;
- custom events allowed by host policy.

If live tool-call streaming is enabled, the bridge must not also emit post-turn
tool proposals for the same calls. This follows the `eino-agui/stream` contract.

Transport write failures are not durable conversation failures by themselves.
They may cancel the active request through the emitter's disconnect handling,
after which runtime interruption determines durable status.

## Snapshot Safety

Message snapshots and state snapshots must be scrubbed before persistence or
replay:

- encrypted reasoning is excluded;
- provider-private state, its base64 representation, digests, and source bindings are excluded;
- provider-private reasoning is excluded unless explicitly allowed;
- plain reasoning is excluded unless a host attests
  `GateProviderReasoningStorage` is satisfied (see `includeReasoning` above
  -- this package trusts the attestation, it does not independently verify
  it);
- raw oversized tool output is replaced by bounded output and durable
  attachment references;
- state snapshots are stored only when host policy marks them replay-safe;
- custom event payloads default to audit-only and live-only.

## Error Handling

Errors have two projections:

- durable status: run, message, tool call, and event records show terminal or
  interrupted state;
- live transport: AG-UI error events notify connected clients.

Replay should prefer durable status summaries over raw historical `RUN_ERROR`
frames. Redaction is governed by `session.RedactionClass`,
`runtime.RedactionClass`, the `obs` field policy model, and
`docs/architecture/observability.md`.

## Implementation Requirements for Bridges

AG-UI bridge implementations must:

- use `eino-agui` for conversion, emission, stream tapping, and client-tool
  binding;
- apply `agui.Rules()` or a host-validated override;
- persist only durable facts described above;
- mark live-only runtime events with `LiveOnly`;
- avoid storing raw SSE frames as replay source data;
- exclude encrypted reasoning from all snapshots and replay projections;
- use durable cursor boundaries from `session.Store` for replay and live tail.


## Typed session watch adapter

agui.WatchBridge consumes watch.Subscription updates through the existing
eino-agui emitter. It constructs public AG-UI Message values with durable IDs;
the conversion helper that generates IDs is not used. Initial emits the
durable baseline, then Apply consumes increasing DeliverySequence updates.
Durable updates replace the bounded message window. Live updates replace
text for a matching visible unfinalized message in a nonterminal run.
Unavailable notices and publication versions suppress delayed replacements.
Finalization, terminal run state and absence from a newer window purge overlays
without retaining retired-message tombstones.

MESSAGES_SNAPSHOT contains only user/assistant display text. STATE_SNAPSHOT
contains the exported WatchState shape: watermark, existence/omission flags,
allowlisted run/tool views and qualified live availability. It contains no
tool arguments/results, arbitrary error text, configuration or reasoning.
This current-state API coalesces revisions and does not promise every
RUN_STARTED or TOOL_CALL event. The historical classification table above
describes the separate event/replay policy, not the watch allowlist.

## W7: Agentic committed-projection replay and live emission

This section states only what W7's implementation and tests actually prove;
it does not describe an aspirational end state. See
`docs/dependency-status.md` for the pinned `eino-agui` commit.

### Durable identity (session.Message.TurnID / AgentPath)

`session.Message` carries `TurnID` and `AgentPath`, stamped at most (not
quite every -- see below) append sites (admission, turn admission,
continuation dispatch, approval response, tool settlement, tool search
settlement, crash-reconciled carrier turns, and now the compaction boundary
message -- `session/compaction`). Both are record-JSON-only correlation
fields, matching the pre-existing `EventRecord.TurnID`/`AgentPath` and
`ModelRequestRecord.TurnID`/`AgentPath` convention (no SQL column or index
backs any of the four); no migration was needed. `agui.agenticIdentity`
keys the projected agent-path segment off `AgentPath` specifically (not the
separate `Agent` field, which carries the configured agent's display name
and is set only on assistant messages) so every message in one turn --
user and assistant alike -- projects the same agent path; `AgentPath` is
always empty today (a single root-agent path segment named `"root"` is
synthesized at projection time) because subagent nesting is not wired end
to end anywhere in the runtime yet.

Not every durable message actually gets a `TurnID`: all pre-W7 data, every
compaction boundary message before this fix, and
`runtime.settleInterruptedTool`'s deliberate crash-reconciliation stamp
(that function's own doc comment explains why: no by-ID message read exists
to recover the calling message's turn) all carry an empty `TurnID`.
`convert.validateIdentity` rejects an empty `TurnID` outright, so
`agui.agenticIdentity` substitutes a deterministic synthetic id
(`"msg:" + message.ID`) whenever `TurnID` is empty -- mirroring
`blockContexts`' existing synthetic-block-id precedent -- so a single such
message can never fail `loadCommittedProjections`' whole batch and brick
replay for an entire session.

An assistant message's "attempt identity" for AG-UI purposes is the
`InvocationID` of the `ModelRequestRecord` that produced it (existing
`AssistantMessageID` linkage); a user message, or an assistant message with
no ledger row, uses its own `MessageID` as a stable, never-retried attempt id
(`agui/agentic_projection.go`'s `attemptResolver`).

`session.MessageCommittedEventKind` ("message_committed") is a new,
non-canonical durable event, published (best-effort; its own failure never
unwinds an already-committed write) once after `persistAssistantTurn`
commits an assistant message's content and once after each tool call
settles (`runtime/message_commit_event.go`), carrying the message id and the
session's observation-watermark revision at commit time.

### Committed-projection emission (agui/bridge.go, agui/replay.go)

Both the durable replay path and the live path build an eino-agui
`convert.AgenticProjection` per durable message and emit it through
`emitter.Emitter.EmitCommittedProjection`:

- **Replay** (`emitMessageSnapshot`): every durable message in a session,
  projected via `history.ProjectAgentic` and `convert.ToAgenticProjection`,
  emitted with `DeliveryModeReplay` (a native AG-UI representation for the
  content kinds `convert.nativeEventsForBlock` maps one to -- `reasoning`,
  `assistant_gen_text`, `function_tool_call`, text-only
  `function_tool_result` -- plus one `eino.agentic.v1` custom content-block
  supplement per block, always; nothing is silently dropped, but a kind
  outside that native list, e.g. `tool_search_result` or `mcp_*`, gets the
  custom supplement only, never a native event). No `MESSAGES_SNAPSHOT` is
  emitted (see the Replay Projection section above for why).
- **Live** (`Bridge.Emit`'s `session.MessageCommittedEventKind` case,
  `emitLiveMessageCommitted`): reprojects the *whole session's* durable
  history and emits only the one message the event named, with
  `DeliveryModeLiveContinuation` (custom supplement only -- representable
  native content already streamed live via `emitMessageDelta`/
  `emitToolCallUpdated` before the message committed, so this must not
  duplicate it). Reprojecting the whole session per commit is a known
  O(session history) cost: `session.Store` exposes no by-ID single-message
  read today, so this reuses the same tested path replay uses rather than
  an unverified narrower one. A future single-message store read should
  remove this cost without changing behavior.

Every emitted projection's `CommitReceiptV1` binds `Domain: "projection"`,
the exact `Identity` eino-agui computed, and `Digest: ProjectionDigestV1`.
Replay's shared revision comes from `session.ObservationReader.
ReadObservationRevision`, queried once per replay call; every message in
that call safely shares the same revision string, because eino-agui's
receipt dedup key (`agenticReceiptKey`) folds the full `Identity` in
alongside `Revision` -- the identity fields still disambiguate messages that
share one revision. That same revision-folding means the SAME message
re-projected under a **different** revision is not deduplicated at the
receipt layer at all: this is why `Bridge` maintains its own
connection-lifetime record of which message IDs it has already emitted
through the committed-projection path (`Bridge.projectedMessages`) and
`emitLiveMessageCommitted` consults it before re-projecting, rather than
relying on `agenticReceiptKey` to catch a redundant `message_committed`
notification for a message the snapshot (or an earlier live continuation)
already delivered.

### Not yet implemented (deferred, not silently dropped)

- Full lifecycle mapping: `run_paused` with `InterruptTargetV1` built from
  validated durable approval/interrupt records, `run_resumed`,
  `attempt_replaced`, and subagent events. The runtime does not emit
  `session.SubagentStartedEventKind`/`SubagentFinishedEventKind`/
  `SubagentErrorEventKind` anywhere yet (subagent nesting is not wired), so
  there is nothing for a bridge mapping to consume for that family today.
- Transient per-block live deltas via `convert.TransientEventForBlock` (the
  live path today still uses the classic `TextStart`/`TextContent`/
  `ToolStart`/... emitter methods for in-flight deltas; only the *committed*
  projection at commit time uses the agentic path).
- `transport.DecodeUserMessage` (rich AG-UI input decode into
  `runtime.UserMessage` blocks) exists and is tested but is not yet wired as
  the default ingress path in `SSEHandler`/`examples/minimal-server`; the
  classic `DecodeMessages` remains the default for existing callers.
- Watch (`watch/`) bounded public block state and a block-indexed live
  overlay, and the observability typed-callback adapters with a single
  accounting source, are untouched by W7.
