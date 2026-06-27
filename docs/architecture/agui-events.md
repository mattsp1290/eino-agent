# AG-UI Event Durability and Replay

Date: 2026-06-27

This document defines how `eino-agent` treats AG-UI-facing events as durable
history, live-tail transport, replay projections, audit records, or omitted
data. It supports the future AG-UI streaming bridge bead without replacing
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
When a client reconnects, it receives replayed durable state plus a live tail
from the active run, not a best-effort resend of prior transport writes.

Encrypted reasoning is never persisted, never included in message snapshots,
and never replayed.

## Classification Table

| Family | Persisted durable fact | Replay behavior | Live-tail behavior | Omitted |
| --- | --- | --- | --- | --- |
| Run lifecycle | `session.EventRecord` audit with run status metadata. | Not replayed as raw `RUN_STARTED`/`RUN_FINISHED`; replay exposes current run/message state. | Emit live through `eino-agui/emitter`. | None, except transport-only write failures. |
| Text | Settled `session.Part{Kind: PartText}` on assistant message. | Replay as AG-UI assistant message content projected from durable parts. | Emit `TEXT_MESSAGE_*` deltas live. | Empty deltas. |
| Plain reasoning | `session.Part{Kind: PartReasoning}` only when provider and host policy allow storage. | Replay as reasoning content only from durable reasoning parts. | Emit `REASONING_*` live while allowed. | Provider-private or policy-denied reasoning. |
| Encrypted reasoning | Never persisted. | Never replayed. | Not emitted by `eino-agent`; scrub from snapshots. | All encrypted reasoning payloads. |
| Tool calls | `session.ToolCall` plus `PartToolCall` state transitions. | Replay call state from durable tool-call records and parts. | Emit live tool-call events only when bridge enables `eino-agui/stream.WithLiveToolCallEvents`. | Duplicate post-turn proposals when live tool calls were already emitted. |
| Tool results | `PartToolResult` plus settled `session.ToolCall` output/error. | Replay bounded model-facing tool result from durable part. | Emit live result through `eino-agui/emitter.ToolResult`. | Oversized raw output beyond retention policy. |
| State snapshots | `PartState` only when host marks snapshot replay-safe. | Replay latest replay-safe snapshot or host-projected state. | Emit live snapshot when state changes. | Sensitive or non-replay-safe host state. |
| State deltas | Optional `EventRecord` audit. | Do not replay raw deltas; replay starts from snapshot. | Emit live deltas. | Deltas superseded by snapshot. |
| Messages snapshots | Not stored as raw AG-UI frames. | Reconstruct from durable messages/parts using `eino-agui/convert`. | May emit live snapshot for UI synchronization. | Raw snapshot frame payload. |
| Activity | Optional `EventRecord` audit metadata. | Not replayed as conversation content. | Emit live activity. | Transient activity with no audit value. |
| Steps | `PartStep` plus `EventRecord` correlation. | Replay as annotations/status where UI supports it. | Emit live `STEP_*`. | None. |
| Custom events | Optional audit `EventRecord`. | Not replayed unless promoted to a future typed replay contract. | Emit live. | Unknown sensitive payloads by policy. |
| Errors | `EventRecord` plus terminal run/message status. | Replay terminal status/error summary, not necessarily raw `RUN_ERROR`. | Emit live `RUN_ERROR` or related error event. | Provider/internal details redacted by policy. |

## Type Contract

The `agui` package exposes a compileable classification model:

- `agui.EventFamily`: coarse event family names.
- `agui.Disposition`: `persist`, `replay`, `live`, `audit`, or `omit`.
- `agui.Rule`: durable part kind, event kind, redaction class, snapshot-safety,
  and notes for each family.
- `agui.Rules()`: default policy used by bridge implementations.

These types are policy definitions only. They do not emit protocol events and
do not convert messages.

## Replay Projection

Replay uses this order:

1. Read durable messages and parts with `session.Store.ListMessages`.
2. Exclude encrypted reasoning and policy-denied content.
3. Materialize assistant/user/tool messages from durable parts.
4. Convert replayable messages through `eino-agui/convert`.
5. Emit a messages snapshot or replay response through the transport adapter.
6. If a run is active, attach to live tail from the current run cursor.

Replay must preserve durable message/part ordering from `store/storetest`.
Replay must not infer conversation content from `session.EventRecord.Payload`.

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
- provider-private reasoning is excluded unless explicitly allowed;
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
frames. Redaction is governed by `session.RedactionClass` and the future Datadog
policy bead.

## Implementation Requirements for the Bridge

The future AG-UI bridge implementation must:

- use `eino-agui` for conversion, emission, stream tapping, and client-tool
  binding;
- apply `agui.Rules()` or a host-validated override;
- persist only durable facts described above;
- mark live-only runtime events with `LiveOnly`;
- avoid storing raw SSE frames as replay source data;
- exclude encrypted reasoning from all snapshots and replay projections;
- use durable cursor boundaries from `session.Store` for replay and live tail.
