# Ensemble Future Adapter And Migration Sketch

`ensemble` is currently a Go/Eino backend with an internal dispatcher event stream, OpenTelemetry/Datadog-oriented observability, persistence, and worker orchestration. It is not currently an AG-UI SSE backend: no AG-UI SDK imports were found in the researched backend, and this guide does not claim AG-UI parity.

This document sketches how a future integration can map ensemble events into `eino-agent` replay/tail and Datadog observability primitives.

## Current Ensemble Surface

The relevant backend boundary is `internal/dispatcher`:

- `Dispatcher.Dispatch(ctx, IssueAssignment) (<-chan RunEvent, error)` starts work and streams typed events.
- `RunEventKind` includes run, session, turn, tool, model fallback, notification, forensic message, finalization, and failure events.
- `EventSink.Emit(ctx, RunEvent)` is the orchestrator-side persistence/observability boundary.
- The production `EventSinkAdapter` persists validated events asynchronously and drops invalid events with a counter and warning log.

Observability currently flows through ensemble's `internal/obs` OpenTelemetry wrapper and Datadog LLM Observability `gen_ai.*` attributes. High-cardinality IDs belong on spans/logs, not metric labels.

## Migration Options

### Option A: Direct AG-UI In Ensemble

Add AG-UI transport directly to the ensemble backend:

1. keep `dispatcher.RunEvent` as the worker/orchestrator boundary;
2. add a durable AG-UI replay store or adapt the existing events table to `session.Store` semantics;
3. add a live tail for active runs;
4. project `RunEvent` into `runtime.Event`;
5. use `agui.Bridge`/`transport.SSEHandler`-equivalent wiring for replay and live streaming;
6. use `eino-obs` or ensemble's existing OTel path for Datadog observations, with one authority for redaction.

This option keeps one deployment but requires ensemble to own an AG-UI route contract, reconnect cursors, and message snapshot reconstruction.

### Option B: Adapter Service

Build a sidecar or adjacent service that consumes ensemble events and exposes AG-UI:

1. subscribe to ensemble run events or persisted event rows;
2. map events to `runtime.Event` using the sketch in `examples/ensemble-adapter`;
3. write replayable records into an `eino-agent` `session.Store`;
4. fan live events into `stream.Tail`;
5. serve AG-UI SSE through `transport.SSEHandler`;
6. forward Datadog observations through `eino-obs` with ensemble IDs in correlation fields.

This option avoids changing ensemble's primary HTTP/API surface at first, but it introduces another process and a synchronization boundary.

## Event Mapping

| Ensemble event | AG-UI/runtime projection | Replay policy | Observability |
| --- | --- | --- | --- |
| `run_started` | `runtime.EventRunStarted` | Durable | run observation |
| `session_started`, `turn_started` | context/step marker, likely `EventContextEpochChanged` or custom AG-UI step | Durable | session/turn span boundary |
| `turn_completed` | context/step close marker, likely `EventContextEpochChanged` or custom AG-UI step | Durable | token usage and turn success |
| `turn_failed`, `turn_cancelled` | `EventRunFinished` with error/interrupted payload | Durable | error/cancellation |
| `tool_call_started`, `tool_call_finished` | `EventToolCallUpdated` | Durable | tool call span/event |
| `unsupported_tool_call`, `malformed_tool_call` | `EventToolCallUpdated` with rejected/malformed status | Durable | validation/error classification |
| `notification` | `EventMessageDelta` or settled assistant message, depending on product semantics | Live-only unless persisted as a message | user-visible progress |
| `other_message` | omitted from AG-UI by default | Omit | forensic-only, redacted |
| `model_fallback_engaged` | optional custom event or omitted | Usually omit from AG-UI replay | fallback observation |
| `run_finalized`, `run_failed` | `EventRunFinished` | Durable | terminal run observation |

`notification` is the hardest product decision. If it is user-visible chat content, the adapter must also persist a replayable `session.Message`/`session.Part`; if it is progress text, keep it live-only.

## Storage And Replay

AG-UI reconnect requires more than a stream of past events:

- a stable durable session ID, likely ensemble `ThreadID` or `SessionID`;
- a durable run ID mapping from ensemble `RunAttemptID`;
- replayable messages and ordered parts for `MESSAGES_SNAPSHOT`;
- durable lifecycle/tool/error events with cursors;
- live-only token/progress deltas in a bounded `stream.Tail`.

Ensemble's async events table can be a source, but it is not automatically an `eino-agent/session.Store`. A direct integration should either implement the `session.Store` interface over ensemble persistence or project ensemble rows into an `eino-agent` store used by the AG-UI adapter.

## Datadog Observability

Preserve ensemble's cardinality policy:

- keep issue ID, run attempt ID, session ID, thread ID, turn ID, tool name, and model IDs as span/log attributes, not metric labels;
- map provider names to GenAI systems intentionally;
- record token usage per model call, not just per turn;
- do not export raw prompts, outputs, tool payloads, reasoning, or AG-UI custom payloads by default.

For an adapter service, use the `examples/datadog` observer wiring and carry ensemble IDs through `einoobs.Correlation` or runtime metadata.

## Non-Parity Caveat

This is a migration sketch only. Ensemble does not currently expose AG-UI transport, AG-UI replay cursors, or AG-UI message snapshots. Any future adoption request must state whether the chosen path is direct backend AG-UI support or an adapter service, and it must include replay-store, live-tail, and Datadog redaction validation work.
