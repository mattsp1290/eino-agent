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
2. add a durable AG-UI replay store, or add a new lossless projection from ensemble events into `session.Store`;
3. add a live tail for active runs;
4. project `RunEvent` into `runtime.Event`;
5. use `agui.Bridge`/`transport.SSEHandler`-equivalent wiring for replay and live streaming;
6. use `eino-obs` or ensemble's existing OTel path for Datadog observations, with one authority for redaction.

This option keeps one deployment but requires ensemble to own an AG-UI route contract, reconnect cursors, and message snapshot reconstruction.

The existing ensemble event persistence is not enough by itself for this option. Its event sink is async best-effort and can drop events during queue overflow, failed batch inserts, or shutdown drain limits. Treat those rows as forensic/audit input unless the AG-UI path adds a lossless write of sessions, runs, messages, parts, tool calls, context epochs, and event cursors.

### Option B: Adapter Service

Build a sidecar or adjacent service that consumes ensemble events and exposes AG-UI:

1. subscribe to live ensemble run events, or use persisted rows only as a forensic/backfill source;
2. map events to `runtime.Event` using the sketch in `examples/ensemble-adapter`;
3. write replayable records into an `eino-agent` `session.Store` on the hot path;
4. fan live events into `stream.Tail`;
5. serve AG-UI SSE through `transport.SSEHandler`;
6. forward Datadog observations through `eino-obs` with ensemble IDs in correlation fields.

This option avoids changing ensemble's primary HTTP/API surface at first, but it introduces another process and a synchronization boundary.

## Event Mapping

| Ensemble event | AG-UI/runtime projection | Replay policy | Observability |
| --- | --- | --- | --- |
| `run_started` | `runtime.EventRunStarted` | Durable | run observation |
| `session_started`, `turn_started` | session/turn status or custom AG-UI step, not a context-epoch event unless compaction changed | Durable audit/status record | session/turn span boundary |
| `turn_completed` | turn/step close status, not a terminal run event | Durable audit/status record | token usage and turn success |
| `turn_failed`, `turn_cancelled` | nonterminal turn error/status annotation; wait for `run_failed` or `run_finalized` before `EventRunFinished` | Durable audit/status record or live notification | error/cancellation |
| `tool_call_started`, `tool_call_finished` | `EventToolCallUpdated` only after a stable tool-call ID exists | Durable if correlated | tool call span/event |
| `unsupported_tool_call`, `malformed_tool_call` | validator/audit event; do not imply executed tool start/finish | Durable audit/status record | validation/error classification |
| `notification` | `EventMessageDelta` or settled assistant message, depending on product semantics | Live-only unless persisted as a message | user-visible progress |
| `other_message` | omitted from AG-UI by default | Omit | forensic-only, redacted |
| `model_fallback_engaged` | `runtime.EventModelFallbackEngaged` | Durable runtime event when the adapter persists its `session.EventRecord` before live delivery; the current sketch only maps data and does not persist it | fallback observation |
| `run_finalized`, `run_failed` | `EventRunFinished` | Durable | terminal run observation |

`notification` is the hardest product decision. If it is user-visible chat content, the adapter must also persist a replayable `session.Message`/`session.Part`; if it is progress text, keep it live-only.

Ensemble currently emits `ToolName` and `ToolOutcome` for tool events, not a durable tool-call ID. A production adapter must either make ensemble emit a stable tool-call ID or synthesize one from a turn-local ordinal/correlation key before writing AG-UI/session tool-call records. Do not use `ToolName` as the ID: repeated calls to the same tool in one turn would collapse into one logical call.

## Storage And Replay

AG-UI reconnect requires more than a stream of past events:

- a stable durable session ID, likely ensemble `ThreadID` plus any cross-run continuation namespace;
- ensemble `SessionID` retained as correlation metadata, because it is turn/session-scoped rather than the stable conversation key;
- a durable run ID mapping from ensemble `RunAttemptID`;
- replayable `session.Message` rows and ordered `session.Part` rows for `MESSAGES_SNAPSHOT`;
- durable `session.ToolCall` rows for correlated tool activity;
- selected `session.EventRecord` rows for status/audit events, redaction class, and cursor ordering;
- live-only token/progress deltas in a bounded `stream.Tail`.

AG-UI replay must reconstruct conversation state from durable messages and parts, not by inferring chat content from arbitrary runtime event payloads or replaying historical SSE frames. Ensemble's async events table can help with audit and backfill, but it is not automatically an `eino-agent/session.Store` and it is lossy under backpressure or write failure. A direct integration should either implement the `session.Store` interface over new lossless ensemble persistence or project live ensemble events into an `eino-agent` store used by the AG-UI adapter.

## Datadog Observability

Preserve ensemble's cardinality policy:

- keep issue ID, run attempt ID, session ID, thread ID, turn ID, tool name, and model IDs as span/log attributes, not metric labels;
- map provider names to GenAI systems intentionally;
- record token usage per model call, not just per turn;
- do not export raw prompts, outputs, tool payloads, reasoning, or AG-UI custom payloads by default.

For an adapter service, use the `examples/datadog` observer wiring and carry ensemble IDs through `einoobs.Correlation` or runtime metadata.

## Non-Parity Caveat

This is a migration sketch only. Ensemble does not currently expose AG-UI transport, AG-UI replay cursors, or AG-UI message snapshots. Any future adoption request must state whether the chosen path is direct backend AG-UI support or an adapter service, and it must include replay-store, live-tail, and Datadog redaction validation work.
