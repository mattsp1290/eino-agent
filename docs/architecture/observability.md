# Observability and Redaction Policy

Date: 2026-06-27

This document defines `eino-agent`'s end-to-end observability contract for
Datadog AI/LLM observability. Runtime instrumentation must use
`github.com/mattsp1290/eino-obs`; this repo owns when observations are emitted,
which fields are attached, and which data is redacted.

## Boundary

`eino-obs` owns:

- observer creation and configuration;
- no-network default exporter and fake exporters for tests;
- Datadog exporter implementation;
- session, run, model, stream, tool, AG-UI tool, and lifecycle observation
  helpers;
- token usage, latency, retry, cancellation, and error classification fields;
- input/output summary redaction helpers.

`eino-agent` owns:

- mapping runtime/session/run/tool/provider/AG-UI IDs into correlation;
- deciding which runtime boundaries create observations;
- ensuring raw content is not added to metadata or attributes;
- requiring explicit host opt-in before bounded summaries are exported;
- keeping Datadog exporter setup opt-in and config-driven.

## Safe Defaults

Default policy:

- no raw prompts;
- no raw model outputs;
- no raw tool inputs or outputs;
- no attachment bytes, paths, URLs, or extracted media content;
- no reasoning content;
- no encrypted reasoning;
- no provider-private state, base64 representation, content-derived digest, or raw codec/decoder error;
- no compaction summary content;
- no state snapshot payloads;
- no AG-UI custom event payloads;
- no summaries unless the host enables bounded summaries explicitly.

Tests and examples must use the `eino-obs` no-network or fake exporter by
default.

## Observation Boundaries

| Runtime boundary | `eino-obs` helper | Required correlation |
| --- | --- | --- |
| Session start/end/error | `StartSession` / `Session.End` / `Session.Error` | session ID; trace/observation IDs when available |
| Run start/end/error | `StartRun` / `Run.End` / `Run.Error` | session ID, run ID, agent ID |
| Provider model call | `StartModelCall` | provider, model, run ID |
| Streaming model turn | `StartStream` / chunk / first token / end/error | provider, model, run ID, assistant message ID |
| Tool registration/materialization | `ToolRegistered` / `ToolMaterialized` | tool name/kind, run ID |
| Tool call execution | `StartToolCall` / end/error | tool call ID, tool name/kind, run ID |
| AG-UI tool materialization/settlement | AG-UI tool helpers | AG-UI thread ID, AG-UI run ID, tool call ID |
| Interrupt/resume/recovery | lifecycle/error observations | run ID, error classification |
| AG-UI event audit | runtime event observation | AG-UI thread ID, AG-UI run ID, session/run IDs |

## Correlation IDs

Correlation fields are carried on observations and logs, not metric labels
unless a specific bounded metric explicitly allows it.

| Field | Attribute | Cardinality | Required |
| --- | --- | --- | --- |
| Trace ID | `trace.id` | high | no |
| Observation ID | `observation.id` | high | no |
| Parent observation ID | `observation.parent_id` | high | no |
| Session ID | `session.id` | high | yes |
| Run ID | `run.id` | high | yes |
| Agent ID | `agent.id` | bounded | no |
| Assistant message ID | `assistant_message.id` | high | no |
| Tool call ID | `tool_call.id` | high | no |
| Provider | `genai.provider` | bounded | no |
| Model | `genai.model` | bounded | no |
| AG-UI thread ID | `agui.thread.id` | high | no |
| AG-UI run ID | `agui.run.id` | high | no |

## Allowed Default Fields

Allowed by default:

- service name, environment, and version;
- observation kind/name/status;
- `gen_ai.operation.name` or equivalent operation name classification;
- provider and model;
- total latency and first-token latency;
- token usage counts;
- tool name, kind, and status;
- error operation, classification, retryable, canceled, and dropped flags;
- correlation IDs listed above.

High-cardinality IDs are allowed on spans/observations for traceability. They
must not become unbounded metric labels.

## Forbidden Fields

Never export these by default:

- raw prompt or message text;
- raw model output;
- raw tool input/output JSON;
- tool stdout/stderr beyond bounded summaries;
- attachment bytes, attachment URLs, file paths, image/PDF/media contents;
- reasoning text;
- encrypted reasoning;
- provider-private state bytes, base64, content-derived digests, and raw restore errors;
- compaction summary text;
- state snapshot payloads;
- AG-UI custom event payloads;
- secrets, environment dumps, headers, tokens, cookies, API keys, or auth
  metadata.

If any forbidden value reaches the instrumentation layer, it must be dropped and
recorded as an `eino-obs` redaction record rather than exported.

## Opt-In Summaries

Summaries are disabled by default. Host configuration may enable bounded
summaries through `eino-obs` redaction options:

- `CaptureInputSummary`;
- `CaptureOutputSummary`;
- `MaxSummaryBytes`.

Allowed summary surfaces after opt-in:

- model input/output summaries;
- stream input/output summaries;
- tool input/output summaries.

Summaries must be structured, bounded, and scrubbed. They must not include raw
prompt text, raw output text, raw tool payloads, attachments, reasoning,
encrypted reasoning, secrets, or compaction summaries. `MaxSummaryBytes` must be
positive and small enough for Datadog attribute limits before summaries are
enabled.

Provider-private state is never eligible for opt-in summaries.

## Tags and Attributes

Use low-cardinality values for metric-style tags:

- service;
- env;
- version;
- operation name;
- provider;
- model family or configured model ID when bounded;
- tool kind;
- status;
- error classification;
- retryable/canceled flags.

Use span/observation attributes for high-cardinality IDs:

- session ID;
- run ID;
- message ID;
- tool call ID;
- AG-UI thread/run IDs;
- trace and observation IDs.

Do not put prompt text, output text, tool payloads, file paths, or custom event
payloads into tags or attributes.

## Error Classification

Errors should use stable classifications:

- `invalid_config`;
- `invalid_schema`;
- `provider_error`;
- `provider_timeout`;
- `rate_limited`;
- `tool_error`;
- `permission_denied`;
- `interrupted`;
- `canceled`;
- `recovery_interrupted`;
- `exporter_failure`;
- `unknown`.

Retryability must be explicit. Canceled/interrupted work is not retryable by
default unless the runtime has a durable retry-safe point.

## AG-UI Event Correlation

AG-UI event observations should carry:

- session ID;
- run ID;
- AG-UI thread ID;
- AG-UI run ID;
- message ID, part ID, tool call ID, or epoch ID when available;
- event family/audit kind from `agui.Rule.AuditKind`;
- redaction class from AG-UI policy.

AG-UI live-only payloads are not exported as raw event content. The observation
records the event family, timing, IDs, status, and redaction classification.

## Runtime Implementation Requirements

Runtime instrumentation must:

- create observers through `einoobs.New`;
- use no-network/fake exporters in tests by default;
- configure Datadog exporter only through explicit runtime configuration;
- propagate `einoobs.Correlation` through runtime contexts;
- map `runtime.Event` and `session.EventRecord` IDs into correlation fields;
- call `eino-obs` session/run/model/stream/tool helpers at runtime boundaries;
- pass only allowed metadata fields;
- use `eino-obs` summary helpers only after explicit host opt-in;
- add redaction records whenever content is dropped or summarized.

## Type Contract

The `obs` package exposes a compileable policy model:

- `FieldClass`: allowed, summary-only, or forbidden.
- `ObservationKind`: runtime boundaries that create observations.
- `FieldPolicy`: default attribute/redaction policy.
- `CorrelationField`: propagated correlation ID policy.
- `SummaryPolicy`: opt-in bounded summary behavior.

These types are policy definitions only. They do not export to Datadog and do
not replace `eino-obs`.
