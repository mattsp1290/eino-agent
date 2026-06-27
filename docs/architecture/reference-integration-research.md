# Reference Integration Research

Date: 2026-06-27

This note maps the local integration/reference repositories to `eino-agent`
responsibilities. It complements `docs/dependency-status.md`, which records the
exact pins, response artifacts, and validation gates.

## Summary

`eino-agent` should be a runtime/orchestration layer above three reusable
libraries:

- `eino-agui` owns AG-UI/Eino conversion, typed SSE event emission, stream
  tapping, and AG-UI client-tool binding.
- `eino-tools` owns reusable coding-agent leaf tools and their model-facing
  JSON envelopes.
- `eino-obs` owns agent observability helpers, redaction defaults, no-network
  test exporters, and Datadog export.

The local consumers show what `eino-agent` must add:

- `ag-ui-go-server-example` proves AG-UI behavior and resume/interrupt lessons,
  but its route config, run store, approval policy, and app state stay
  application-owned.
- `ensemble` proves adjacent Go/Eino worker and OpenTelemetry constraints, but
  it is not currently an AG-UI SSE backend. Ensemble integration must be framed
  as an adapter or migration design, not as an existing parity claim.

## Local Repository Status

| Repo | Local path | Current role for `eino-agent` |
| --- | --- | --- |
| `eino-agui` | `~/git/eino-agui` | Pinned AG-UI protocol bridge dependency. |
| `eino-tools` | `~/git/eino-tools` | Pinned reusable coding-agent tool dependency. |
| `eino-obs` | `~/git/eino-obs` | Pinned Datadog AI/LLM observability dependency. |
| `ag-ui-go-server-example` | `~/git/ag-ui-go-server-example` | Reference AG-UI server behavior and adoption target. |
| `ensemble` | `~/git/ensemble` | Future adapter/migration target and observability/cardinality reference. |

## `eino-agui`

Evidence:

- `~/git/eino-agui/README.md`
- `~/git/eino-agui/docs/architecture/package-origins.md`
- `~/git/eino-agui/{convert,emitter,stream,tools}/doc.go`
- `~/git/eino-agui/docs/decisions/0002-ensemble-shared-surface.md`
- Response artifact:
  `~/.agents/projects/eino-agui/responses/2026-06-26-ag-ui-adapter-surface-for-eino-agent.md`

Public package responsibilities:

- `convert`: converts AG-UI `types.Message` histories to Eino
  `schema.Message` values and back. This includes provider-gated vision
  behavior, multimodal image parts, message text extraction, and tool-call
  conversion.
- `emitter`: emits typed AG-UI SSE events through the AG-UI SDK writer pair
  (`*bufio.Writer` and `*sse.SSEWriter`). It owns lifecycle, text, reasoning,
  tool, state, messages snapshot, activity, step, custom event, transport-vs-
  encoding error, block-closing, and encrypted-reasoning scrub behavior.
- `stream`: taps one Eino model stream, emits live AG-UI reasoning/text/tool
  call deltas, optionally streams live tool-call events, closes open blocks, and
  returns the concatenated assistant message.
- `tools`: binds AG-UI client tool definitions to Eino `ToolInfo` values and
  classifies model tool calls into client-owned and server-owned sets.

What `eino-agent` should consume directly:

- Use `convert.ToEinoMessages` and `convert.ToAGUIMessages` for protocol
  conversion rather than implementing a second converter.
- Use `emitter.NewEmitter` or a thin wrapper around it for AG-UI SSE output.
- Use `stream.StreamTurn` for live model delta to AG-UI event tapping.
- Use `tools.ClientToolInfos` and `tools.ClassifyToolCalls` for client-defined
  AG-UI tools.

What remains outside `eino-agui` and belongs to `eino-agent` or consumers:

- Durable session admission and history.
- Durable/live-only AG-UI event classification.
- Replay and live-tail APIs.
- Transport route wiring, auth, and disconnect lifecycle.
- Tool execution, approval interrupts, resume behavior, and settlement.
- Post-turn proposal emission policy for non-live routes.
- App-specific state snapshots, activity semantics, and custom events.

Validation commands recorded for this pin:

- `go test ./...`
- `go build ./... && make check && go test ./... -run Parity -count=1`

## `eino-tools`

Evidence:

- `~/git/eino-tools/README.md`
- `~/git/eino-tools/docs/inventory/*.md`
- `~/git/eino-tools/docs/adr/0008-workspace-filesystem-serialization.md`
- Response artifact:
  `~/.agents/projects/eino-tools/responses/2026-06-26-coding-agent-tool-parity-for-eino-agent.md`

Public package responsibilities:

- `fileops`: workspace-rooted `file_read`, `file_write`, `file_edit`, and
  `file_list`; `file_read` supports backward-compatible prefix reads and
  line-windowed reads with line numbers and caps.
- `glob`: doublestar-backed workspace path discovery.
- `search`: ripgrep-backed search with regex/literal modes, glob filters,
  ignore-case, context lines, limits, partial-result handling, and output caps.
- `applypatch`: multi-file `apply_patch` with add/update/delete/move grammar,
  path containment, preflight, context matching, and partial-write signaling.
- `shell`: workspace command execution with cwd, stdin, timeout, output caps,
  and cancellation.
- `urlfetch`: raw text fetch from `file://` and `https://` URLs.
- `userinteract`: CLI or MCP user question/answer tool.
- `tracker`, `tracker/beads`, `trackerwrite`: tracker abstractions and Beads
  close/transition/comment operations.

Hard contract for `eino-agent`:

- Serialize `fileops`, `glob`, `search`, and `apply_patch` calls per canonical
  workspace root.
- Allow independent workspace roots to run concurrently.
- Treat filesystem containment, network policy, secrets, and sandboxing for
  `shell` as caller/runtime responsibilities.
- Preserve stable model-facing JSON envelopes, `outcome` values, error
  categories, truncation metadata, duplicate-key rejection, and `RawJSON`
  forward compatibility.

What remains outside `eino-tools` and belongs to `eino-agent`:

- Session-bound tool registration/materialization.
- Permission and approval policy.
- Tool output retention/spooling.
- Subagent/task orchestration.
- Plan/todo/session state.
- Skill loading.
- Web search connector policy and schema.
- LSP/diagnostics lifecycle and schema, unless a later optional package exists.
- Datadog instrumentation wrappers.

Validation commands recorded for this pin:

- `make test`
- `make vet && make lint && go mod tidy -diff && go test -race ./...`

## `eino-obs`

Evidence:

- `~/git/eino-obs/README.md`
- `~/git/eino-obs/doc.go`
- `~/git/eino-obs/exporter/{datadog,fake}/doc.go`
- `~/git/eino-obs/recorder/doc.go`
- Response artifact:
  `~/.agents/projects/eino-obs/responses/2026-06-26-datadog-llm-observability-api-for-eino-agent.md`

Public package responsibilities:

- `Observer` creation and configuration.
- No-network default exporter and snapshot/reset helpers.
- Fake exporter for deterministic tests.
- Datadog LLM Observability HTTP exporter.
- Correlation propagation for trace, observation, session, run, parent,
  provider/model, assistant message, tool call, AG-UI thread, and AG-UI run
  identity.
- Session/run lifecycle observations.
- Model call and streaming turn observations, token usage, latency, retryable
  and canceled error classification.
- Tool registration, materialization, tool-call execution, AG-UI tool
  materialization/settlement, and settlement status observations.
- Retry, compaction, interrupt, resume, cancellation, and generic error events.
- Safe defaults that do not capture prompts, outputs, tool payloads,
  attachments, reasoning, or compaction summaries unless explicitly enabled.

What `eino-agent` should consume directly:

- Create a runtime observer through `einoobs.New`.
- Put correlation into runtime contexts with `ContextWithCorrelation`.
- Start and end session/run/model/tool observations at the runtime boundaries.
- Use no-network or fake exporters in tests and examples by default.
- Use the Datadog exporter only through explicit configuration.

What remains outside `eino-obs` and belongs to `eino-agent`:

- Deciding which runtime boundaries start spans or events.
- Mapping session/run/tool IDs into correlation values.
- Ensuring raw prompt/tool/reasoning payloads are not placed into metadata.
- Wiring exporter configuration into consumer-facing runtime options.

Validation command recorded for this pin:

- `make check` (`fmt-check`, `vet`, `test`, and `race`)

## `ag-ui-go-server-example`

Evidence:

- `~/git/ag-ui-go-server-example/internal/agent/agui.go`
- `~/git/ag-ui-go-server-example/internal/agent/loop.go`
- `~/git/ag-ui-go-server-example/internal/agent/runconfig.go`
- Existing tests under `~/git/ag-ui-go-server-example/internal/agent/*_test.go`

Current relationship to `eino-agent`:

- The example now delegates its reusable AG-UI seam to `eino-agui` through
  wrapper functions in `internal/agent/agui.go`.
- `Run` shows the runtime shape that `eino-agent` must generalize: start an
  AG-UI run, assemble per-request model/tool state, convert messages, stream
  model turns, validate tool calls, propose/execute/settle tools, interrupt for
  approval, persist paused run state, and resume without double execution.

AG-UI behavior to preserve:

- Each HTTP/SSE response is a self-contained AG-UI sequence from `RUN_STARTED`
  to terminal event. A resume may reuse thread/run IDs but is still a separate
  response sequence.
- Client-defined tools are bound per request using a fresh model returned by
  `WithTools`; shared model state must not be mutated.
- Live tool-call streaming and post-turn proposal emission are mutually
  exclusive to avoid duplicate tool events.
- Resume validates approval coverage before claiming the paused run. Claiming
  happens atomically before tool settlement to avoid double execution.
- Client disconnect cancels the stream and should not produce noisy terminal
  errors that cannot reach the client.

What belongs in `eino-agent`:

- Generalize these lessons into durable session/run APIs, store interfaces,
  replay/tail semantics, interruption/resume semantics, tool settlement, and
  transport-neutral embedding contracts.

What remains consumer-owned:

- Fiber routes and request parsing.
- Auth.
- Product-specific route configurations and prompts.
- App state/doc state.
- Concrete run store choice unless using an `eino-agent` provided store.
- Final HTTP/SSE endpoint shape.

## `ensemble`

Evidence:

- `~/git/ensemble/internal/dispatcher/doc.go`
- `~/git/ensemble/internal/dispatcher/event.go`
- `~/git/ensemble/internal/obs/doc.go`
- `~/git/ensemble/docs/llm-obs-genai-attributes.md`
- `~/git/ensemble/docs/multi-repo-workspace-routing.md`
- Response artifact:
  `~/.agents/projects/ensemble/responses/2026-06-26-observability-contract-for-eino-agent.md`
- Negative AG-UI import check recorded in `docs/dependency-status.md`.

Current backend status:

- `ensemble` is a Go/Eino backend with `internal/dispatcher`, `internal/worker`,
  and `internal/obs` boundaries.
- `RunEvent` is the orchestrator-to-worker event stream. Event kinds cover run,
  session, turn, tool, malformed/unsupported tool calls, fallback, notification,
  and terminal failure/finalization cases.
- `internal/obs` wraps OpenTelemetry SDK initialization, tracing, metrics, and
  log correlation.
- `docs/llm-obs-genai-attributes.md` records Datadog LLM Observability
  `gen_ai.*` attribute expectations and cardinality constraints.
- `docs/multi-repo-workspace-routing.md` shows multi-repo workspace and secret
  routing concerns that future `eino-agent` workspace/tool policies should not
  ignore.

Important observability constraints:

- High-cardinality identifiers such as issue ID, run attempt ID, session ID,
  thread ID, turn ID, tool name, and model identities belong on spans/logs, not
  metric labels, except explicitly bounded model fallback metrics.
- `gen_ai.operation.name` classifies spans for Datadog LLM Observability.
- Provider names must be mapped to GenAI system names intentionally.
- Token usage is per model call, not just per turn.
- Raw prompt/message attributes are high-cardinality and span-only where used.

AG-UI status:

- No AG-UI SDK imports were found in `ensemble`.
- Ensemble should not be treated as an AG-UI API proof point.
- Future work should decide whether to expose AG-UI directly from ensemble or
  through an adapter service.

What `eino-agent` should learn from ensemble:

- Preserve a clean dispatcher/runtime boundary.
- Keep event streams typed and validated before persistence/observability.
- Carry high-cardinality IDs in context/correlation, not metric labels.
- Design workspace routing and tool serialization with multi-repo deployments in
  mind.
- Keep ensemble adoption as a migration/adaptation task, not a direct code
  replacement.

## Integration Implications For Upcoming Beads

Architecture and API work should:

- Import `eino-agui`, `eino-tools`, and `eino-obs` at the pinned versions.
- Define runtime/store interfaces around session admission, run execution,
  replay/tail, tool settlement, and interrupt/resume without duplicating the
  upstream packages.
- Add a per-canonical-workspace filesystem-tool scheduler before exposing
  `fileops`, `glob`, `search`, or `apply_patch` concurrently.
- Treat token deltas and transport chunks as live-only unless deliberately
  persisted.
- Persist only durable events/snapshots needed for replay.
- Use no-network/fake observability in tests.
- Keep Datadog exporter setup opt-in and environment/config driven.
- Keep web search, LSP/diagnostics, permissions, subagents, skills, and retained
  output in the `eino-agent` runtime layer.

Documentation and examples should:

- Link `docs/dependency-status.md` for pins and validation evidence.
- Explain that `~/.agents/projects/*/responses/` artifacts are local workspace
  evidence, not repo-distributed files.
- Include an `ag-ui-go-server-example` adoption sketch based on the reference
  app's existing `Run` loop.
- Include an ensemble adapter/migration sketch that maps `dispatcher.RunEvent`
  to AG-UI only as future work.

## Validation Performed For This Note

No code was changed in referenced repos for this research note. Evidence was
gathered by reading the files listed above and by using the validation results
already recorded in `docs/dependency-status.md`.
