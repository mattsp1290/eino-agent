#!/bin/bash
# Project: eino-agent Go runtime for AG-UI and Datadog
# Generated: 2026-06-27

set -e

if [ ! -d ".beads" ]; then
    bd init
fi

echo "Creating eino-agent project bead graph..."

EPIC=$(bd create "Epic: eino-agent reusable Go runtime for AG-UI and Datadog" -t epic -p 0 --description "Organizational rollup for the complete eino-agent runtime project. This epic is not dispatchable implementation work and must not have dependency edges." --acceptance "All implementation, testing, documentation, and adoption beads are parented under this epic; no bd dep add edge references this epic." --silent)
bd update "$EPIC" --status in_progress

REQ_VERIFY=$(bd create "Verify prerequisite repos, request artifacts, and dependency pins" -p 0 --parent "$EPIC" --description "Reserve: docs/dependency-status.md, docs/prompts/**, ~/.agents/projects/eino-agui/requests/**, ~/.agents/projects/eino-obs/requests/**, ~/.agents/projects/eino-tools/requests/**, ~/.agents/projects/ensemble/requests/**. Inspect prerequisite request files and local repos or clones. Verify eino-agui v0.1.1 at 453c69c9bdb1006c585407510342ea5a03d1b48d, eino-tools e6ee664be93bb830b4bf6215907865b6366662b5, eino-obs a9a6f8bb478b479c1e48ab353261a60c4a19195a, and ensemble non-AG-UI status." --acceptance "Exact pins and evidence sources are confirmed; missing response artifacts are documented or created as prerequisite follow-up notes; implementation blockers are listed before any runtime code starts." --silent)
DEP_STATUS=$(bd create "Write durable dependency status note" -p 0 --parent "$EPIC" --description "Reserve: docs/dependency-status.md. Create a durable note with exact pins for eino-agui, eino-tools, eino-obs, the eino-tools response artifact path, missing response artifact decisions for eino-agui/eino-obs, and ensemble AG-UI non-parity evidence from eino-agui docs." --acceptance "docs/dependency-status.md exists and includes all required pins, response or tag evidence, validation gates, and ensemble scope warning." --silent)
REF_PI_OPEN=$(bd create "Research pi and opencode runtime concepts" -p 1 --parent "$EPIC" --description "Reserve: docs/architecture/reference-runtime-research.md. Clone or inspect https://github.com/earendil-works/pi and https://github.com/anomalyco/opencode as needed. Summarize adopted and rejected concepts: session admission, history replay, providers, typed tools, stale registrations, context epochs, compaction, interruption, and config/plugin lifecycle." --acceptance "Research doc identifies concrete patterns to adopt or avoid without copying TypeScript implementation details." --silent)
REF_LOCAL=$(bd create "Research local AG-UI, tools, observability, and consumer repos" -p 1 --parent "$EPIC" --description "Reserve: docs/architecture/reference-integration-research.md. Inspect eino-agui, eino-obs, eino-tools, ag-ui-go-server-example, and ensemble. Capture package APIs, validation commands, AG-UI event expectations, filesystem serialization contract, observability helpers, and consumer integration constraints." --acceptance "Research doc maps each referenced repo to the eino-agent responsibilities and explicitly marks responsibilities that must remain external." --silent)

bd dep add "$DEP_STATUS" "$REQ_VERIFY"
bd dep add "$REF_PI_OPEN" "$REQ_VERIFY"
bd dep add "$REF_LOCAL" "$REQ_VERIFY"

MOD_SETUP=$(bd create "Initialize Go module, repository layout, and dependency pins" -p 0 --parent "$EPIC" --description "Reserve: go.mod, go.sum, Makefile, .github/workflows/**, internal/** skeleton, README.md. Initialize module github.com/mattsp1290/eino-agent with Go 1.26.3 baseline, CloudWeGo Eino v0.8.13, eino-agui v0.1.1, eino-tools e6ee664be93bb830b4bf6215907865b6366662b5, and eino-obs a9a6f8bb478b479c1e48ab353261a60c4a19195a." --acceptance "go test ./..., go vet ./..., gofmt/goimports checks, go mod tidy -diff, race-test target, and CI workflow are defined and pass on an empty/skeleton module." --silent)
ARCH_API=$(bd create "Design runtime architecture and public package APIs" -p 0 --parent "$EPIC" --description "Reserve: docs/architecture/runtime.md, runtime/**, session/**, model/**, config/**. Define package boundaries for session admission/execution, runtime orchestration, stores, tools, AG-UI, observability, model catalogs, config hooks, context sources, and consumer embedding." --acceptance "Architecture doc and initial public interfaces exist; responsibilities do not duplicate eino-agui, eino-obs, or eino-tools; package APIs are stable enough for downstream implementation beads." --silent)
CONFIG_CORE=$(bd create "Implement minimal agent, model, provider, and observability config" -p 1 --parent "$EPIC" --description "Reserve: config/**, model/**, catalog/**. Implement config structs and validation for default agent, named agents, model/provider selection, tool permissions, instructions/context sources, and observability settings. Keep provider transports behind interfaces or optional packages." --acceptance "Config validation tests cover missing defaults, unknown agents/models, permission defaults, and observability redaction defaults." --silent)
CONTEXT_CORE=$(bd create "Implement runtime context and context-source boundaries" -p 1 --parent "$EPIC" --description "Reserve: runtime/context.go, context/**, docs/architecture/context.md. Provide session ID, agent ID, assistant message ID, tool call ID, provider/model identity, trace/span context, cancellation, and bounded context sources for system prompt, project instructions, attachments, and future references." --acceptance "Unit tests verify context propagation, cancellation, and no baked local-only paths in runtime logic." --silent)
PROVIDER_BIND=$(bd create "Implement provider/model adapter interfaces and optional Eino bindings" -p 0 --parent "$EPIC" --description "Reserve: model/**, providers/**, runtime/provider.go, testdata/providers/**. Implement the model/provider abstraction used by runtime orchestration, including provider identity, model metadata, streaming callbacks, token usage, error normalization, and optional bindings for compatible Eino/provider transports such as eino-providers or Codex/OpenAI-style transports without making them mandatory dependencies unless selected." --acceptance "Unit tests cover fake provider streaming, provider errors, token usage propagation, model/provider identity in runtime context, optional-provider build behavior, and no shared mutable provider state across sessions." --silent)

bd dep add "$MOD_SETUP" "$DEP_STATUS"
bd dep add "$MOD_SETUP" "$REF_LOCAL"
bd dep add "$ARCH_API" "$MOD_SETUP"
bd dep add "$ARCH_API" "$REF_PI_OPEN"
bd dep add "$ARCH_API" "$REF_LOCAL"
bd dep add "$CONFIG_CORE" "$ARCH_API"
bd dep add "$CONTEXT_CORE" "$ARCH_API"
bd dep add "$PROVIDER_BIND" "$CONFIG_CORE"
bd dep add "$PROVIDER_BIND" "$CONTEXT_CORE"

STORE_IFACE=$(bd create "Define durable store interfaces and transaction boundaries" -p 0 --parent "$EPIC" --description "Reserve: store/**, session/**, docs/architecture/storage.md. Define storage interfaces for session admission, run records, history projection, durable events, pending tool calls, settlement, replay cursors, and atomic claim semantics." --acceptance "Interface tests or contract tests describe transaction boundaries, idempotency, pending tool claim rules, replay ordering, and interrupt/resume invariants." --silent)
SQLITE_STORE=$(bd create "Implement local SQLite durable store" -p 1 --parent "$EPIC" --description "Reserve: store/sqlite/**, internal/sqlite/**, migrations/**, testdata/store/**. Implement Go-native SQLite persistence for sessions, runs, messages, tool calls, durable events, and replay cursors with migrations." --acceptance "Store contract tests pass against SQLite; migrations are deterministic and schema-versioned; upgrade/downgrade compatibility decisions are documented; malformed rows and duplicate claims return typed errors rather than panics." --silent)
SESSION_ADMIT=$(bd create "Implement session admission and run creation" -p 0 --parent "$EPIC" --description "Reserve: session/**, runtime/admission.go, testdata/session/**. Implement durable session admission separate from execution, including session IDs, run IDs, assistant message IDs, config snapshotting, and admission observability hooks." --acceptance "Tests prove admission is durable before execution, duplicate admission is idempotent or rejected explicitly, and failed execution does not erase session history." --silent)
HISTORY_REPLAY=$(bd create "Implement replayable session history projection" -p 0 --parent "$EPIC" --description "Reserve: session/history/**, runtime/history.go, testdata/session/**. Implement durable message and tool-result projection for model context without persisting live-only token deltas unless explicitly configured." --acceptance "Golden tests verify replayed history, excluded live deltas, tool results, state snapshots, and compaction boundary behavior." --silent)
RUN_ORCH=$(bd create "Implement streaming turn orchestration" -p 0 --parent "$EPIC" --description "Reserve: runtime/**, session/run.go, testdata/runtime/**. Orchestrate provider/model calls, stream callbacks, tool-call loops, retries, cancellation, bounded queues, and run lifecycle states." --acceptance "Unit tests cover successful turns, provider errors, cancellation, retry boundaries, bounded queue backpressure, and no panics on malformed model stream input." --silent)
INTERRUPT_RESUME=$(bd create "Implement interrupt and resume without double execution" -p 0 --parent "$EPIC" --description "Reserve: runtime/interrupt.go, session/resume.go, store/** tests. Implement interruption, cancellation, resume, pending tool-call persistence, atomic claim before settlement, and stale run detection." --acceptance "Race and integration tests prove pending tool calls are claimed once, resumes do not double-execute tools, and disconnect cancellation is honored." --silent)
COMPACTION=$(bd create "Implement compaction boundaries and context epochs" -p 2 --parent "$EPIC" --description "Reserve: session/compaction/**, runtime/epochs.go, docs/architecture/compaction.md. Add context epoch tracking and compaction boundary records inspired by opencode, while keeping first milestone behavior small and explicit." --acceptance "Tests verify epoch changes are durable, compaction records are replayable, and compacted history does not leak omitted raw prompts by default." --silent)

bd dep add "$STORE_IFACE" "$ARCH_API"
bd dep add "$SQLITE_STORE" "$STORE_IFACE"
bd dep add "$SESSION_ADMIT" "$STORE_IFACE"
bd dep add "$SESSION_ADMIT" "$CONFIG_CORE"
bd dep add "$HISTORY_REPLAY" "$SESSION_ADMIT"
bd dep add "$RUN_ORCH" "$SESSION_ADMIT"
bd dep add "$RUN_ORCH" "$HISTORY_REPLAY"
bd dep add "$RUN_ORCH" "$CONTEXT_CORE"
bd dep add "$INTERRUPT_RESUME" "$RUN_ORCH"
bd dep add "$INTERRUPT_RESUME" "$SQLITE_STORE"
bd dep add "$COMPACTION" "$HISTORY_REPLAY"

TOOL_REGISTRY=$(bd create "Implement typed tool registry and scoped materialization" -p 0 --parent "$EPIC" --description "Reserve: tools/**, internal/tools/**, testdata/tools/**. Implement typed tool definitions, decoded input, encoded output, execution context, per-session materialization, model-facing assembly without shared model mutation, and stale-registration protection." --acceptance "Tests cover type validation, stale registrations, per-session scoped tools, concurrent sessions, and malformed tool input." --silent)
TOOL_PERMS=$(bd create "Implement permission and approval hooks" -p 0 --parent "$EPIC" --description "Reserve: permissions/**, tools/permissions.go, runtime/tool_permissions.go. Implement permission policy hooks, approval request plumbing, expected model-visible denial, interruption, and operational failure distinctions." --acceptance "Tests cover allow, deny, approval required, user interruption, and operational policy errors with correct model-visible output behavior." --silent)
EINO_TOOLS_ADAPT=$(bd create "Integrate eino-tools leaf tools with workspace serialization" -p 0 --parent "$EPIC" --description "Reserve: tools/einotools/**, internal/workspace/**, testdata/tools/**. Wrap eino-tools glob, file_read, apply_patch, search, shell, url_fetch, fileops, user_interact, and tracker adapters. Enforce per-canonical-workspace serialization for fileops, glob, search, and apply_patch while allowing independent roots to run concurrently." --acceptance "Concurrency tests prove same-root serialized execution and different-root parallel execution; wrappers preserve bounded outputs and eino-tools contracts." --silent)
TOOL_OUTPUTS=$(bd create "Implement tool settlement and bounded model-facing output" -p 0 --parent "$EPIC" --description "Reserve: tools/output.go, session/tool_settlement.go, store/**. Persist tool execution state, encoded results, bounded model-facing output, operational errors, and durable settlement events." --acceptance "Tests cover output truncation, settlement idempotency, expected failures, operational failures, interruption, and no raw oversized payload leaks." --silent)
SESSION_TOOLS=$(bd create "Implement session-bound tools for plan, subagent, skills, and retained output" -p 2 --parent "$EPIC" --description "Reserve: tools/session/**, runtime/session_tools.go. Add first-version session-owned tools for todo/plan state, task/subagent orchestration interfaces, skill loading hooks, permissions, and managed output retention. Do not duplicate eino-tools leaf tools." --acceptance "Tests validate session scoping, bounded retained output, permission integration, and clear separation from reusable file/search/shell tools." --silent)
PLUGIN_LIFECYCLE=$(bd create "Implement config and plugin lifecycle hooks" -p 1 --parent "$EPIC" --description "Reserve: config/lifecycle.go, config/plugins/**, runtime/config_snapshot.go, docs/architecture/config-lifecycle.md, testdata/config/**. Add deliberately small opencode-inspired lifecycle hooks for loading config, freezing per-run config snapshots, registering optional provider/tool/context plugins, validating plugin errors, and documenting what reload/reconfiguration does and does not affect in the first milestone." --acceptance "Tests prove run config snapshots are immutable after admission, plugin registration order is deterministic, invalid plugin/config state fails before execution, and docs explain first-version reload limitations." --silent)

bd dep add "$TOOL_REGISTRY" "$ARCH_API"
bd dep add "$TOOL_REGISTRY" "$CONTEXT_CORE"
bd dep add "$TOOL_PERMS" "$TOOL_REGISTRY"
bd dep add "$EINO_TOOLS_ADAPT" "$TOOL_REGISTRY"
bd dep add "$EINO_TOOLS_ADAPT" "$TOOL_PERMS"
bd dep add "$TOOL_OUTPUTS" "$TOOL_REGISTRY"
bd dep add "$TOOL_OUTPUTS" "$TOOL_PERMS"
bd dep add "$TOOL_OUTPUTS" "$STORE_IFACE"
bd dep add "$SESSION_TOOLS" "$TOOL_OUTPUTS"
bd dep add "$PLUGIN_LIFECYCLE" "$CONFIG_CORE"
bd dep add "$PLUGIN_LIFECYCLE" "$TOOL_REGISTRY"
bd dep add "$SESSION_ADMIT" "$PLUGIN_LIFECYCLE"
bd dep add "$RUN_ORCH" "$TOOL_REGISTRY"
bd dep add "$RUN_ORCH" "$PROVIDER_BIND"

AGUI_RULES=$(bd create "Define AG-UI event durability and replay rules" -p 0 --parent "$EPIC" --description "Reserve: docs/architecture/agui-events.md, agui/**. Define durable versus live-only classification for lifecycle, text, reasoning, tool calls/results, state snapshots/deltas, messages snapshots, activity, steps, custom events, and errors. Use eino-agui for protocol conversion and typed emitters." --acceptance "Documentation and type definitions clearly state what is persisted, replayed, tailed live, or omitted, with encrypted reasoning excluded from snapshots." --silent)
AGUI_STREAM=$(bd create "Implement AG-UI streaming bridge via eino-agui" -p 0 --parent "$EPIC" --description "Reserve: agui/**, stream/**, testdata/agui/**. Use eino-agui convert, emitter, stream tap, and tool-binding packages to emit full AG-UI event surface from runtime runs without reimplementing protocol adaptation." --acceptance "Golden fixtures verify emitted AG-UI events for run lifecycle, text/reasoning, tool calls/results, state, messages, activity, steps, custom events, and errors." --silent)
AGUI_REPLAY_TAIL=$(bd create "Implement AG-UI replay and live tail APIs" -p 0 --parent "$EPIC" --description "Reserve: agui/replay.go, stream/tail.go, store/** tests. Provide reconnect APIs that replay durable events and tail live ephemeral deltas without pretending token deltas are durable." --acceptance "Integration tests cover disconnect, reconnect, durable replay, live-only delta omission, tail continuation, cancellation on disconnect, and bounded queues." --silent)
AGUI_CLIENT_TOOLS=$(bd create "Support client-defined AG-UI tools" -p 1 --parent "$EPIC" --description "Reserve: agui/client_tools.go, tools/agui/**, testdata/agui/**. Integrate eino-agui/tools client-defined tool binding with server-side tool registry and per-session model-facing tool assembly." --acceptance "Tests cover client tool definitions, server tool coexistence, permission hooks, stale definitions, and no shared model mutation across sessions." --silent)

bd dep add "$AGUI_RULES" "$ARCH_API"
bd dep add "$AGUI_STREAM" "$AGUI_RULES"
bd dep add "$AGUI_STREAM" "$RUN_ORCH"
bd dep add "$AGUI_REPLAY_TAIL" "$AGUI_STREAM"
bd dep add "$AGUI_REPLAY_TAIL" "$SQLITE_STORE"
bd dep add "$AGUI_CLIENT_TOOLS" "$AGUI_STREAM"
bd dep add "$AGUI_CLIENT_TOOLS" "$TOOL_REGISTRY"

OBS_POLICY=$(bd create "Define Datadog AI/LLM observability redaction policy" -p 0 --parent "$EPIC" --description "Reserve: docs/architecture/observability.md, obs/**. Define e2e observability contract using eino-obs safe defaults: no raw prompts, outputs, tool payloads, attachments, reasoning, or encrypted reasoning by default; opt-in bounded summaries only." --acceptance "Policy doc lists fields, tags, redaction defaults, opt-in summary behavior, and trace correlation IDs across sessions, runs, tools, providers, and AG-UI events." --silent)
OBS_RUNTIME=$(bd create "Implement eino-obs runtime instrumentation" -p 0 --parent "$EPIC" --description "Reserve: obs/**, telemetry/**, runtime/observability.go, testdata/obs/**. Instrument session admission, run lifecycle, provider calls, model streams, token usage, retries, compaction, interrupts, resumes, cancellations, and errors through eino-obs." --acceptance "Fake/no-network exporter tests verify span/event shape, trace correlation, redaction defaults, error tagging, and cancellation paths." --silent)
OBS_TOOLS=$(bd create "Instrument tool registration, materialization, execution, and settlement" -p 1 --parent "$EPIC" --description "Reserve: obs/tools.go, tools/** observability hooks, testdata/obs/**. Add observability around tool registry changes, per-session materialization, permission checks, execution, output bounding, settlement, and failures." --acceptance "Fake exporter tests verify tool spans/events redact payloads, distinguish expected failure/interruption/operational error, and correlate with session/run/tool call IDs." --silent)
DATADOG_EXPORT=$(bd create "Add Datadog exporter wiring and configuration example" -p 1 --parent "$EPIC" --description "Reserve: examples/datadog/**, docs/integrations/datadog.md. Wire eino-obs Datadog exporter configuration into runtime options and provide a no-secret example with environment-based setup." --acceptance "Example compiles, docs explain setup without exposing tokens, and tests use fake/no-network exporters by default." --silent)

bd dep add "$OBS_POLICY" "$ARCH_API"
bd dep add "$OBS_RUNTIME" "$OBS_POLICY"
bd dep add "$OBS_RUNTIME" "$RUN_ORCH"
bd dep add "$OBS_RUNTIME" "$COMPACTION"
bd dep add "$OBS_TOOLS" "$OBS_RUNTIME"
bd dep add "$OBS_TOOLS" "$TOOL_OUTPUTS"
bd dep add "$DATADOG_EXPORT" "$OBS_RUNTIME"

HTTP_EMBED=$(bd create "Define embeddable HTTP and SSE adapter contracts" -p 1 --parent "$EPIC" --description "Reserve: transport/**, agui/http/**, examples/minimal-server/** tests, docs/architecture/embedding.md. Define small embeddable contracts for consuming servers to wire their own HTTP routes, auth, session admission, AG-UI SSE stream, replay cursors, live tail, disconnect cancellation, and interrupt/resume endpoints without eino-agent owning product route sets." --acceptance "Adapter contract tests or example tests cover auth/context injection boundaries, SSE replay cursor handling, live tail cancellation on disconnect, interrupt/resume calls, and docs distinguish library primitives from application-owned routes." --silent)
EX_MINIMAL=$(bd create "Build minimal embedded AG-UI server example" -p 1 --parent "$EPIC" --description "Reserve: examples/minimal-server/**, docs/examples/minimal-server.md. Implement a small embedded server using the HTTP/SSE adapter contracts that admits one session, runs the runtime, streams AG-UI events, handles disconnect cancellation, exposes replay, and uses local durable storage." --acceptance "Example builds and runs with documented command; integration test or smoke test validates an AG-UI event stream and replay after reconnect." --silent)
EX_AGUI_GO=$(bd create "Build ag-ui-go-server-example integration sketch" -p 1 --parent "$EPIC" --description "Reserve: examples/ag-ui-go-server-example/**, docs/integrations/ag-ui-go-server-example.md. Show how ag-ui-go-server-example can embed eino-agent runtime APIs, AG-UI stream/replay, client tools, and local storage without copying internals." --acceptance "Sketch compiles where practical or is clearly marked as illustrative; docs identify exact adapter points and stable public APIs." --silent)
EX_ENSEMBLE=$(bd create "Write ensemble future adapter and migration sketch" -p 1 --parent "$EPIC" --description "Reserve: docs/integrations/ensemble.md, examples/ensemble-adapter/**. Map ensemble dispatcher.RunEvent semantics to AG-UI replay/tail requirements and Datadog observability. Do not claim completed AG-UI parity unless ensemble has gained transport support." --acceptance "Guide describes direct AG-UI versus adapter-service options, required event mapping, storage/replay implications, and explicit non-parity caveat." --silent)
ADOPT_REQS=$(bd create "File post-implementation adoption requests for consumers" -p 2 --parent "$EPIC" --description "Reserve: ~/.agents/projects/ag-ui-go-server-example/requests/**, ~/.agents/projects/ensemble/requests/**. After public APIs stabilize, file adoption requests for ag-ui-go-server-example and ensemble. Ensemble request must be adapter/migration design unless AG-UI transport exists by then." --acceptance "Request files exist with concrete API pins, integration steps, validation commands, and ensemble non-parity wording where applicable." --silent)

bd dep add "$HTTP_EMBED" "$AGUI_REPLAY_TAIL"
bd dep add "$HTTP_EMBED" "$INTERRUPT_RESUME"
bd dep add "$EX_MINIMAL" "$AGUI_REPLAY_TAIL"
bd dep add "$EX_MINIMAL" "$OBS_RUNTIME"
bd dep add "$EX_MINIMAL" "$HTTP_EMBED"
bd dep add "$EX_AGUI_GO" "$EX_MINIMAL"
bd dep add "$EX_AGUI_GO" "$AGUI_CLIENT_TOOLS"
bd dep add "$EX_AGUI_GO" "$HTTP_EMBED"
bd dep add "$EX_ENSEMBLE" "$EX_AGUI_GO"
bd dep add "$EX_ENSEMBLE" "$DATADOG_EXPORT"
bd dep add "$ADOPT_REQS" "$EX_AGUI_GO"
bd dep add "$ADOPT_REQS" "$EX_ENSEMBLE"

SECURITY_ROBUST=$(bd create "Audit security, privacy, cancellation, and robustness behavior" -p 0 --parent "$EPIC" --description "Reserve: internal/security/**, runtime/** tests, docs/architecture/security.md. Verify no panics on malformed model/tool/AG-UI input, no encrypted reasoning leaks, bounded tool output, no plaintext remote token export, safe retries, context cancellation, and bounded queues." --acceptance "Negative tests and audit doc cover malformed inputs, redaction, queue bounds, retry bounds, cancellation, token handling, and panic-free error returns." --silent)
GOLDEN_TESTS=$(bd create "Add golden AG-UI, history, and observability fixtures" -p 1 --parent "$EPIC" --description "Reserve: testdata/**, agui/** tests, session/** tests, obs/** tests. Create golden fixtures for AG-UI events, replayable history, tool lifecycle, and fake exporter observability output." --acceptance "Golden tests are deterministic, easy to update intentionally, and cover the core durable/live-only distinctions." --silent)
RACE_INTEGRATION=$(bd create "Add race and concurrent session integration tests" -p 0 --parent "$EPIC" --description "Reserve: runtime/** tests, tools/** tests, store/** tests. Test concurrent sessions, concurrent tools, same-root filesystem serialization, different-root parallelism, interrupts, resumes, disconnects, and store claims under go test -race." --acceptance "go test -race ./... passes; tests catch double execution and workspace serialization violations." --silent)
DOCS_PUBLIC=$(bd create "Write public API, storage, tool lifecycle, and migration docs" -p 1 --parent "$EPIC" --description "Reserve: README.md, docs/**. Document package architecture, public API examples, embedding/transport contracts, integration guides, observability setup, storage semantics, AG-UI event durability rules, tool lifecycle rules, dependency pins, and migration notes." --acceptance "Docs are sufficient for a consuming server to embed the runtime and understand what is durable, live-only, redacted, configurable, and out of scope." --silent)
FINAL_GATE=$(bd create "Run final quality gates and release-readiness review" -p 0 --parent "$EPIC" --description "Reserve: no broad source edits except focused fixes discovered by gates. Run gofmt/goimports, go mod tidy -diff, go test ./..., go test -race ./..., go vet ./..., make check or equivalent, and review public APIs against docs and examples." --acceptance "All quality gates pass; README and docs match implementation; no orphan tasks or missing required examples remain; bead graph can be inspected with bd children on the epic." --silent)

bd dep add "$SECURITY_ROBUST" "$INTERRUPT_RESUME"
bd dep add "$SECURITY_ROBUST" "$AGUI_REPLAY_TAIL"
bd dep add "$SECURITY_ROBUST" "$OBS_TOOLS"
bd dep add "$SECURITY_ROBUST" "$SESSION_TOOLS"
bd dep add "$GOLDEN_TESTS" "$AGUI_STREAM"
bd dep add "$GOLDEN_TESTS" "$AGUI_REPLAY_TAIL"
bd dep add "$GOLDEN_TESTS" "$OBS_RUNTIME"
bd dep add "$GOLDEN_TESTS" "$HISTORY_REPLAY"
bd dep add "$GOLDEN_TESTS" "$TOOL_OUTPUTS"
bd dep add "$RACE_INTEGRATION" "$INTERRUPT_RESUME"
bd dep add "$RACE_INTEGRATION" "$EINO_TOOLS_ADAPT"
bd dep add "$RACE_INTEGRATION" "$SQLITE_STORE"
bd dep add "$DOCS_PUBLIC" "$EX_MINIMAL"
bd dep add "$DOCS_PUBLIC" "$EX_AGUI_GO"
bd dep add "$DOCS_PUBLIC" "$EX_ENSEMBLE"
bd dep add "$DOCS_PUBLIC" "$SECURITY_ROBUST"
bd dep add "$DOCS_PUBLIC" "$HTTP_EMBED"
bd dep add "$ADOPT_REQS" "$DOCS_PUBLIC"
bd dep add "$FINAL_GATE" "$DOCS_PUBLIC"
bd dep add "$FINAL_GATE" "$GOLDEN_TESTS"
bd dep add "$FINAL_GATE" "$RACE_INTEGRATION"
bd dep add "$FINAL_GATE" "$DATADOG_EXPORT"
bd dep add "$FINAL_GATE" "$ADOPT_REQS"
bd dep add "$FINAL_GATE" "$COMPACTION"
bd dep add "$FINAL_GATE" "$SESSION_TOOLS"
bd dep add "$FINAL_GATE" "$PROVIDER_BIND"
bd dep add "$FINAL_GATE" "$PLUGIN_LIFECYCLE"
bd dep add "$FINAL_GATE" "$HTTP_EMBED"

echo "$EPIC" > .beads/eino-agent-epic-id

echo ""
echo "Bead graph created."
echo "Epic ID: $EPIC"
echo ""
echo "Suggested checks:"
echo "  bd show $EPIC"
echo "  bd children $EPIC"
echo "  bd dep tree $EPIC"
echo "  bd ready"
echo ""
echo "Epic ID was also written to .beads/eino-agent-epic-id"
