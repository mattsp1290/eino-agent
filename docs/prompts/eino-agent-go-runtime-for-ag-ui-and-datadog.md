# Project Planning with Beads

## Agent Instructions

You are an expert software architect creating a comprehensive task breakdown. This task graph will be executed by AI agents working in parallel, coordinated through MCP Agent Mail with file reservations to prevent conflicts.

<quality_expectations>
Create a thorough, production-ready task graph. Include all necessary setup, implementation, testing, and documentation tasks. Go beyond the basics - consider edge cases, error handling, security considerations, and integration points. Each task should be specific enough for an agent to execute independently without ambiguity.
</quality_expectations>

## Project Information

### Links to Relevant Documentation

N/A for formal user-supplied docs. Reference material to inspect during planning:

- Reference repos, clone as needed because future agents may not have the `/tmp` clones:
  - `https://github.com/earendil-works/pi`
  - `https://github.com/anomalyco/opencode`
  - `https://github.com/mattsp1290/eino-obs`
- Local prerequisite/reference repos when present:
  - `~/git/eino-agui`
  - `~/git/eino-obs`
  - `~/git/eino-tools`
  - `~/git/ag-ui-go-server-example`
  - `~/git/ensemble`; if unavailable in a future environment, use `~/.agents/projects/ensemble/requests` for backend context. Treat `~/git/ensemble-ui/docs/architecture.md` as UI-side or aspirational unless verified against `~/git/ensemble`; the authoritative current backend status is the negative AG-UI finding in `eino-agui/docs/decisions/0002-ensemble-shared-surface.md` and the adapter-design request in `~/.agents/projects/ensemble/requests/2026-06-27-eino-agui-adapter-design.md`.
- Prerequisite request files created for this project:
  - `~/.agents/projects/eino-agui/requests/2026-06-26-ag-ui-adapter-surface-for-eino-agent.md`
  - `~/.agents/projects/eino-obs/requests/2026-06-26-datadog-llm-observability-api-for-eino-agent.md`
  - `~/.agents/projects/eino-tools/requests/2026-06-26-coding-agent-tool-parity-for-eino-agent.md`
  - `~/.agents/projects/ensemble/requests/2026-06-26-observability-contract-for-eino-agent.md`
- Verified local prerequisite state as of 2026-06-27:
  - `eino-agui`: `github.com/mattsp1290/eino-agui` at tag `v0.1.1`, commit `453c69c9bdb1006c585407510342ea5a03d1b48d`; packages `convert`, `emitter`, `stream`, and `tools`; Go `1.26.3`, `github.com/cloudwego/eino v0.8.13`, AG-UI Go SDK pseudo-version `v0.0.0-20260624151131-d2049debabd9`.
  - `eino-tools`: `github.com/mattsp1290/eino-tools` at commit `e6ee664be93bb830b4bf6215907865b6366662b5`; response artifact `~/.agents/projects/eino-tools/responses/2026-06-26-coding-agent-tool-parity-for-eino-agent.md` marks the parity request complete and records this pin; use `make test`, `make vet`, and `make lint`, and also verify `go mod tidy -diff` and `go test -race ./...` per the response artifact.
  - `eino-obs`: `github.com/mattsp1290/eino-obs` at commit `a9a6f8bb478b479c1e48ab353261a60c4a19195a`; root observer API plus `exporter/datadog`, `exporter/fake`, and no-network recorder are present; Go `1.24`; validation gates are `make fmt-check`, `make vet`, `make test`, `make race`, and `make check`.
  - `eino-agui` and `eino-obs` do not currently have external response artifacts under `~/.agents/projects/<repo>/responses/` in the inspected workspace. Treat their current tag/commit and repo docs as the working implementation evidence, but include an early task to create or verify missing response artifacts so future agents have a durable pin source.
  - `ensemble`: `~/git/ensemble` exists and is a Go backend (`github.com/mattsp1290/ensemble`) but is not currently an AG-UI SSE backend. `eino-agui/docs/decisions/0002-ensemble-shared-surface.md` records that `eino-agui` is reference-app-proven against `ag-ui-go-server-example`, not yet proven through an ensemble AG-UI integration.

### Project Description

Build `eino-agent` as a Go-based reusable agent runtime/library inspired by `pi` and `opencode`, intended to be embedded by projects such as `~/git/ensemble` and `~/git/ag-ui-go-server-example`.

The project should provide the agent-runtime layer above CloudWeGo Eino: session admission and execution, model/provider selection hooks, durable session history, tool registration and execution, permission/approval hooks, streaming turn orchestration, AG-UI event output, and Datadog AI/LLM observability. It should expose stable package APIs so consuming servers can wire their own HTTP routes, auth, storage, and product-specific behavior without copying runtime internals.

The architecture should borrow the useful concepts from the reference systems without cloning their TypeScript implementation details:

- From `pi`: a lightweight agent harness shape, multi-provider model abstraction, tool/state runtime concepts, and a clear separation between runtime, provider, and UI concerns.
- From `opencode`: durable session admission separate from execution, replayable session history, live-only streaming deltas, typed tool definitions, scoped tool registration, stale-registration protection, provider/model catalog concepts, config/plugin lifecycle ideas, context epochs, interruption, and compaction boundaries.
- From `ag-ui-go-server-example`: the concrete AG-UI server expectations, event semantics, resume/interrupt lessons, client-defined tool support, and full AG-UI event surface.
- From `eino-agui`: consume the shipped AG-UI/Eino conversion, typed emitter, stream tap, and AG-UI tool-binding packages. `eino-agent` should not reimplement these surfaces.
- From `eino-tools`: consume reusable coding-agent leaf tools (`glob`, line-windowed `file_read`, `apply_patch`, rich `search`, `shell`, `url_fetch`, `fileops`, `user_interact`, and `tracker_write`/tracker adapter surfaces) while honoring the documented per-workspace filesystem serialization contract.
- From `eino-obs`: consume the shipped observer API, no-network/fake exporters, Datadog exporter, redaction defaults, and lifecycle/model/stream/tool instrumentation helpers. Observability must be part of the runtime contract, not ad hoc logging.

This is not a full CLI/TUI clone of `pi` or `opencode` in the first milestone. The first deliverable is a production-grade Go library/runtime with examples and integration guidance. Application-specific frontends, route sets, and business workflows remain in the consuming repos.

### Technical Stack

- Language/module: Go, module `github.com/mattsp1290/eino-agent`.
- Go baseline: choose a baseline compatible with all dependencies. `eino-agui` and ensemble use Go `1.26.3`; `eino-tools` requires Go `1.26` or newer; `eino-obs` supports Go `1.24` or newer. Default this project to Go `1.26.3` unless implementation discovery finds a stronger reason not to.
- Core agent engine: CloudWeGo Eino `v0.8.13`, matching `eino-agui`, ensemble, and the current AG-UI extraction baseline unless a later prerequisite response says otherwise.
- AG-UI bridge: `github.com/mattsp1290/eino-agui@v0.1.1`.
- Observability: `github.com/mattsp1290/eino-obs` pinned to commit `a9a6f8bb478b479c1e48ab353261a60c4a19195a`.
- Coding-agent leaf tools: `github.com/mattsp1290/eino-tools` pinned to commit `e6ee664be93bb830b4bf6215907865b6366662b5`.
- Initial implementation must use these exact pins. If a newer tag or response artifact exists, create a separate dependency-upgrade bead with compatibility verification; do not change baseline pins inside unrelated implementation beads.
- Model/provider/tool ecosystem: compatible with `github.com/mattsp1290/eino-providers`, `github.com/mattsp1290/eino-tools`, and Codex/OpenAI-style provider transports where useful, but keep them behind interfaces or optional packages when possible.
- Session persistence: define a storage interface and ship an initial durable local implementation suitable for embedded servers, with clear transaction boundaries for session admission, history projection, tool settlement, and replay. Pick the concrete backend during architecture, favoring a simple Go-native SQLite implementation if no existing project standard exists.
- Streaming/event surface: use `eino-agui` for AG-UI/Eino conversion, typed event emission, stream tapping, and client-tool binding. `eino-agent` owns runtime-level event persistence, replay/tail APIs, disconnect cancellation, interrupt/resume semantics, and durable/live-only event classification across run lifecycle, text, reasoning, tool calls, tool results, state snapshot/delta, messages snapshot, activity, steps, custom events, and errors. Persist replayable snapshots/events in `eino-agent`; treat token deltas and transport-level streaming chunks as live-only unless deliberately persisted.
- Observability surface: Datadog AI/LLM observability spans and events through `eino-obs`, with trace correlation across sessions, provider turns, model streams, tool calls, retries, compaction, interrupts, and errors. Use `eino-obs` safe defaults: no raw prompts, outputs, tool payloads, attachments, reasoning, or encrypted reasoning by default; summaries must be opt-in and bounded.
- Tooling: `go test ./...`, `go vet ./...`, `gofmt`, `goimports`, pinned `golangci-lint`, race tests for concurrent session/tool execution, golden event fixtures, and GitHub Actions CI. Prefer a `Makefile` with durable local gates modeled on `eino-tools`/`eino-agui`.

### Specific Requirements

- Treat cross-repo prerequisite requests as blocking setup. Before implementation begins, inspect the four request files listed above and verify their responses or resulting tags/commits. If a prerequisite repo is not checked out locally, clone it or use the request inbox/response files. Do not assume future agents have access to this session's `/tmp/pi`, `/tmp/opencode`, or any uncommitted local clone state.
- Record a local dependency-status note under `docs/` before implementation starts. It must include exact pins for `eino-agui`, `eino-tools`, and `eino-obs`; the `eino-tools` response artifact; whether missing `eino-agui`/`eino-obs` response artifacts were created or intentionally replaced by tags/commits; and the ensemble non-AG-UI finding from `eino-agui/docs/decisions/0002-ensemble-shared-surface.md`.
- Do not duplicate `eino-agui` or `eino-obs` responsibilities in `eino-agent`. `eino-agent` owns orchestration; `eino-agui` owns AG-UI/Eino protocol adaptation; `eino-obs` owns Datadog AI/LLM observability emission.
- Do not duplicate reusable coding leaf tools that belong in `eino-tools`. `eino-agent` should own session-bound tools such as task/subagent orchestration, todo/plan state, skill loading, permissions, managed output retention, and per-session tool materialization; `eino-tools` owns reusable file discovery, file reading/editing, patch, search, shell, URL fetch, user interaction, and tracker leaf tool surfaces.
- Enforce the `eino-tools` filesystem serialization contract in `eino-agent`: calls into `fileops`, `glob`, `search`, and `apply_patch` must be serialized per canonical workspace root. Independent workspace roots may run concurrently.
- Full AG-UI support is non-negotiable. The runtime must be able to power `ag-ui-go-server-example` now, including replay of durable events and live streaming of ephemeral deltas, and it must expose enough stable runtime primitives for a later ensemble AG-UI adapter.
- Do not claim ensemble AG-UI parity until an ensemble adapter or AG-UI transport exists. First-version ensemble work should be an integration/adoption design that maps ensemble `dispatcher.RunEvent` semantics to AG-UI and decides whether ensemble should expose AG-UI directly or through an adapter service.
- Datadog AI/LLM observability is non-negotiable. Instrument session admission, run lifecycle, provider calls, streaming chunks, token usage, tool registration/materialization/execution/settlement, retries, compaction, interrupts, resumes, cancellations, and error paths through `eino-obs`. Redact or omit sensitive prompts/tool payloads/reasoning by default, with explicit configuration for captured summaries.
- Session design must separate durable admission/history from live-only stream deltas. Reconnect/replay APIs must not pretend ephemeral token deltas are durable unless they are explicitly persisted.
- Tool design must provide typed definitions, decoded input, encoded output, execution context, permissions/approval hooks, bounded model-facing output, stale-registration protection, and clear distinction between expected model-visible tool failure, interruption, and operational failure.
- Runtime contexts must include session ID, agent ID, assistant message ID, tool call ID, model/provider identity, trace/span context, and cancellation.
- Support client-defined AG-UI tools via `eino-agui/tools` as well as server-side tools. The model-facing tool set must be assembled per request/session without mutating shared model state.
- Support interrupts and resumes without double execution. Pending tool calls must be durably recorded and claimed atomically before settlement.
- Provide agent/model/catalog config hooks inspired by opencode, but keep the first version deliberately small: default agent, named agents, model/provider selection, tool permissions, instructions/context sources, and observability config.
- Provide context-source boundaries for system prompt, project instructions, explicit prompt attachments, and future configured references. Do not bake local-only paths into runtime logic.
- Include examples:
  - Minimal embedded server that runs one session and streams AG-UI.
  - Integration sketch for `ag-ui-go-server-example`.
  - Integration sketch for ensemble backend as a future adapter/migration, including Datadog observability and the mapping required for AG-UI stream replay/tail. Do not present this as a completed direct import swap.
- File post-implementation adoption requests in `~/.agents/projects/ag-ui-go-server-example/requests` and `~/.agents/projects/ensemble/requests` after the public API stabilizes. The ensemble request should be an adapter/migration design unless ensemble has gained AG-UI transport by then.
- Security and robustness: no panics on malformed model/tool/AG-UI input, no encrypted reasoning leaks in snapshots, no unbounded tool output, no accidental plaintext remote token export, context cancellation on disconnect, bounded queues, safe retries, and race-safe concurrent sessions.
- Documentation must include package architecture, public API examples, integration guides, observability setup, storage semantics, AG-UI event durability rules, tool lifecycle rules, and migration notes for consumers.

---

## Your Task

Analyze this project and create a comprehensive **Beads task graph** using the `bd` CLI. Beads provides dependency-aware, conflict-free task management for multi-agent execution.

---

<critical_constraint>
Your ONLY output is a bash shell script. The script may use shell builtins plus `bd init`, `bd create`, `bd update` only for marking the epic `in_progress`, and `bd dep add`. Do NOT use `bd add` - the correct command to create a bead is `bd create`. Use `bd dep add` for dependencies between task beads. Do not implement project code.

The script MUST create a single parent **epic** first (`bd create -t epic`) and parent **every** task bead to it via `--parent "$EPIC"`, so the whole project is one trackable rollup. The epic is an organizational rollup only - never make it a blocking dependency (do NOT `bd dep add` to or from the epic; `bd dep add` is for real ordering edges between task beads, and a blocking edge on an epic both excludes it wrongly and inverts `bd dep tree`). Membership is the `--parent` relationship, nothing else.
</critical_constraint>

## Output Format

Generate a shell script that creates the full task graph. The script should:

1. **Initialize Beads** (if not already initialized)
2. **Create one parent epic** (`bd create -t epic`) representing the whole project, capturing its ID into `$EPIC`
3. **Create all task beads** with appropriate priorities, each parented to the epic via `--parent "$EPIC"`
4. **Establish dependencies** between task beads (ordering edges only - never to or from the epic)
5. **Emit the epic ID** at the end of the script, either by printing it in final `echo` lines or writing it to `.beads/eino-agent-epic-id`, so a human can run follow-up inspection commands after the script exits.

### Example Output

```bash
#!/bin/bash
# Project: eino-agent
# Generated: 2026-06-26

set -e

# Initialize beads if needed
if [ ! -d ".beads" ]; then
    bd init
fi

echo "Creating project beads..."

# ========================================
# Parent epic - every task below is parented to it (--parent "$EPIC").
# The epic is an organizational rollup: it is NEVER given a blocking dep
# (no `bd dep add` to or from it) and is never dispatched as work itself.
# ========================================

EPIC=$(bd create "Epic: eino-agent" -t epic -p 0 --silent)
bd update "$EPIC" --status in_progress   # rollup, not dispatchable work - keep it out of `bd ready`

# ========================================
# Phase 1: Project Setup & Prerequisite Gates
# ========================================

REQS=$(bd create "Verify prerequisite request responses for eino-agui, eino-obs, eino-tools, and ensemble" -p 0 --parent "$EPIC" --silent)

SETUP_MOD=$(bd create "Initialize Go module and project quality gates" -p 0 --parent "$EPIC" --silent)
bd dep add $SETUP_MOD $REQS

# ... continue for architecture, storage, sessions, tools, AG-UI, observability,
#     tests, examples, docs, and consumer adoption requests ...

echo ""
echo "Bead graph created! View with:"
echo "  bd show $EPIC          # The parent epic and its rollup"
echo "  bd children $EPIC      # All task beads under the epic"
echo "  bd ready              # List unblocked tasks (the epic itself is not work)"
```

---

## Bead Creation Guidelines

### Epic / Hierarchy (REQUIRED)

- Create exactly **one parent epic** for the whole project: `EPIC=$(bd create "Epic: <project summary>" -t epic -p 0 --silent)`.
- Parent **every** task bead to it: add `--parent "$EPIC"` to every `bd create`.
- The epic is a **rollup, not work**: never `bd dep add` to or from it. Membership is `--parent`; `bd dep add` is reserved for real ordering edges *between task beads*. A blocking edge on an epic wrongly keeps it out of (or drops it into) `bd ready` and inverts `bd dep tree`.
- **Keep the epic out of `bd ready`** by marking it active right after creation: `bd update "$EPIC" --status in_progress`. `bd ready` excludes `in_progress`/`blocked`/`deferred`/`hooked`. Do **not** rely on `--exclude-type epic` - that flag is ineffective on some `bd`/`bn` builds, whereas status-based exclusion works everywhere.
- For very large projects you MAY use phase sub-epics (each `--parent "$EPIC"`, each with its own children), but a single top-level epic is the default and is sufficient for most projects.

### Priority Levels

- `-p 0` = Critical (blocking other work)
- `-p 1` = High (important but not blocking)
- `-p 2` = Medium (standard work)
- `-p 3` = Low (nice to have)

### Dependency Rules

1. Never create cycles
2. Every bead should have a clear dependency chain back to setup tasks
3. Use `bd dep add CHILD PARENT` (child depends on parent completing first)
4. Parallel work should share a common ancestor, not depend on each other
5. `bd dep add` is for ordering edges **between task beads only** - never use it to attach a task to the epic (that is `--parent`), and never add a blocking edge to or from the epic

### Task Granularity

- Each bead should be completable in **under 750 lines of code**
- Tasks should be atomic enough for one agent to complete without coordination
- If a task requires multiple file areas, consider splitting by file area
- Each `bd create` for implementation work should include `--description` with file reservation patterns, scope boundaries, and relevant dependency pins, plus `--acceptance` with concrete test or documentation gates.

---

## File Reservation Planning

For each major work area, note the file patterns that will need exclusive reservation:

```bash
# Prerequisite checks: .agents/**, docs/prompts/**, README.md
# Architecture/docs: docs/**, README.md, examples/**
# Session core: session/**, runtime/**, store/**, internal/store/**, testdata/session/**
# Tool runtime: tools/**, permissions/**, internal/tools/**, testdata/tools/**
# Model/provider/catalog: model/**, providers/**, catalog/**, config/**
# AG-UI integration: agui/**, stream/**, examples/agui/**, testdata/agui/**
# Observability: obs/**, telemetry/**, examples/datadog/**, testdata/obs/**
# Storage implementations: store/sqlite/**, migrations/**, internal/sqlite/**
# Consumer adoption requests: ~/.agents/projects/ag-ui-go-server-example/requests/**, ~/.agents/projects/ensemble/requests/**
```

This helps agents claim appropriate file surfaces when they start work.

---

## Context Documentation

Place durable implementation context under `docs/`, for example `docs/architecture/`, `docs/integrations/`, or `docs/dependency-status.md`. Keep generated planning prompts under `docs/prompts/`. Important context includes:

- Architecture decisions
- API documentation
- Design system specs
- External service integration guides

For this project, include at minimum:

- Summary of the `pi` and `opencode` concepts intentionally adopted or rejected.
- Pins, response artifacts, and scope decisions from `eino-agui`, `eino-obs`, `eino-tools`, and ensemble prerequisite work.
- AG-UI event durability and replay rules.
- Datadog AI/LLM observability field/redaction policy.
- Consumer integration notes for `ag-ui-go-server-example` and ensemble.

---

## Script Verification Guidance

The generated script should print the created epic ID and suggested verification commands that a human or later agent can run after the script exits. The prompt generator must not run the script. Suggested post-run checks are:

1. Run the script manually: `chmod +x setup-beads.sh && ./setup-beads.sh`
2. Check the rollup with the printed epic ID: `bd children <EPIC_ID>` should list every task bead, and `bd dep tree <EPIC_ID>` should show them under the epic with no orphan task beads.
3. Check ready work: `bd ready` should show initial setup tasks and **not** the epic. Epics are rollups, never dispatched as work - mark the epic `in_progress` right after creating it so status-based exclusion keeps it out of `bd ready` on every build.

---

## Completeness Checklist

Ensure your task graph includes:

- [ ] A single parent epic (`-t epic`); every task bead parented to it via `--parent "$EPIC"`, with no orphan tasks and no blocking dep to/from the epic
- [ ] Prerequisite request verification before implementation starts
- [ ] Dependency-status note with exact pins and response/tag evidence for `eino-agui`, `eino-obs`, `eino-tools`, and ensemble scope
- [ ] Reference-repo research tasks for `pi`, `opencode`, `eino-agui`, `eino-obs`, `eino-tools`, `ag-ui-go-server-example`, and ensemble context
- [ ] Go module setup, package layout, lint/test/CI gates
- [ ] Runtime architecture and public API design
- [ ] Durable session admission, execution, history, replay, and interrupt/resume
- [ ] Tool registry, typed definitions, permission hooks, stale-registration protection, and output bounding
- [ ] Model/provider/catalog/config hooks
- [ ] Full AG-UI streaming and replay integration via `eino-agui`
- [ ] Datadog AI/LLM observability via `eino-obs`
- [ ] Storage implementation and migration/testing strategy
- [ ] Unit, integration, race, and golden-fixture tests
- [ ] Examples for embedded usage, AG-UI streaming, and Datadog observability
- [ ] Docs and migration guides
- [ ] Consumer adoption requests after API stabilization
- [ ] Security, privacy, redaction, cancellation, and robustness checks
