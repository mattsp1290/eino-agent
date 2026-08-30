# eino-agent

Reusable Go runtime for Eino-based agents with AG-UI streaming and Datadog
AI/LLM observability.

This repository provides embeddable Go packages for durable Eino agent
orchestration. A consuming server supplies auth, routes, provider credentials,
tool definitions, config loading, and deployment policy; `eino-agent` supplies
runtime admission, durable session contracts, AG-UI replay/live-tail adapters,
tool settlement, and observability/redaction policy.

## Module Baseline

- Module: `github.com/mattsp1290/eino-agent`
- Go: `1.26.3`
- Root release candidate: `v0.1.3` (not a supported pin until independent
  post-tag verification passes)
- Generated bindings: `github.com/mattsp1290/eino-agent/wasmext/gen v0.1.0`
  via submodule tag `wasmext/gen/v0.1.0`
- CloudWeGo Eino: `github.com/cloudwego/eino v0.8.13`
- AG-UI bridge: `github.com/mattsp1290/eino-agui v0.1.1`
- Observability: `github.com/mattsp1290/eino-obs v0.0.0-20260627060807-a9a6f8bb478b`
- Coding tools: `github.com/mattsp1290/eino-tools v0.1.1-0.20260825160656-63a3c99272c2`

See `docs/dependency-status.md` for prerequisite evidence,
`docs/consumer-guide.md` for the public embedding contract,
`docs/examples/minimal-server.md` for a runnable server example, and
`docs/architecture/runtime.md` for the package architecture.

Standard coding tools mount atomically through
`tools/einotools.MountStandard` into `composition.Registry`; the runtime uses
that registry as its `runtime.RunPlanProvider`.

## Quick Embed

The smallest complete embedding is in `examples/minimal-server`:

```bash
go run ./examples/minimal-server -addr :8080
```

It wires:

- `store/sqlite` as the durable transactional `session.Store`;
- `runtime.StreamingOrchestrator` for run admission and interruption;
- `stream.Tail` for live AG-UI deltas;
- `transport.SSEHandler` for replay plus live tail;
- a scripted Eino model resolver so the example runs without provider secrets.

Core route shape:

```text
GET  /sessions/minimal/events
POST /sessions/minimal/runs
POST /runs/{run_id}/interrupt
```

Use it as a reference for composition, not as an auth or tenancy policy.

## Local Gates

Run the full skeleton gate:

```bash
make check
```

Individual targets:

```bash
make fmt-check
make vet
make test
make race
make mod-tidy-check
make lint
```

`make lint` uses pinned `golangci-lint` v2.12.2 through `go run`.
`make fmt` applies `gofmt` and pinned `goimports`.

## Current Layout

- `doc.go`: root package documentation.
- `session`: durable session, run, message, part, tool-call, context-epoch, and
  replay contracts.
- `runtime`: orchestration contracts for run admission, turn snapshots,
  interruption, tools, hooks, and internal events.
- `transport`: embeddable HTTP adapters for AG-UI SSE replay/live-tail,
  interrupt, resume, and message decoding.
- `stream`: bounded live-tail implementation for active runtime events.
- `model`: provider/model catalog and Eino model resolution contracts.
- `config`: immutable runtime config snapshot and validation contracts.
- `tools`: typed tool definitions and per-run materialization helpers.
- `permissions`: tool permission policy primitives.
- `agui`: AG-UI durability and replay policy definitions; protocol mechanics
  stay in `eino-agui`.
- `obs`: Datadog/eino-obs observability redaction and correlation policy
  definitions.
- `store/sqlite`: embedded SQLite store implementation.
- `store/storetest`: reusable durable store contract tests for backend
  implementations.
- `examples/`: buildable embedding and integration sketches.
- `docs/`: durable planning and architecture context.
- `.github/workflows/ci.yml`: CI gate matching the local Makefile targets.

## Durability Model

Replayable conversation state is stored as sessions, runs, messages, ordered
parts, tool calls, context epochs, and selected event records. Live AG-UI SSE
frames and model deltas are transport output, not the replay source of truth.
Reconnects reconstruct snapshots from durable messages/parts and then attach to
the active live tail.

See `docs/architecture/agui-events.md` and `docs/architecture/storage.md` for
the detailed rules.

## Integration Guides

- `docs/consumer-guide.md`: public API, storage, tool lifecycle, observability,
  configuration, and migration guidance.
- `docs/integrations/datadog.md`: Datadog/eino-obs exporter wiring and safe
  default redaction.
- `docs/integrations/ag-ui-go-server-example.md`: AG-UI server migration sketch.
- `docs/integrations/ensemble.md`: future ensemble adapter options and
  non-parity caveat.
