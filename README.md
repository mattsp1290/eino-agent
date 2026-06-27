# eino-agent

Reusable Go runtime for Eino-based agents with AG-UI streaming and Datadog
AI/LLM observability.

This repository is in the early runtime design phase. The current tree
establishes the Go module, dependency baseline, quality gates, CI skeleton, and
initial public package contracts for durable agent orchestration.

## Module Baseline

- Module: `github.com/mattsp1290/eino-agent`
- Go: `1.26.3`
- CloudWeGo Eino: `github.com/cloudwego/eino v0.8.13`
- AG-UI bridge: `github.com/mattsp1290/eino-agui v0.1.1`
- Observability: `github.com/mattsp1290/eino-obs v0.0.0-20260627060807-a9a6f8bb478b`
- Coding tools: `github.com/mattsp1290/eino-tools v0.0.0-20260627192031-e6ee664be93b`

See `docs/dependency-status.md` for prerequisite evidence,
`docs/architecture/reference-integration-research.md` for integration
boundaries, and `docs/architecture/runtime.md` for the runtime package
architecture.

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
- `model`: provider/model catalog and Eino model resolution contracts.
- `config`: immutable runtime config snapshot and validation contracts.
- `agui`: AG-UI durability and replay policy definitions; protocol mechanics
  stay in `eino-agui`.
- `store/storetest`: reusable durable store contract tests for backend
  implementations.
- `internal/deps`: temporary dependency anchor that keeps the initial pins
  tidy-stable until runtime packages import the concrete dependencies directly.
- `docs/`: durable planning and architecture context.
- `.github/workflows/ci.yml`: CI gate matching the local Makefile targets.
