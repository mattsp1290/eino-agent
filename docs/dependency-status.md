# Dependency Status

Date: 2026-06-27

This note records the prerequisite state for `eino-agent` before runtime
implementation starts. The exact pins below are the initial baseline. Do not
upgrade them inside unrelated implementation work; file a dependency-upgrade
bead with compatibility gates instead.

## Summary

No runtime implementation blocker remains from the three required library
dependencies. The local checkouts match the requested pins and their relevant
validation gates pass.

Two durable response artifacts are missing in this workspace:

- `~/.agents/projects/eino-agui/responses/**`
- `~/.agents/projects/eino-obs/responses/**`

For now, `eino-agent` should treat the checked repo state, tags or commits, and
the docs named below as the implementation evidence. A follow-up response note
should still be created in those project inboxes so future agents do not need
to rediscover this state from local clones.

## Pins

| Dependency | Pin | Evidence |
| --- | --- | --- |
| `github.com/mattsp1290/eino-agui` | tag `v0.1.1`, commit `453c69c9bdb1006c585407510342ea5a03d1b48d` | `~/git/eino-agui`, `go.mod`, `README.md`, `docs/decisions/0002-ensemble-shared-surface.md` |
| `github.com/mattsp1290/eino-tools` | commit `e6ee664be93bb830b4bf6215907865b6366662b5` | `~/git/eino-tools`, response artifact `~/.agents/projects/eino-tools/responses/2026-06-26-coding-agent-tool-parity-for-eino-agent.md` |
| `github.com/mattsp1290/eino-obs` | commit `a9a6f8bb478b479c1e48ab353261a60c4a19195a` | `~/git/eino-obs`, `README.md`, root/exporter/fake/exporter/datadog packages |
| `github.com/mattsp1290/ensemble` | commit `a709ad8ed2e9d8962b73b228859433cc6554ee2c` for current discovery only | `~/git/ensemble`, `eino-agui/docs/decisions/0002-ensemble-shared-surface.md` |

Version floors and shared dependency pins:

- `eino-agent` should default to Go `1.26.3`.
- `eino-agui` targets Go `1.26.3`, CloudWeGo Eino `v0.8.13`, and AG-UI Go SDK `v0.0.0-20260624151131-d2049debabd9`.
- `eino-tools` targets Go `1.26` and CloudWeGo Eino `v0.8.13`.
- `eino-obs` supports Go `1.24` or newer.
- `ensemble` currently uses Go `1.26.3` and CloudWeGo Eino `v0.8.13`.

## Request And Response Artifacts

Required request files inspected:

- `~/.agents/projects/eino-agui/requests/2026-06-26-ag-ui-adapter-surface-for-eino-agent.md`
- `~/.agents/projects/eino-obs/requests/2026-06-26-datadog-llm-observability-api-for-eino-agent.md`
- `~/.agents/projects/eino-tools/requests/2026-06-26-coding-agent-tool-parity-for-eino-agent.md`
- `~/.agents/projects/ensemble/requests/2026-06-26-observability-contract-for-eino-agent.md`

Response files found:

- `~/.agents/projects/eino-tools/responses/2026-06-26-coding-agent-tool-parity-for-eino-agent.md`

Response files not found:

- No `eino-agui` response file was present under `~/.agents/projects/eino-agui`.
- No `eino-obs` response file was present under `~/.agents/projects/eino-obs`.
- No ensemble observability response file was present under
  `~/.agents/projects/ensemble/responses` in this workspace.

The missing `eino-agui` and `eino-obs` responses are documentation gaps, not
current implementation blockers, because the pinned repos exist locally and
their validation gates pass. The missing ensemble observability response remains
a future integration planning gap; it does not block initial runtime API work as
long as `eino-agent` keeps observability behind the `eino-obs` contract and does
not claim ensemble parity.

## Validation Run

Validation performed locally on 2026-06-27:

- In `~/git/eino-agui`:
  - `go test ./...`: passed.
  - `go build ./... && make check && go test ./... -run Parity -count=1`: passed.
- In `~/git/eino-tools`:
  - `make test`: passed.
  - `make vet && make lint && go mod tidy -diff && go test -race ./...`: passed.
- In `~/git/eino-obs`:
  - `make check`: passed, including `make fmt-check`, `make vet`, `make test`, and `make race`.
- In `~/git/ensemble`:
  - AG-UI SDK import search returned no matches for `github.com/ag-ui-protocol`,
    `pkg/core/events`, `pkg/core/types`, `pkg/encoding/sse`, or `SSEWriter`.

## Dependency Responsibilities

`eino-agent` must not duplicate these upstream responsibilities:

- `eino-agui` owns AG-UI/Eino conversion, typed event emission, stream tapping,
  and AG-UI tool binding/classification.
- `eino-tools` owns reusable coding-agent leaf tools: `fileops`, `glob`,
  line-windowed `file_read`, `apply_patch`, rich `search`, `shell`,
  `url_fetch`, `user_interact`, and tracker adapters.
- `eino-obs` owns Datadog AI/LLM observability emission, no-network/fake
  exporters, safe redaction defaults, and lifecycle/model/stream/tool helpers.

`eino-agent` owns orchestration above those libraries: session admission,
history, durable replay, live stream classification, provider/model selection,
permission and approval hooks, tool materialization and settlement, runtime
contexts, interruption/resume, compaction, and consumer embedding APIs.

## Eino Tools Contract

The `eino-tools` response marks the parity request complete at commit
`e6ee664be93bb830b4bf6215907865b6366662b5`.

Important downstream decisions:

- `eino-agent` must serialize `fileops`, `glob`, `search`, and `apply_patch`
  calls per canonical workspace root.
- Independent workspace roots may run concurrently.
- `web_search` is out of scope for `eino-tools`; `eino-agent` owns the
  model-facing schema and runtime connector policy.
- LSP/diagnostics are out of scope for `eino-tools` in this slice;
  `eino-agent` owns the model-facing schema unless a later optional package is
  created.

## Ensemble Status

`eino-agui/docs/decisions/0002-ensemble-shared-surface.md` records the current
ensemble finding:

- The backend is `~/git/ensemble`, module `github.com/mattsp1290/ensemble`.
- It is a Go backend, but it is not currently an AG-UI SSE backend.
- It does not import the AG-UI Go SDK event/type/SSE packages.
- The first `eino-agui` API is reference-app-proven against
  `ag-ui-go-server-example`, not proven through ensemble.

`eino-agent` must not claim ensemble AG-UI parity until an ensemble adapter or
direct AG-UI transport exists. First-version ensemble work should be an
adapter/migration design that maps ensemble `dispatcher.RunEvent` semantics to
AG-UI and decides whether ensemble should expose AG-UI directly or through an
adapter service.

## Blockers Before Runtime Code

Required before starting runtime implementation:

- Use the exact dependency pins above.
- Create or preserve a durable status note with this evidence.
- Keep missing `eino-agui`, `eino-obs`, and ensemble response artifacts visible
  as follow-up documentation gaps.
- Do not start runtime code with an upgraded dependency baseline unless a
  separate compatibility bead verifies the change.

No code-level blocker remains for the initial runtime architecture and module
setup beads.
