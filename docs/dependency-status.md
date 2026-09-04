# Dependency Status

Date: 2026-06-27
Last updated: 2026-09-04

This note records the prerequisite state for `eino-agent` before runtime
implementation starts. The exact pins below are the initial baseline. Do not
upgrade them inside unrelated implementation work; file a dependency-upgrade
bead with compatibility gates instead.

## Summary

No runtime implementation blocker remains from the three required library
dependencies. The local checkouts match the requested pins and their relevant
validation gates pass.

The supported root pin is `v0.3.3` at commit
`36fe8d8a046b4dd193e97b8f49a580a71bf07bbc`. Its generated-bindings
dependency is published as module version `v0.1.0` through repository tag
`wasmext/gen/v0.1.0` at commit
`f8a2784061bb9df52ccb0db3a431c5100a99b798`.

Durable provider-private state is supported by `v0.3.3`. On 2026-09-04, the
exact clean tag target passed `make check`, the remote annotated tag peeled to
that commit, and a fresh published-mode consumer exercised the state-aware API
and delegated web-search runtime contract without a `replace`, workspace,
vendor tree, or checkout access.

The previously missing response artifacts now exist in this inspected local
agent workspace. They are stored outside this repository under
`~/.agents/projects/*/responses/`, so a clean clone on another machine may not
contain them:

- `~/.agents/projects/eino-agui/responses/2026-06-26-ag-ui-adapter-surface-for-eino-agent.md`
- `~/.agents/projects/eino-obs/responses/2026-06-26-datadog-llm-observability-api-for-eino-agent.md`
- `~/.agents/projects/ensemble/responses/2026-06-26-observability-contract-for-eino-agent.md`

The response notes preserve the checked repo state, tags or commits, validation
evidence, and blocker status for this local agent workflow. The ensemble
response remains a planning artifact: it does not block initial runtime API
work while `eino-agent` keeps observability behind the `eino-obs` contract and
does not claim ensemble parity.

If a future agent cannot access these external artifacts, it should not infer
new dependency status from this note alone. Recreate or re-request the response
evidence before changing pins, validation claims, or ensemble integration
status.

## Pins

| Dependency | Pin | Evidence |
| --- | --- | --- |
| `github.com/mattsp1290/eino-agui` | tag `v0.1.1`, commit `453c69c9bdb1006c585407510342ea5a03d1b48d` | `~/git/eino-agui`, `go.mod`, `README.md`, `docs/decisions/0002-ensemble-shared-surface.md` |
| `github.com/mattsp1290/eino-tools` | commit `63a3c99272c2359e24484698f2bd62e6fac849b6`, pseudo-version `v0.1.1-0.20260825160656-63a3c99272c2` | `~/git/eino-tools`, completed catalog commits `cc35e50` and `63a3c99`, and `.agents/requests/eino-agent-composition-tool-registration/` |
| `github.com/mattsp1290/eino-obs` | commit `a9a6f8bb478b479c1e48ab353261a60c4a19195a` | `~/git/eino-obs`, `README.md`, root/exporter/fake/exporter/datadog packages |
| `github.com/mattsp1290/ensemble` | commit `a709ad8ed2e9d8962b73b228859433cc6554ee2c` for current discovery only | `~/git/ensemble`, `eino-agui/docs/decisions/0002-ensemble-shared-surface.md` |
| `github.com/mattsp1290/eino-agent` | version `v0.3.3`, commit `36fe8d8a046b4dd193e97b8f49a580a71bf07bbc` | `make check` on the exact clean commit plus a fresh published-mode consumer using Go 1.26.3, `proxy.golang.org`, and `sum.golang.org`, with no `replace`, workspace, vendor tree, or checkout access. |
| `github.com/mattsp1290/eino-agent/wasmext/gen` | version `v0.1.0`, tag `wasmext/gen/v0.1.0`, commit `f8a2784061bb9df52ccb0db3a431c5100a99b798` | Fresh-cache standard-proxy download plus the local and published external-consumer gates; no consumer replacement is required. |

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

- `~/.agents/projects/eino-agui/responses/2026-06-26-ag-ui-adapter-surface-for-eino-agent.md`
- `~/.agents/projects/eino-tools/responses/2026-06-26-coding-agent-tool-parity-for-eino-agent.md`
- `~/.agents/projects/eino-obs/responses/2026-06-26-datadog-llm-observability-api-for-eino-agent.md`
- `~/.agents/projects/ensemble/responses/2026-06-26-observability-contract-for-eino-agent.md`

No required prerequisite response artifact is missing in the inspected local
workspace.

## Validation Run

The module-graph release adds `make external-consumer-check`. It
creates a fresh unrelated main module, imports `runtime`, `store/sqlite`,
`stream`, `composition`, `model`, and `providers/fake`, and runs `go mod tidy`,
`go list -m all`, `go mod verify`, `go test ./...`, and `go build ./...`. Local
mode replaces only the root checkout and proves the dependency module's local
replacement is ignored. Published mode adds no replacement and rejects
nonstandard proxy, checksum, private-module, `GOFLAGS`, workspace, vendor, or
checkout selection.

Post-tag validation performed on 2026-09-04:

- `make check` passed on exact commit
  `36fe8d8a046b4dd193e97b8f49a580a71bf07bbc`.
- Remote annotated tag `v0.3.3` peeled to that exact commit.
- `EINO_AGENT_CONSUMER_VERSION=v0.3.3 testdata/external-consumer/check.sh`
  passed from a fresh published-mode consumer with Go 1.26.3,
  `GOPROXY=https://proxy.golang.org,direct`, `GOSUMDB=sum.golang.org`, and
  empty `GOFLAGS`, `GOPRIVATE`, `GONOSUMDB`, and `GONOPROXY`; it reported
  `github.com/mattsp1290/eino-agent v0.3.3`, `replacement=false`, and
  `external-consumer: published verification passed`.

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

The original parity request completed at `e6ee664`. The later
`eino-agent-composition-tool-registration` request completed at `63a3c99` and
adds the runtime-neutral `catalog` package now consumed by this repository.

Important downstream decisions:

- `tools/einotools.MountStandard` translates the catalog into one atomic
  composition mount and carries leaf schema/executor hashes into durable plan
  identity.
- The adapter serializes every catalog definition with `Concurrent=false`
  through one process-wide, ref-counted lock domain. Workspace tools share the
  canonical-root key; non-concurrent static tools share their catalog-ID key.
- Independent workspace roots may run concurrently.
- `web_search` is out of scope for `eino-tools`.
  [`eino-agent-extensions` owns](architecture/web-search-extension-ownership.md)
  the reusable model-facing schema, strict query semantics, bounded result
  records, and host-search adapter. `eino-agent` owns the generic composition,
  JSON-object validation, permission, durable execution, retention, and resume
  seams. The embedding host owns provider credentials, network/backend policy,
  freshness and rate limits, presentation, and backend lifecycle.
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

## Runtime Start Criteria

Already satisfied for initial runtime architecture and module setup:

- The dependency baseline is pinned to the versions and commits above.
- This durable status note preserves the implementation evidence.
- The checked local repositories passed the validation gates listed above.

Ongoing constraints for runtime implementation:

- Use the exact dependency pins above.
- Preserve the response artifacts listed above when updating prerequisite
  evidence. Preserve each artifact's pin, validation, and blocker-status
  evidence; if any artifact moves or is superseded, update this note in the same
  change.
- Do not start runtime code with an upgraded dependency baseline unless a
  separate compatibility bead verifies the change.
- A dependency-upgrade compatibility bead must record the proposed new pin,
  upstream release or commit evidence, `go mod tidy -diff`, relevant upstream
  validation gates, and downstream `eino-agent` compatibility tests.
- Do not claim ensemble AG-UI parity until an ensemble adapter or direct AG-UI
  transport exists.

No code-level blocker remains for the initial runtime architecture and module
setup beads.
