# Coding Agent Handoff

## First Actions

1. Run `bd prime`, inspect ready work, and create/claim one Beads issue for the
   next roadmap slice. Use Beads dependencies to represent slice order; do not
   copy this plan into a Markdown checkbox list.
2. Re-read the current code at the paths named below. The plan is pinned to a
   snapshot; code wins if main has moved, but any contract change requires a
   plan/doc update rather than a silent reinterpretation.
3. For Slice 1, add characterization benchmarks/tests before implementing the
   kernel. For runtime slices, add sequence characterization tests before
   moving a call site.
4. Keep each PR limited to one slice or a smaller coherent part of it.

## Code Landmarks

- Construction and loop: `runtime/options.go`, `runtime/orchestrator.go`.
- Admission/component persistence: `runtime/admission.go`, `session/types.go`.
- Resume/tool recovery: `runtime/interrupt.go`, `session/resume.go`.
- Existing public seams/events: `runtime/types.go`.
- Provider request boundary: `runtime/provider.go`, `model/provider.go`.
- Typed tools: `tools/registry.go`, `runtime/tool_permissions.go`.
- Durable history: `session/history/projector.go`, `runtime/history.go`.
- New composition inversion point: `composition/` may import `extension`,
  `runtime`, and `tools`; `runtime` must never import `composition` or `tools`.
- Store contracts and SQLite: `session/types.go`, `store/storetest/contract.go`,
  `store/sqlite/store.go`, `store/sqlite/migrations/`.
- Live transports: `agui/bridge.go`, `agui/replay.go`, `stream/tail.go`.
- Observability: `runtime/observability.go`, `obs/`.
- Wasm: `wit/`, `wasmext/`, `examples/wasm-extensions/`.
- Config-time plugins are a separate lifecycle: `config/lifecycle.go`,
  `config/plugins/`.

## Binding Decisions

- Do not merge durable, transport, and interception events into one bus.
- Do not port Cordis or introduce ambient service lookup.
- Do not use string event names as the core dispatch key.
- Do not use `Agent.Name` for unique scoping.
- Do not let registration-specific flags choose fail-open/fail-closed behavior.
- Do not allow a wrapper to call a model or tool continuation twice.
- Do not accept successful model/tool around-interceptor output unless the core
  continuation ran exactly once.
- Do not move permissions into reorderable middleware.
- Do not let result transforms alter protected tool disposition or permission
  metadata.
- Do not persist provider options by default or infer which config values are
  secrets.
- Do not mutate WIT `@0.1.0`.
- Do not promise exact resume reproducibility for legacy unversioned seams.
- Do not extend `session.Store` for the request ledger; use an optional
  capability interface and SQLite migration 002.

## Implementation Questions To Resolve In Code

These are bounded design choices the coding agent may resolve with tests and a
short ADR/doc note:

- The exact `RunPlanProvider` and `composition.Registry` concrete type names.
- The canonical encoding of all scope-applicable plan entries. It includes
  disabled tool registrations so selection remains side-effect-free before
  admission; it must not invoke providers early.
- The exact database representation for canonical messages/tool schemas, as
  long as it is lossless, bounded, paginatable, and store-neutral at the domain
  boundary.
- The initial numeric order bands for host policy, compatibility adapters, and
  application extensions. Publish constants if applications must target them.
- The public sanitized error-code vocabulary and local raw-cause observation
  path.

Escalate instead of guessing if resolving one of these would change a binding
security, durability, compatibility, or plane-separation decision.

## Suggested Beads Graph

Create an epic for the deeper extensibility program, then one feature bead per
roadmap slice. Within a slice, separate public API/kernel, runtime integration,
store migration, Wasm, docs, and verification only when they can be reviewed
and merged coherently. Encode the roadmap arrows as dependencies. Slice 0 may
run independently; Slice 6 depends on Slice 0 and the native slices.

Acceptance text on every implementation bead should name:

- the exact exported contract being added or preserved;
- its failure/durability/resume semantics;
- relevant unit, race, integration, and compatibility gates;
- documentation updated in the same change.

## Quality Gates Per Slice

Use the repository's discovered commands, with at least:

```bash
go test ./...
go test -race ./...
make check
```

Run targeted benchmarks and fuzz seeds for `extension`, runtime sequence tests,
store contract tests, and Wasm tests when those areas change. Verify generated
WIT output is clean whenever `wit/` or bindings change.

At session end, follow `bd prime`'s close protocol: file remaining work, update
or close the claimed bead, run gates, commit selectively, pull/rebase, push
Beads data, push Git, verify clean/up-to-date status, and leave a concise
handoff.
