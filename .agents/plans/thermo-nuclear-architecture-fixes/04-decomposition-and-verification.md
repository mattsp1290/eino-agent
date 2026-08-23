# Runtime Decomposition And Verification

## Goal

Leave extension and tool lifecycle code in focused modules with explicit ownership and prove the refactor did not change durable behavior.

## Repository evidence

- `runtime/extensions.go` is a new 1,086-line production file.
- `runtime/orchestrator.go` grew from 1,109 to 1,428 lines on this branch.
- The planned boundary and settlement changes provide natural module seams instead of a mechanical split.

## Exact change surface

Delete `runtime/extensions.go` after moving responsibilities into these proposed files under the existing `runtime/` package:

- `extension_plan.go`: `RunPlan`, provider acquisition, descriptor validation, release, and settlement predicate.
- `extension_execution.go`: explicit run state and extension/event dispatch helpers.
- `extension_lifecycle.go`: run/model/tool notice data and point declarations.
- `extension_context.go`: prompt, context assembly, bounded turn metadata, and validators.
- `extension_model.go`: model-stream input, cloning, sanitization, hashing, and validation.
- `extension_tool.go`: guards, tool prepare/execute/transform contracts, cloning, sanitization, and validation.
- `tool_execution.go`: shared claimed-call state machine.
- `tool_settlement.go`: canonical output and settlement construction.
- `model_stream.go` (new or extracted from `orchestrator.go`): provider request lifecycle, stream consumption, and usage accumulation.
- `tool_preparation.go` (new or extracted from `orchestrator.go`): tool-call normalization, durable creation/claim, and fresh-only transport publication.

Move only after ownership changes are implemented. Do not retain forwarding wrappers whose only purpose is to preserve the monolith's old internal layout.

Update:

- `docs/architecture/extension-points.md` with data-only callable handling, explicit lease scopes, and canonical tool settlement ownership.
- `docs/architecture/tools.md` with the single runtime settlement encoder and tools wrapper relationship.
- `docs/architecture/runtime.md` with explicit run execution state.
- `internal/deps/deps_test.go` only if package dependency assertions need clarification; no new package cycle is expected.

## Structural acceptance criteria

- Every new or modified production Go file is below 1,000 lines, with no exception for `runtime/orchestrator.go`.
- `runtime/orchestrator.go` loses provider-stream lifecycle, tool preparation, the post-claim state machine, and extension-context plumbing so it is below 1,000 lines.
- `runtime/interrupt.go` contains resume classification/reconciliation, not a second execution pipeline.
- Public exported symbols have GoDoc and stable ownership.
- `go list -deps ./runtime ./tools` succeeds without a cycle.
- `go test ./internal/deps` passes.

## Verification matrix

Run focused gates after each work package:

```text
go test ./extension ./composition
go test ./runtime ./tools ./session ./store/sqlite
go test -race ./extension ./composition ./runtime ./session ./store/sqlite
```

Run final repository gates:

```text
make fmt
make check
git diff --check
```

Inspect final structure:

```text
wc -l runtime/*.go composition/*.go extension/*.go tools/*.go
rg -n 'sameInterfaceIdentity|interfaceWords|withRunPlan|runPlanFromContext|"unsafe"|leasePoint|composition/lease' runtime composition extension --glob '*.go' --glob '!**/*_test.go'
rg -n 'BuildToolSettlement|ToolSettlement\{' runtime tools --glob '*.go' --glob '!**/*_test.go'
```

## Risks and exclusions

- Formatting and generated WIT checks may touch generated files; accept no generated diff unless the implementation actually changes WIT, which is out of scope.
- Preserve unrelated worktree changes if any appear during implementation.
- Do not count test files against the 1,000-line production threshold, but split tests when a focused package file becomes hard to navigate.
- Fail the final gate if any modified production Go file is 1,000 lines or more; do not use a reviewer waiver.
