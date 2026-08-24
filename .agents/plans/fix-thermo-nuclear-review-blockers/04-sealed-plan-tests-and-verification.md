# Sealed Plan Tests And Verification

## Goal and prerequisite state

Make tests use the same plan-construction invariants as production and prove the other work packages end to end. Apply after the API changes in work packages 1 through 3 compile.

## Repository evidence

- `runtime/run_plan_test.go` mutates `plan.tools` after `NewRunPlan` and manually hashes incomplete descriptors.
- Runtime tests directly instantiate private `RunPlan` fields in orchestration, resume, ledger, observability, and tool-execution cases.
- `composition/registry_test.go` already demonstrates production-shaped mount and resume fixtures.

## Exact change surface

- `runtime/run_plan_test.go`: delete `testRunPlanWithTools`, `setTestTools`, and `strictToolDescriptor`; add constructors that call `NewRunPlan` with complete `PlanTool` identities and behavior.
- Runtime test files using `&RunPlan{...}`: migrate them to the constructor, or use `composition.Registry` when the behavior under test crosses selection, fingerprinting, mount leases, or resume.
- `runtime/extension_plan.go`: if tests reveal that valid test construction remains awkward, prefer a smaller canonical constructor surface; do not expose fields or test-only production APIs.
- Runtime plan fixtures use bounded `PlanTool.Resolve` functions and cannot receive `TurnSnapshot`; remove `runtime.ToolRegistry`, `ToolRegistryFunc`, and custom full-snapshot registry fixtures.
- `composition/registry_test.go`: add end-to-end assertions for bounded tool context and AG-UI mounts where those invariants belong.
- Documentation and plan files from earlier work: remove statements that the old bypass or aggregate registry remains supported.

## Test strategy

- Unit tests use `NewRunPlan` only when plan selection is irrelevant.
- Integration tests use `composition.Registry` for tool identity, schema fingerprint, session scope, mount lease, and resume behavior.
- Store contract tests exercise only the public atomic settlement API.
- Mutation tests cover nested Eino message pointers and bounded tool context containers.
- Structural searches fail when removed compatibility symbols reappear.
- A custom plan-tool resolver test demonstrates that its compile-time input contains no messages, configuration, model clients, or executable tool graph.

## Acceptance gates

- No test assigns `RunPlan.tools`, `RunPlan.dispatch`, or `RunPlan.descriptor` directly after construction.
- No test constructs an executable `RunPlan` literal.
- Every descriptor used for a successful resume comes from `RunPlan.Descriptor` or `composition.Registry`, not a hand-built hash.
- `go test ./...` and `go test -race ./...` pass without test-only production hooks.
- `make check` passes from a clean worktree except for the intended implementation and plan files.

## Risks and exclusions

- Tests may use package-private inspection for read-only assertions, but not to create states impossible through production constructors.
- Do not weaken identity validation to make fixtures shorter.
