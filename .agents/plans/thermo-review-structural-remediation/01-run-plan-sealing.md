# Run-Plan Sealing

## Goal and prerequisite state

Make `runtime.NewRunPlan` the only boundary that derives durable capability identity from executable capability inputs. Preserve the existing `runtime.RunPlanProvider` acquisition and release lifecycle.

Prerequisites:

- The application context in `00-overview.md` remains valid.
- Beads issue `eino-agent-cdk` is claimed.
- The worktree contains no unrelated edits in `runtime/extension_plan.go` or `composition/registry.go`.

## Repository evidence

- `composition.Registry.acquire` builds both behavior and `session.*PlanIdentity` values in `composition/registry.go`.
- `runtime.NewRunPlan` validates and copies those values in `runtime/extension_plan.go`.
- `runtime.renderSystemPrompt` rejects duplicate prompt section names only after execution begins.
- `sealedPlanTools.ResolveTools` validates the output of dynamic resolvers and clones the materialized tools.
- `extension.Plan.HandlerComponents` already provides authoritative handler ownership and registration identity at seal time.

## Exact change surface

- `runtime/extension_plan.go`
  - Replace `PlanTool.Identity` with direct registration fields: name, registration ID, scope, order, schema hash, and executor hash.
  - Replace `PlanPrompt.Identity` with direct registration fields plus `PromptProvider`.
  - Replace `PlanGuard.Identity` with direct registration fields plus `ToolGuard`.
  - Replace `PlanRestriction.Identity` with registration ID and scope plus raw allowed and denied rules.
  - Add the target `session.ID` to `RunPlanSpec` so fresh plan sealing binds session-scoped handlers and capabilities to the admitting session.
  - Keep `PlanComponent` and `RunPlanSpec` as the minimal cross-package sealing input unless implementation can remove a type without widening another public surface.
  - Derive `session.ToolPlanIdentity`, `session.PromptPlanIdentity`, `session.GuardPlanIdentity`, and `session.RestrictionPlanIdentity` only inside `NewRunPlan`.
  - Validate every registration field with existing `extension.ValidateIdentifier` and `extension.ValidateScope` patterns.
  - Reject duplicate prompt names after scope resolution, duplicate tool names, duplicate guard IDs within an owner/scope, duplicate restriction IDs within an owner/scope, target-session scope mismatches, empty owners, and owner conflicts before sealing.
  - Reserve the runtime-owned prompt name `agent/system` and reject a mounted prompt with that name during sealing.
  - Preserve deterministic sort comparators, but make them compare derived identity or sealed capability fields rather than caller-provided identity objects.
  - Keep resolver output name equality and defensive cloning in `sealedPlanTools.ResolveTools`.
  - Remove redundant nil and duplicate checks from execution paths only when `NewRunPlan` proves the invariant.
- `runtime/extension_context.go`
  - Remove execution-time duplicate prompt-name rejection after seal-time validation proves extension uniqueness and rejects the reserved runtime name.
  - Keep provider call errors and context cancellation behavior unchanged.
- `composition/registry.go`
  - Build capability inputs from mounted registrations and frozen callbacks without constructing session identity values.
  - Pass the acquisition target session into `RunPlanSpec`.
  - Pass raw canonical restriction rules and let the runtime sealing boundary derive their durable hash.
  - Preserve snapshot release through `RunPlan.Release` and all constructor failure paths.
- Runtime and composition tests outside `docs/` and `examples/`
  - Update fixtures to use capability inputs rather than session identity structs.
  - Prefer registry acquisition in cross-package behavior tests. Keep direct constructor tests for seal invariants and deterministic descriptors.

## Intended behavior and invariants

- One capability input contains both its executable behavior and the registration fields from which durable identity is derived.
- No caller can supply a restriction hash that disagrees with its rules.
- Every invalid static plan fails before `AcquireRunPlan` or `AcquireResumePlan` returns to the orchestrator.
- Every session-scoped handler or capability matches the target session. A global plan has no session-scoped entries.
- Handler-only components and capability-only components retain their owning component artifact in the descriptor.
- Global and session-scoped winner selection remains in composition, before sealing.
- Sorting produces the same descriptor and executable order for equivalent mounts regardless of input iteration order.
- A dynamic resolver may fail or return a mismatched tool name at execution. Runtime returns `ErrExtensionPlanMismatch` for the mismatch and never exposes the invalid tool.
- Any seal failure releases the dispatch snapshot exactly once.

## Tests and acceptance criteria

- `go test ./runtime -run 'TestNewRunPlan|TestRunPlan|TestRenderSystemPrompt|TestRunExecution'`
- `go test ./composition -run 'TestRegistry|TestComposition|TestMount'`
- Add or adapt tests proving:
  - a restriction identity cannot be forged because callers no longer provide one;
  - duplicate prompt names are rejected by plan acquisition rather than prompt rendering;
  - a mounted prompt named `agent/system` is rejected before rendering;
  - duplicate tool names and invalid scopes fail before admission;
  - a fresh provider plan scoped to a different session fails before admission and releases the snapshot exactly once;
  - equivalent input permutations produce identical descriptors and capability order;
  - conflicting handler and capability owners release the dispatch plan once;
  - a dynamic resolver returning the wrong name still fails during resolution.
- Acceptance is observable when searches outside tests find no construction of `session.ToolPlanIdentity`, `session.PromptPlanIdentity`, `session.GuardPlanIdentity`, or `session.RestrictionPlanIdentity` in `composition`.

## Dependencies, risks, and exclusions

- This package boundary must be complete before changing resume identity or tool provenance.
- Do not replace the per-kind inputs with an `any` payload or tagged union; that increases invalid states.
- Do not expose `RunPlan` fields or dispatch ownership to composition.
- Do not remove checks for behavior that executes after sealing.
- Do not edit `docs/`. Keep any `examples/` edit mechanical and limited to removed-API compilation.
