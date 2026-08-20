# Extension Plan and Wasm Adapter Correctness Plan

## Change Information

### Change Type

OTHER — focused correctness and regression fixes across existing extension-plan paths.

### Description

Resolve all seven findings from the current branch review: function-safe protected tool validation, nested session-scope recovery during strict resume, complete bounded metadata propagation through registered Wasm context sources and hooks, instance-unique context contribution identities, unconditional hook cleanup after after-turn errors, and deep cloning of attachment metadata for notifications.

### Links to Relevant Documentation

N/A. The authoritative inputs are the review findings and the existing extension-point contracts and tests in this repository.

### Affected Areas

- `runtime/extensions.go`, `runtime/orchestrator.go`, and `runtime/admission.go`: protected tool invariants, bounded context/run/turn metadata carriers, a post-tool-resolution hook point, and defensive result cloning.
- `runtime/extensions_test.go`: direct regressions for function-bearing tools and notification copy isolation.
- `tools/registry.go`: pointer-backed identity for materialized executor and decoder adapters.
- `composition/registry.go` and `composition/registry_test.go`: strict-resume scope reconstruction from grouped handler registrations.
- `wasmext/points.go` and `wasmext/phase_b_test.go`: registered adapter metadata, source naming, and hook lifecycle behavior.

### Success Criteria

- Unchanged tools materialized by `tools.Registry` pass both prepare and execute extension validation while protected tool/call mutations remain rejected.
- Strict resume reconstructs the session scope when it appears only in a handler entry's nested `Registrations`, and rejects conflicting session keys.
- Registered Wasm context sources receive the same bounded agent/provider/model/system-prompt projection available from the real turn snapshot.
- Context contribution source strings include the component instance and remain globally unique across registrations with the same ID.
- Registered Wasm hooks receive bounded admission metadata and post-tool-resolution turn metadata, including tool names; cached metadata reaches both after phases.
- `AfterRun` is invoked even when `AfterTurn` fails, both errors are preserved, and the per-run cache entry is removed.
- Every `ToolSettledPoint` observer receives attachment metadata maps isolated from the source and from other observers.
- Targeted regressions and the repository quality gates pass.

### Constraints

- Preserve public behavior and existing ordering, containment, redaction, and bounded-metadata guarantees.
- Avoid `reflect.DeepEqual` over callable-bearing tool interfaces; compare ordinary protected fields separately and use an explicit function-safe identity/equivalence helper for interface implementations.
- Carry only an explicit bounded metadata projection through extension points. Do not expose full snapshot/config/model/message containers to registered observers.
- Do not change WIT contracts or expose prompt/message contents beyond the existing bounded projection.

## Implementation Plan

1. Add targeted failing regressions first.
   - Exercise `ToolPreparePoint` and `ToolExecutePoint` with a real tool materialized from `tools.Registry` in an external runtime test package (avoiding an import cycle), plus negative same-type callable-replacement cases including same-code closures and non-comparable value implementations.
   - Verify `ToolSettledPoint` attachment metadata cannot leak mutations across observers or back to the source.
   - Persist and resume a descriptor where one component owns both global and session-scoped handlers, and cover conflicting nested session keys.
   - Invoke registered Wasm context and hook adapters through the real orchestrator ordering with full host snapshots, delimiter-collision registration identities, duplicate IDs across instances/scopes, and after-turn/after-run failures.
2. Replace whole-value tool comparisons.
   - Compare stable `Tool` and `ToolCall` fields after cloning mutable containers and masking only the documented mutable prepare fields (`Call.Input` and `Call.Pattern`).
   - Give function-bearing registry executor/decoder adapters pointer identity, then compare `Executor`, `InputDecoder`, and `Approval` through an explicit identity helper that never sends callable-bearing values through `reflect.DeepEqual` or panics on non-comparable dynamic types. Define and test conservative behavior for unsupported non-comparable implementations so replacements cannot be accepted merely because function PCs match.
   - Reuse the same protected comparison in prepare and execute validation.
3. Recover strict-resume scope from the full descriptor.
   - Scan both each entry's top-level scope and every nested registration scope.
   - Accept zero or one unique session key and return `ErrExtensionPlanMismatch` for conflicts before acquiring a plan.
4. Carry only bounded metadata through extension points at the correct lifecycle boundaries.
   - Define a runtime-owned bounded turn metadata projection containing IDs, agent/provider/model identity, tool names, message/role counts, and only a boolean system-prompt indicator.
   - Add that defensively cloned projection to context assembly and run-admitted notices, mark it protected, and populate it from the real snapshot without exposing config, prompts, message contents, model clients, executors, or other callable interfaces.
   - Add a required post-tool-resolution turn hook point and invoke it after planned tools are resolved but before legacy native hooks. Register Wasm `before-turn` there so tool names are complete and failures still abort the turn.
   - Add internal Wasm wrapper methods that consume the bounded projection directly while preserving the legacy `LoadedContextSource` and `LoadedHook` interfaces.
5. Make registered adapter identities and cleanup deterministic.
   - Generate contribution sources from an unambiguous length-prefixed encoding of instance ID, registration ID, and scope plus the contribution index, preventing delimiter and mixed-scope collisions.
   - Call `AfterRun` unconditionally after `AfterTurn` and return `errors.Join` so cleanup and notification both happen and both failures remain observable.
6. Deep-clone attachment metadata in all runtime tool-result clone paths used by notifications and outcomes.
7. Run formatting and focused package tests, then the repository's full `make check` gate. Update and close Beads issue `eino-agent-4tu`, commit only related files, rebase, push Beads and Git, and verify the branch is up to date with origin.

## Risk Checks

- Ensure interface comparison does not treat distinct pointer-backed executors as identical and does not panic on functions, maps, slices, or unexported fields.
- Treat unsupported non-comparable callable implementations conservatively; the supported registry adapters are pointer-backed so unchanged composed tools have a stable comparable identity.
- Ensure bounded metadata carriers do not alter descriptor fingerprints, contain raw prompt/message/config data, or serialize secrets into durable extension descriptors.
- Ensure context assembly still permits only contribution/message mutation and rejects bounded metadata changes.
- Ensure joined hook errors remain contained by the notification policy while the guest after-run call still executes.
