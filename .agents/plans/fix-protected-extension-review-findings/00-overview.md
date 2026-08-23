# Fix Protected Extension Review Findings

Status: Implementation-ready. Planning only; implementation has not occurred.

## Requested outcome

Resolve the five supplied review findings, have two independent subagents review this plan, incorporate evidence-backed corrections, implement the accepted plan, verify the repository, commit the result, and push the branch.

## Success criteria

- A tool-result transform cannot remove permission metadata captured before extension dispatch, and durable/model-visible settlement derives permission outcomes from the sealed disposition.
- A strict resume plan is accepted only when its returned descriptor content recomputes to the persisted fingerprint.
- `tools.BuildToolSettlement` fills the reserved result message and part records required by `session.ToolSettlementStore`.
- Runtime extension clones retain an independent `einoschema.ToolInfo.ParamsOneOf`, and protected validation detects schema replacement.
- `model.Request.Clone` keeps aliased slices with different headers distinct.
- Focused package tests and `make check` pass.
- Beads issue `eino-agent-b3x` is closed, Beads data is pushed, and the Git branch is committed and pushed.

## Scope

The implementation changes `runtime/orchestrator.go`, `runtime/extensions.go`, `runtime/types.go`, `tools/output.go`, and `model/provider.go`. It adds regression coverage in their existing package tests and a SQLite integration assertion in `store/sqlite/store_test.go`.

## Non-goals

- Change extension contracts, durable schemas, or public function signatures. Adding reserved-ID fields to the released public `runtime.ToolCall` struct is an accepted compatibility break for external unkeyed struct literals; keyed literals continue to compile and must populate reservations when using the settlement builder.
- Redesign permission policy or approval semantics.
- Preserve aliasing between distinct cloned slice views; the clone must preserve each view's values and shape without conflating headers.
- Modify unrelated archival reviews or earlier plans.

## Repository findings

- `runtime.afterToolOutcome` overlays `ToolOutcome.PermissionMetadata` before `ToolResultTransformPoint`, but assigns the transformed outcome without overlaying it again. The runtime then calls `encodeToolOutput`, which currently treats every nil-error outcome as completed and does not consume the sealed disposition.
- `runtime.acquireRunPlan` recomputes a descriptor fingerprint, while `runtime.acquireResumePlan` currently compares only the provider-supplied fingerprint string.
- `tools.BuildToolSettlement` accepts `runtime.ToolCall`, whose current shape lacks reserved result IDs. It returns a payload part attached to the assistant message and leaves `ToolSettlement.ResultMessage` and `ResultPart` empty. `store/sqlite.Store.SettleToolCall` requires those records to match the durable call's reserved IDs.
- `runtime.cloneTool` JSON-round-trips `ToolInfo`; Eino's `ParamsOneOf` fields are unexported and disappear from JSON. `runtime.sameProtectedToolInfo` has the same blind spot.
- `model.cloneReflectValue` caches slices by type, kind, and data pointer. Two slice headers sharing their first element but differing in length collide.
- `go test ./runtime ./tools ./model ./store/sqlite` passes before implementation, so regression tests must reproduce the reported cases.

## Key decisions

1. Reapply the sealed permission metadata after a successful result transform. Pass the sealed `ToolOutcome.Disposition` into runtime output encoding so denied and approval-required outcomes settle as expected failures, interrupted outcomes settle as interrupted, and transforms cannot change this classification.
2. Recompute the resume provider's descriptor fingerprint from descriptor content, validate any supplied fingerprint, canonicalize the returned descriptor, and compare the computed value with the persisted fingerprint before settlement capability checks or execution.
3. Add `ResultMessageID` and `ResultPartID` to `runtime.ToolCall`, populate them from newly reserved or resumed durable calls, and protect them through cloning/validation. This is the narrowest viable data path without replacing the existing builder signature, but downstream unkeyed `runtime.ToolCall` literals must migrate. Make the builder reject missing reservations and build the complete result envelope at one `CompletedAt` instant.
4. Clone and compare `ParamsOneOf` through a panic-safe `ToJSONSchema` plus a JSON deep copy. Treat a nonnil wrapper that converts to nil as failure and retain runtime's fail-closed clone behavior.
5. Extend slice visit identity with length and capacity. Keep cloning each distinct header independently so a full view and its prefix cannot reuse one cloned header.

## Change model

```text
permission outcome -> middleware -> protected metadata overlay -> result transform
                                                        -> protected overlay again -> settlement

persisted descriptor fingerprint <-> recomputed provider resume descriptor fingerprint

claimed tool call reserved IDs -> BuildToolSettlement -> terminal call + tool message + tool-result part
```

## Risks and gates

- Runtime integration tests must prove the real `encodeToolOutput` path uses protected disposition; a test against the separate `tools` classifier is insufficient.
- Eino schema conversion can error, return nil from a nonnil wrapper, or panic on malformed public input. Runtime clones must recover and fail closed instead of crashing or passing callbacks a partial protected tool.
- Settlement timestamps and IDs participate in SQLite idempotence. One timestamp and identical result records must be used throughout the builder result.
- The exported struct field addition breaks external unkeyed `runtime.ToolCall` literals. This pre-1.0 compatibility cost is accepted because the existing builder otherwise cannot receive the reserved IDs; note it in the delivery summary.
- Slice cache changes must retain cycle/alias protection for exact same-header visits while separating different shapes.
- There are no blocking decisions. The branch was clean at planning start.

## Document map

- [01-runtime-protection-and-resume.md](01-runtime-protection-and-resume.md): permission outcome and strict resume changes.
- [02-settlement-and-cloning.md](02-settlement-and-cloning.md): atomic settlement, tool schema cloning, and aliased slice changes.
- [03-verification.md](03-verification.md): regression matrix and repository gates.
- [04-execution-handoff.md](04-execution-handoff.md): dependency-ordered implementation and delivery sequence.
