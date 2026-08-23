# Runtime Protection and Strict Resume

## Work package 1: preserve permission classification through result transforms

Goal: keep permission metadata and durable classification immutable even though extensions may replace `ToolOutcome.Result`.

Repository evidence:

- `runtime/orchestrator.go:830` defines `(*StreamingOrchestrator).afterToolOutcome`.
- `runtime/extensions.go:909` seals `Disposition` and `PermissionMetadata`, while `validateToolOutcomeInput` deliberately excludes `Result` from protected comparison.
- `runtime.encodeToolOutput` currently maps every nil-error outcome to `ToolCallCompleted`; permission denial and approval-required results intentionally carry nil errors.
- The separate `tools/output.go` classifier is not used by the orchestrator persistence path.

Change surface:

- Modify `runtime.afterToolOutcome` in `runtime/orchestrator.go`.
- Modify `runtime.encodeToolOutput` and its two orchestration call sites in `runtime/orchestrator.go` and `runtime/interrupt.go` to accept the sealed disposition.
- Add a small unexported helper only if it removes duplicate metadata-overlay logic.
- Add proposed regression test(s) near existing tool outcome tests in `runtime/extensions_test.go` or `runtime/orchestrator_test.go`.

Required behavior:

- Preserve middleware's ability to transform result content and non-protected metadata.
- After `ToolResultTransformPoint` returns successfully, copy every sealed `PermissionMetadata` entry onto the transformed result.
- Do not allow a transform to change `Disposition`, `RawError`, `Error`, the call identity, or the protected metadata map; existing validation remains authoritative.
- If transform dispatch fails, keep the existing operational-error path.
- Do not alias extension-owned maps into the returned outcome.
- Encode `ToolDenied` and `ToolApprovalRequired` with model-visible status `expected_failure` and durable status `ToolCallFailed` without fabricating an operational error.
- Encode `ToolInterrupted` with model-visible status `interrupted` and durable status `ToolCallInterrupted`; retain safe result content when interruption is model-visible and has no raw error.
- Keep `ToolExecuted` completed and `ToolFailed` operationally failed with the existing error-redaction behavior.

Verification:

- Register a result transformer that returns a fresh `ToolResult` without metadata for a denied or approval-required outcome.
- Assert transformed content is retained and `permission_status` is restored.
- Exercise the actual orchestrator with an atomic settlement store and assert the persisted status, model-facing payload status, and `ToolSettledNotice` status all reflect the sealed disposition.
- Exercise strict `Resume` separately with an atomic settlement store and a fresh-result transformer; assert resumed durable status, model-facing payload status, and `ToolSettledNotice` status all reflect the sealed disposition.
- Cover the no-transform denied/approval-required path and model-visible interruption so the new disposition mapping is complete.

## Work package 2: recompute strict resume descriptors

Goal: reject a resume provider that changes descriptor contents while copying the persisted fingerprint string.

Repository evidence:

- `runtime.acquireRunPlan` in `runtime/extensions.go` already recomputes fresh plan fingerprints and rejects an inconsistent supplied value.
- `runtime.acquireResumePlan` currently compares `plan.Descriptor.Fingerprint` directly with the persisted descriptor.
- `session.FingerprintExtensionPlan` canonicalizes entries and registrations and excludes the fingerprint field from the digest.

Change surface:

- Modify `runtime.acquireResumePlan` in `runtime/extensions.go`.
- Add proposed tests beside `TestAcquireResumePlanUsesStrictToolSettlementPredicate` in `runtime/extensions_test.go`.

Required behavior:

- After the provider returns a non-nil plan, compute `session.FingerprintExtensionPlan(plan.Descriptor)` before capability checks or execution.
- Release the plan on every new rejection path.
- Reject a nonempty provider-supplied fingerprint that does not equal the recomputed descriptor fingerprint.
- Set the returned descriptor fingerprint to the recomputed canonical value.
- Reject when that recomputed value differs from `descriptor.Fingerprint`.
- Preserve legacy resume behavior and the existing strict tool-settlement store gate.

Verification:

- A provider descriptor whose content changed but whose fingerprint string was copied from persistence returns `ErrExtensionPlanMismatch` and invokes `Release` once.
- A semantically matching descriptor with a valid or omitted provider fingerprint is canonicalized and accepted.
- Existing ordering and settlement-store predicate tests continue to pass.
