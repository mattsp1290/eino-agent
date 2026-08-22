# Fix Strict Extension Plan and Tool Lifecycle Correctness

## Change classification

- Type: correctness bug fix (`OTHER`)
- Tracking: `eino-agent-561`
- Source: the five supplied P1/P2 review findings
- Documentation/RFC links: N/A

## Goal

Make fresh and resumed strict extension plans describe exactly the capabilities that can execute, keep idempotent admission bound to its persisted plan, and make durable tool lifecycle notifications and settlements complete even when cancellation races execution.

## Constraints

- Preserve `config.ToolConfig` semantics: `Enabled == nil` means all tools, a non-nil empty `Enabled` list means no tools, and `Disabled` wins over `Enabled`.
- Preserve strict resume determinism without changing the durable descriptor schema. A resumed plan must select the exact persisted tool identities `(InstanceID, Scope, CapabilityID)`, including when one extension instance registered multiple tools or names/IDs collide across scopes.
- Keep extension registration identifiers validated as stable identifiers, while treating a runtime target `session.ID` as an opaque, nonempty scope key.
- Do not execute or notify against a freshly acquired plan when an idempotent admission already persisted a different fingerprint.
- Keep terminal settlement authoritative after cancellation by detaching only the persistence operation from cancellation; retain context values.
- Avoid unrelated API or behavior changes.

## Affected areas

1. `composition/registry.go` and `composition/registry_test.go`
   - Apply the run request's enabled/disabled tool selection before building the frozen tool snapshot and strict descriptor.
   - On resume, filter current registrations by the full persisted tool identity `(InstanceID, Scope, CapabilityID)` before name-based global/session shadowing rather than merely by extension instance, so the reconstructed descriptor and executable tool set remain identical.
   - Verify disabled, explicitly-empty, multi-tool/same-instance, and colliding global/session identity resume cases, including the no-effective-tool settlement-store predicate.

2. `runtime/admission.go` and `runtime/admission_test.go`
   - In the existing/idempotent admission path, recompute canonical fingerprints from both the persisted and requested descriptors, validate any nonempty stored fingerprint against its canonical contents, and compare the canonical values.
   - Return `ErrExtensionPlanMismatch` before building a snapshot or launching execution when they differ; retain existing idempotent behavior when they match.

3. `runtime/orchestrator.go` and `runtime/orchestrator_test.go`
   - Use `context.WithoutCancel(ctx)` for atomic strict `SettleToolCall` after a tool outcome is known.
   - Add a SQLite-backed regression test in which cancellation occurs during tool execution and assert that the durable call reaches its terminal interrupted/successful settlement rather than remaining `running`.

4. `extension/types.go`, `extension/registry.go`, and `extension/extension_test.go`
   - Separate registration-scope validation from snapshot-target validation.
   - Allow any nonempty opaque session key for snapshot targets while retaining current registration identity validation and global-scope rules.
   - Cover values such as `user@example.com` and padded base64, plus the empty-session rejection.

5. `runtime/interrupt.go` and resume lifecycle tests
   - Emit `ToolStartedPoint` immediately after a pending resumed tool call is successfully claimed and before execution/settlement.
   - Verify an observer receives `started` then `settled` exactly once for a resumed pending call; no started notice is emitted for an unsuccessful claim or an already-running call that is only reconciled.

## Implementation sequence

1. Add focused failing tests for all five findings, including selection/resume edge cases.
2. Split target-vs-registration scope validation and make opaque session targets legal.
3. Thread fresh tool selection and persisted capability selection through composition plan acquisition before descriptor construction.
4. Enforce extension-plan fingerprint equality on idempotent admission.
5. Detach strict settlement persistence from cancellation and add the resumed `ToolStartedPoint` notification.
6. Run formatting, focused package tests, then repository-wide build, test, and vet gates.

## Success criteria

- All five review findings have direct regression tests.
- A strict descriptor contains only config-selected tools, and a run with no effective mounted tool does not require `ToolSettlementStore`.
- Resume reconstructs the exact selected tool set from the persisted descriptor, including multiple tools registered by one instance and colliding identities across global/session scope shadowing.
- An idempotent retry with a canonically changed frozen plan or a stale/corrupt fingerprint fails with `ErrExtensionPlanMismatch` before execution; an identical retry remains idempotent, including schema-v1 canonicalization.
- Strict SQLite settlement commits terminal tool state even when the run context has been canceled.
- Opaque nonempty durable session IDs are accepted as snapshot targets without weakening registration identity checks.
- A resumed pending tool emits `ToolStartedPoint` after claim and before `ToolSettledPoint`.
- `gofmt`, focused tests, `go build ./...`, `go test ./...`, and `go vet ./...` pass.
