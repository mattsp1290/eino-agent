# Specialist Review Disposition

## Review Pass

The first complete draft received three independent read-only reviews:

- repository/API accuracy and current sequencing;
- comparison with DeepSeek Harness at pinned commit `141eb6f` and whether the
  proposal should go further;
- adversarial implementation feasibility, concurrency, durability, security,
  migration, and package-boundary review.

This file records material decisions, not every copyedit.

After applying that pass, the implementation reviewer performed a new final
adversarial review of the revised packet. Its remaining findings were also
resolved before this disposition was finalized.

## Accepted And Applied

- Corrected `EventSink` behavior: callback errors are ignored; only bounded
  delta enqueue can block/cancel.
- Kept legacy `Hook.BeforeRun` at its synchronous post-admission location and
  separated the new run gate. Durable-admission observation now precedes the
  legacy hook.
- Replaced overloaded `Run.Components` use with a versioned
  `ExtensionPlanDescriptor` supporting strict, partial-legacy, and legacy
  modes.
- Split artifact identity from mount `InstanceID`; added artifact hash,
  behavior-affecting config hash, kind, required flag, registration/capability
  identities, and canonical plan fingerprint.
- Added arbitrary mount-owned cleanup effects with reverse-order rollback,
  drain-before-cleanup, retry, and failure aggregation.
- Added a concrete `composition` package inversion and `RunPlanProvider` so
  `runtime` never imports `tools`; rejected non-atomic generation revalidation.
- Fully specified global/session tool layers, shadowing, monotonic restrictions,
  deterministic ordering, shared schema/executor generation, and lifecycle
  leasing.
- Added reversible named prompt sections evaluated per provider step. Kept the
  currently inert configured system prompt default-off and made activation an
  explicit ordered compatibility-tested option.
- Added scoped deny-only `ToolGuard` composition and a protected immutable
  `ToolOutcome`; guards may preempt, while abstention preserves the current
  sequential permission/approval loop exactly.
- Required success-producing around middleware to call the core continuation
  exactly once. Short-circuiting may only produce typed failure/rejection.
- Added atomic tool-call/result settlement for transactional stores plus
  idempotent reconciliation and resume repair elsewhere.
- Removed the proposed model-options point because the current raw Eino path
  ignores `model.Request.Options`. Recorded request mutation and request-error
  recovery as explicit gaps.
- Reframed request auditing as the exact canonical runtime-to-adapter
  projection, not unknowable final provider wire input. Added system content,
  safe call config, canonical DTO conversion, and rejection of unsupported
  extras.
- Changed the ledger to `prepared -> dispatch_started -> completed/failed`,
  accepting orphan states after crashes and claiming only that adapter dispatch
  implies a durable prepared record.
- Kept `session.Store` source-compatible by using an optional
  `ModelRequestStore` capability and specifying SQLite migration 002.
- Added point-owned clone/validate functions, a complete resume matrix, Phase B
  WIT adapter mappings, deterministic tool order, privacy-safe error handling,
  and corrected durable-event claims.
- Narrowed the Harness claim from dispatch-mode parity to semantic separation:
  the first kernel implements contained notifications and guarded waterfalls,
  not Cordis parallel/bail/serial behavior.

## Final Adversarial Corrections Applied

- Removed process-local registration generations from persisted fingerprints;
  they now serve only ABA protection/leasing. Strict plans fingerprint stable
  contract, artifact/config, registration, schema/executor, restriction, and
  guard identities.
- Added stable point `Contract.ID` and semantic version. Private points without
  those identities force partial-legacy mode.
- Added point-owned validation for every candidate input passed to `next`, not
  only final outputs.
- Moved deterministic tool snapshot/provenance primitives into the coordinator
  foundation slice so Slice 2 is independently implementable.
- Added descriptor-driven resume acquisition that resolves exactly persisted
  entries and ignores unrelated later mounts.
- Placed deny-only mounted guards before the unchanged legacy sequential
  permission/approval loop, preserving callback count/order and interruption
  semantics whenever guards abstain.
- Defined `AuditedModelInput` as a safe subset of the full `model.Request`;
  opaque identity/options/observer/client state remains attached outside the
  equality and persistence claim.
- Removed a nonexistent generic concurrency-boundary claim and documented
  current serial-within-run/cross-run behavior.
- Replaced vague settlement reconciliation with a concrete optional
  `ToolSettlementStore`, reserved result IDs, idempotency, unreconciled listing,
  SQLite transaction, and strict-plan capability requirement.
- Added a nonrecursive diagnostic reporter and required the ledger terminal
  state to commit before `model-completed` observation.

## Considered But Deliberately Deferred Or Rejected

- No Cordis service container, dependency-state machine, config patch layers,
  HMR system, hierarchical agent scope, or generic string/JSON Wasm bus.
- No `turn-stopping` event until `eino-agent` has a precise durable step/turn
  model.
- No model retry-decision point in this series. Core keeps exclusive bounded
  retry authority; a future extension may only stop or tighten it.
- No model-options transform until every adapter path consumes one normalized
  request-aware execution contract.
- No automatic WIT `@0.2.0` world. Native contracts must stabilize and receive
  a separate capability/security review first.
- No claim that request-ledger persistence proves provider receipt or the
  adapter's final HTTP body. That requires provider-side receipts or a prepared
  adapter API.

## Net Effect

The review expanded the plan where Harness exposed high-value missing concepts
(resource ownership, prompt composition, scoped coherent capabilities,
monotonic guards, and durable request provenance) and narrowed it where the
initial draft overclaimed current Go/runtime behavior. The roadmap remains a
bounded sequence rather than a Cordis port.
