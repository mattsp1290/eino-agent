# Final Thermo Structural Simplification

Status: Implemented, verified, and re-reviewed. Two independent final audits report no Critical, Important, or P1 findings.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "ed568623bc12e25a9da5e1411e873f53e0f23d12c389441822ce78c6cc4351ac",
    "confirmed_at": "2026-08-30T01:51:00Z"
  }
}
```

The repository has no active users or external consumers. Change public Go APIs and internal ownership contracts directly when that simplifies the design. Do not add aliases, compatibility wrappers, old-shape decoders, feature flags, migrations, or dual paths.

## Change type and affected areas

This is a behavior-preserving structural refactor across two production boundaries introduced by `main...HEAD`:

- `composition` and `extension`: centralize scope applicability and decompose capability selection/compilation.
- `runtime`: replace defer-driven model-stream lifecycle state with an explicit attempt result and a focused stream receiver.

Tracked work: Beads issue `eino-agent-5m4`.

## Requested outcome

Eliminate the remaining Critical, Important, and P1 findings from the thermo-nuclear code-quality review while ignoring `docs/` and `examples/`, then verify, commit, push, and re-run the review.

Success means:

- `composition.Registry.acquire` is a short ownership coordinator rather than a capability compiler.
- Scope applicability has one canonical implementation in `extension`.
- Tools, prompts, guards, and restrictions project through focused typed helpers without repeated filtering or error-release branches in `acquire`.
- Tool definitions and durable hashes are frozen once during mount, not recomputed for every run-plan acquisition.
- `runtime.streamModel` no longer mirrors `err` into `streamErr` or encodes request lifecycle through `modelRequested` and `ledgerTransitionOK` booleans.
- Provider stream reading and message concatenation are isolated from durable request settlement, observability, notifications, and panic containment.
- Existing behavior and failure ordering remain covered by focused tests.
- The thermo-nuclear re-review reports no Critical, Important, or P1 findings.
- `make check` and `git diff --check` pass, and the committed branch is pushed and clean.

## Scope

In scope:

- `extension/registry.go` scope-selection ownership.
- `composition/registry.go` fresh/resume snapshot acquisition and capability compilation.
- `composition` and `extension` tests that prove scope selection, precedence, ownership, and release behavior.
- `runtime/model_stream.go` request-attempt coordination and provider-stream receiving.
- Runtime tests that prove ledger, notification, observer, usage, retry, panic, malformed-stream, and cancellation behavior.
- Plan documents under this directory.

Out of scope:

- All review and design changes under `docs/` and `examples/`.
- New extension point kinds or capability families.
- Changes to prompt precedence, tool selection, plan fingerprints, event payloads, retry eligibility, model-request state transitions, or stream usage semantics.
- Parallelizing tool execution or changing tool-result ordering.
- New compatibility, rollout, migration, or feature-flag machinery.

## Repository-grounded findings

1. Important/P1: `composition/registry.go:345` implements snapshot selection, prompt precedence, four capability-family filters, tool hashing/cloning/wrapping, plan assembly, and manual release in one 102-line function. The function duplicates the scope predicate at `extension/registry.go:523` through `composition/registry.go:498`.
2. Important/P1: `runtime/model_stream.go:16` is a 116-line lifecycle knot. Its deferred closure owns panic recovery, aggregate usage, durable model-request settlement, stream observation, and completion notification. The body synchronizes named `err` with `streamErr` on every exit and uses two booleans to represent lifecycle state.
3. The scoped audit of other changed production code found no blocker-grade findings. No production file crosses 1,000 lines, compatibility-only surfaces were deleted, and current storage, provider, tool, and Wasm boundaries remain cohesive.
4. The pre-change `make check` gate passes, including race tests, lint, module tidiness, and generated-code verification.

## Key decisions

1. Export one direct `extension.ScopeApplies(registration, target Scope) bool` predicate from the existing private implementation. `composition` consumes it instead of maintaining a near-duplicate.
2. Keep instance selection in `extension.Registry.Snapshot`/`SnapshotInstances`; keep capability policy in one substantive `composition` selection/compiler boundary. Do not add thin pass-through applicability or per-family wrappers merely to shorten `acquire`.
3. Freeze each tool definition and compute its durable schema/executor hashes during `Registrar.Tool`. Acquisition becomes non-failing projection through focused helpers for tools, prompts, guards, and restrictions. `runtime.NewRunPlan` receives dispatch ownership directly.
4. Change private `streamModel` to return one named proposed `modelStreamResult` containing message, usage, received-delta state, and error. The receiver mutates that named result progressively so deferred panic/finalization changes and partial state reach retry logic.
5. Use `session.ModelRequestRecord.State` as the sole lifecycle authority. Keep one small deferred coordinator in `streamModel` for panic recovery and guaranteed finalization; do not replace removed booleans with renamed flags.

Rejected alternatives:

- Moving capability compilation into generic `extension.Registry` would leak runtime-specific tools, prompts, guards, and restrictions into the generic lifecycle package.
- Adding a generic capability interface would replace four direct typed shapes with indirection and casts.
- Merely extracting chunks of the existing `acquire` body without centralizing scope selection would move the duplication without deleting it.
- Rechecking resume-instance membership after `SnapshotInstances` would preserve two selection authorities and a nullable fresh/resume mode.
- Re-hashing and re-cloning immutable tools during every acquisition would preserve unreachable errors and repeated cleanup branches.
- Retaining `streamErr` inside a new state wrapper would preserve the mirrored-error problem.
- Removing all deferred finalization would make panic settlement optional on some exits; the intended simplification is one explicit result plus one guaranteed finalizer.

## Target control flow

```text
Registry.acquire
  -> acquire selected extension snapshot
       SnapshotInstances is the sole resume-instance filter
  -> newPlanSelection(target, selectTool, snapshot values)
  -> selection.projectComponents()
       -> project mount-frozen tools and hashes
       -> select/project prompt winners
       -> project guards
       -> project restrictions
  -> runtime.NewRunPlan(dispatch + components)

streamModel
  -> prepare and durably mark request dispatch
  -> notify ModelRequested
  -> receiveModelStream(reader, *modelStreamResult, onDelta)
       -> progressively retain message chunks, usage, receivedDelta, and err
  -> deferred finalizeModelStreamAttempt(explicit state)
       -> recover panic into result error
       -> accumulate usage
       -> switch on ModelRequestRecord.State and settle durable request
       -> finish observation
       -> notify ModelCompleted only after a successful ledger transition
```

## Risks, assumptions, and stop/go gates

- Snapshot dispatch ownership is subtle. Stop if `runtime.NewRunPlan` cannot remain the sole release owner after it receives a dispatch plan; do not add a second refcount or compatibility wrapper.
- Prompt precedence must remain session-over-global and deterministic. Tests must cover mount-order permutations and resume selection before implementation is considered complete.
- A provider panic can occur during invocation, receive, or close. The finalizer must settle the request failed exactly once, retain already-observed usage/delta state, and omit completion when the durable transition fails.
- A model-request terminal transition failure currently replaces the provider/stream error. Preserve that observable error precedence unless repository tests prove a different canonical rule.
- Reader close runs exactly once. It has no returned error; a close panic supersedes success with the canonical provider-panic error, clears the message, and retains usage/delta state.
- No blocking decision remains. The user's compatibility and scope decisions are explicit.
- `.agents/plans/` is ignored by default, but this user-requested reviewed plan is a committed deliverable. Delivery must force-stage this exact plan directory and prove it exists in the commit tree.

## Document map

- [01-capability-plan-compilation.md](01-capability-plan-compilation.md): canonical scope applicability, selection, compilation, and dispatch ownership.
- [02-model-stream-attempt.md](02-model-stream-attempt.md): explicit attempt result, stream receiving, and lifecycle finalization.
- [03-verification.md](03-verification.md): focused regression matrix, structural thresholds, and full gates.
- [04-execution-handoff.md](04-execution-handoff.md): dependency order, files, commands, and final delivery protocol.
