# Runtime Boundary Simplification

Status: Implemented. Planning and all required reviews completed before implementation began; at plan approval, the changes described here had not been implemented.

Tracking issue: `eino-agent-0ub`

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "afaaeeb97434731aca17923e190f2780138075d7ddd3b7d4390e1f8f8bebd6dd",
    "confirmed_at": "2026-08-29T16:08:05Z"
  }
}
```

The user confirmed that this code has no users and that backward compatibility is dead code. Implementation must replace obsolete paths directly. It must not add shims, migrations, feature flags, aliases, deprecated entry points, or parallel implementations.

## Change classification

This is a maintainability refactor with intentional greenfield API and error-policy breaks. It affects:

- resumed and fresh run settlement in `runtime/interrupt.go` and `runtime/orchestrator.go`;
- per-run execution invariants in `runtime/extension_execution.go`;
- required-around error ownership in `extension/dispatch.go`;
- tool execution error flow in `runtime/orchestrator.go`;
- model request cloning and audit projection in `runtime/ledger.go` and `runtime/model_stream.go`;
- provider-request assembly in `runtime/provider.go`;
- focused tests in `extension/*_test.go` and `runtime/*_test.go`.

No stored schema, wire contract, configuration, or external workflow changes are required.

## Requested outcome

Remove the three structural defects found by the thermo-nuclear review:

1. Give one outer lifecycle boundary exclusive ownership of run settlement.
2. Make `extension.InvokeAround` the canonical owner of delegated-plus-callback error preservation so runtime tool execution can return ordinary errors.
3. Clone and validate each model request once, then derive the ledger projection from that canonical request.

## Success criteria

- Every fresh or nonterminal resumed run calls one shared settlement function from one outer boundary.
- Resume work returns a `Result`; it never settles a run or mutates a shared `settled` flag.
- A terminal `Resume` returns an already-completed handle without acquiring a plan, execution fence, or lease.
- `newRunExecution` rejects a nil `RunPlan`; the nil-plan compatibility fallback is deleted.
- Required-around dispatch preserves both a delegated failure and an independent callback failure while keeping callback error text bounded.
- `executeToolOutcome` has no `executorErr` or `callbackErr` side channels and returns the tool executor error through `InvokeAround` normally.
- Model stream preparation performs one defensive `model.Request.Clone` inside audit preparation and does not pre-clone messages, options, or trace attributes that the canonical clone immediately replaces.
- The duplicate message safety visitors in `runtime/ledger.go` are deleted.
- Targeted extension, runtime, model, and race tests pass, followed by `make check` and `git diff --check`.
- The task is committed and pushed with the branch clean and up to date with its upstream.

## Repository findings

Verified facts:

- `runtime/interrupt.go` currently passes `*bool` settlement state through `resumeRunWithSettlement`, calls `finishResume` on selected branches, and conditionally calls it again in `executeResume`.
- `runtime/orchestrator.go` has a separate `finish` implementation with the same lease-stop, durable-settlement, and event-publish responsibilities.
- `runLeaseHeartbeat.stopOnce` prevents a channel-close panic, but repeated callers still obscure lifecycle ownership and wait on the same heartbeat.
- `runtime/extension_execution.go:newRunExecution` silently replaces a nil plan with `&RunPlan{}`. Production reaches that path only for terminal resume; `runtime/event_sink_test.go` is the other direct caller.
- `runtime/orchestrator.go:executeToolOutcome` captures executor and callback failures in mutable closure variables because its terminal callback returns nil errors.
- `extension/dispatch.go:InvokeAround` tracks the delegated failure but discards an independent callback failure when the callback error tree contains `delegatedError`.
- `model.Request.Clone` rejects deprecated `MultiContent`, streaming metadata, message `extra`, and tool `Extra` while deep-copying the request.
- `runtime/ledger.go:AuditModelRequest` repeats the deprecated-field and recursive `extra` checks after `runtime/model_stream.go` has already cloned the request.
- `runtime/provider.go:ProviderRequest` shallow-clones the snapshot message slice and deep-clones option and trace maps before `runtime/model_stream.go` replaces the messages and `model.Request.Clone` clones the complete graph again.
- `AuditModelRequest` has no production caller outside `runtime/model_stream.go`; its remaining callers are same-package tests.

## Key decisions

1. Rename the shared run finalizer to the proposed `settleRun` and call it only from the outer fresh-run and resume execution boundaries. Delete `finishResume` and pointer-based settlement bookkeeping.
2. Short-circuit terminal resume with the proposed `terminalRunHandle`. This preserves the useful result contract without manufacturing a nil-plan execution object.
3. Treat direct return of the private `delegatedError` as pure propagation. If a callback adds or joins another error after delegation failed, join the original delegated failure with a bounded `CallbackError` for the callback-authored error tree.
4. Make the proposed private `auditModelRequest` return `(model.Request, AuditedModelInput, string, error)`. It clones first and projects only from the returned canonical request.
5. Keep the deprecated-shape rejection in `model.Request.Clone`. It is a canonical safety invariant, not compatibility support.

Rejected alternatives:

- Keep both run finalizers behind a common helper: this retains two settlement owners and the pointer protocol.
- Keep runtime error side channels while also fixing `InvokeAround`: this creates two competing error policies.
- Add a compatibility wrapper for exported `AuditModelRequest`: there are no users and no compatibility requirement.
- Add a rollout flag: both application-context booleans are false, so flags are not applicable.
- Roll forward through a shim after a regression: recovery is a whole-commit Git revert followed by the full quality gates and push.

## Target control flow

```text
fresh Start ──> run work ───────┐
                               ├──> settleRun exactly once ──> observation end ──> RunSettled notice
nonterminal Resume ─> work ────┘

terminal Resume ─> terminalRunHandle (no plan, fence, lease, or settlement)

tool executor error ─> InvokeAround delegated error policy ─> permission wrapper ─> toolOutcome

provider request ─> auditModelRequest clone ─> canonical request + audited projection + hash
```

## Scope and constraints

In scope:

- The three reviewed structural findings and tests that establish their invariants.
- Direct deletion of now-unused helpers and imports.
- Test helper updates required by the non-nil `RunPlan` invariant.

Out of scope:

- New extension points, persistence schemas, provider behavior, permission semantics, or public features.
- Broad removal of unrelated nil guards on `runExecution` methods.
- Changes to persisted event ordering beyond making observation and settlement ownership explicit.
- Documentation rewrites unrelated to changed public API surface.

## Risks and gates

- Blocking decisions: none.
- Go gate: verify the exact error tree for direct delegated propagation versus a callback-added error before deleting runtime side channels.
- Lifecycle gate: prove settlement is attempted once for success, panic, cancellation, no-unfinished-call, and pre-tool resume failures.
- Failure-composition gate: prove work, lease, and settlement failures remain discoverable when more than one occurs.
- Observation gate: preserve `RunSettled` notification only after durable settlement and finish the observed run with the final settled result.
- Ledger gate: compare audited projection and content hash against the canonical clone, not the caller-owned request.
- Recovery gate: this change has no data migration. Rollback is a whole-commit revert; it must not restore compatibility through an alias, flag, or dual path.
- Dirty-worktree gate: implementation started from a clean tracked worktree; preserve unrelated changes if any appear.

## Document map

- `01-run-settlement.md`: single-owner fresh/resume lifecycle and execution invariants.
- `02-around-error-policy.md`: canonical required-around error composition and runtime simplification.
- `03-model-request-boundary.md`: one canonical clone plus audited ledger projection.
- `04-execution-handoff.md`: dependency order, verification gates, delivery, and definition of done.
