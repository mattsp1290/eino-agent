# Work Package 2: Model Lifecycle Pairing

## Goal and prerequisites

Prevent `ModelCompletedPoint` from being emitted for attempts that failed before `ModelRequestedPoint`. This package has no code prerequisite.

## Evidence

- `runtime/orchestrator.go` function `streamModel` installs its completion defer before `renderSystemPrompt` and `prepareModelRequest`.
- The deferred `ledgerTransitionOK` variable begins true and only changes when final request-record transition fails.
- `ModelRequestedPoint` is emitted after rendering and request-record preparation/dispatch-start transition.
- `runtime/ledger_test.go` contains an unsafe request test that proves provider dispatch is skipped and a provider-panic test that proves dispatched failures receive completion.

## Exact change surface

- Modify existing `runtime/orchestrator.go` function `(*StreamingOrchestrator).streamModel`.
- Add or extend lifecycle regression tests in existing `runtime/ledger_test.go`; use another existing `runtime/*_test.go` file only if its helpers make the event sequence substantially clearer.
- Do not change `ModelRequestedNotice`, `ModelCompletedNotice`, extension contracts, or ledger schemas.

## Intended behavior and invariants

- Introduce a per-call boolean such as `modelRequested`, initialized false.
- Set it at the existing `ModelRequestedPoint` notification site when a dispatch plan exists.
- Emit deferred `ModelCompletedPoint` only when `modelRequested && ledgerTransitionOK` and the plan dispatch remains available.
- Pre-dispatch prompt-render, audit, oversized-input, request-create, and dispatch-start-transition failures emit neither request nor completion notification.
- After request notification, provider open failure, nil reader, receive failure, cancellation, panic, and successful completion emit exactly one completion notification.
- Preserve usage accumulation and observed-stream completion/error behavior on every path.
- Preserve the rule that a failed final ledger transition suppresses completion notification.

## Tests and acceptance criteria

Create a test component that records both `ModelRequestedPoint` and `ModelCompletedPoint`. Install the same non-nil dispatch containing both observers for every lifecycle case, including pre-dispatch failures. Prove the fixture is live with the success case and verify the pre-dispatch regression fails against the pre-change implementation. Exercise:

1. A mandatory dispatch-start transition failure. Wrap a `session.ModelRequestStore` so its first `UpdateModelRequest` call for `session.ModelRequestDispatchStarted` fails. Assert the provider was not called and the lifecycle sequence is empty.
2. A pre-dispatch audit failure such as unsafe audited input. Assert the provider was not called and the lifecycle sequence is empty while the same active dispatch is installed.
3. A dispatched failure such as provider panic or provider-open error. Assert the sequence contains one requested event followed by one completed event with a classified error.
4. A successful provider response. Assert the sequence contains one requested event followed by one completed event.

Reuse existing setup when practical, but assert the pair in one focused test or tightly related tests so future changes cannot preserve only one side.

Run:

```bash
go test ./runtime -run 'Test.*Model.*(Lifecycle|Notice|Request|Ledger)'
```

Acceptance requires zero requested and completed notifications at both audited-request rejection and dispatch-start transition failure while a live dispatch is installed. Completion must never occur without a preceding request. Counts are equal when final ledger transition succeeds. Preserve the intentional ledger-finalization-failure exception with `requested=1` and `completed=0`.

## Dependencies, risks, and exclusions

- This package can be implemented independently of Work Package 1.
- Do not gate observability span finalization or usage accumulation on `modelRequested`; only gate the extension completion notification.
- Do not move `ModelRequestedPoint` earlier to make the events pair. Earlier notification would incorrectly report rejected inputs as provider attempts.
- Do not suppress completion for provider failures that occur after request notification.
- Do not use a nil plan or nil dispatch for the zero-event regressions; that fixture passes against the broken implementation.
