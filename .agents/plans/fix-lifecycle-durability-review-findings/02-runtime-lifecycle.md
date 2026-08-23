# Runtime Lifecycle Corrections

## Work package 3: gate run-settled notifications on durable completion

Goal: never publish `RunSettledPoint` unless terminal `FinishRun` succeeds, while preserving notices for every successfully persisted terminal status.

Repository evidence:

- Existing `runtime/orchestrator.go:execute` and `runtime/interrupt.go:executeResume` notify unconditionally after their run helpers return.
- `finish` and `finishResume` are the only terminal persistence boundaries for those execution paths.
- Early resume failures before `finishResume`, including reconciliation and tool resolution errors, return a failed in-memory result while leaving the run nonterminal.

Exact change surface:

- Change the private fresh-run flow so `finish` reports both the resulting `Result` and whether `Store.FinishRun` returned nil; thread that flag through `run` to `execute`.
- Change the private resume flow so `finishResume` reports the same boolean; early returns from `resumeRun` report `false`; thread it to `executeResume`.
- Emit `RunSettledPoint` only when the boolean is true. Preserve plan release, done-channel delivery, notification context, and resume duration behavior.
- Update direct private-helper tests for any internal signature changes.
- Add outcome-matrix tests through `execute` and `executeResume`: fresh completed result plus successful terminal `FinishRun` emits exactly one notice; fresh failed result plus successful terminal `FinishRun` emits exactly one failed notice; fresh terminal persistence failure emits none; resume pre-finalization failure emits none; resume terminal `FinishRun` failure emits none; resumed interrupted/failed result plus successful terminal `FinishRun` emits exactly one notice.
- Make the controlled store failure apply only to terminal-status `FinishRun` calls so the resume terminal-failure case passes the earlier running-state persistence boundary.

Acceptance criteria:

- In-memory `RunFailed` does not imply settlement.
- The notification gate depends only on the terminal `FinishRun` return value, never `Result.Status`, `Result.Error`, or `Result.Interrupted`.
- A later successful retry/resume can emit the single settlement notice.
- Already-terminal `Resume` behavior does not acquire a plan or emit a new notice.

## Work package 4: preserve the execution-only legacy after phase

Goal: skip legacy `AfterToolCall` when preparation fails without removing result transformation from the failed durable tool outcome.

Repository evidence:

- `runtime/orchestrator.go:afterToolOutcome` currently performs legacy `afterToolCall` and `ToolResultTransformPoint` in one helper.
- `executePreparedTools` calls it for both executed outcomes and `prepared.middlewareErr` outcomes.
- The existing tool middleware contract describes the after phase as paired with execution, while extension result transformation operates on terminal outcomes.

Exact change surface:

- Refactor existing `afterToolOutcome` responsibilities into an execution-only legacy-after step and a shared extension result-transform step.
- For `prepared.middlewareErr == nil`, execute the tool, run legacy after middleware, then run the extension transform.
- For `prepared.middlewareErr != nil`, construct the failed outcome and run only the extension transform.
- Preserve permission metadata reapplication, cloning, sealing, error joining, disposition calculation, durable settlement, sibling-call continuation, and model-visible output.
- Extend runtime tests so both a legacy `BeforeToolCall` error and a `ToolPreparePoint` error prove: executor count is zero, legacy after count is zero, transform count is one when configured, and the durable/model-visible result remains failed.

Acceptance criteria:

- Legacy after middleware remains reverse ordered after actual tool execution.
- Neither preparation failure path calls any legacy after middleware.
- `ToolResultTransformPoint` can still transform the failed outcome.

## Work package 5: fail panicking model-request ledger records

Goal: finish a dispatched ledger record as `ModelRequestFailed` when provider stream creation, receipt, or close panics.

Repository evidence:

- `runtime/orchestrator.go:streamModel` transitions a non-nil request record to completed when `streamErr` is nil in its defer.
- The outer `run` defer recovers provider panics only after the inner ledger defer has already recorded completion.
- `streamModel` has named `message` and `err` returns, so the inner boundary can recover and return the panic as a normal stream failure.

Exact change surface:

- Recover at the start of the existing `streamModel` finalization defer.
- Preserve the existing `provider stream panic: %v` error shape, assign it to `streamErr` and named `err`, and clear named `message`.
- Let the existing ledger update, observed-stream error, completion notification, retry, and run-failure paths consume that error.
- Add a focused case in `runtime/ledger_test.go` with a ledger-enabled streamer that panics after dispatch. Assert the run fails, the single durable record is `ModelRequestFailed` and never `ModelRequestCompleted`, and one `ModelCompletedPoint` notice carries a non-empty classified error rather than a successful completion.
- Cover either stream creation or `Recv`; prefer the smallest deterministic provider double. The recovery location must protect both.

Acceptance criteria:

- The ledger transition is `dispatch_started -> failed`.
- The caller receives a failed run with the existing provider-panic error shape.
- Exactly one model-completed signal is produced for the panicking request and it is error-classified, never success-shaped.

Risk: recovery must occur before choosing the ledger state. A second outer recovery alone cannot correct the already-terminal ledger record because completed-to-failed is not a valid transition.
