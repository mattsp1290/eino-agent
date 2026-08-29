# Run Settlement Ownership

## Goal and prerequisites

Give fresh and resumed nonterminal runs one durable settlement implementation and one settlement caller per execution path. This work assumes the current fenced `session.ExecutionStore`, lease heartbeat, and `RunSettledPoint` contracts remain unchanged.

## Existing evidence

- `runtime/orchestrator.go:run` defers `finish`, which stops the lease, derives a default completed status, settles durably, and publishes the committed finish event.
- `runtime/interrupt.go:resumeRunWithSettlement` calls `finishResume` on four branches and returns unsettled results on other branches.
- `runtime/interrupt.go:executeResume` stops the lease separately, inspects `settled`, may call `finishResume`, and decides whether to notify extensions.
- `runtime/interrupt.go:Resume` constructs `runExecution` even for terminal runs, which depends on `newRunExecution` accepting a nil plan.
- `runtime/extension_plan_lifecycle_test.go` already tests settled-notice durability and resume duration; `runtime/tool_execution_test.go` covers resumed panic settlement.

## Exact change surface

- `runtime/orchestrator.go`
  - Rename existing `(*StreamingOrchestrator).finish` to proposed `settleRun`.
  - Keep its `(Result, bool)` result where the boolean means the durable settlement committed.
  - Call it exactly once from the deferred outer boundary in `run`.
- `runtime/interrupt.go`
  - Add proposed `terminalRunHandle(session.Run) Handle` beside `Resume`.
  - Return that handle immediately when `run.Terminal()` is true.
  - Rename `resumeRunWithSettlement` to proposed `resumeRun` and remove the `*bool` parameter.
  - Make `resumeRun` return only work results. Remove every settlement call from its branches.
  - Move resume lifecycle ownership to a named-result deferred closure in `executeResume`. Establish that defer before resume orchestration can panic. The defer must recover unexpected panics into `RunFailed`, call `settleRun` exactly once, finish observation with the final result, emit `RunSettledPoint` only when the settlement committed, deliver the result, and close `Done`.
  - Within that boundary, record resume, choose the resume start time, start observation, and invoke `resumeRun`.
  - Delete `finishResume`.
- `runtime/extension_execution.go`
  - Make `newRunExecution` panic on nil `plan` as part of its existing constructor invariant.
  - Delete the empty-plan substitution.
- `session/types.go:Store.Execution`
  - Document the existing invariant already assumed by `runtime/admission.go:admitDurable`: a valid fence returns a non-nil `ExecutionStore`; implementations must not use nil as an operational error channel.
- `store/storetest/contract.go`
  - Add an explicit contract assertion that `Execution` returns non-nil for an admitted run's valid fence.
- Tests and helpers
  - Replace the nil plan in `runtime/event_sink_test.go` with a real empty test plan.
  - Update direct `resumeRunWithSettlement` callers in `runtime/orchestrator_resume_test.go` to exercise the outer resume boundary when settlement is part of the assertion.
  - Update symbol references in `runtime/extension_plan_lifecycle_test.go` and `runtime/tool_execution_test.go`.

## Required behavior and invariants

- Terminal resume returns `Result{RunID, Status, Interrupted, Error}` equivalent to the durable terminal run and closes `Done` before returning the handle.
- Interrupting a terminal handle is a no-op and cannot panic; its cancel function must be non-nil.
- Nonterminal resume acquires a plan and durable claim before constructing `runExecution`.
- `Store.Execution` is a total capability constructor for a valid fence. A nil return is a store contract violation, not a recoverable runtime branch.
- `runExecution` always owns a real acquired or test-created `RunPlan` and releases it exactly once.
- Fresh and resumed execution stop the lease only inside `settleRun`.
- `settleRun` joins lease shutdown failure with any existing work failure and sets `RunFailed`.
- `settleRun` joins durable settlement failure with any existing work or lease failure and sets `RunFailed`.
- `settleRun` publishes `EventRunFinished` only from the committed settlement event.
- `RunSettledPoint` fires only when `settleRun` reports a durable commit.
- The observed run ends after settlement so it sees the final status and settlement error.
- An unexpected panic from resume orchestration becomes a failed result and cannot bypass lease shutdown, plan release, durable settlement attempt, observation completion, or handle completion.
- No resume worker branch may call `SettleRun` directly.

## Tests and acceptance criteria

Add or strengthen focused tests to prove:

- terminal `Resume` does not call the plan provider, claim the run, request an execution store, start a lease, or emit a new settlement;
- terminal handle `Done` is immediately readable and `Interrupt` is safe;
- a counting execution-store wrapper observes exactly one `SettleRun` call for each resumed path: no unfinished calls, tool panic, tool cancellation, tool-resolution failure, and successful tool processing;
- an injected panic at an unrecovered orchestration seam such as unfinished-call loading produces one failed result, one settlement attempt, one plan release, stopped lease activity, and a closed `Done` channel;
- a forced settlement failure yields no `RunSettledPoint` notice;
- fresh and resumed runs where work or cancellation fails and `SettleRun` also fails expose both causes through `errors.Is`, emit no `RunSettledPoint`, and finish observation with the combined error;
- a successful resumed settlement emits one notice after the durable finish event and observation uses the final result;
- existing fresh-run settlement tests remain green under the renamed shared method;
- constructing `runExecution` with a nil plan fails immediately.

Run at minimum:

```bash
go test ./runtime -run 'Test(StreamingOrchestratorResume|ExecuteResume|RunSettled|PendingResume|TerminalResume|RunExecution)'
go test -race ./runtime -run 'Test(StreamingOrchestratorResume|ExecuteResume|RunSettled|PendingResume|TerminalResume|RunExecution)'
go test ./store/... -run 'Test.*Contract'
```

## Dependencies, risks, and exclusions

- Complete this package before the model-boundary package only to reduce simultaneous churn in `runtime`; there is no semantic dependency.
- Do not alter durable run statuses or introduce a new terminal-resume error.
- Do not convert `Store.Execution` into a nullable result or add a parallel error-returning constructor. Enforce its existing non-nil contract at the interface and store contract-test boundary.
- Do not move event publication ahead of `SettleRun`.
- Do not broaden this package into removing every defensive nil check on `runExecution`; only remove the constructor fallback made unreachable by terminal short-circuiting.
