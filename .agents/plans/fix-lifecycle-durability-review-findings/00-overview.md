# Fix Lifecycle and Durability Review Findings

Status: Implemented and verified with `make check` on 2026-08-23.

## Requested outcome

Correct all five supplied review findings without widening public contracts:

1. Session-scoped extension registrations accept every non-empty durable session ID.
2. `RunSettledPoint` fires only after the run's terminal state is durably stored on fresh and resumed execution.
3. Legacy `AfterToolCall` middleware runs only after tool execution, while extension result transformation remains available for preparation failures.
4. A panic after model dispatch leaves the optional model-request ledger in `failed`, not `completed`.
5. SQLite reconciliation repairs a missing reserved result part even when its reserved message already exists.

Success requires focused regression tests for each behavior plus the repository quality gates in [03-execution-handoff.md](03-execution-handoff.md).

## Scope

- `extension/types.go` and extension/composition tests for opaque session scope keys.
- `runtime/orchestrator.go`, `runtime/interrupt.go`, and runtime tests for settlement, middleware, and panic lifecycle behavior.
- `store/sqlite/store.go` and SQLite tests for partial tool-result reconciliation.
- No public API additions, schema migrations, generated WIT changes, or changes to identifier validation for contracts, registrations, components, or artifacts.

## Repository findings

- `session.ID` is an opaque string and runtime admission requires only a non-empty session ID. `extension.validateTargetScope` already follows that rule, but `validateScope` incorrectly applies `identifierPattern`.
- `execute` and `executeResume` currently emit `RunSettledPoint` based on an in-memory `Result`. Their finish helpers do not expose whether `Store.FinishRun` succeeded.
- `preparedToolCall.middlewareErr` represents both legacy `BeforeToolCall` and `ToolPreparePoint` failures. `executePreparedTools` skips execution for these outcomes but calls `afterToolOutcome`, which combines legacy after middleware with `ToolResultTransformPoint`.
- `streamModel` has named return values and a ledger-finalization defer, but that defer interprets `streamErr == nil` as success during panic unwinding. `run` catches the panic only outside that boundary.
- `ListUnreconciledToolSettlements` checks only `GetMessage`. `SettleToolCall` appends both records idempotently, so a repair settlement must preserve any exact existing reserved record to avoid a false conflict on legacy metadata.

## Key decisions

- Treat session scope keys as opaque and reject only the empty string. Do not trim or normalize because durable identity equality is byte-for-byte.
- Thread an internal durable-settlement boolean from `finish`/`finishResume` through their execution paths. Do not infer settlement from `Result.Status`, because failure results can describe work that was not durably finished.
- Split legacy after middleware from extension result transformation. Preparation failures take only the transform path.
- Recover provider panics inside `streamModel`, preserve the existing provider-panic error shape, and let the existing defer write `ModelRequestFailed` before returning the error to normal run failure handling.
- Check both reserved records during SQLite reconciliation. Reuse an existing full message or part record in the returned settlement and reconstruct only the missing record.

## Change model

```text
fresh/resume execution
  -> terminal FinishRun succeeds -> settled=true -> RunSettledPoint
  -> FinishRun fails              -> settled=false -> no RunSettledPoint

tool preparation
  -> succeeds -> execute -> legacy AfterToolCall -> ToolResultTransformPoint
  -> fails    -> no execute -> no legacy AfterToolCall -> ToolResultTransformPoint

model dispatch
  -> panic inside stream boundary -> recover as stream error
  -> ledger dispatch_started -> failed -> run failure
```

## Risks and constraints

- Internal signature changes must update direct package tests of `resumeRun` without changing exported `Resume` or `Result`.
- Panic recovery must not swallow failure or return a successful message. It must set the named error return and clear the named message return before ledger finalization.
- Existing message/part records used for repair must remain exact copies. Reconstructing an already-present message can conflict on fields unavailable from `ToolCall`, such as `ModelID`.
- The current branch is clean at planning start. The plan documents and implementation are the only intended Git changes.

## Decisions and open questions

- Decision: implement all five supplied findings. The request selects the full review set.
- Blocking questions: none.
- Non-blocking questions: none.

## Non-goals

- Redesigning run hooks, event sinks, or all lifecycle notifications.
- Adding panic containment for arbitrary legacy `AfterRun`, event-sink, or observability callbacks.
- Changing the semantics of `EventRunFinished` or legacy `AfterRun` hooks beyond the requested `RunSettledPoint` gate.
- Expanding durable store interfaces with public message/part lookup methods.

## Document map

- [01-scope-and-reconciliation.md](01-scope-and-reconciliation.md): opaque scope validation and SQLite partial-record repair.
- [02-runtime-lifecycle.md](02-runtime-lifecycle.md): durable run settlement, preparation failure phases, and model panic ledger state.
- [03-execution-handoff.md](03-execution-handoff.md): dependency order, focused verification, full gates, and definition of done.
