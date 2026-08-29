# Run Settlement and Execution Capability

## Goal and prerequisites

Make terminal run state and its final event one required, replay-safe transition. Construct runtime mutation authority once from the admitted or claimed run fence.

Prerequisite: none for session/store work. Finish the session contract before adapting runtime and test doubles.

## Repository evidence

- `session.ExecutionStore.SettleRun` accepts `Run` plus nullable `*EventRecord` and returns nullable `*EventRecord`.
- `runtime.finalRunEvent` can return nil, although configured fresh and resume paths require an ID generator.
- `store/sqlite.executionStore.SettleRun` writes an identical terminal run as a no-op and can then append a second `run_finished` event with a new ID.
- `store/sqlite/migrations/001_sqlite_store.sql` reserves tool transition phases with a partial unique index but does not reserve run settlement.
- `runtime.runExecution.bindRun` mutates the execution capability. `ensureStore` retrieves the current run from the top-level store and rebuilds authority.
- Fresh and resume entry points know the admitted or claimed `Run` before asynchronous execution begins.

## Work package C: canonical required run settlement

Proposed session symbols, anchored beside `session/tool_transition.go`:

- `session.RunSettlementEventKind = "run_finished"`.
- `session.RunSettlement`: caller-owned terminal fields only: status, finished time, and error text.
- `session.RunSettlementEvent`: bounded fields that terminal run state cannot derive: event ID, message ID, usage, error code, and retryability.
- `session.SettleRunRequest { Settlement, Event }`.
- `session.RunSettlementResult { Run, Event }`.
- `session.ApplyRunSettlement(currentRun, settlement)` (proposed name): copies caller-owned terminal fields onto the current fenced run while retaining store-owned session, ownership, claim, lease, config, and plan fields.
- `session.RunSettlementRecord(canonicalRun, event)`: validates terminal state and required identities, derives the complete payload and fixed event fields, and returns the canonical `EventRecord`.

Change `session.ExecutionStore.SettleRun` to:

```go
SettleRun(context.Context, SettleRunRequest) (RunSettlementResult, error)
```

Exact validation:

- Require a terminal settlement status and non-zero finished time.
- Require a non-empty event ID.
- Load the current run through the fence. Construct the canonical terminal run from that stored record, replacing only status, finished time, and error text.
- Derive `EventRecord.SessionID`, `RunID`, `ProviderID`, `ModelID`, `Kind`, and `CreatedAt` from the canonical run; callers cannot override them.
- Derive the payload from `Run.Status`, with `interrupted` true only for `RunInterrupted`.
- Derive `EventError.Message` from `Run.Error` and fix redaction to `RedactionMetadata`.
- Reject a successful run with non-empty error code or retryability. Failed/interrupted runs may carry classification code and retryability because those values are not stored on `Run`.
- Require settlement through the matching run fence.
- Define replay equality over the complete canonical terminal run and canonical event. A stale caller snapshot cannot overwrite current store-owned lease or ownership fields.

SQLite changes:

- In `store/sqlite/migrations/001_sqlite_store.sql`, add a partial unique index on `(run_id, kind)` for `kind = 'run_finished'`.
- In `store/sqlite/schema.go`, add the index to the exact schema fingerprint. Do not add a migration or increment the schema version.
- In `store/sqlite/execution.go`, validate the request's context-independent fields before opening the fenced transaction.
- Within one transaction, load the fenced stored run, derive the canonical terminal run and event, compare any existing terminal state and event, then write both.
- On replay, load the existing event for the run/phase. Return the original `RunSettlementResult` if both terminal run and canonical event are identical.
- Return `session.ErrConflict` for a different event ID, envelope, terminal status, error, usage, or payload.
- In `ExecutionStore.AppendEvent`, reject records whose kind is `RunSettlementEventKind`, so ordinary append cannot consume the reserved phase.

Contract tests:

- Replace nil-event settlement helpers in `store/storetest` with deterministic valid envelopes.
- Prove the method signature has no nullable event parameter and a zero-valued event envelope returns `session.ErrConflict`.
- Prove terminal state and event roll back together when event insertion fails.
- Prove identical replay returns the original run and event.
- Prove replay with a new event ID conflicts and event count stays one.
- Prove replay with the same event ID but changed usage, classification, run status, or run error conflicts.
- Prove ordinary `AppendEvent` rejects `run_finished`.
- Keep stale-fence, cross-session, and nonterminal rejection coverage.
- Prove concurrent identical requests both return the same canonical run/event.
- Prove concurrent different events produce one canonical success and one `ErrConflict`, with the stored run/event matching the winner.
- Renew a run lease after taking a caller snapshot, settle from terminal fields derived from that stale snapshot, and prove the canonical result retains the renewed lease. Identical replay must return that same canonical result.

## Work package D: immutable execution capability

Change surface:

- `runtime/extension_execution.go`
  - Change `newRunExecution` to accept the authoritative `session.Run`.
  - Derive `store` once through `host.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})`.
  - Remove `bindRun` and `ensureStore`.
  - Keep `store` private and never reassign it.
- `runtime/orchestrator.go`
  - Keep plan acquisition and admission ordering unchanged.
  - Publish the persisted admission event using a small plan-scoped helper or `runEventSink` before execution construction.
  - Construct `runExecution` immediately after successful admission.
  - Make `finalRunEvent` return `session.RunSettlementEvent` by value and construct `session.RunSettlement` from result status, one finish timestamp, and error text.
  - Adapt `finish` to the new required settlement request/result and publish the committed result event.
- `runtime/interrupt.go`
  - Construct execution after loading or claiming the run.
  - Remove nil-store rebinding from `executeResume` and `resumeRunWithSettlement`.
  - Adapt `finishResume` to the required settlement request/result.
- `runtime/tool_execution.go`
  - Call the sealed `ExecutionStore` directly in create, claim, and settle helpers.
  - Retain cancellation-free context for terminal tool settlement.
- Adapt runtime test doubles and fixtures to supply a valid admitted or claimed run when constructing execution.

Invariants:

- A production `runExecution` never exists without a store capability scoped to its run ID and claim token.
- No method can replace or reacquire that capability.
- Admission notifications still use the exact frozen plan that is later owned and released by execution.
- A failed admission or claim releases the plan exactly once.
- A successful fresh or resumed execution releases the plan exactly once.
- Terminal resume can construct a fenced store for read-only result handling, but performs no mutations.

Acceptance tests:

- Existing fresh, resume, interrupt, tool, extension plan lifecycle, and race tests pass.
- A focused constructor/lifecycle test verifies the execution store is derived once from the supplied fence.
- Removing or corrupting the claim token causes construction or the first fenced operation to fail; runtime never calls `Store.GetRun` to recover.
- Admission event and `RunAdmittedPoint` ordering remains before asynchronous execution.

## Dependencies, risks, and exclusions

- Implement Work package C before adapting Work package D because runtime construction and settlement tests share `ExecutionStore` doubles.
- Put portable concurrent settlement semantics in `store/storetest` and retain SQLite-specific transaction/constraint coverage where the reusable interface cannot inspect rows directly.
- Cover lease-renewal replay in the reusable store contract so all backends preserve store-owned fields.
- Preserve atomic transaction boundaries and `context.WithoutCancel` on terminal run settlement.
- Do not add a public unfenced mutation method.
- Do not preserve nullable result/event handling for compatibility.
- Do not create a database backup or migration path; the confirmed system has no users or deployed data.
