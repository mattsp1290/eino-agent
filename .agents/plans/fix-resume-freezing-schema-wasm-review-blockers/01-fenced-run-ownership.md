# Fenced Run Ownership

## Goal and prerequisite

Make admission and resume establish a single durable run executor, and make every subsequent execution-owned durable mutation conditional on that executor's unique claim token. This package is the prerequisite for changing resume recovery behavior.

## Existing evidence

- `session/types.go` defines `Run` and `Store`; `Run` currently has `OwnerID` and `LeaseUntil` but no execution token.
- `session/resume.go` grants `ResumeOwned` solely from owner-string equality.
- `runtime/admission.go` and `runtime/orchestrator.go` create the initial run and lease.
- `runtime/interrupt.go` classifies, mutates, and finishes a resumable run without an atomic store claim.
- `store/sqlite/store.go` implements `RenewRunLease` as read plus `FinishRun`, and `FinishRun` conditions only on current status.

## Change surface

- `session/types.go`: add `Run.ClaimToken`; add `RunFence` and proposed `RunClaim` values; split raw store authority from a proposed `ExecutionStore` bound by `Store.Execution(RunFence)`; add atomic `ClaimRun` and `SettleRun` contracts.
- `session/resume.go`: remove `ResumeOwned` and owner input from classification; classify only terminal, live lease, or stale lease.
- `session/errors.go` or the existing session error insertion point: reuse `ErrConflict` for a lost claim and `ErrSessionBusy` for a live lease; do not add compatibility aliases.
- `store/sqlite/migrations/001_sqlite_store.sql`: persist indexed/current `claim_token` and `lease_until` columns required for conditional updates.
- `store/sqlite/store.go`: implement claim and renewal as single conditional SQL transactions/updates using SQLite time expressions and caller-provided durations; implement an execution-scoped wrapper whose writes atomically verify its fence.
- `runtime/orchestrator.go`: allocate a fresh claim token with the configured ID generator in `Start` and pass it in `AdmissionIDs`.
- `runtime/admission.go`: add `AdmissionIDs.RunClaimToken`, validate it, persist it in the initial run, and include it in duplicate-admission identity checks.
- `runtime/interrupt.go`: acquire the persisted plan, attempt `ClaimRun`, release the plan on loss, and execute only the returned claimed record.
- `runtime/orchestrator.go`: remove owner equality as authority; carry the token through execution and settlement.
- `runtime/lease.go` (new, parent `runtime/`): run-scoped heartbeat lifecycle shared by Start and Resume.
- `session/types.go` and model-request/store extension interfaces: move every post-admission message, part, event, tool-call, model-request, and context-epoch mutation onto the single proposed `ExecutionStore` capability bound to `RunFence{RunID, ClaimToken}`. Remove equivalent unscoped mutation methods from runtime-facing interfaces.
- `runtime` execution, ledger, event, tool, and compaction paths: pass the current fence through every durable mutation; admission-only bootstrap writes remain explicitly outside the claimed execution boundary.
- Store fakes in `runtime/*_test.go` and the conformance subject in `store/storetest/contract.go`: implement the new contract with the same atomic semantics.

Proposed conceptual API shape:

```text
RunFence{RunID, ClaimToken}
RunClaim{RunID, OwnerID, ClaimToken, LeaseDuration}
Store.AdmitRun(ctx, run, leaseDuration) (Run, error)
Store.ClaimRun(ctx, claim) (Run, error)
Store.Execution(fence) ExecutionStore
ExecutionStore.RenewRunLease(ctx, leaseDuration) (Run, error)
ExecutionStore.AppendMessage/AppendPart/UpdatePart/AppendEvent(...)
ExecutionStore.CreateToolCall/ClaimToolCall/SettleToolCall(..., leaseDuration where applicable)
ExecutionStore.Create/UpdateModelRequest(...)
ExecutionStore.Start/FinishContextEpoch(...)
ExecutionStore.SettleRun(ctx, run, finalEvent) error
```

Exact record-method names may follow established naming, but this single scoped capability boundary, duration inputs, store clock, and conditional semantics are mandatory. `runExecution` owns the `ExecutionStore`; runtime code must not retain an unscoped write alternative.

## Invariants and error paths

- Admission rejects an empty owner label, empty token, or non-positive/out-of-bounds lease duration.
- Every admission/resume attempt gets a new unpredictable-enough unique token from the existing ID generator; tokens are not reused across attempts.
- Repeating `Admit` with the identical caller-supplied IDs and token is idempotent; reusing a run ID with another token is `session.ErrConflict`.
- `AdmitRun`, `ClaimRun`, `RenewRunLease`, and `ClaimToolCall` accept positive bounded durations. The store compares and stamps deadlines from one authoritative store clock and returns the persisted deadline. `WithClock` remains for domain and observability timestamps only.
- `ClaimRun` succeeds only for a nonterminal run whose persisted lease is expired according to the store clock.
- Two simultaneous `ClaimRun` calls against one stale run yield one success and one `session.ErrConflict` or `session.ErrSessionBusy` after reread.
- `RenewRunLease` and `SettleRun` affect exactly one current row matching `run_id + claim_token`; zero rows return `session.ErrConflict`.
- A stale worker cannot renew or finish after another worker claims the run.
- A stale worker cannot append/update messages or parts, append events, mutate model-request ledger state, create/claim/settle tool calls, or mutate context epochs after another worker claims the run. SQLite checks the current nonterminal `runs.id + claim_token` in the same statement or transaction as each mutation.
- `SettleToolCall` validates both the tool-call claim token and the current run fence.
- `SettleRun` validates the run fence, changes terminal state, and appends the final durable event in one transaction. Live `EventSink` transport receives the completion notice only after commit and carries no durability authority.
- Intermediate durable events use `ExecutionStore.AppendEvent`; transport-only publication remains separate and cannot write session state.
- Normalized `runs.status`, `owner_id`, `claim_token`, and `lease_until` columns are authoritative. Every run read overlays them on decoded JSON. Lease renewal updates only `lease_until` under the token fence and cannot overwrite another run field.
- The heartbeat interval is bounded below for tests and derived from `RunLeaseDuration` (for example one third of the duration). It renews before expiry and cancels the run context on failure.
- The execution path stops and joins the heartbeat before terminal settlement so renewal cannot race with `SettleRun`. Resume starts its heartbeat only after `ClaimRun`; Start begins only after the initial fenced transition is durable.
- Starting or claiming a running tool call sets the run lease to at least the tool-call lease deadline before external execution begins.
- Resume may mark a persisted `running` tool call interrupted only after the store has granted the stale run claim. If a contradictory live tool lease is observed, fail closed and do not invoke the tool again.

## Tests and acceptance

- `session/resume_test.go`: same owner plus live lease is busy; different/same owner plus expired lease is stale; terminal remains terminal.
- `store/sqlite/store_test.go`: atomic claim success, live-lease rejection, concurrent single winner, stale-token renew rejection, stale-token finish rejection, current-token finish success.
- Store regression tests block an old executor, transfer the claim, and prove every old-fence message/part/event/model-request/tool/context mutation returns `session.ErrConflict` and persists nothing.
- Run read tests prove renewal is visible through `GetRun`, `ActiveRun`, and `ListUnfinishedRuns` without rewriting concurrent status/record fields.
- `store/storetest/contract.go`: generic versions of token fencing and claim behavior.
- `runtime/interrupt_test.go`: two resumptions race and only one starts plan execution; a same-owner live execution cannot be resumed; plan references release on claim loss.
- Runtime heartbeat tests use a short lease and blocking fake model/tool to prove the run remains busy past its original deadline.
- Clock-skew tests give competing orchestrators deliberately fast/slow injected clocks and prove those clocks cannot reclaim a live store-timed lease or create an already-expired lease.
- Settlement tests prove terminal status and the final durable event appear together, and rollback together on a forced event-write or fence failure.
- Admission tests prove identical IDs plus token are idempotent and a different token for the same run conflicts.
- A forced renewal failure cancels execution, returns/persists a failure through the still-valid token when possible, and never continues model/tool work.
- `go test ./session ./store/sqlite ./store/storetest ./runtime` passes with the race-sensitive tests repeated where practical.

## Dependencies and exclusions

- Coordinate schema column changes with [03-current-only-schemas.md](03-current-only-schemas.md); there is no migration from the old shape.
- Do not add distributed consensus, a second lock service, or cross-database leases.
- Do not use `OwnerID` as a conditional fence.
- Do not pass runtime `now` or absolute deadlines into store lease decisions.
