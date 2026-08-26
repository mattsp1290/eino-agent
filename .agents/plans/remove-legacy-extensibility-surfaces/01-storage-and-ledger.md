# Storage and Ledger Boundary

## Goal and prerequisites

Make execution fencing structurally unavoidable for run-owned writes and make the optional model-request ledger an execution-scoped responsibility.

Prerequisite: preserve the existing `AdmitRun`/`ClaimRun` to `RunFence` to `ExecutionStore` lifecycle and the current SQLite schema.

## Repository evidence

- `session/types.go` places run-owned mutation methods on `ExecutionStore`.
- `store/sqlite/store.go` duplicates those methods as exported methods on concrete `*Store`.
- `store/sqlite/execution.go` performs the fence check and delegates to the duplicate methods.
- `session/model_request.go` combines read and write methods in `ModelRequestStore`.
- `runtime/options.go` checks the wrong object for that combined capability.
- `runtime/ledger.go` writes through `runExecution.store`, which is already a `session.ExecutionStore`.

## Interface changes

Change `session/model_request.go`:

- Replace existing `ModelRequestStore` with existing-file, proposed symbols `ModelRequestReader` and `ModelRequestWriter`.
- `ModelRequestReader` contains `GetModelRequest` and `ListModelRequests`.
- `ModelRequestWriter` contains `CreateModelRequest` and `UpdateModelRequest`.

Change `session/types.go`:

- Embed `ModelRequestReader` in `Store` so audit records have a backend-neutral read contract.
- Embed `ModelRequestWriter` in `ExecutionStore` so every ledger write is fenced.
- Do not expose model-request reads through `ExecutionStore` unless an existing call site proves they are needed; current runtime writes do not require them.

## SQLite changes

Change `store/sqlite/store.go`:

- Rename the raw writer implementations to unexported methods: `appendMessage`, `appendPart`, `updatePart`, `appendEvent`, `createToolCall`, `claimToolCall`, `settleToolCall`, `startContextEpoch`, `finishContextEpoch`, `createModelRequest`, and `updateModelRequest`.
- Keep `GetModelRequest` and `ListModelRequests` exported to satisfy `ModelRequestReader`.
- Keep non-run-owned store operations and readers unchanged.

Change `store/sqlite/execution.go`:

- Delegate fenced writes to the unexported SQLite methods.
- Remove execution-level `GetModelRequest` and `ListModelRequests` methods.
- Preserve `withFence`/`withFenceState` checks and transactional fence validation for every writer.

Acceptance criteria:

- External packages cannot compile a direct run-owned write against `*sqlite.Store`.
- Current and stale fence tests continue to prove atomic validation.
- `var _ session.Store = (*sqlite.Store)(nil)` and the execution implementation's interface assertion compile.

Extend `store/storetest/contract.go` with a backend-neutral model-request contract:

- Admit a run and create a prepared request through its `ExecutionStore`.
- Transition it through `dispatch_started` to a terminal state through the same execution capability.
- Read and paginate records through the top-level `Store` reader.
- Reject writes made with a foreign or stale run fence.
- Reject invalid request-state transitions.
- Prove transaction rollback hides model-request writes.

This contract must run for SQLite through the existing storetest factory. Interface assertions alone are not sufficient evidence that writers enforce fences atomically.

## Optional ledger boundary changes

Change `runtime/options.go`:

- Remove the top-level model-request capability assertion from `NewStreamingOrchestrator`.
- Retain `WithModelRequestLedger`, `WithModelRequestSafeOptions`, and `WithModelRequestMaxBytes`.
- Keep disabled-ledger behavior unchanged: audit data may feed extension notifications, but no request record is persisted and no idempotency key is added.

Change `runtime/ledger.go` and `runtime/model_stream.go`:

- Keep `prepareModelRequest` conditional on `modelRequestLedger`.
- Accept and return `session.ModelRequestWriter` where a narrow writer type is useful.
- Use `execution.store` directly as the writer when enabled; do not use a type assertion.
- Set the provider idempotency key from the created durable request ID only when the ledger is enabled.
- Preserve state transitions `prepared -> dispatch_started -> completed|failed` and the existing failure-before-provider-dispatch behavior.

Change tests:

- Retain `WithModelRequestLedger(true)` in ledger-persistence tests.
- Replace the obsolete constructor-rejection test with one proving a top-level store that does not duplicate writer methods is accepted because its `ExecutionStore` supplies `ModelRequestWriter`.
- Simplify `dispatchStartFailingStore` so only its returned `ExecutionStore` intercepts `UpdateModelRequest`; remove the duplicate top-level model-request wrapper.
- Remove read methods from fake execution stores and add `ModelRequestReader` methods to fake top-level stores only where required by `session.Store`.
- Retain tests proving the disabled path creates no request records and the enabled path creates terminal request records.

Error-path invariants:

- Pre-admission request validation creates no run and makes no provider call.
- Post-admission audit rejection makes no provider call and creates no request record, but settles the admitted run as failed.
- Create or dispatch-start ledger failure makes no provider call.
- Terminal ledger update failure becomes the run error and suppresses the completed lifecycle notice.
- Panic and provider failure leave a terminal failed request record when the store transition succeeds.

Add an SQLite-backed post-admission audit test using a deliberately small `WithModelRequestMaxBytes` value. Assert one durable failed run, zero provider calls, and zero model-request records.

## Fixture migration

Update direct SQLite-writing tests in `agui/replay_test.go`, `transport/http_test.go`, the settlement integration test, and affected SQLite tests:

1. Create the session.
2. Admit the run and retain the returned run.
3. Derive `session.ExecutionStore` from `RunFence{RunID: admitted.ID, ClaimToken: admitted.ClaimToken}`.
4. Perform all run-owned setup and settlement through that execution store.

Prefer small package-local fixture helpers when a package repeats these four operations. Do not add a production bypass or an unsafe seed API.

Verification:

- `go test ./session/... ./store/... ./runtime/... ./agui/... ./transport/...`
- `go test -race ./session/... ./store/... ./runtime/...`
