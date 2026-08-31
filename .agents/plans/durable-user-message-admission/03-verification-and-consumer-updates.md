# Verification and consumer updates

## Goal and prerequisite state

Prove the required lifecycle behavior through public APIs and update every
repository consumer so none owns duplicate history.

Prerequisite: the public request and atomic admission packages are complete.

## Lifecycle verification matrix

Use public `StreamingOrchestrator.Start`, `Handle`, and `LoadHistory` wherever
possible. Private admission tests remain responsible only for transaction
fault injection and record-shape detail.

| Scenario | Existing or proposed test location | Observable acceptance |
| --- | --- | --- |
| Empty session, successful run | `runtime/orchestrator_fresh_test.go` (existing) | Provider receives one user message; history returns user then assistant once. |
| Second run | `runtime/orchestrator_fresh_test.go` (existing) and `runtime/admission_sqlite_test.go` (new) | Under a frozen clock, the second provider call receives first durable user, first durable assistant, then only the new user. Final and reopened history has two ordered pairs and no duplicate. |
| Concurrent stale contender | `runtime/concurrency_integration_test.go` (existing) | While run A is active, start B cannot reach transactional history loading and returns busy without records. After A settles, a B retry includes A's complete pair exactly once. |
| Provider failure | `runtime/orchestrator_provider_test.go` (existing) | `Handle.Done` is failed; history retains the user and empty assistant placeholder. |
| Interruption | `runtime/concurrency_integration_test.go` or `runtime/orchestrator_fresh_test.go` (existing) | After `Interrupt` and settlement, history retains the user and the run is interrupted. |
| Admission rollback | `runtime/admission_test.go` (existing) and `runtime/admission_sqlite_test.go` (new) | Empty-session failure leaves no records; failed second admission preserves prior state and adds no failed-run records. |
| Parent IDs | `runtime/admission_test.go` (existing) | Run and assistant point to the generated user message; no consumer ID is used. |
| SQLite restart | `runtime/admission_sqlite_test.go` (new under existing `runtime/`) | Two-run close/reopen replay returns exact Unicode content and pair order with reasoning excluded. |
| SQLite ordering | `store/sqlite/store_test.go` (existing) | Reverse-lexical IDs, exact-second timestamps, and cursor pagination retain chronological order. |

Use distinct synthetic prompt values such as `first-user` and `second-user` so
duplicate history cannot pass an equality assertion accidentally. Use an
assistant response that differs from both prompts.

## Test fixture updates

- `runtime/admission_test.go` (existing): extend `admissionStore` rollback
  assertions to include parts and the new canonical user records.
- `runtime/concurrency_integration_test.go` (existing): add deterministic
  barriers around the active first run and transactional history read. Prove a
  contender cannot freeze pre-fence history and later succeed; it must return
  `ErrSessionBusy` with no records, then a post-settlement retry must include
  the first durable pair exactly once.
- `runtime/orchestrator_test_support_test.go` (existing): add a proposed helper
  for constructing `Request{Message: UserMessage{Content: ...}}` only if it
  reduces repeated setup without hiding per-test config or session identity.
- Existing runtime test files listed in
  [01-public-admission-contract.md](01-public-admission-contract.md): migrate
  fresh-run request literals and remove obsolete Eino imports when no longer
  used.
- `session/history/projector_test.go` (existing, verification only unless a
  failing acceptance test exposes a projector defect): retain reasoning-off
  and user-text projection coverage.
- `store/storetest/contract.go` (existing, verification only): rerun its
  transaction, fenced-write, replay, cursor, and idempotency contracts. No
  interface expansion is planned.

## Example adapters

### Minimal server

- `examples/minimal-server/main.go` (existing): replace
  `decodeRunMessages` with proposed `decodeRunMessage` returning one
  `runtime.UserMessage`.
- Accept only the documented `{"message":"..."}` body. Remove the
  caller-supplied `messages` array path rather than retaining a compatibility
  alias.
- Pass the returned value through `runtime.Request.Message`.
- `examples/minimal-server/main_test.go` (existing): assert blank input fails
  and two sequential POSTs replay the first prompt through runtime durability,
  not request history.
- `docs/examples/minimal-server.md` (existing): keep the single-message curl
  shape and state that the server must not resend transcript history.

### AG-UI sketch

- `examples/ag-ui-go-server-example/sketch.go` (existing): inspect the raw
  terminal `aguitypes.Message` before calling any lossy conversion helper.
- Remove the `eino-agui/convert` import if no other sketch code uses it after
  terminal text extraction.
- Add a proposed private helper `terminalTextUserMessage` at the existing
  `StartRequest` insertion point. Require `input.Messages` to end in
  `aguitypes.RoleUser`, require its raw content to satisfy `ContentString`, and
  reject multimodal arrays, structured content, unknown content forms, blank
  text, and invalid UTF-8.
- Extract only that validated terminal text into `runtime.UserMessage`; do not
  scan backward for an earlier user and do not call `convert.MessageText`,
  which drops non-text fragments.
- Remove changing `agui_run_id` from `Request.Metadata`. Keep only
  session-stable metadata such as `agui_thread_id`; the host retains AG-UI run
  identity in its existing response/control mapping.
- Never pass the full AG-UI message list to runtime. Durable history remains
  runtime-owned even when the wire request contains client history.
- `examples/ag-ui-go-server-example/sketch_test.go` (new under existing
  `examples/ag-ui-go-server-example/`): cover prior history plus a terminal
  plain-text user, trailing assistant/tool rejection, blank input, mixed
  text/image rejection, unknown content-kind rejection, invalid UTF-8, and
  proof that only the terminal prompt reaches `Request.Message`.
- `docs/integrations/ag-ui-go-server-example.md` (existing): update the adapter
  table and flow to say that the adapter validates and extracts only the raw
  terminal user message and that runtime supplies prior messages.

These adapter changes are repository examples. They do not authorize edits to
the `eino-tui` repository.

## Public documentation

- `runtime/doc.go` (existing): state that fresh-run admission persists the
  current user text before provider execution.
- `docs/consumer-guide.md` (existing): replace the `Request.Input` example,
  define runtime transcript ownership, and document completion, failure,
  interruption, replay, second-run, and no-dual-write semantics.
- `docs/architecture/runtime.md` (existing): insert the user message and part
  before the assistant placeholder in the run lifecycle and target flow.
- `docs/architecture/storage.md` (existing): define the atomic admission set,
  parent-ID invariant, timestamp ordering representation, and rollback rule.
- `README.md` and `docs/dependency-status.md` (existing): update the supported
  root release only in the release package described by
  [04-release-and-response.md](04-release-and-response.md).

Documentation must distinguish:

- synchronous admission failure, which commits no transcript;
- failed later admission, which preserves the existing session and transcript
  while committing none of the failed run's records;
- admitted execution failure or interruption, which retains the user and
  assistant placeholder;
- successful settlement, which retains both visible contents;
- history input, which runtime loads, from the one new message a consumer
  submits.

## External compile contract

- `testdata/external-consumer/consumer.go` (existing): construct or type-check
  the proposed `runtime.Request.Message` and `runtime.UserMessage` fields so
  both local and published fixture modes compile the exact public shape.
- `testdata/external-consumer/check.sh` (existing, verification only): reuse its
  fresh-module, no-workspace, no-vendor, no-replacement published mode.

## Verification and acceptance criteria

Before consumer updates, complete the review-discovered run-wide replay gate
as its own PR. This is deliberately separate from the immutable admission-pair
PR contract.

- `runtime/orchestrator.go` (existing): initialize a run-local durable-message
  ordering floor from the committed admission assistant and allocate every
  follow-up assistant placeholder strictly after that floor.
- `runtime/tool_execution.go` and `runtime/tool_settlement.go` (existing): use
  the same sequencer for tool-result message and part timestamps without
  changing tool status, output, parentage, or elapsed-time observations.
- Existing runtime execution state may carry the private sequencing value; do
  not widen `session.Store`, `session.ExecutionStore`, or public request types.
- Add a real SQLite regression test with a frozen clock and reverse-lexical
  message IDs. Close and reopen the database, paginate with `Limit: 1`, and
  assert raw replay and `LoadHistory` both return user, tool-calling assistant,
  tool result, and final assistant in conversational order.

Focused run-wide ordering commands:

```text
go test ./runtime -run 'Tool.*History|History.*Tool|Frozen.*Tool|Tool.*Frozen'
go test ./store/sqlite -run 'ListMessages'
go test -race ./runtime ./store/sqlite
git diff --check
```

Focused commands:

```text
go test ./runtime ./session/history ./store/sqlite ./store/storetest
go test -race ./runtime ./store/sqlite
go test ./examples/minimal-server ./examples/ag-ui-go-server-example ./examples/native-extension
go test ./tools/einotools ./wasmext
make external-consumer-check
git diff --check
```

Acceptance:

- frozen-clock tool loops remain chronological after SQLite restart and
  one-record pagination, independent of message ID lexical order;
- every scenario in the matrix asserts durable public behavior;
- no example sends previous transcript messages to `Start`;
- no test passes only because of a shadow history fixture;
- public docs describe ownership, ordering, compatibility, and failure
  semantics consistently;
- the external compile fixture exercises the new request shape.

## Dependencies, risks, and exclusions

- Candidate docs may be written alongside tests after the API shape is fixed,
  but supported-version claims wait for publication.
- Empty assistant placeholders are part of the current durable contract. Do
  not silently filter them in history as part of this request.
- Do not weaken reasoning exclusion or expose content in metadata-redacted
  events to make assertions easier.
