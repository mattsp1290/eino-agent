# Public admission contract

## Goal and prerequisite state

Give `StreamingOrchestrator.Start` one unambiguous, runtime-owned current user
message. Complete this package before changing durable admission writes.

Prerequisites:

- The implementation Beads issue is claimed.
- The confirmed application context in [00-overview.md](00-overview.md) remains
  unchanged.
- No compatibility field or feature flag is required.

## Repository evidence

- `runtime/types.go` defines `Request.Input` as a slice of Eino messages and
  separately accepts `ParentID`.
- `runtime/orchestrator.go:providerInput` loads durable history and appends the
  entire slice, so a caller can resubmit prior messages.
- `runtime/orchestrator.go:Start` generates run, assistant, epoch, event, and
  claim-token IDs but no user-message or user-part IDs.
- `runtime.IDGenerator` already provides `NewMessageID` and `NewPartID`; a new
  identity service is unnecessary.
- `runtime.validate` currently checks only orchestrator construction and
  `SessionID`.

## Exact change surface

### Public types

- `runtime/types.go` (existing): add proposed public type
  `runtime.UserMessage` with one `Content string` field.
- `runtime/types.go` (existing): replace `Request.Input` and `Request.ParentID`
  with proposed field `Request.Message runtime.UserMessage`.
- `runtime/types.go` (existing): document that the message is the one current
  user submission, that runtime owns durable persistence and IDs, and that
  history must not be copied into the request.

Conceptual shape:

```go
type UserMessage struct {
    Content string
}

type Request struct {
    SessionID session.ID
    Message   UserMessage
    Config    config.Snapshot
    Metadata  map[string]string
}
```

`UserMessage` and `Request.Message` are proposed symbols. Insert them at the
existing `Request` declaration in `runtime/types.go`.

### Request validation and provider assembly

- `runtime/orchestrator.go` (existing): extend `validate` to reject a message
  whose `strings.TrimSpace(Content)` is empty or whose content fails
  `utf8.ValidString`. Preserve the exact original `Content` after validation.
- `runtime/orchestrator.go` (existing): perform this validation before
  `acquireRunPlan`, model resolution, history reads, or ID generation.
- `runtime/orchestrator.go` (existing): remove the pre-admission
  `providerInput` call. `Start` must not read durable history before the
  transactional active-run fence.
- `runtime/orchestrator.go` (existing): generate IDs in this logical order:
  run, user message, user part, assistant message, epoch, start event, run
  claim token. Do not infer replay order from that generation order.
- `runtime/admission.go` (existing): add proposed private fields
  `admissionIDs.UserMessageID` and `admissionIDs.UserPartID` and validate both.
- `runtime/admission.go` (existing): require all generated admission IDs to be
  pairwise distinct before opening the store transaction.
- `runtime/admission.go` (existing): add the canonical submitted user message
  and a cloned `history.Options` value to the private `admissionRequest`. Do not
  carry a preassembled Eino history slice across the transaction boundary.
- `runtime/admission.go` (existing): after `AdmitRun` succeeds inside
  `Store.WithinTx`, call the normal `LoadHistory` path through the transactional
  store, append exactly one
  `einoschema.UserMessage(request.UserMessage.Content)`, and call
  `FreezeTurnSnapshot` before any provider execution.

## Intended behavior and invariants

- `Request.Message` represents only the current human submission.
- Durable history is always runtime-loaded. A caller cannot pass prior user,
  assistant, tool, or system messages through this API.
- A successful provider snapshot is assembled after the run fence and in the
  same transaction as admission. A contender encountering an active run fails
  with `session.ErrSessionBusy` before reading provider history.
- A whitespace-only or invalid-UTF-8 message returns
  `ErrInvalidOrchestrator` synchronously.
- Validation does not trim, normalize, redact, or otherwise alter accepted
  content.
- The provider sees the same accepted content that the durable text part
  stores.
- Model resolution and run-plan acquisition remain pre-admission operations.
  Failures there create no durable transcript.
- `Resume` remains unchanged because it resumes already durable work and does
  not admit a new user message.

## Repository call-site migration

Update every existing `runtime.Request` construction that supplies `Input` or
`ParentID`. The current repository evidence identifies these existing files:

- `runtime/concurrency_integration_test.go`
- `runtime/event_sink_test.go`
- `runtime/extension_context_materialize_test.go`
- `runtime/extension_plan_lifecycle_test.go`
- `runtime/ledger_test.go`
- `runtime/observability_test.go`
- `runtime/orchestrator_fresh_test.go`
- `runtime/orchestrator_provider_test.go`
- `runtime/orchestrator_resume_test.go`
- `runtime/orchestrator_tool_test.go`
- `runtime/tool_execution_test.go`
- `examples/native-extension/plugin_test.go`
- `examples/minimal-server/main.go`
- `examples/ag-ui-go-server-example/sketch.go`
- `tools/einotools/einotools_test.go`
- `wasmext/phase_b_test.go`
- `wasmext/wasmext_test.go`

Use `rg -n 'runtime\.Request|Request\{' --glob='*.go' .` after migration and
inspect every result for removed request fields. Distinguish them from
`RunPlanRequest`, permission/tool request types, unrelated tool input, and
durable message parent fields. Do not mechanically rename unrelated `Input`
fields.

## Tests and acceptance criteria

Add or update tests so that:

- empty and whitespace-only messages fail before run-plan acquisition, model
  resolution, history reads, ID generation, or store writes;
- invalid UTF-8 fails at the same pre-side-effect boundary;
- accepted content with leading/trailing whitespace reaches the provider and
  durable part unchanged;
- valid non-ASCII Unicode reaches the provider and survives SQLite close/reopen
  unchanged;
- the new transactional assembly path produces durable history followed by
  exactly one new Eino user message;
- a contender cannot read history while another run owns the session; after
  that run settles, a retry includes its durable pair or fails without
  admission;
- compile-time repository coverage has no use of removed request fields;
- resume tests remain behaviorally unchanged apart from fresh-run setup.

Focused verification:

```text
go test ./runtime
go test ./examples/minimal-server ./examples/ag-ui-go-server-example ./examples/native-extension
go test ./tools/einotools ./wasmext
rg -n 'runtime\.Request|Request\{' --glob='*.go' .
```

Acceptance: one public value carries the current user text, no request field
accepts caller history or a caller-selected user parent ID, and invalid input
has no downstream side effects.

## Dependencies, risks, and exclusions

- This package blocks [02-atomic-persistence-and-ordering.md](02-atomic-persistence-and-ordering.md).
- The mass call-site edit is mechanical, but tests that inspect provider input
  must preserve their original intent.
- Do not redesign Eino provider messages or the model-request audit ledger.
- Do not add multimodal fields speculatively. The session part model requires a
  separate projection design before those fields can be admitted faithfully.
