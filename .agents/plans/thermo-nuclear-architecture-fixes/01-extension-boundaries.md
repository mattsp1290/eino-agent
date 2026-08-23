# Extension Boundaries And Explicit Run State

## Goal

Remove runtime-layout magic from protected extension values and make one run's frozen plan an explicit dependency.

## Repository evidence

- `runtime/extensions.go:sameInterfaceIdentity` uses `unsafe.Pointer` to compare interface data words.
- `validateModelStreamInput`, `sameProtectedTool`, and `sameProtectedToolCall` depend on that helper.
- `runtime/orchestrator.go`, `runtime/interrupt.go`, `runtime/admission.go`, and `runtime/ledger.go` recover `RunPlan` from context.
- The model-stream, tool-prepare, and tool-execute points currently forbid replacing host callables, so callback-visible callable fields do not represent supported mutation.

## Exact change surface

- `runtime/extension_execution.go` (new, under existing `runtime/`): proposed `runExecution` type and plan-owned dispatch/event/settlement helpers.
- `runtime/extension_model.go` (new): model-stream point declarations, data cloning, sanitization, and validation.
- `runtime/extension_tool.go` (new): tool point declarations, data cloning, sanitization, guards, protected outcomes, and validation.
- `runtime/orchestrator.go`: construct and pass `runExecution`; make model/tool methods receive it explicitly or use it as the receiver.
- `runtime/interrupt.go`: construct and pass the same execution type for nonterminal resume.
- `runtime/admission.go` and `runtime/ledger.go`: receive explicit dispatch/plan inputs instead of reading context.
- `runtime/extensions_test.go` and focused runtime tests: replace context helpers and prove host callables remain authoritative.

## Intended design

`runExecution` is unexported and contains:

- the owning `*StreamingOrchestrator`;
- one non-nil `*RunPlan`;
- release-once behavior delegated to `RunPlan.release`;
- methods that dispatch notices/interceptors without repeated nil checks;
- the canonical strict-settlement predicate;
- infrastructure-plus-extension event sink construction.

Fresh and nonterminal resumed runs create this value before asynchronous execution. Terminal resume returns without creating it. Runtime methods pass ordinary request contexts for cancellation and values; contexts no longer carry plan identity.

Before model dispatch, construct a callback view that sets these host-owned fields to nil:

- `ModelStreamInput.Resolved.Client`;
- `ModelStreamInput.Resolved.Streamer`;
- `ModelStreamInput.Request.Observer`.

The terminal closes over the original `model.Resolved` and `model.Request`. Validators compare the complete sanitized data view with `reflect.DeepEqual`; no callable identity comparison remains.

Before tool prepare, execute, or result-transform dispatch, construct callback views that set these host-owned fields to nil:

- `Tool.Executor`;
- `Tool.InputDecoder`;
- `ToolCall.Approval`, including `ToolOutcome.Call` and the protected outcome seal.

Preparation applies only the documented mutable `Call.Input` and `Call.Pattern` fields back to the authoritative call. Tool execution closes over the authoritative tool executor and call. Result transformation restores the authoritative call after validating the sanitized protected view. Inner callbacks receive sanitized copies. If any callback injects a nonnil callable, validation returns `extension.ErrProtectedMutation`; neither inner callbacks nor the host terminal run.

## Invariants and error paths

- A callback cannot change which client, streamer, observer, executor, decoder, or approval requester the host invokes. Callable injection fails closed rather than being ignored.
- Existing protected data fields still reject mutation with `extension.ErrProtectedMutation`.
- Required delegation and returned stream identity checks remain unchanged.
- A nil plan is normalized at `runExecution` construction; downstream code does not encode nil as a mode.
- Plan release occurs exactly once on every asynchronous exit, including panic recovery.
- Cancellation remains in the ordinary request context and is never replaced by plan storage.

## Tests and acceptance criteria

- Update callable-replacement tests to assert the terminal still invokes the original callable while dispatched callable fields are nil.
- Add nested-interceptor tests proving an outer callback cannot make an inner callback observe a substituted executor/client/approval requester and that injection prevents terminal execution.
- Add result-transform tests with pointer-backed and function-backed approval requesters; unchanged sanitized delegation succeeds without false protected-mutation errors.
- Preserve schema, request, tool, and call protected-mutation tests.
- Add a lifecycle test proving fresh and resumed execution release the acquired plan exactly once on success, early failure, and panic.
- `rg -n 'sameInterfaceIdentity|interfaceWords|withRunPlan|runPlanFromContext|"unsafe"' runtime --glob '*.go' --glob '!**/*_test.go'` returns no production matches.

## Dependencies and exclusions

- Complete this before canonical tool settlement so the shared state machine receives explicit execution state.
- Do not remove exported callable fields from public structs in this work package.
- Do not change contract IDs, callback ordering, or error classification.
