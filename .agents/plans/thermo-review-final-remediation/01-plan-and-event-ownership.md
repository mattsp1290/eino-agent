# Plan And Event Ownership

## Goal and prerequisite state

Delete duplicate handler ownership and event-kind-driven durability inference without changing frozen plan behavior, leases, descriptor fingerprints, event ordering, or store atomicity.

The application context in [00-overview.md](00-overview.md) authorizes direct breaking API changes.

## Handler ownership

### Existing evidence

- `extension/registry.go`: `Plan.handlerComponents` and `MountedValue.handlers` hold equivalent selected handler identities.
- `composition/registry.go`: `snapshotComponents` retains the mounted handler copy and `acquire` places it in `runtime.PlanComponent.Handlers`.
- `runtime/extension_plan.go`: `NewRunPlan` compares the supplied copy against `Dispatch.HandlerComponents()` using `sameHandlerIdentities` and `matchedHandlers`.

### Exact change surface

- `extension/registry.go`
  - Remove `MountedValue.handlers` and `MountedValue.Handlers`.
  - Continue building `Plan.handlerComponents` from scope-filtered entries.
  - Construct mounted values with component, payload, and callback token only.
- `composition/registry.go`
  - Remove `selectedComponent.handlers` and the corresponding copy in `snapshotComponents`.
  - Construct capability-only `runtime.PlanComponent` values.
  - Omit a component when it contributes no selected capability; handler-only ownership comes from the dispatch plan.
- `runtime/extension_plan.go`
  - Remove `PlanComponent.Handlers` and `sameHandlerIdentities`.
  - Change `NewRunPlan` to seed one private component accumulator from `spec.Dispatch.HandlerComponents()`.
  - Merge each capability-only `PlanComponent` by exact component identity.
  - Reject duplicate capability component records, conflicting artifacts, invalid owners, and empty capability records.
  - Build `session.ComponentPlan.Handlers` only from dispatch handler identities.
  - Sort the final component records before fingerprinting.
- Update direct `RunPlanSpec` literals in `runtime/*_test.go`, `composition/*_test.go`, examples, and test helpers.

### Invariants and error paths

- One dispatch component creates one durable component even when it has no capabilities.
- One capability-only component creates one durable component even when it has no handlers.
- A component present in both sources merges only when its complete `extension.Component` identity matches.
- A duplicate capability owner remains `runtime.ErrExtensionPlanMismatch`.
- A failed `NewRunPlan` releases the dispatch lease exactly once.
- Global execution ordering of tools, prompts, guards, and restrictions remains defined by the existing comparators, not descriptor order.
- Resume still compares the persisted fingerprint with the newly acquired live plan before mutation.

### Tests and acceptance criteria

- Update runtime plan construction tests to cover handler-only, capability-only, and combined components.
- Preserve tests for conflicting artifacts, duplicate owners, lease release on failure, stable fingerprints, and resume mismatch.
- Add or retain a test proving that a handler-only mount appears in `RunPlan.Descriptor()` after `MountedValue.Handlers` is removed.
- Add or retain a test proving that handlers filtered out by session scope do not enter the descriptor.
- Run `go test -race ./extension ./composition ./runtime ./session`.
- Declaration-targeted searches find no `handlers []HandlerIdentity` on `MountedValue`, `func (v MountedValue[T]) Handlers`, `Handlers []extension.HandlerIdentity` on `PlanComponent`, `selectedComponent.handlers`, or `sameHandlerIdentities`. The intended `extension.Plan.handlerComponents` and `session.ComponentPlan.Handlers` remain.

## Event publication ownership

### Existing evidence

- `runtime/event_sink.go:runEventSink.Emit` persists every non-live event except run start and finish.
- Admission and run settlement already persist start/finish records atomically before calling the sink, but admission discards the record returned by `AppendEvent` and both paths reconstruct their publication envelopes.
- Tool create, claim, and settlement publish the canonical `session.EventRecord` returned by the fenced store methods through `publishPersisted`.
- Model deltas set `LiveOnly` and are the only other production events sent through `runEventSink.Emit`.
- `TestRunEventSinkPersistsOnlyIntermediateDurableEventsThroughFence` manufactures the only current intermediate durable call into the generic sink.

### Exact change surface

- `runtime/event_sink.go`
  - Remove the `execution` field from `runEventSink`.
  - Make `Emit` fan out only: infrastructure first, then contained `EventPublishedPoint` notification.
  - Retain `publishPersisted` as the explicit conversion and best-effort fanout for canonical store records.
  - Delete `durableEventRecord` and its `fmt`/`time` dependencies.
- `runtime/admission.go`
  - Retain the canonical `session.EventRecord` returned by `AppendEvent` in `admittedRun`.
  - Remove event and lifecycle fanout from `admitter`; it returns the committed start record without rebuilding it from request fields.
- `runtime/orchestrator.go` and `runtime/interrupt.go`
  - After admission commits, publish its returned start record and then notify `RunAdmittedPoint`, preserving the current order and explicit cancellation contexts before binding the execution for the run loop.
  - Publish the exact final `session.EventRecord` returned by successful fenced `SettleRun` through persisted-record fanout in fresh and resumed paths.
  - Remove the result-based reconstruction in `publishRunFinished`.
- `session/types.go`, `store/sqlite/execution.go`, store contract tests, and all execution-store fakes
  - Change `ExecutionStore.SettleRun` to return `(*session.EventRecord, error)` because settlement may omit an event.
  - Return the committed, store-normalized final record from SQLite and every compliant store; publication uses only that returned value.
- `runtime/extension_execution.go`
  - Build a sink from infrastructure plus dispatch only.
  - Preserve the nil fast path when both are absent.
  - Keep `publishPersisted` as the explicit path used after admission, settlement, and atomic tool transitions.
- `runtime/event_sink_test.go`
  - Replace the synthetic intermediate-persistence test with fanout-only assertions.
  - Assert that `Emit` never writes to the execution store.
  - Preserve failure and ordering tests for canonical persisted transitions.
- Update `docs/architecture/extension-points.md`, `docs/architecture/runtime.md`, and `docs/architecture/storage.md` where they describe durable facts and event publication.
- `runtime/types.go`, `runtime/events_test.go`, `runtime/observability.go`, `agui`, `examples/ensemble-adapter`, and `docs/integrations/ensemble.md`
  - Inventory every event kind. Retain `run_started`, `tool_call_updated`, and `run_finished` as canonically persisted; retain message deltas and tail overflow as live-only transport events.
  - Delete `EventModelFallbackEngaged`, `ModelFallbackPayload`, `NewModelFallbackEvent`, `EventContextEpochChanged`, and their adapter/observability/example/tests/docs because no canonical mutation owns either promised durable event.
- `stream/tail.go` and `stream/tail_test.go`
  - Set `LiveOnly: true` when constructing `EventTailOverflow` and assert the flag in the overflow test.

### Invariants and error paths

- Admission and run settlement remain atomically durable before publication, and publication uses the store-returned record rather than a reconstruction.
- Start publication precedes `RunAdmittedPoint` notification as it does today; a cancellation/ordering test pins the sequence.
- Tool transition publication remains byte-for-byte based on the store-returned record.
- Live deltas remain non-durable.
- Infrastructure `Emit` errors remain return values for ordinary `Emit` calls.
- `publishPersisted` remains best effort because transport failure cannot roll back committed state.
- `EventPublishedPoint` remains contained and runs after the infrastructure sink call.

### Tests and acceptance criteria

- A run-start test proves one durable record and one publication.
- A run-finish test proves one durable record and one publication.
- Admission and finish tests use a normalizing fake store and prove the published value equals the canonical record returned by the store.
- Tool transition tests prove no duplicate durable events and publication of the canonical returned record.
- A live-delta test proves no store append.
- `rg -n 'durableEventRecord|event.Kind != EventRunStarted|event.Kind != EventRunFinished|EventModelFallbackEngaged|NewModelFallbackEvent|ModelFallbackPayload|EventContextEpochChanged' runtime agui examples docs` returns no production match.
- Run `go test -race ./runtime ./store/sqlite`.

## Dependencies and exclusions

- Complete handler ownership before event cleanup only to keep reviewable commits and test failures localized; the implementations do not otherwise depend on each other.
- Do not change `session.ExtensionPlanDescriptor`, SQLite schema, or transition uniqueness. Delete only the orphan model-fallback and context-epoch event wire surfaces identified above.
- Do not add a generic public event writer. A future durable event producer must persist through the execution store that owns its state transition.
