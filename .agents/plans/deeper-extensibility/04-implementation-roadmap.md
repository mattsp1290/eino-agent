# Implementation Roadmap

<!-- markdownlint-disable MD013 -->

## Delivery Strategy

Implement this as seven mergeable slices (0 through 6). Each slice must leave the repository
green and keep public compatibility. Do not start the request ledger or new WIT
work until the native registry and runtime point contracts have settled.

## Slice 0: Close The Existing Baseline

Purpose: finish already-published commitments before adding another API layer.

Work:

- Implement and test Phase B wrappers for the existing WIT `@0.1.0` worlds:
  context source, event sink, hook, and tool middleware.
- Implement the deterministic partial/full hook metadata cache, ToolResult JSON
  mapping, protected-field preservation, bounded event summary, and plain-text
  context mapping defined in the contract catalog.
- Add their guest fixtures, active timeout/trap/size tests, concurrent-use
  tests, loader close tests, and credential-sentinel tests specified by the
  original prompt.
- Update `docs/architecture/extensibility.md` status cells from “gap” only when
  each wrapper actually ships.

Exit gate: all `@0.1.0` worlds have real wrappers or the implementation records
a separate explicit decision to withdraw an unpublished world. Never mutate a
published WIT signature to fit the new registry.

## Slice 1: Generic Extension Kernel

Purpose: land reusable mechanics without changing orchestrator behavior.

Files:

- New `extension/doc.go`, tokens, registry, mount, plan, scope, errors, and
  tests.
- Dependency test ensuring `extension` stays independent of runtime domains.

Work:

- Implement typed `Notification[T]` and `Interceptor[I,O]` tokens plus generic
  package functions.
- Implement validation, atomic staged mount, deterministic ordering, global and
  session scope, immutable snapshots, `Next` once-guard, panic recovery,
  point-owned cloning/output/continuation-input validation, stable point
  contract IDs/versions, a nonrecursive diagnostic reporter, arbitrary reverse-order cleanup effects,
  diagnostics, deactivate, close, and quiescent draining.
- Add benchmarks for snapshot creation, zero-handler dispatch, 10-handler
  dispatch, and concurrent mount/snapshot/close.

Exit gate: unit, fuzz, race, leak, and benchmark tests prove the generic
contract without importing `runtime`.

## Slice 2: Runtime Observation And Plan Provenance

Purpose: integrate a run-frozen plan and immutable notifications without yet
moving control authority.

Files:

- `runtime/options.go`, `runtime/orchestrator.go`, `runtime/admission.go`,
  `runtime/interrupt.go`, new `runtime/extensions.go` and tests.
- New `composition/` coordinator, `session.ExtensionPlanDescriptor`, and store
  round-trip tests. Do not overload existing `Run.Components`.
- Docs for event taxonomy and producer/consumer map.

Work:

- Add `WithRunPlanProvider`; implement `composition.Registry` as the provider
  coordinating handlers and exact tool-definition generations under one
  snapshot/lease.
- Add deterministic `tools.Registry.Snapshot`/provenance primitives needed by
  that coordinator in this slice. Local generations protect against in-process
  ABA only; stable artifact/config/schema/executor identities drive persistence.
- Persist the versioned plan mode, instance/artifact/config identities, ordered
  registrations, stable capability identities, and canonical fingerprint in a new
  run field. Add strict, partial-legacy, and legacy resume behavior.
- Implement descriptor-driven resume acquisition that resolves exactly the
  persisted entries and ignores unrelated mounts added later.
- Publish contained run, model, tool, and runtime-event notifications at the
  positions in the catalog.
- Add a composite event publication helper without changing `EventSink`'s
  current transport/backpressure semantics.
- Expose diagnostics listing plan components and registered point IDs, with no
  callback values or secrets.
- Split callback failures into bounded/redacted public classification for
  durable records/events and a local raw cause for trusted diagnostics; do not
  persist arbitrary `error.Error()` text from extensions.

Exit gate: notification failure cannot alter run results; plan changes affect
only later runs; close drains an in-flight run; resume mismatch fails before
execution.

## Slice 3: Narrow Interception Pipelines

Purpose: replace broad new customization needs with typed points while keeping
legacy adapters.

Work in order:

1. Add `runtime/context-assemble` and provenance-aware contributions. Preserve
   the exact legacy order: context sources, tool resolution, `Hook.BeforeTurn`.
2. Add immutable `runtime/model-stream`. Require successful delegation exactly
   once; defer model-options until every model path has a request-aware contract.
3. Add `runtime/tool-prepare`, `runtime/tool-execute`, and
   `runtime/tool-result-transform`, protected `ToolOutcome`, and deny-only
   mounted guards around the durable/policy sequence. Guards run first; when
   they abstain the existing sequential permission/approval loop is unchanged.
   Successful execution requires the body exactly once.
4. Add optional `ToolSettlementStore` with reserved result IDs, idempotent
   atomic settlement, unreconciled listing/repair, and SQLite implementation
   before the immutable settled notice.
5. Keep legacy `BeforeRun` at its synchronous post-admission location. Add the
   new run gate separately and publish durable-admission observation before the
   legacy hook.
6. Replace scattered loop calls with named dispatch helpers so subject/scope
   binding and failure policy cannot diverge.

Exit gate: sequence tests cover every stage, permissions see final input,
`next` cannot execute a model/tool twice, and all existing runtime tests pass
unchanged or with strictly additive assertions.

## Slice 4: Reversible Capability Registration

Purpose: let mounted plugins contribute and remove capabilities safely.

Work:

- Add generation-checked `tools.Registry.Unregister` and exact-generation
  tests for register/replace/unregister races.
- Implement global/exact-session tool layers in `composition`: same-layer
  duplicates fail, session definition shadows global, restrictions intersect
  monotonically, and one entry supplies both schema and executor.
- Sort frozen tools deterministically and lease their executable resources in
  the same plan as handlers. Do not use post-hoc generation revalidation.
- Add reversible named prompt sections evaluated per provider step and the
  explicit default-off system-prompt materialization option. Add
  `model.Request.System`; pass it directly to Streamers and materialize it as
  the first system message in the fallback Eino path.
- Route context contributions and deny-only `ToolGuard`s through the mount
  registrar.
- Provide one example native plugin that contributes a scoped tool, prompt
  section, context, guard, prepare interceptor, cleanup effect, and settled
  observation, then cleanly unmounts.

Exit gate: the example can serve concurrent sessions, one session-scoped mount
is invisible to another, and unmount prevents new runs while allowing an
already-admitted run to finish on its frozen plan.

## Slice 5: Durable Prepared-Request Ledger

Purpose: make every new provider attempt's canonical runtime-to-adapter input
auditable without pretending the runtime sees final provider wire encoding.

Domain model:

```go
type ModelRequestRecord struct {
    ID                 ModelRequestID
    SessionID          ID
    RunID              RunID
    AssistantMessageID MessageID
    Attempt            int
    Step               int
    ProviderID         string
    ModelID            string
    State              ModelRequestState // prepared, dispatch_started, completed, failed
    Messages           json.RawMessage
    System             string
    Tools              json.RawMessage
    SafeCallConfig     json.RawMessage // explicit allowlist; `{}` by default
    ContentSHA256      string
    ExtensionPlanHash  string
    CreatedAt          time.Time
    UpdatedAt          time.Time
}

type AuditedModelInput struct {
    Messages       []AuditedMessage
    System         string
    Tools          []AuditedToolSchema
    SafeCallConfig map[string]string
}
```

Work:

- Add optional `session.ModelRequestStore` / transactional capability
  interfaces rather than extending `session.Store`. Add SQLite migration 002,
  ordered version application, old-database upgrade/future-version rejection,
  store contract coverage, pagination, and bounded canonical encoding.
- Add an explicit ledger option. Enabling it on a store without
  `ModelRequestStore` fails construction; legacy/default operation remains
  source-compatible and does not pretend to provide the audit invariant.
- Assign explicit attempt and step ordinals. A step is one provider request
  plus its resulting tool batch; document that this does not introduce a
  user-visible turn abstraction.
- Define canonical audited DTOs for messages and tool schemas. Convert
  `ParamsOneOf` through JSON Schema; reject unsupported/nonserializable or
  disallowed `Message.Extra` / `ToolInfo.Extra`. Derive the audited subset and
  durable record from one `AuditedModelInput`; then attach existing opaque
  options, identity/trace metadata, observer, and clients unchanged outside
  that projection when constructing the full `model.Request`.
- Persist `prepared`, then `dispatch_started` immediately before `openStream`,
  and terminal state afterward. If prepare/start persistence fails, do not call
  the adapter. A crash may leave an orphaned prepared/start record; the
  realizable invariant is “adapter call implies a durable prepared record,” not
  a false exactly-once network claim. Pass the record ID as an idempotency key
  to adapters that explicitly support it.
- Record only an allowlisted option projection; default to none. Never store
  runtime config maps, credentials, endpoints, trace attributes, observers,
  clients, or provider-specific raw payloads.
- Add a verification helper/test comparing the canonical runtime-to-adapter
  projection—including messages, system, tools, and explicitly safe effective
  call config—sent to a recording streamer with the durable record for successful,
  retried, failed, tool-follow-up, context-injected, and resumed paths.
- Commit the request-record terminal transition before publishing
  `runtime/model-completed`. A transition failure emits only a nonrecursive
  diagnostic and fails the run; it must not publish false durable completion.
- Define retention/size limits and a classified error when content exceeds
  them. Do not silently truncate model-visible content in the ledger.

Exit gate: every adapter invocation has a durable prepared record whose
canonical projection equals what runtime submitted, and credential sentinels
never appear in records, events,
diagnostics, errors, or guest DTOs.

## Slice 6: Curated Wasm Integration And Documentation

Purpose: expose stable, safe subsets after native APIs prove out.

Work:

- Implement adapters from existing `@0.1.0` context/hook/event/tool-middleware
  wrappers into the new point helpers while retaining old constructor return
  types.
- Keep event sink Wasm calls bounded and off any new unbounded observer path.
- Decide through a separate WIT design review whether a `@0.2.0` package is
  justified for prompt contribution or around execution. Default decision is no.
- If a new package is approved, define explicit DTOs and one world per curated
  seam; do not add generic event name plus JSON payload functions.
- Update architecture, security, runtime, tools, permissions, embedding,
  consumer guide, WIT README, and examples.
- Generate a producer/consumer table from core point declarations and
  registrations if feasible; otherwise add a test that every published point
  appears in the maintained catalog.

Exit gate: native and Wasm examples use the same runtime insertion points,
published WIT remains immutable, dependency isolation holds, and documentation
matches exported symbols.

## Dependency Order

```text
Slice 0 can proceed independently
Slice 1 -> Slice 2 -> Slice 3 -> Slice 4 -> Slice 5 -> Slice 6
                    Slice 0 -------------------------> Slice 6
```

Slice 5 may begin after Slice 3 if Slice 4 is delayed, but it must still record
the frozen plan's stable component/tool semantic identities.
