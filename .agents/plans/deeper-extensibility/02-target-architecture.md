# Target Architecture

<!-- markdownlint-disable MD013 -->

## Plane Separation

The implementation must preserve these boundaries:

| Plane | Source of truth | Mutability | Failure behavior | Primary consumers |
| --- | --- | --- | --- | --- |
| Durable facts | `session.Store` records | Append/settle through store contracts | Failure prevents the action whose fact could not be recorded | Resume, replay, history, audit |
| Live transport | `runtime.Event` through `EventSink` | Immutable projection | Existing queue/backpressure behavior remains compatible | AG-UI, reconnect tail, host transports |
| Runtime extensions | Typed points in an immutable per-run plan | Only a point's documented output may change | Fixed per point; never chosen by a registration | Native plugins and curated Wasm adapters |
| Capability registries | Tools and context contributors | Reversible registration between snapshots | Registration/resolve errors are explicit | Runtime materialization |

An extension notification never becomes durable merely because it was
published. A durable fact is written first, then projected to transport and
extension observation where applicable.

## Package Boundaries

### New `extension` package

`extension/` owns generic mechanics only. It must not import `runtime`,
`session`, `model`, `tools`, Wasmtime, Eino, or AG-UI.

It provides:

- typed notification and interceptor point tokens;
- deterministic registrations with component identity and exact scope;
- staged, atomic mount and reversible handles;
- immutable dispatch plans with reference-counted quiescence;
- contained notification dispatch and awaited interceptor invocation;
- diagnostics describing active components and points.

Point declarations carry point-specific clone, validation, and panic/error
mapping functions. The generic kernel cannot deep-copy arbitrary `T`; runtime
declarations own correct cloning for Eino messages, tool schemas, JSON, maps,
and other domain values. Interceptor declarations also validate every candidate
input passed to `next` against the original protected fields. The dispatcher
clones before every callback/continuation and validates each continuation input
and returned value before proceeding.

`Registry` accepts a nonrecursive diagnostic reporter at construction. It
receives bounded handler error/panic facts but is not itself dispatched through
the registry. Fixed point policy decides whether a failure is contained or
returned; individual registrations cannot choose.

### Runtime-owned vocabulary

`runtime/extensions.go` defines the concrete public point tokens and payloads,
plus helpers that bind session identity and prevent subject/scope mismatch.
`StreamingOrchestrator` imports `extension`; the generic package never imports
`runtime` back.

### New `composition` package

Today `tools` imports `runtime`, so `runtime` cannot import `tools` to offer a
tool registrar. `composition/` is the deliberate inversion point: it may import
`extension`, `runtime`, and `tools`. It coordinates extension registrations,
scoped tool layers, effects, one atomic applicable-plan snapshot, and one
quiescence lease. `runtime` depends only on a narrow `RunPlanProvider`
interface; it never imports `composition` or `tools`.

Generation revalidation across separate registries is not an acceptable
substitute: it cannot prevent a change immediately after revalidation. The
coordinator owns one lock/generation and returns one frozen plan containing
dispatch handlers, exact tool-definition generations, capability restrictions,
and one release handle.

### Existing domain packages

- `tools.Registry` gains generation-safe `Unregister` and snapshot provenance.
- `session` gains a separate versioned extension-plan descriptor and, later,
  model-request records. Existing `Run.Components` continues to describe
  config snapshot plugins.
- `store/sqlite` gains only the migration required by the model-request ledger.
- `wasmext` adapts curated worlds to runtime points; it does not depend on a
  generic event name/payload ABI.

## Generic API Shape

Exact names can change to follow repository idiom, but these properties are
binding:

```go
package extension

type Artifact struct {
    Name       string
    Version    string
    Hash       string // code/artifact bytes
    ConfigHash string // canonical behavior-affecting, non-secret configuration
    SourceKind SourceKind
}

type Scope struct {
    Kind ScopeKind // global or session
    Key  string    // empty only for global
}

type Registration struct {
    ID         string // stable within one mount
    InstanceID string // unique active mount/config-row identity
    Order      int
    Scope      Scope
}

type Notification[T any] struct { /* unexported identity + metadata */ }
type Interceptor[I, O any] struct { /* unexported identity + metadata */ }

type Contract struct {
    ID      string // stable reverse-DNS or module-qualified ID
    Version string // payload/semantic contract version
}

type Next[I, O any] func(context.Context, I) (O, error)
type Around[I, O any] func(context.Context, I, Next[I, O]) (O, error)
type Observer[T any] func(context.Context, T) error
type Reporter interface { Report(context.Context, Diagnostic) }
type Failures []Failure

func NewNotification[T any](contract Contract, policy NotificationPolicy, clone CloneFunc[T]) Notification[T]
func NewInterceptor[I, O any](contract Contract, clone CloneFunc[I], validateNext NextValidator[I], validateOut ValidateFunc[O]) Interceptor[I, O]

func On[T any](r Registrar, point Notification[T], spec Registration, fn Observer[T]) error
func Use[I, O any](r Registrar, point Interceptor[I, O], spec Registration, fn Around[I, O]) error
func Notify[T any](p *Plan, ctx context.Context, point Notification[T], value T) Failures
func Invoke[I, O any](p *Plan, ctx context.Context, point Interceptor[I, O], in I, terminal Next[I, O]) (O, error)
```

Go does not permit methods with their own type parameters, so registration and
dispatch are generic package functions over non-generic `Registry`,
`Registrar`, and `Plan` values. Do not replace this with `any`-typed callbacks
at the public boundary.

Point tokens carry an unexported in-process dispatch identity plus a stable,
unique `Contract.ID` and semantic version. Runtime publishes package variables
such as `runtime.ToolExecutePoint`. Resumable descriptors use contract
ID/version, never pointer/token identity. A private host point without a stable
contract makes the containing plan partial-legacy rather than strict.

## Ordering And Onion Semantics

Registrations sort by:

1. ascending explicit `Order`;
2. scope rank (`global` before `session`);
3. mount `InstanceID`;
4. registration ID.

Duplicate `(point, scope, InstanceID, registration ID)` tuples fail at
mount. This makes order independent of goroutine scheduling, map iteration, and
plugin load timing. The first sorted interceptor is the outermost wrapper:
pre-`next` work runs in sorted order and post-`next` work unwinds in reverse.

Every `Next` is guarded by the dispatcher and may be called at most once.
Calling it twice returns `extension.ErrNextCalledTwice` without executing the
terminal operation again. An interceptor may intentionally short-circuit only
where that point's result type defines a valid rejection/decision. A generic
zero value is never interpreted as a decision. Points that wrap core model or
tool execution additionally require exactly one successful `next` call: a
short-circuit may return only a typed rejection/failure, never fabricated
success.

## Registration And Mount Lifecycle

```go
type Installer interface {
    Install(context.Context, extension.Registrar) error
}

mount, err := registry.Mount(ctx, component, installer)
// ... use registry in an orchestrator ...
err = mount.Close(ctx)
```

Required semantics:

- `Mount` stages all registrations and publishes them atomically only after
  `Install` succeeds. `Registrar.Defer(cleanup)` stages arbitrary resource
  cleanup beside registrations. Failure rolls back registrations and cleanup
  effects in reverse order.
- `Mount.Close` removes the set from future snapshots immediately, then waits
  for plans already using it to drain, then runs cleanup effects in reverse
  order. Cleanup never races a callback/tool body leased from the mount.
- Close is idempotent. A timed-out close may be called again to finish waiting.
  Cleanup errors are bounded, aggregated, and retry semantics are explicit:
  successfully completed effects are not rerun; unfinished effects may be
  retried on the next close.
- A callback must not synchronously wait for its own mount to close. Provide a
  nonblocking `Deactivate` operation for self-removal; document and detect a
  self-wait where practical.
- Panics in trusted native callbacks are recovered at the dispatcher boundary,
  classified with component and point identity, and handled according to the
  point's fixed failure policy.
- `InstanceID` is nonempty and unique among active mounts; the same artifact may
  be mounted globally and for many sessions under different instance IDs.
  Artifact name/version/hash and effective non-secret config hash are required
  for strict resume. Persist only a validated `SourceKind` enum, not an
  arbitrary source string or URI.

## Scope Model

The registry supports only:

- global: visible to every run using that registry;
- session: visible only when dispatch target `SessionID` exactly matches.

Applicable global handlers run before applicable session handlers at an equal
order. Registration scope controls routing and ownership, not sandboxing or
authorization. Native callbacks remain trusted in-process code.

Do not add agent-name scoping. `config.Agent.Name` is a configuration label,
not a unique runtime identity. A later durable `AgentID` may add a new scope
kind without changing the current two.

Runtime helpers construct the dispatch target from the actual admitted run and
do not accept an independently supplied session scope alongside a payload. This
prevents a payload for session A from being dispatched through session B's
handlers.

## Scoped Tool Composition

The coordinator maintains global and exact-session tool layers. Within one
layer, duplicate names fail. An exact-session definition shadows a global
definition with the same tool name. The effective set is then reduced by the
intersection of config enable/disable rules and every applicable mounted
restriction; a scoped restriction can hide a tool but cannot re-enable one
hidden by a broader restriction.

One frozen merged entry supplies both the model-facing schema and executor, so
presentation, lookup, permission metadata, and execution cannot select
different generations. Effective tools sort by explicit registration order,
then tool name, mount instance ID, and stable registration ID. Tool
materialization remains after admission, as today, but uses only leased frozen
definitions and is therefore immune to unregister/replace races.

## Per-Run Immutable Plan

`Start` resolves one extension plan before durable admission:

1. validate orchestrator/request;
2. resolve model and load durable provider input as today;
3. ask `RunPlanProvider` for one side-effect-free snapshot of every applicable
   handler, prompt/context capability, restriction, guard, and tool-definition
   entry (including disabled definitions, so selection need not run before
   admission);
4. persist its versioned `ExtensionPlanDescriptor` on the run;
5. durably admit the run;
6. execute and publish every point through that immutable plan;
7. release the plan after final settlement and notifications complete.

Registrations mounted or removed after step 3 affect later runs, not the
in-flight run. A close waits for this plan to release.

Do not overload `session.Run.Components`, which already stores config plugin
identities. Add a backward-compatible field:

```go
type ExtensionPlanDescriptor struct {
    SchemaVersion int
    Mode          PlanMode // strict, partial-legacy, legacy
    Fingerprint   string
    Entries       []ExtensionPlanEntry
}

type ExtensionPlanEntry struct {
    InstanceID    string
    Kind          ExtensionKind
    Artifact      ArtifactIdentity // name/version/hash/config-hash/source-kind
    Required      bool
    Scope         ExtensionScope
    Registrations []RegistrationIdentity // stable contract/capability ID, version, order, scope
}
```

The fingerprint is over restart-stable semantics: contract ID/version,
instance/registration ID/order/scope, artifact/config hash, tool name/schema
digest/executor artifact digest, and restriction/guard identity. In-memory
registration generations are used only for ABA protection and leasing and are
never persisted or fingerprinted.

Fresh acquisition snapshots every applicable entry. Resume uses a distinct
`AcquireResumePlan(ctx, persistedDescriptor)` operation that resolves exactly
the persisted required identities and ignores unrelated registrations mounted
later unless they conflict with a persisted selected capability. It
reconstructs and compares the descriptor before changing durable state.
Missing or changed
required entries return `runtime.ErrExtensionPlanMismatch`; no tool executes.
Mixed new registry plus anonymous legacy fields is `partial-legacy`: strict
matching applies to described entries, but diagnostics state that full
reproducibility is not guaranteed. Old runs with no descriptor are `legacy`.

Legacy options/fields have no stable component manifest. Runs using only those
paths keep current resume behavior and are marked as legacy/unversioned in
diagnostics rather than receiving a false reproducibility guarantee.

## Runtime Plugin Surface

Add a runtime-owned interface and option:

```go
type RunPlanProvider interface {
    AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error)
    AcquireResumePlan(context.Context, session.ExtensionPlanDescriptor) (*RunPlan, error)
}

type RunPlan struct {
    Dispatch   *extension.Plan
    Tools      ToolRegistry // frozen/leased resolver; materialized after admission
    Descriptor session.ExtensionPlanDescriptor
    Release    func()
}

func WithRunPlanProvider(RunPlanProvider) Option
```

`RunPlan` fields are immutable/defensively cloned. `Release` is idempotent and
is always deferred immediately after acquisition. A callback-only provider may
return no tools; the `composition.Registry` provider is the recommended path
when mounted tools, prompts, guards, and handlers must share one atomic lease.

When both legacy `StreamingOrchestrator.Tools` and `RunPlan.Tools` are present,
the run is `partial-legacy`. Resolve both at the existing post-admission tool
resolution point, reject duplicate effective names, and deterministically sort
the merged result for the opt-in plan path. A nil provider preserves the legacy
resolver's current behavior and ordering. Hosts seeking strict resume move all
tool registrations into `composition.Registry`.

The orchestrator may use both the registry and legacy fields. Compatibility
adapters are inserted at well-defined positions in the point order so existing
behavior is unchanged:

- context sources, tool resolution, and then legacy `Hook.BeforeTurn` retain
  their exact current order. New per-step prompt/context points have separate
  order bands and do not silently move these callbacks;
- legacy tool `BeforeToolCall` / `AfterToolCall` remain an onion around the
  executor at their current points;
- legacy lifecycle hooks retain current error/discard behavior;
- the infrastructure `EventSink` remains the live transport destination, while
  new immutable event observations are contained and cannot fail the run.

Document the exact adapter orders as constants/tests, not comments only.

## Security Boundaries

- Point payloads are capability objects. Expose the smallest DTO that supports
  the point; never hand every interceptor a mutable `TurnSnapshot`.
- Permissions and durable settlement remain core operations outside
  reorderable middleware.
- Provider credentials, endpoints, resolved clients, observers, and raw config
  snapshots do not appear in extension diagnostics, durable request records,
  or Wasm DTOs.
- Native plugins are trusted. Scope is routing, not isolation. Wasm remains the
  boundary for untrusted code and receives only curated, bounded WIT data.
- Callback errors include host-controlled component/point identity and bounded
  sanitized public text. Preserve the raw cause only for local trusted logging;
  durable errors/events never copy `error.Error()` blindly or serialize
  arbitrary payloads.
