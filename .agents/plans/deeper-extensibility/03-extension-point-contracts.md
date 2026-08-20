# Extension Point Contracts

<!-- markdownlint-disable MD013 -->

## Contract Rules

Every runtime point must document and test:

- exact insertion point relative to durable writes and core policy;
- input ownership and defensive-copy behavior;
- allowed output/decision variants;
- ordering and `next` semantics;
- cancellation and timeout source;
- error, panic, and short-circuit behavior;
- replay/resume behavior;
- whether it is exposed to native Go, Wasm, or both;
- a stable diagnostic name.

The tables below are the minimum catalog. Adding a point during implementation
requires updating this document, runtime architecture docs, and the
producer/consumer map; it is not a private one-line callback.

## Notification Points

Notifications are immutable observations. Each handler receives its own
defensive container copy where a payload contains slices, maps, messages, tool
schemas, or JSON. Handlers run in deterministic order. Errors and panics are
contained per handler, reported through the plan's nonrecursive diagnostic
reporter, and never starve later handlers. `Notify` returns a bounded failure
set only for fixed policies whose runtime caller must act; contained points
report and return no run-affecting error.

| Diagnostic name | Payload | Publish point | Durable relationship |
| --- | --- | --- | --- |
| `runtime/run-admitted` | IDs, frozen config identity summary, plan descriptor, admission time | Immediately after the admission transaction, before legacy `BeforeRun` | References the durable run-start `EventRecord`; fires even if `BeforeRun` later fails |
| `runtime/run-started` | Run/session IDs and start time | After run status becomes running | Run record is already updated |
| `runtime/run-settled` | Immutable `Result`, usage, duration, classified error | Only after `FinishRun` succeeds; before plan release | References the durable run ID/status; there is no core terminal `EventRecord` today |
| `runtime/model-requested` | Attempt/step identity, provider/model, request-record ID, message/system/tool counts and hashes | After request record reaches `dispatch-started`, immediately before adapter call | A durable prepared record already exists; dispatch may still fail before network I/O |
| `runtime/model-completed` | Attempt/step identity, usage, finish/error classification | After stream closes/fails and the ledger terminal transition commits | Request record is already `completed` or `failed`; on ledger failure emit only a diagnostic and fail the run |
| `runtime/tool-prepared` | Tool/call identity, final normalized input, component provenance | After all input transforms, before `CreateToolCall` | No durable tool call yet; observation failure cannot affect admission |
| `runtime/tool-started` | Immutable call and start time | After durable claim/running update | Tool-call record is running |
| `runtime/tool-settled` | Immutable final call/outcome/status/public error | After the atomic settlement unit or idempotent reconciliation commits | Tool-call record and model-visible result message/part are both authoritative |
| `runtime/event-published` | Defensive `runtime.Event` copy | After handing the event to the infrastructure sink at that call site's existing point | Carries durable event ID when applicable; live-only remains live-only |

Do not publish every token delta twice by default. `runtime.EventMessageDelta`
already traverses the bounded event queue and can be observed through
`runtime/event-published`. If benchmarks show generic observer dispatch on that
hot path violates the budget, expose an explicit opt-in high-frequency flag at
plan snapshot time and skip delta observers when none are registered.

## Run Gate Point

Use an interceptor whose terminal returns a decision instead of treating a
zero value or first non-nil error as an implicit veto.

### `runtime/run-before-execute`

Input contains the durable run identity and safe config/model summaries. The
terminal decision is `Continue`. An interceptor may return `Reject{Code,
Message}`. This point runs after admission, so rejection settles the run failed
or interrupted according to its explicit code; it never rolls the admission
back.

This is new-registry-only. Legacy `Hook.BeforeRun` remains synchronously inside
`Admitter.afterDurableAdmission`, after durable admission and before the live
`EventRunStarted` sink call. Its error still makes `Start` return an error while
leaving the admitted run pending. Characterize that behavior; do not adapt,
move, settle, or invoke the legacy hook twice in this series.

### No generic terminal “turn stopping” point yet

The current runtime does not persist the same turn/step abstraction as Harness.
Its `executeOne` loop performs multiple provider/tool steps inside one admitted
run, while `Hook.AfterTurn` runs once for the run. Do not publish a misleading
`turn-stopping` point. First introduce explicit durable step identity in the
request-ledger slice; a later proposal may add turn control with precise
semantics.

## Context And Prompt Assembly

Diagnostic name: `runtime/context-assemble`.

```go
type ContextAssembly struct {
    SessionID     session.ID
    RunID         session.RunID
    EpochID       session.EpochID
    Base          []*schema.Message
    Contributions []ContextContribution
}

type ContextContribution struct {
    Source  string
    Order   int
    Message *schema.Message
}
```

The terminal begins with durable history plus admitted input. New interceptors
may change only `Contributions`; changes to IDs or `Base` are rejected.
Contributions sort by `(Order, Source)` and duplicate source IDs in one assembly
fail. The runtime materializes `Base + Contributions` and later writes that
canonical sequence to the request ledger.

`ContextSource` adapters append sourced contributions using a stable synthetic
source based on registration order. Legacy `Hook.BeforeTurn` still receives a
whole cloned snapshot for compatibility after context sources; mutations it
makes are captured by the request ledger but new APIs do not grant this broad
authority.

Current exact legacy preparation order is context sources, then
`Tools.ResolveTools`, then `Hook.BeforeTurn`. Preserve it. New context assembly
does not move tool resolution or claim to support dynamic between-step
injection; it is evaluated once at current snapshot preparation for
compatibility.

Native exposure: yes. Curated WIT exposure: existing `context-source@0.1.0`
world through an adapter, with its current bounded plain-text messages. No
generic assembly object crosses Wasm.

### Named prompt contributions

Add a separate reversible capability rather than letting context interceptors
replace a whole string:

```go
type PromptSection struct {
    Name  string
    Order int
    Text  string
}
```

Global and exact-session sections merge deterministically; a session section
with the same name shadows its global section. Duplicate names within one layer
fail. Providers are evaluated per provider step against a bounded
`PromptContext` so dynamic state can refresh without mutating durable history.
Sections render in `(Order, Name, InstanceID)` order into one system string.

`TurnSnapshot.SystemPrompt` is currently frozen but never placed in
`model.Request`. Do not activate it incidentally. Add an explicit opt-in such
as `WithSystemPromptMaterialization(true)`: when enabled, configured
`Agent.SystemPrompt` is the built-in section and the final rendered string is
carried as a new cloned `model.Request.System` field. `model.Streamer` receives
that field directly; the fallback Eino path prepends one system message derived
from it immediately before calling the client. Existing durable system messages
keep their relative order after that generated first message.
Default-off characterization preserves legacy behavior; enabling it is an
intentional behavior change with exact token-order tests.

## Model Request Interceptors

Do not add `runtime/model-options` in this series. The normalized
`model.Streamer` path receives `model.Request.Options`, but the fallback Eino
`ToolCallingChatModel` path ignores them after its client was built during
resolution. A portable options transform requires a later request-aware model
execution contract used by every adapter. Record this as a Harness
`agent/request` parity gap rather than shipping path-dependent behavior.

### `runtime/model-stream`

This around point wraps exactly one call to `openStream`. It supports timing,
tracing, circuit-breaking, and a host-owned timeout while preserving request
identity. The guarded `next` may run once. Its return is the stream reader or a
classified error; a wrapper cannot replace messages, tools, identity, or
observer.

A successful result requires `next` exactly once. A wrapper that does not
delegate may return only a typed failure; it cannot fabricate a reader/success.
This point observes the canonical runtime-to-adapter request, not the adapter's
final provider wire encoding.

Retries remain orchestrator-owned. An interceptor must not call `next` twice to
implement retries. This point is native-only; provider adapters remain the
place for credential/header logic.

Harness-like request-error recovery is deferred. The orchestrator retains
exclusive retry ownership. A future typed decision may stop or tighten bounded
core retries, but must never exceed configured attempts or retry a normalized
non-retryable failure without separate policy review.

## Tool Pipeline

The binding sequence is:

```text
decode and normalize model input
legacy BeforeToolCall + runtime/tool-prepare waterfall
compute final pattern
durable CreateToolCall(pending) and claim(running)
evaluate mounted monotonic ToolGuard set (deny/abstain only)
if guards abstain: run the existing permission/approval loop unchanged
if allowed: runtime/tool-execute waterfall (tool body next exactly once)
if denied/ask unresolved: skip execution waterfall and create protected outcome
legacy AfterToolCall + runtime/tool-result-transform waterfall
encode payload without changing protected outcome
atomically settle tool call + model-visible result, or reconcile idempotently
runtime/tool-settled immutable notification
```

### `runtime/tool-prepare`

Input/output is a `PreparedToolCall` containing immutable identity/tool fields
and replaceable JSON input. Replacement must be valid normalized JSON and stay
within host bounds. It cannot change ID, session, run, message, tool name,
scope, permission set, retry-safety, or executor.

This point may return a typed failure but cannot return allow/deny. Permission
decisions remain in `permissions.Policy` after final input/pattern computation.

### Mounted monotonic guards

Plugins may register scoped `ToolGuard` callbacks evaluated on the final
normalized call before the existing permission/approval loop. A guard returns only `Deny` or
`Abstain`; no guard can grant, override an earlier deny, or observe credentials.
All applicable guards run so audit does not depend on order, and any deny wins.
This supports plan mode, sandbox, and session restrictions without moving core
permissions into reorderable middleware.

When all guards abstain, call `ExecuteToolWithPermissions` with its exact
current sequential decision/approval behavior, callback count, interruption
timing, model-visible strings, and durable completed status. Wrap its result in
the protected internal disposition. This preserves legacy behavior while a
mounted guard may preempt the whole legacy loop with denial.

### `runtime/tool-execute`

The input contains the immutable prepared call, tool metadata, and an execution
context. Guards and permissions have already allowed the call. `next` invokes
the tool body exactly once. The current runtime executes a run's prepared calls
serially but has no generic cross-run keyed concurrency gate; some executors may
provide their own locking. Around middleware may tighten the context
deadline, observe duration, or short-circuit with a typed failure. It cannot
replace the executor, loosen an existing deadline, call the body twice, or mark
a denied call successful.

Success requires `next` exactly once. Not calling it produces a protected
short-circuit-failure disposition. A denied or unresolved-ask call never enters
this waterfall.

This first version does not provide retry middleware. A future retry decision
must be a separate typed core policy gated by `RetrySafe` and durable attempt
records.

### `runtime/tool-result-transform`

Input is a protected `ToolOutcome` with disposition `executed`, `denied`,
`approval-required`, `interrupted`, or `failed`, immutable raw/classified error,
and a candidate model-visible `ToolResult`.
Transformers may replace result content/structured data/attachments/metadata
within existing retention bounds. They cannot change disposition, protected
permission metadata, raw/classified error, call identity, or final encoding.
The runtime validates every return. This closes the current gap where a content
transformer can erase permission metadata even though a deny result uses a nil
Go error.

### Legacy ordering

Treat the existing `ToolMiddleware` slice as one compatibility onion at order
`0`. New registrations with negative orders wrap it; positive orders run
inside it. Write a sequence test that names every stage. If preserving current
behavior requires treating each legacy middleware as its own ordered entry,
use stable synthetic IDs `legacy-tool-middleware/%06d` and document that
instead; do not leave the order implicit.

Native exposure: all three points. Wasm exposure: existing
`tool-middleware@0.1.0` maps only prepare and result-transform. Around execution
is native-only until a separate security review defines a bounded WIT
contract.

### Atomic settlement prerequisite

Before publishing the final notification, define an optional capability:

```go
type ToolSettlementStore interface {
    SettleToolCall(context.Context, ToolSettlement) error
    ListUnreconciledToolSettlements(context.Context, RunID) ([]ToolSettlement, error)
}
```

`ToolSettlement` carries call status/output/error plus result message/part IDs
reserved and persisted when the call is created. `SettleToolCall` is idempotent
by call ID and those IDs, and commits tool state plus result message/part as one
domain operation. SQLite implements it transactionally. Other implementations
may use a durable prepared/committed marker but must satisfy the same
idempotency/list contract. Strict registry-backed tool runs require this
capability; legacy runs/stores retain current behavior and do not receive the
new immutable-final-notice guarantee. Resume repairs unreconciled settlements
before unfinished calls. Add crash-point tests for every state.

## Reversible Capabilities

### Tools

Add:

```go
func (r *tools.Registry) Unregister(reg tools.Registration) error
```

It removes only the exact active generation and returns `ErrStaleRegistration`
otherwise. A `ResolveTools` call snapshots cloned definitions under the lock;
unregister does not invalidate already materialized run tools. Definitions gain
optional component provenance copied into the run plan/tool metadata without
exposing executable values.

Snapshot definitions in deterministic registration order instead of current
map iteration order. The composition coordinator leases exact generations and
materializes them outside its lock.

### Context contributors

New plugins register context assembly interceptors instead of mutating a global
slice. Multiple contributions have stable source IDs and order. The existing
`WithContextSource` path remains append-only and is adapted.

### Explicitly not converted

`session.Store`, `session.Transactor`, `model.Resolver`, `IDGenerator`, config
loaders/validators, and the observer remain construction dependencies. Config
snapshot plugins remain config-lifecycle plugins; they do not become runtime
events.

## Resume Matrix

| Point/capability | Fresh run | Pending-call resume | Running-call resume | Terminal-run resume |
| --- | --- | --- | --- | --- |
| Run admitted / legacy `BeforeRun` / run gate | Yes | No | No | No |
| Context and prompt assembly | Yes | No | No | No |
| Model stream/completion | Every provider attempt | No | No | No |
| Tool prepare / prepared notice | Before durable tool admission | No; input is already final and durable | No | No |
| Permissions and monotonic guards | Before fresh execution | Yes, using persisted final input and matched plan | No execution | No |
| Tool execute around point | Allowed fresh calls | Allowed re-execution only | No | No |
| Tool result transform | After actual fresh execution | After actual re-execution | No | No |
| Tool settled notice | After authoritative settlement | After settlement/reconciliation | After interrupted reconciliation | No |
| Run settled notice | At fresh terminal settlement | After resume settlement succeeds | After resume settlement succeeds | No new notice; return stored result |

`Resume` acquires and validates the persisted plan before changing durable
state, holds it through reconciliation and settlement notifications, and
releases it afterward. Legacy resume keeps current middleware behavior when
there is no strict descriptor.

## Existing WIT Phase B Mapping

- `hook@0.1.0`: build a partial turn DTO from `session.Run` at `BeforeRun`
  (known run/session/epoch/agent/provider/model; zero message/tool counts), cache
  the full bounded DTO by run ID at `BeforeTurn`, reuse it for `AfterTurn` and
  `AfterRun`, then delete it. The cache is concurrency-safe and bounded by
  active runs. If no full snapshot occurred, after methods use the documented
  partial DTO.
- `tool-middleware@0.1.0`: choose `ToolResult.Structured` when valid/nonempty;
  otherwise JSON-encode `Output` as a string. `unchanged` preserves attachments
  and metadata. Replacement JSON changes only output/structured payload while
  preserving attachments, metadata, protected disposition, and error.
- `event-sink@0.1.0`: map only the WIT bounded event fields and a host-generated
  bounded/redacted summary; never pass raw payload JSON.
- `context-source@0.1.0`: map bounded plain-text messages to sourced
  contributions and reject tool-call-shaped or multimodal output.

These mappings are part of Slice 0 acceptance; generated bindings alone do not
make the wrappers implementable.

## Custom Host Points

The generic package may be used by an embedder to define private typed points,
but the core runtime dispatches only its published tokens. There is no
string-based `Emit(name, any)` escape hatch and no promise that private host
points can cross process or Wasm boundaries.
