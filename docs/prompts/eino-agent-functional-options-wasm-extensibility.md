# Implement Functional-Options Construction And WIT-Based Wasm Extensibility

## Objective

Make `eino-agent` construction customizable through the Go functional options
pattern, and make its extension seams pluggable in two interchangeable ways:

1. A native Go implementation of the seam's existing public interface.
2. An `eino-agent`-provided wrapper that adapts a WebAssembly component
   conforming to a published WIT (WebAssembly Interface Types) contract into
   that same Go type.

Every `With<OptionName>` option accepts the seam's Go interface. Wasm never
appears in an option signature: a Wasm-backed extension is loaded first through
a wrapper constructor that returns the seam's native Go type, then wired in
exactly where a Go implementation would be. The orchestrator must not know or
care whether a dependency is native or Wasm-backed.

Use WIT as the interface definition language, the WebAssembly Component Model
as the packaging format, and `go.bytecodealliance.org` (the Bytecode Alliance
`go-modules` repository, providing `wit-bindgen-go` and the `cm` runtime
package) to generate Go code from the WIT contracts.

The benchmark for configurability is `earendil-works/pi`: an embedder should be
able to supply custom tools, veto tool calls, inject context before model
turns, subscribe to the event stream, swap persistence, and register providers
— programmatically, without forking — the way pi supports `customTools`,
`tool_call` blocking, `before_agent_start`/`context` injection,
`subscribe(listener)`, `sessionManager`, and `registerProvider`. `eino-agent`
is an embeddable runtime, not a TUI, so pi's presentation-layer extension
points (commands, shortcuts, renderers, themes) are explicitly out of scope.
pi can also *rewrite* tool-call arguments before execution; `eino-agent`'s
permission path (`runtime.ExecuteToolWithPermissions`) has no mutation seam,
so argument rewrite is a disclosed non-goal here, not a promise (see Explicit
Non-Goals and section 8).

## Relationship To The Previous Draft

This document replaces the Extism catalog foundation prompt. The Extism SDK,
the refreshable catalog store, catalog discovery plugins, the catalog fetch
broker, and the catalog ABI are dropped entirely. Do not add
`github.com/extism/go-sdk`. The provider-boundary invariants that draft
established remain binding here: model execution stays native, credentials and
endpoints stay host-owned, and nothing secret crosses the Wasm boundary.

## Design Principles

- The Go interface is the source of truth for every seam. Wasm wrappers are
  ordinary implementations of those interfaces, isolated in leaf packages.
- Keep model execution native. Do not stream model traffic, tokens, or
  provider credentials through Wasm.
- Core packages (`runtime`, `model`, `config`, `session`, `tools`,
  `permissions`) must not import any Wasm runtime or generated bindings.
- WIT files are versioned public contracts. Evolve them by adding new versioned
  packages, not by mutating published interfaces.
- Generated code is committed, reproducible via `go:generate`, and never
  hand-edited.
- Deny filesystem, network, environment, and process access to guests by
  default. Every host capability is opt-in and narrow.
- Do not serialize Go structs, Eino messages, interfaces, callbacks, or
  secrets across the Wasm boundary. Cross-boundary data is defined entirely in
  WIT.
- A misbehaving guest (trap, timeout, hang, oversized output) degrades only
  its own seam with a bounded, classified error; it must not corrupt
  orchestrator state or hang the run loop.
- Functional options are additive. Existing struct-literal construction of
  `runtime.StreamingOrchestrator` keeps working.

## Required Work

### 1. Functional Options For Orchestrator Construction

Add a constructor to `runtime`:

```go
func NewStreamingOrchestrator(opts ...Option) (*StreamingOrchestrator, error)
```

with one `With<FieldName>` option per public dependency of
`runtime.StreamingOrchestrator` (see `runtime/orchestrator.go`), including at
least:

- `WithStore(session.Store)`, `WithTransactor(session.Transactor)`
- `WithModelResolver(model.Resolver)` — provider registration flows through
  this option: hosts compose native adapters with
  `model.AdapterResolver{Adapters: ..., Catalog: ...}` (`model/provider.go`)
  and pass the resolver here.
- `WithToolRegistry(ToolRegistry)`
- `WithContextSource(ContextSource)` — appendable, may be given multiple times
- `WithEventSink(EventSink)`
- `WithHook(Hook)` — appendable
- `WithPermissions(permissions.Policy)`
- `WithIDGenerator(IDGenerator)`, `WithClock(func() time.Time)`
- `WithOwnerID(string)`, `WithTrace(agentcontext.TraceContext)`
- `WithAttempts(int)`, `WithToolTurns(int)`, `WithQueueSize(int)`,
  `WithLease(time.Duration)`
- `WithHistory(history.Options)`
- `WithObserver(*einoobs.Observer)`

`Admit *Admitter` is intentionally excluded: it is a derived aggregate the
orchestrator synthesizes from `Store`, `Transactor`, `Events`, `Hooks`, and
`Clock` (`runtime/orchestrator.go`, `admitter()`), not an independent
dependency. Document that carve-out in the constructor's doc comment.

Rules:

- Options apply in order; a later option for a scalar field overrides an
  earlier one, and appendable options accumulate deterministically.
- `NewStreamingOrchestrator` validates the result and returns an error listing
  every missing required dependency (`Store`, `Model`, `IDs`), rather than
  deferring failure to `Start`. Supply the same defaults the zero-value struct
  currently receives for optional fields.
- A nil value passed to an interface-typed option (`WithStore`,
  `WithTransactor`, `WithModelResolver`, `WithToolRegistry`,
  `WithContextSource`, `WithEventSink`, `WithHook`, `WithPermissions`,
  `WithIDGenerator`) is a construction error, not a silent no-op. Func-,
  struct-, and scalar-typed options (`WithClock`, `WithHistory`,
  `WithAttempts`, `WithToolTurns`, `WithQueueSize`, `WithLease`,
  `WithOwnerID`, `WithTrace`) and the pointer-typed `WithObserver` keep
  today's documented zero-value-means-default behavior
  (`runtime/orchestrator.go` fallbacks).
- Do not remove or deprecate the exported struct fields in this milestone;
  document the constructor as the preferred path.
- Apply the same pattern to `tools.NewRegistry` only if it needs new
  configuration for this work; do not gratuitously convert existing
  constructors.

### 2. Function Adapters For Single-Method Seams

Add `http.HandlerFunc`-style adapters so plain Go functions satisfy the
single-method interfaces that participate in options. Net-new adapters:

- `runtime.EventSinkFunc`, `runtime.ContextSourceFunc`,
  `runtime.ToolRegistryFunc`
- `model.ResolverFunc`

Already exist — do not re-add; add compile-time interface assertions and reuse
them in examples/tests:

- `permissions.PolicyFunc` (`permissions/policy.go`)
- `agentcontext.LoaderFunc` (`context/types.go`; note the package's Go name is
  `agentcontext`, not `context`)

Multi-method interfaces (`session.Store`, `model.Adapter`) do not get func
adapters. For `runtime.Hook`, provide a `HookFuncs` struct with optional
per-phase function fields whose nil members are no-ops.

`config.Loader`, `config.Validator`, and `config.SnapshotPlugin` do not
participate in orchestrator options (the orchestrator has no config-lifecycle
field, and the repo has no production `config.PluginRegistry`/`Lifecycle`
wiring today), so they get no adapters in this milestone.

### 3. WIT Contracts

Create a top-level `wit/` directory containing the versioned contract package
`eino-agent:extensions@0.1.0`. Define a shared `types` interface plus one
world per Wasm-extensible seam, delivered in two phases:

**Phase A (implement fully in this milestone):**

- `tool` — one guest-implemented tool: metadata (name, description, JSON
  schema for parameters, retry-safety, required permission names) and an
  `execute` function taking a tool-call ID, normalized JSON input, and the
  bounded turn metadata defined below, returning JSON output or a structured
  error. Maps to a `tools.Definition`.
- `permissions-policy` — `decide` over a permission request DTO (tool name,
  the single permission name being decided, bounded arguments summary,
  session/run identity — mirroring `permissions.Request`, which carries one
  `Permission` per call; the runtime calls `Decide` once per required
  permission), returning exactly one of allow, deny, or ask plus reason. Maps
  to `permissions.Policy`, giving Wasm parity with pi's `tool_call` blocking.

**Phase B (WIT authored now; wrappers, fixtures, and tests may land in a
follow-up PR series within this milestone, after the Phase A pattern is
proven):**

- `context-source` — `load-context` over the bounded turn metadata, returning
  an ordered list of role-plus-text messages. v1 messages are plain-text
  system/user/assistant content only; multimodal and tool-call-shaped
  `einoschema.Message` fields are out of scope. Maps to
  `runtime.ContextSource`, the analogue of pi's
  `before_agent_start`/`context` injection.
- `event-sink` — `emit` receiving a bounded event DTO (kind, session/run
  identity, timestamps, bounded payload summary). Fire-and-forget from the
  guest's perspective; maps to `runtime.EventSink` and pi's `subscribe`.
- `hook` — `before-run`, `before-turn`, `after-turn`, `after-run` receive the
  bounded snapshot DTO below and return only success or a structured error.

**Bounded turn metadata** (the only snapshot projection that crosses the
boundary, used by `tool`, `context-source`, and `hook`): run ID, session ID,
epoch ID, agent name/mode, model provider/model IDs, ordered tool names,
message count and per-role counts, and whether a system prompt is set.
`TurnSnapshot.Config` (the full `config.Snapshot`) and `TurnSnapshot.Model`
(`model.Resolved`, which holds live Eino client interfaces) are explicitly
excluded wholesale — never projected, however partially, into guest input.

**Dropped from v1:** a `config-plugin` world. `config.SnapshotPlugin.Apply`
mutates `*config.Snapshot` in place, the repo has no production
`PluginRegistry`/`Lifecycle` wiring to host it, and `config.Snapshot` has no
secret/non-secret field classification (`ProviderConfig.Options`,
`Agent.Options`, etc. are untyped string maps), so a "non-secret config
patch" contract cannot be defined honestly yet. Revisit only after a
config-side milestone establishes secret classification and patch/merge
semantics.

Explicitly not Wasm-extensible: `session.Store` and `session.Transactor`
(chatty, transactional, latency-sensitive), `model.Resolver`, `model.Adapter`,
and `model.Streamer` (model execution stays native), and `runtime.IDGenerator`
(durable identity stays host-owned).

WIT rules:

- Every DTO field is an explicit WIT type; no opaque pass-through of host
  structs. JSON payloads cross the boundary as `string` fields with documented
  size bounds and host-side validation.
- DTOs carry identity and bounded summaries — never credentials, provider
  endpoints, raw provider payloads, or conversation content beyond what the
  seam requires.
- Record the contract-evolution policy in `wit/README.md`: published packages
  are immutable; breaking changes mean `@0.2.0` worlds living alongside
  `@0.1.0` support.

### 4. Code Generation With go.bytecodealliance.org

- Pin `go.bytecodealliance.org` in `go.mod` and drive
  `wit-bindgen-go generate` from a `go:generate` directive behind a
  `make wit` target.
- Toolchain provisioning must be pinned and reproducible, locally and in CI:
  install `wit-bindgen-go` via `go run go.bytecodealliance.org/cmd/wit-bindgen-go@<pinned version>`
  (version from `go.mod`), and fetch a pinned `wasm-tools` release binary in
  the CI workflow (the current workflow only sets up Go — add the install
  step). CI then runs `make wit` and fails on any diff.
- Generated bindings live under a dedicated tree such as `wasmext/gen/` with a
  package comment marking them generated; they are the canonical Go form of
  the DTOs.
- `wit-bindgen-go` primarily targets guest-side bindings. Use it for two
  things: (a) the guest SDK — so extension authors writing Go/TinyGo guests
  compile against generated exports for the same WIT worlds — and (b) shared
  DTO types where practical. Host-side lifting/lowering goes through the
  component runtime's typed API (see section 5); if hand-written host glue is
  required, isolate it next to the generated code and cover it with round-trip
  tests against the fixture guests.
- Ship a buildable example guest for each Phase A world under
  `examples/wasm-extensions/`, written in Go/TinyGo against the generated
  bindings, with build instructions. Phase B worlds get example guests when
  their wrappers land.

### 5. Wasm Host Runtime Leaf Package

Add a leaf package (suggested name `wasmext`) that owns component loading and
invocation. Runtime selection: use `github.com/bytecodealliance/wasmtime-go`'s
component-model API (`NewComponent` and typed function calls) as the initial
engine — wazero does not support the component model — but hide it behind an
internal engine interface inside `wasmext` so the engine can be swapped
without touching wrapper APIs. Note in the package documentation that
wasmtime-go requires CGO, and record that trade-off.

Loading and execution posture (carried over from the previous draft's security
model):

- Load only explicitly configured local component files. Resolve the canonical
  path under an allowed root, reject escaping symlinks and URL loads, enforce
  a maximum file size before compilation, read the bytes exactly once, verify
  an expected SHA-256 over those bytes, and compile from those same bytes to
  avoid a verification-to-load race.
- Verify at load time that the component exports the expected world/interface
  version; fail closed with a classified error otherwise.
- Compile once per module; never share a single instance across concurrent
  calls. Use instance-per-call or a serialized instance pool.
- Enforce nonzero host limits on execution time, memory, input size, and
  output size per call. Guest configuration may tighten but never relax host
  limits.
- Timeouts must use an **active interrupt mechanism** (wasmtime epoch
  deadlines or an equivalent engine interrupt), not context passthrough
  alone: Go context cancellation is cooperative and cannot preempt a hung
  synchronous guest call. This matters concretely because the orchestrator's
  event queue `close()` blocks until every buffered event has been emitted
  (`runtime/orchestrator.go`, `eventQueue`); a hung guest `Emit` with no
  forced trap would deadlock the run permanently.
- WASI is disabled by default. No filesystem mappings, no sockets, no
  environment, no clocks/random beyond what the engine requires for
  determinism-safe defaults.
- Convert traps, timeouts, malformed payloads, contract violations, and
  oversized outputs into bounded, classified Go errors that identify the
  module by configured name and hash — never by guest-supplied strings.
- Lifecycle ownership: the orchestrator has no shutdown method and never
  learns a dependency is Wasm-backed, so module lifetime is owned by the
  embedding host. Every concrete wrapper type additionally implements
  `io.Closer`; `wasmext` also provides a `Loader` (or similarly named) handle
  that tracks the modules it opened and exposes `Close(ctx) error` for
  one-call shutdown. Close semantics: stop accepting calls, interrupt
  in-flight work with a bounded drain, release instances and compiled state
  exactly once; further calls on closed wrappers return a classified error.
- Add a dependency test (a Go test driving `go list -deps` over the module)
  proving only `wasmext` and its subpackages import the engine and generated
  bindings.

### 6. Per-Seam Wasm Wrappers

For each implemented world, `wasmext` exposes a constructor returning the
seam's native Go type (Phase A first, Phase B when its wrappers land):

```go
func LoadTool(ctx context.Context, cfg ModuleConfig) (tools.Definition, error)
func LoadPermissionsPolicy(ctx context.Context, cfg ModuleConfig) (permissions.Policy, error)
func LoadContextSource(ctx context.Context, cfg ModuleConfig) (runtime.ContextSource, error)
func LoadEventSink(ctx context.Context, cfg ModuleConfig) (runtime.EventSink, error)
func LoadHook(ctx context.Context, cfg ModuleConfig) (runtime.Hook, error)
```

`ModuleConfig` carries path, expected SHA-256, per-call limits, and bounded
non-secret guest configuration. Wrapper semantics:

- Wrappers translate between runtime types and WIT DTOs at the boundary,
  applying the bounds from section 3 in host code on both directions.
- A guest error or resource-limit violation surfaces as an ordinary Go error
  from the wrapped method; the orchestrator's existing failure handling for
  that seam applies unchanged.
- `LoadTool` returns a `tools.Definition` (a struct of closures, not an
  interface) whose `Execute` performs the guest call and whose `Decode` and
  `Encode` are JSON passthroughs validating size bounds; `Normalize` is unset.
  It reaches the orchestrator the same way native definitions do:
  `tools.NewRegistry()` → `Register(definition)` → `WithToolRegistry(registry)`.
- The Wasm-backed `runtime.Hook` echoes its input snapshot unmodified from
  `BeforeTurn`. This is a wrapper-implementation policy, not a change to
  `runtime.Hook` — the interface's `BeforeTurn(ctx, TurnSnapshot)
  (TurnSnapshot, error)` contract still permits native hooks to mutate, and
  the orchestrator continues to apply returned snapshots.
- `runtime.EventSink` reality check (verified): every `Emit` call site in the
  orchestrator and admitter discards the error today — behavior is
  silent-drop, with no observer logging, and the tool-call status emits are
  synchronous on the run loop's critical path (up to three per tool call).
  The Wasm sink wrapper must therefore enforce a small per-call timeout and
  return promptly; any observer logging of sink failures is new behavior and
  must be listed as its own scoped change if added. Document the added
  per-tool-call latency of a Wasm sink in `docs/architecture/extensibility.md`.
- Wrappers are safe for concurrent use if and only if the underlying
  instance strategy is; make that true and test it under `-race`.

End-to-end, native and Wasm-backed dependencies wire in identically:

```go
policy, err := wasmext.LoadPermissionsPolicy(ctx, wasmext.ModuleConfig{ /* ... */ })
// or: policy := permissions.PolicyFunc(func(ctx context.Context, req permissions.Request) (permissions.Decision, error) { /* ... */ })

def, err := wasmext.LoadTool(ctx, wasmext.ModuleConfig{ /* ... */ })
registry := tools.NewRegistry()
_, err = registry.Register(def) // same registration path as a native tools.Definition

orch, err := runtime.NewStreamingOrchestrator(
    runtime.WithStore(store),
    runtime.WithModelResolver(resolver),
    runtime.WithIDGenerator(ids),
    runtime.WithToolRegistry(registry),
    runtime.WithPermissions(policy),
)
// The embedder, not the orchestrator, later closes what it loaded:
// defer loader.Close(ctx)
```

### 7. Host Capability Imports

v1 defines exactly one host import interface in WIT, `eino-agent:host/log`,
letting guests emit leveled, size-bounded log lines that the host routes
through its observer with the module's configured identity attached. No
filesystem, network, environment, shell, or credential host functions. The
WIT contract package structure must make adding a future narrow capability
(e.g. a fetch broker) an additive, versioned change.

### 8. Parity Map Against pi

Add `docs/architecture/extensibility.md` containing an explicit mapping table:
pi extension point → eino-agent seam → native path → Wasm path → or "out of
scope (presentation layer)". Cover at minimum: custom tools, tool-call veto,
tool-call argument rewrite (record as a gap: no mutation seam exists in
`runtime.ExecuteToolWithPermissions`), system-prompt/context injection, event
subscription, session persistence, provider registration (native path:
`model.AdapterResolver` via `WithModelResolver`; no Wasm path by design), and
model selection. Every "supported" cell must name a real exported symbol;
every gap must say gap, not hand-wave.

## Tests And Acceptance Criteria

- Options: construction succeeds with the minimal required set; every missing
  required dependency is named in one error; ordering/override and appendable
  accumulation are covered; nil interface-typed options fail; struct-literal
  construction still passes the existing suite.
- Func adapters (new and pre-existing) compile-time-assert their interfaces
  and are exercised in at least one orchestrator-level test.
- One checked-in fixture component per implemented world (source plus
  reproducible build) proves the wrapper round-trip: tool execute, policy
  decide (all three decisions); context load, event emit, and hook observe
  when Phase B lands.
- Contract-violation coverage per wrapper: trap, timeout of a deliberately
  hung guest (proving active interruption, including the event-queue drain
  path), oversized output, malformed payload, wrong world/version, hash
  mismatch, calls after Close, and Close during in-flight calls — all fail
  closed with classified errors, under `go test -race`.
- Sentinel test: a credential-like sentinel placed in
  `config.Snapshot.Providers[].Options`, `Agent.Options`, provider runtime
  options, and resolved model state never reaches any guest input,
  guest-visible DTO, or wrapper error string.
- A black-box test constructs an orchestrator via
  `NewStreamingOrchestrator` mixing native and Wasm-backed seams, runs a
  scripted model turn with a Wasm tool gated by a Wasm policy, and asserts on
  the durable session records.
- Dependency test (`go list -deps`-based): core packages import neither the
  engine nor generated bindings; only `wasmext` does.
- Regeneration check: `make wit` produces no diff in CI using the pinned
  toolchain.
- `go test ./...` and `make check` remain green.

## Documentation

- New: `docs/architecture/extensibility.md` (options pattern, seam table,
  Wasm trust model, resource bounds, lifecycle and Close ownership, sink
  latency note, pi parity map including the disclosed rewrite gap).
- New: `wit/README.md` (contract versioning and evolution policy).
- Update `docs/consumer-guide.md` to lead with `NewStreamingOrchestrator` and
  show one native and one Wasm-backed option side by side, including the
  tool-registration path and embedder-owned Close.
- Update `docs/architecture/security.md` (guest threat model, capability
  defaults, module verification, active-interrupt timeout posture),
  `runtime.md` (constructor, Admit carve-out, hook contract),
  `permissions.md`, and `tools.md` where their seams gained a Wasm path.
- Update the minimal example only if the constructor changes its wiring;
  Wasm usage belongs in `examples/wasm-extensions/`, not the minimal server.

## Explicit Non-Goals

- No Extism, no catalog discovery, no refreshable catalog store, no fetch
  broker — the entire previous catalog scope is dropped, not deferred into
  this milestone.
- No Wasm model execution, token streaming, or provider adapters in Wasm.
- No tool-call argument rewrite from any extension path (native or Wasm);
  `permissions.Decision` carries no replacement input, and adding a mutation
  seam to tool execution is out of scope. Disclose it in the parity map.
- No `config-plugin` Wasm world, and no config secret-classification work —
  both deferred until the config lifecycle has production wiring and a
  secret/non-secret field model.
- No guest access to credentials, endpoints, filesystem, network,
  environment, or shell.
- No plugin marketplace, package installer, discovery of modules from
  directories, file watching, or hot reload.
- No TUI, slash commands, keyboard shortcuts, renderers, or themes.
- No removal of existing struct-literal construction or existing public
  interfaces.

Deliver the smallest coherent implementation satisfying these contracts.
Avoid unrelated runtime, durability, AG-UI, or observability redesigns.
