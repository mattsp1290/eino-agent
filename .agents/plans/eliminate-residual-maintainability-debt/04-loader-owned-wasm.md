# Loader-owned Wasm Lifetime

## Repository evidence

- `wasmext.Loader.Load*` tracks modules and closes them as a group.
- `wasmext/wrappers.go` also exports six `Open*` constructors that return
  independently closable adapters.
- Those constructors are used only by package tests; production composition uses
  the loader path.
- Exported `Close` methods let callers race or interleave per-adapter shutdown
  with loader shutdown even when the loader created the adapter.

## Exact change surface

- `wasmext/wrappers.go`
  - Replace public `OpenTool`, `OpenPermissionsPolicy`, `OpenContextSource`,
    `OpenEventSink`, `OpenHook`, and `OpenToolMiddleware` with private open
    helpers shared by the loader.
  - Remove public `Close` methods from loaded adapter types. Make concrete loaded
    tool, permission-policy, and event-sink types private because loader methods
    already return interfaces/definitions; retain exported concrete types only
    where a public loader signature needs them.
  - Expose private resource finalizers that perform adapter cleanup (including
    clearing hook turn caches) plus module shutdown.
- `wasmext/loader.go`
  - Use a two-phase load transaction: fully validate/build the adapter and clone
    any returned definition before tracking; pre-track failure finalizes
    privately; successful tracking transfers sole ownership and no later branch
    closes directly.
  - Track internal owned resources containing module and adapter-specific
    finalizer, exactly once, in load order.
  - Keep reverse-order, idempotent aggregate shutdown as the only public close.
- `wasmext/points.go`, `wasmext/projections.go`, tests, and docs
  - Change registration APIs that name privatized loaded types to accept their
    public behavior interface (for example `runtime.EventSink`) and update
    compile-time assertions accordingly.
  - Construct a loader, load adapters through it, and close the loader in test
    cleanup. Delete tests whose sole purpose is independent open ownership.
  - Assert loader close invalidates every returned adapter and repeated close is
    safe.

## Invariants and tests

- Every successfully returned Wasm adapter belongs to exactly one loader.
- A module is tracked once or closed before `Load*` returns an error.
- Closing a loader stops new loads, interrupts active calls, waits for finalizers,
  and closes modules in reverse load order.
- No exported `Open*` symbol or adapter `Close()` remains.
- Loader-returned interface values can be passed directly into each applicable
  `Register*` adapter; no public signature names a privatized concrete type.
- Component projection or definition-clone failure is never retained and is
  finalized exactly once. Loader close racing a slow load either owns the fully
  built resource or rejects and finalizes it; it cannot split ownership.
- Hook cache cleanup runs before module finalization. Close timeout may be
  followed by a successful repeated close that returns the final aggregate.

## Dependencies and risks

- Independent of model and tool-input work after shared test fixtures compile.
- Preserve WIT contracts, error classification, size limits, and concurrency
  behavior; this changes ownership only.
