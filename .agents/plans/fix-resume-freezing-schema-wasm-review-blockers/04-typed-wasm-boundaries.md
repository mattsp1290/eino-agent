# Typed Wasm Boundaries

## Goal and prerequisite

Replace the generic operation/type switchboard with compile-time typed component methods, retain one small Wasmtime raw-call primitive, and split multi-world files into cohesive units.

## Existing evidence

- `wasmext/engine.go` defines `compiledComponent.Call(ctx, operation string, input, output any)`.
- `wasmext/module.go` wraps the generic call with limits and timeout accounting.
- `wasmext/wasmtime_abi.go` has separate lower/invoke/lift switches keyed by the same operation string.
- `wasmext/wrappers.go` implements tool, permission, context, event, hook, and middleware wrappers in one file.
- Fake components in `wasmext/wasmext_test.go` reproduce the stringly generic interface.

## Target interface

Replace `compiledComponent` with a minimal lifecycle interface (`Interrupt`, `Close`) plus one narrow typed interface per existing WIT world. Each world interface embeds lifecycle and uses existing `wittypes` values plus small proposed request/result structs where an operation has multiple fields:

```text
ToolMetadata(ctx) (wittypes.ToolMetadata, error)
ExecuteTool(ctx, proposed ToolExecuteInput) (string, error)
DecidePermissions(ctx, proposed PermissionInput) (wittypes.PermissionDecision, error)
LoadContext(ctx, proposed ContextInput) ([]wittypes.ContextContribution, error)
EmitEvent(ctx, wittypes.EventRecord) error
BeforeModel / AfterModel / BeforeTool / AfterTool (typed inputs and outputs)
```

Names must match current domain language; this sketch is conceptual. Hook operations may share one typed hook method plus a closed typed enum only when their WIT payload/result types are identical. They must not return to `any`.

The shared module core changes its runner to accept an operation label, byte count, and closure independent of a universal component interface. World-specific modules hold only `toolComponent`, `permissionsComponent`, `contextComponent`, `eventComponent`, `hookComponent`, or `middlewareComponent`. The label is metrics/error context only and never selects types or functions.

## Change surface

- `wasmext/engine.go`: replace generic `Call` with lifecycle plus six narrow world interfaces and define minimal typed transport structs at the existing interface insertion point.
- Engine compile/load boundary: add explicit world-specific compile/factory entry points (or a closed internal typed adapter selected at load time) that return the corresponding narrow interface; do not require a component to implement unrelated worlds.
- `wasmext/module.go`: keep a shared lifecycle/resource core and replace generic input/output dispatch with a closure-based timeout/resource wrapper; add small world-specific module holders; preserve poisoned-instance and inflight semantics.
- `wasmext/engine_wasmtime.go`: implement/delegate typed methods instead of `Call`.
- `wasmext/wasmtime_abi.go`: reduce to component lifecycle, canonical ABI allocation/free helpers, and a proposed typed-neutral raw export invocation helper.
- `wasmext/wasmtime_abi.h` (new, parent `wasmext/`): shared static C bridge declarations/definitions if multiple cgo Go files require the helpers.
- `wasmext/wasmtime_tool.go`, `wasmtime_permissions.go`, `wasmtime_context.go`, `wasmtime_event.go`, `wasmtime_hooks.go`, and `wasmtime_middleware.go` (new, parent `wasmext/`): world-specific lower/invoke/lift code. Combine only genuinely tiny worlds with identical data flow.
- `wasmext/wrappers.go`: delete after moving wrappers into `tool.go`, `permissions.go`, `context.go`, `event.go`, `hooks.go`, and `middleware.go` (new, parent `wasmext/`), plus a small shared loader/projection file where required.
- `wasmext/wasmext_test.go`: replace generic fake dispatch with typed fake methods or focused fakes per world; split tests when the file's responsibilities mirror the old giant source.
- `wasmext/engine_stub.go` or build-tag peers: implement the final typed contract for non-cgo builds.

## Invariants and error paths

- Each wrapper can invoke only the typed component method for its world.
- Each backend and fake implements only one world interface plus lifecycle. Adding a method to one world does not change other world implementations.
- Each Wasmtime method binds one fixed export name and one fixed lower/lift pair; no operation string chooses codecs.
- Shared raw invocation owns allocation/free and validates result discriminants exactly once.
- Timeout, cancellation, fuel/epoch behavior, memory limits, poisoned-instance handling, and inflight exclusion remain observable as before.
- Unsupported/missing exports return the current typed extension error with the operation name for diagnosis.
- Input-size accounting uses the canonical typed payload encoded for that operation; it does not rely on `any` reflection.
- C allocations are freed on success and every lowering/invocation/lifting error path.
- New production Go files should remain comfortably below 500 lines; no modified production file may reach 1,000 lines.

## Tests and acceptance

- Existing conformance tests pass for every WIT world and middleware/hook phase.
- Focused tests prove a typed wrapper cannot accidentally dispatch to another export and that malformed result discriminants fail.
- Timeout, cancellation, poisoned-instance, inflight, memory-limit, and missing-export tests remain present after splitting.
- `CGO_ENABLED=0 go test ./wasmext` passes against the stub contract.
- Normal `go test ./wasmext` passes where Wasmtime is available.
- `rg -n "Call\(ctx context\.Context, operation string|callABI\(|input, output any|switch operation" wasmext` returns no production generic dispatcher.
- `wc -l wasmext/*.go` shows no production file at or above 1,000 lines and confirms the two former giant responsibilities are decomposed.

## Dependencies and exclusions

- Apply checked definition contracts from [02-checked-tool-freezing.md](02-checked-tool-freezing.md) while moving tool wrappers.
- Do not change WIT files, export names, component wire representation, or public extension semantics.
- Do not generate a broad reflection framework or a new generic request envelope.
