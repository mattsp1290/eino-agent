# Checked Tool Freezing

## Goal and prerequisite

Make tool definitions immutable only after a successful deep clone. Propagate schema conversion failures to registration, plan acquisition, and Wasm loading instead of returning aliases or zero values.

## Existing evidence

- `tools.Definition.Clone` calls `cloneParamsOneOf`, whose error path returns the original `ParamsOneOf` pointer.
- Registry snapshot construction clones definitions through the unchecked path.
- `runtime.cloneTool` discards `cloneToolChecked` errors and returns a zero-value `Tool`.
- `runtime.sealedPlanTools.ResolveTools` and execution copies assume cloning cannot fail.
- `wasmext.LoadedTool.Definition` returns an unchecked clone.

## Change surface

- `tools/registry.go`: delete `cloneParamsOneOf`; expose or use an error-returning definition clone; make registry insertion and snapshot creation fail before storing any alias.
- `tools/registry_test.go`: replace fallback expectations with invalid-schema error tests and post-registration mutation isolation tests.
- `runtime/tool.go` and plan/tool resolution insertion points: delete `cloneTool`; use `cloneToolChecked` and propagate errors with tool identity and operation context.
- `runtime/plan.go` or the current sealed-tool implementation: make plan freezing and `ResolveTools` return an error if any definition cannot be cloned.
- `composition/registry.go`: propagate checked tool snapshot/plan-freeze errors without publishing a partial plan.
- `wasmext/wrappers.go` during the split in [04-typed-wasm-boundaries.md](04-typed-wasm-boundaries.md): make `LoadedTool.Definition` error-returning or freeze the definition once during load and return only a proven-safe clone. Prefer the former if cloning remains fallible.
- All callers and fakes of changed `Definition.Clone`, registry snapshot, and Wasm definition APIs: handle errors explicitly; no `Must` helper in production.

## Intended behavior

- Validation and clone are one fail-closed boundary: unsupported `ParamsOneOf` encodings reject the operation.
- Registry state changes only after every supplied definition is validated and independently cloned.
- Snapshot/plan acquisition is all-or-nothing. A failed tool clone publishes no partially frozen snapshot and acquires no executable plan.
- Returned definitions never share mutable schema pointers with registry or plan state.
- Runtime execution never substitutes a zero-value definition after a clone failure.
- Errors include the stable tool name and whether the failure occurred during registration, snapshot/plan acquisition, Wasm load, or execution copy.

## Tests and acceptance

- A deliberately non-serializable/custom schema representation returns a non-nil error from each checked public boundary.
- Mutating the caller's schema after registration does not change `Registry.Get`, `Snapshot.Get`, resolved plan tools, or Wasm-loaded definitions.
- Mutating a returned definition does not alter later reads.
- A multi-tool snapshot with one invalid definition fails without returning the valid subset.
- Plan acquisition failure releases any registry references and leaves no published descriptor/executable mismatch.
- `rg -n "cloneParamsOneOf\(|func cloneTool\(" tools runtime` returns no unchecked fallback implementation.
- `go test ./tools ./composition ./runtime ./wasmext` passes.

## Dependencies and exclusions

- Complete checked interfaces before mechanically splitting Wasm wrappers so the new files expose only the final contract.
- Do not preserve the infallible API for compatibility.
- Do not use JSON round trips whose errors are ignored, panic on caller data, or retain the original pointer on failure.
