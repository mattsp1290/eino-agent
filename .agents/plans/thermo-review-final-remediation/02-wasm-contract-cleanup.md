# Wasm Contract Cleanup

## Goal and prerequisite state

Make the pre-release WIT contract express the native runtime invariants directly. Wasm tools own permission-pattern derivation through a typed operation, and context sources cannot return an assistant message that runtime must reject.

No backward-compatible guest ABI is required.

## Explicit tool permission pattern

### Existing evidence

- Native `tools.Definition.Pattern` receives final normalized JSON and returns the persisted permission pattern.
- `wasmext.toolDefinition` currently implements that callback by probing a host-reserved `permission_pattern` JSON property.
- `tool-api` has no permission-pattern export, while Wasm permission policy receives the resulting value as `arguments-summary`.
- `runtime.prepareToolCalls` invokes the pattern resolver after input normalization and tool-prepare transforms, then persists the returned value.

### Proposed contract

Add this function to the existing `tool-api` interface in `wit/eino-agent-extensions.wit`:

```text
permission-pattern: func(input-json: string) -> result<string, structured-error>
```

The operation receives the final normalized JSON object. It returns one non-empty permission identity. Its ABI lift uses `min(module.limits.MaxOutputBytes, 4096)` so the runtime permission boundary is enforced before a larger result is allocated or returned.

### Exact change surface

- `wit/eino-agent-extensions.wit`
  - Add the required `permission-pattern` export to `tool-api`.
  - Document that its input is final normalized input and that the result is permission identity, not an authorization decision.
- `wasmext/engine.go`
  - Add a method to `toolComponent` for permission derivation.
  - Add `permission-pattern` to `toolContract.functions`.
- `wasmext/wrappers.go`
  - Replace JSON property probing with a bounded guest call.
  - Validate JSON/input bounds before the call.
  - Reject empty output as `ErrorContract`; reject oversized output as `ErrorSize`.
- `wasmext/wasmtime_worlds.go`
  - Lower the input string, invoke the new ABI export, and lift the structured result with an operation-specific output limit of `min(module.limits.MaxOutputBytes, 4096)`.
  - Map `errModuleTooLarge` at that boundary to `ErrorSize`.
- `examples/wasm-extensions/tool/main.go`
  - Register the generated permission-pattern export.
  - Derive a deterministic fixture pattern from input so tests prove the guest, not the host, owns the value.
- `wasmext/wasmext_test.go`, `wasmext/phase_b_test.go`, and relevant fake components
  - Update `toolComponent` fakes.
  - Test success, guest error, trap, timeout, empty pattern, 4,096-byte success, 4,097-byte `ErrorSize`, a configured output bound below 4,096, and final-input visibility.
  - Remove assertions that depend on the `permission_pattern` property convention.
- Generated files under existing `wasmext/gen/eino-agent/extensions/v0.1.0/` are regenerated outputs, not hand-edited sources.
- Rebuild checked-in components under `examples/wasm-extensions/fixtures/` with `make wasm-fixtures`.
- Update `wit/README.md`, `docs/architecture/extensibility.md`, `docs/architecture/tools.md`, and `docs/architecture/security.md`.

### Invariants and error paths

- Permission derivation runs after native normalization and `ToolPreparePoint` transforms.
- The guest cannot authorize a permission; it only derives the pattern supplied to the native policy.
- The runtime persists and reuses the derived pattern on resume.
- Guest errors remain bounded Wasm extension errors.
- Empty, malformed, trapped, timed-out, and oversized results fail the tool call before authorization.
- No host fallback probes input or substitutes the old reserved property.

## Context-source role contract

### Existing evidence

- `types.text-role` contains `system`, `user`, and `assistant`.
- `loadedContextSource.loadContextMetadata` converts all three values to Eino messages.
- `runtime.validateContextContributionMessage` rejects assistant messages before provider dispatch.

### Exact change surface

- `wit/eino-agent-extensions.wit`
  - Remove `assistant` from `text-role`.
  - Do not change the separate `role-counts.assistant` field.
- `wasmext/wrappers.go` and `wasmext/wasmtime_worlds.go`
  - Delete assistant conversion and decoding branches.
  - Treat unknown enum values as contract errors.
- `wasmext/phase_b_test.go`
  - Delete the test that constructs an advertised assistant context value and expects late runtime rejection.
  - Retain host rejection tests for malformed native callback values; WIT narrowing does not weaken the native boundary.
- Regenerate bindings and rebuild every fixture because the shared WIT types package changes.
- Update `wit/README.md` and extension-point documentation to state that context-source output is system/user text only.

### Tests and acceptance criteria

- A Wasm tool integration test proves the persisted `session.ToolCall.Pattern` equals the guest function result derived from transformed input.
- A failure test proves permission policy execution does not begin when permission-pattern derivation fails.
- Boundary failures prove neither permission policy execution nor durable tool-call creation begins.
- A resume test proves a pending persisted Wasm tool call reuses its stored pattern and never invokes the guest permission-pattern resolver again.
- Generated tool bindings expose the new function and generated text-role bindings contain no assistant case.
- `rg -n 'PermissionPattern.*json:"permission_pattern"|TextRoleAssistant' wasmext examples/wasm-extensions` returns no production match.
- `make wit-check` passes after regeneration.
- `make wasm-fixtures` rebuilds and validates all checked-in components.
- Run `go test -race ./wasmext ./runtime` after fixture regeneration.

## Risks and exclusions

- Adding one Wasm call per tool invocation increases pre-execution latency. The existing configured timeout and serialization rules apply; do not add a second concurrency mechanism.
- Do not add optional-export detection or old-world acceptance.
- Do not rename `arguments-summary` in the permissions-policy world in this work package; it already carries the native permission pattern and is not the source of the hidden convention.
- Do not add assistant context support to runtime as a workaround for the incorrect WIT enum.
