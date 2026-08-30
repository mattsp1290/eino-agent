# Wasm and Dead Surfaces

## Goal and prerequisite

Publish only Wasm lifecycle/configuration behavior that exists. Start after semantic extension point APIs from work package 1 are stable.

## Turn-hook decision

Delete `before-turn` and `after-turn` from the pre-release hook WIT.

Repository evidence shows that `runtime.TurnPreparePoint` runs once before all retry and tool-turn loops, while `loadedHook.finish` calls `after-turn` only beside `after-run`. Implementing true turn hooks would require defining behavior for retries, partial model failures, tool loops, cancellations, and resume. No consumer requires that new lifecycle.

The retained Wasm hook world exposes:

- `before-run` at the existing durable admission notice;
- `after-run` at the existing settled-run notice.

Both remain contained extension callbacks with bounded metadata. Extend `runtime.RunSettledNotice` to carry the bounded admission metadata needed by the Wasm `after-run` call so the adapter remains stateless. Do not retain a run-ID metadata cache: runs that fail after admission but before durable settlement do not emit `RunSettledPoint`, so cache cleanup cannot be guaranteed at this layer.

## Exact Wasm change surface

- `wit/eino-agent-extensions.wit`: remove the two exports from `hook-api`.
- `wasmext/engine.go`: remove `BeforeTurn` and `AfterTurn` from `hookComponent` and the hook contract function list.
- `wasmext/wasmtime_worlds.go`: remove the corresponding methods.
- `runtime/extension_lifecycle.go` and settlement callers: carry bounded run metadata directly in `RunSettledNotice`.
- `wasmext/wrappers.go`: remove before-turn helpers, the after-turn call, and the hook metadata cache; simplify `finish` to call only `AfterRun` from the settlement notice.
- `wasmext/points.go`: register only run-admitted and run-settled handlers for the hook adapter.
- `examples/wasm-extensions/hook/main.go`: remove exports.
- `wasmext/gen/eino-agent/**`: regenerate from WIT using `make wit`.
- `examples/wasm-extensions/fixtures/hook.wasm`: regenerate using `make wasm-fixtures`.
- Update phase-B and integration tests to assert only the retained operations.

## Inert configuration removal

- Delete `GuestConfig` from `wasmext.ModuleConfig`.
- Delete its byte-count validation from `wasmext.loadModule` and all tests that only prove that inert behavior.
- Remove documentation claiming guest configuration is retained.
- Reintroduce configuration only with an explicit WIT import and end-to-end guest test in a future bead.

## Ignored parameter removal

- Change private `tools/session.sessionScope` to take no `kind` argument.
- Update all definition call sites.
- Do not retain the parameter in anticipation of a future concurrency key.

## Tests and acceptance criteria

- Generated bindings and fake components contain no turn-hook method.
- A mounted Wasm hook receives one before-run and one after-run callback for a successful run.
- Run success, settled failure, rejection, and cancellation prove exactly-once retained hooks and contained after-run errors. A post-admission/pre-settlement failure test proves the adapter retains no run-scoped state.
- `rg -n "before-turn|after-turn|BeforeTurn|AfterTurn|GuestConfig" wit wasmext examples docs` has no unintended match.
- `rg -n "sessionScope\(" tools/session/session.go` shows only the zero-argument helper and callers.
- `make wasm-fixtures` validates every checked-in component.

## Risks and exclusions

- Do not preserve the old WIT package as another version; there are no consumers.
- Do not rename turn hooks to another ambiguous lifecycle term.
- Do not add configuration plumbing without a guest-visible contract.
