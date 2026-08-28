# Semantic Extension Points

## Goal and prerequisite

Replace the configurable `extension.Interceptor` policy matrix with point types whose public callback shape states the execution behavior. The application context in [00-overview.md](00-overview.md) is the only compatibility prerequisite.

## Existing evidence

- `extension/types.go` defines `Interceptor[I,O]` with six policy fields and five constructors.
- `extension/dispatch.go` implements cloning, optional or required delegation, delegated-error unwrapping, output validation, result validation, and delegated-output identity in one recursive function.
- `runtime/extension_lifecycle.go` uses that abstraction for seven materially different points.
- `wasmext/points.go` and `examples/native-extension/plugin.go` use `next` for callbacks that are only ordered transforms or hooks.

## Proposed semantic contracts

Names are proposed. They must be implemented in existing `extension/types.go`, `extension/dispatch.go`, and `extension/registry.go` or in focused new files under the existing `extension/` package.

| Semantic point | Callback contract | Failure/order contract | Runtime mapping |
| --- | --- | --- | --- |
| Existing notification | `func(context.Context, T) error` | deterministic order; report and continue | admitted/started/settled/model/tool/event notices |
| Proposed fail-fast hook | `func(context.Context, T) error` | deterministic order; stop on first callback error | `TurnPreparePoint` |
| Proposed ordered transform | `func(context.Context, T) (T, error)` | feed each validated output to the next callback | context assembly, tool preparation, tool-result transformation |
| Proposed gate | `func(context.Context, I) (D, error)` | evaluate in order until the point-owned continuation predicate is false | run-before-execute |
| Proposed required around | `func(context.Context, I, Next[I,O]) (O,error)` | every callback delegates exactly once before return | model stream and tool execution |

The public API must not expose `requireNext`, nullable validator combinations, or constructor names that encode combinations such as `NewRequiredInterceptorWithResultValidation`.

## Invariants and error behavior

- Every callback receives a point-owned clone.
- Transform validation compares each candidate against the original protected identity and validates the complete candidate before the next callback runs.
- `ToolResultTransformPoint` becomes a homogeneous transform over `runtime.ToolResultTransform`; its terminal result is `transformed.Result`.
- Hook callbacks cannot mutate the authoritative input. Clone each callback input and validate it after callback return when the payload has mutable containers.
- Gate callbacks receive immutable input. `RunContinue` proceeds and `RunReject` stops evaluation. Invalid decisions fail the run.
- Required-around callbacks retain the existing synchronous `Next` contract: at most once, before callback return, and no concurrent use.
- Model-stream around callbacks must return the exact delegated stream handle.
- Tool-execute around callbacks may transform a valid `ToolResult` after delegating once.
- Callback panics become bounded callback failures. Notification failures remain contained. Hook, transform, gate, and around failures propagate as `extension.CallbackError`.
- `Plan.Diagnostics` records the semantic handler kind. Update the session handler-kind validation in work package 2 instead of retaining the old notification/interceptor dichotomy.

## Exact change surface

- `extension/types.go`: replace `Interceptor` constructor matrix with the proposed semantic point types and registration helpers.
- `extension/dispatch.go`: add direct dispatchers for hook, transform, and gate; reduce recursive around dispatch to required delegation only.
- `extension/registry.go`: extend private `entryKind`, registration validation, ordering, and diagnostics for the semantic kinds.
- `runtime/extension_lifecycle.go`: construct each core point with its semantic type.
- `runtime/orchestrator.go`, `runtime/model_stream.go`, and `runtime/tool_preparation.go`: call the corresponding semantic dispatcher.
- `runtime/extension_context.go`, `runtime/extension_tool.go`, and `runtime/extension_model.go`: delete validators made redundant by homogeneous transforms or semantic dispatch; retain protected-boundary checks.
- `wasmext/points.go` and `examples/native-extension/plugin.go`: replace transform/hook `next` callbacks with direct callback results.
- `extension/*_test.go`, `runtime/extensions_test.go`, and focused runtime point tests: migrate and expand semantic behavior coverage.

## Tests and acceptance criteria

- A transform chain proves ordered input flow without `Next`.
- A hook chain proves fail-fast behavior and no later callback invocation.
- A gate chain proves continue, reject, invalid decision, callback failure, and deterministic order.
- Required-around tests prove zero, double, retained, concurrent, and delegated-error behavior.
- Model-stream identity and protected tool-execution tests remain green.
- `rg -n "NewRequiredInterceptor|NewInterceptorWithResultValidation|NewRequiredDelegatingInterceptor" extension runtime wasmext examples` returns no production match.
- Production transform and hook registrations contain no `extension.Next` parameter.

## Risks and exclusions

- Do not turn semantic types into one public configuration struct with optional function fields; that recreates the rejected matrix.
- Do not change notification containment or mount lease ordering.
- Do not add new product extension points beyond the semantics required by existing runtime points.
