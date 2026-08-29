# Required-Around Error Policy

## Goal and prerequisites

Make `extension.InvokeAround` preserve the complete failure produced by delegated work and the current callback. Then simplify runtime tool execution to use that canonical policy without mutable closure side channels.

## Existing evidence

- `extension/dispatch.go:InvokeAround` stores `delegatedFailure` when `next` fails and returns a private `delegatedError` to the callback.
- If the callback returns any error tree containing `delegatedError`, the dispatcher currently returns only `delegated.cause`. A callback-created sibling error in `errors.Join` is lost.
- `extension.CallbackError` deliberately bounds public text while exposing the raw cause through `Unwrap` for trusted `errors.Is` and `errors.As` checks.
- `runtime/orchestrator.go:executeToolOutcome` currently forces the terminal to return nil error, captures `executorErr` and `callbackErr`, and joins them after permission execution.
- `runtime/extensions_test.go:TestToolExecutionPreservesExecutorAndCallbackErrors` depends on that side channel instead of testing the generic dispatcher contract.

## Exact change surface

- `extension/dispatch.go:InvokeAround`
  - Preserve the direct-propagation case: when `callbackErr` is exactly the private `*delegatedError` returned by this callback's `next`, return its cause without classifying delegated work as a callback failure.
  - When delegated work failed and the callback returns a different error tree, call `propagateCallbackFailure` for that callback error and return `errors.Join(finalDelegatedFailure, boundedCallbackFailure)`.
  - When no delegated failure exists, keep the existing bounded callback failure behavior.
  - Keep next-call count, outlived-callback, protected-input, and output-validation semantics unchanged.
- `extension/extension_test.go`
  - Add a generic required-around test where terminal work fails and the callback joins that delegated error with an independent callback error.
  - Assert `errors.Is` for both causes, `errors.As` for `*CallbackError`, one reporter diagnostic for the callback, delegated error text remains visible under its existing contract, and raw callback-authored text is absent from the returned top-level error string.
  - Preserve a test proving that direct `return next(...)` exposes the original delegated error without `CallbackError` classification.
  - Add two-layer cases where the inner callback adds an error and the outer callback directly propagates, and where both callbacks add independent errors. Assert every cause, one diagnostic per authoring callback, no diagnostic for direct propagation, and bounded callback-authored text.
  - Add single- and two-layer cases using `fmt.Errorf("context: %w", delegatedErr)`. Assert the delegated cause remains discoverable, the wrapper is one bounded `CallbackError`, raw wrapper text is absent, and only the callback that authored the wrapper emits a diagnostic.
- `runtime/orchestrator.go:executeToolOutcome`
  - Delete `executorErr` and `callbackErr`.
  - Make the terminal passed to `InvokeAround` return `tool.Executor.Execute` directly after cloning the call.
  - Make the wrapped executor return the result and error from `InvokeAround` directly.
  - Pass only the permission wrapper's returned error to `newToolOutcome`; it already includes executor and middleware errors from the canonical chain.
- `runtime/extensions_test.go`
  - Change the test middleware to join the `next` error with its independent callback error.
  - Keep the assertions that the final `toolOutcome.RawError` matches both errors and yields `ToolFailed`.

## Required behavior and invariants

- Required delegation remains mandatory and at most once.
- A callback cannot swallow a delegated error by returning nil.
- Returning the exact private delegated marker means pure propagation and does not create a callback diagnostic.
- Adding, wrapping, or joining an error after delegated failure is callback-authored behavior. The dispatcher reports it and returns a bounded `CallbackError` joined with the original delegated failure.
- `errors.Is` must discover the original terminal error and the independent callback error.
- Delegated failure text retains its existing direct public representation. Raw callback-authored text must remain hidden behind bounded `CallbackError` text.
- In nested chains, an outer direct propagation must not reclassify an inner callback failure or emit a duplicate diagnostic.
- Runtime tool permission and disposition logic must receive the same complete error tree without an out-of-band variable.

## Tests and acceptance criteria

Run at minimum:

```bash
go test ./extension -run 'Test(RequiredDelegation|Invoke|Interceptor)'
go test ./runtime -run 'TestToolExecutionPreservesExecutorAndCallbackErrors'
go test -race ./extension ./runtime
```

Acceptance requires the new generic extension tests to fail on the current implementation because independent callback errors are lost or misattributed. They must pass after the dispatcher change with no runtime-specific preservation mechanism.

## Dependencies, risks, and exclusions

- Implement and test the generic dispatcher policy before deleting runtime side channels.
- Do not expose the private `delegatedError` type or add a new public error interface.
- Do not change notification, hook, transform, or gate error policies.
- Do not change permission decisions, tool result transformation, or panic classification.
- Nested around callbacks are an edge case: each dispatcher layer must preserve the downstream error as delegated work and classify only errors added by its own callback.
