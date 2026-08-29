# Extension Dispatch Contract

## Goal and prerequisites

Make around delegation lifecycle-safe and make point identity semantic rather than pointer-local. The greenfield application context in [00-overview.md](00-overview.md) is the prerequisite; no old constructors, enum aliases, or matching behavior remain.

## Existing evidence

- `extension/types.go` documents that `Next` must finish before its callback returns, but `extension/dispatch.go:InvokeAround` does not enforce that boundary before returning.
- `pointKey` is a pointer token. Two constructors with the same `Contract` create unrelated points.
- `entryKind` is private, `Plan.Diagnostics` converts it to a string, and `session/extensions.go` redefines the same five values as `HandlerKind`.
- `stagingRegistrar.register` detects duplicates by point pointer, scope, and registration ID, so semantically identical independently constructed points evade early rejection.

## Work package A: admitted-next lifecycle

Change `extension/dispatch.go:InvokeAround` and add focused coverage in `extension/dispatch_around_test.go` (proposed under existing `extension/`).

Required behavior:

1. Admit at most one `next` call while the callback is open.
2. Close admission as soon as the callback returns or panics.
3. If an admitted call remains active, record an early-return violation and wait for it to finish.
4. Read delegated output and errors only after the admitted call finishes.
5. Return proposed `extension.ErrNextOutlivedCallback` for the early-return violation after draining. Preserve the delegated error as an internally observable joined/unwrapped cause only if doing so does not expose callback text through public error formatting.
6. Return `ErrNextNotCalled` only when no call was admitted and `ErrNextCalledTwice` for repeated calls.
7. A retained `next` invoked after callback closure fails without entering terminal work.

Use a completion channel or condition protected by the existing mutex. Do not poll, spin, cancel terminal work implicitly, or return before completion. Every admitted path, including clone failure and panic recovery, must signal completion exactly once.

Tests must prove:

- the outer call remains blocked after the callback returns until terminal release;
- terminal side effects are complete before the outer call returns;
- the race detector reports no access races;
- a late retained call never invokes terminal;
- clone failure, delegated failure, callback failure, panic, and double-call precedence remain deterministic.

Use this precedence after the admitted call drains:

| Observed state | Returned error |
| --- | --- |
| more than one call attempted | `ErrNextCalledTwice` |
| callback returned while the admitted call was active | `ErrNextOutlivedCallback` |
| callback returned/panicked with a non-delegated error | bounded `CallbackError` |
| callback returned the delegated wrapper | the delegated cause |
| no call was admitted and callback otherwise succeeded | `ErrNextNotCalled` |
| delegated and callback succeeded, returned output invalid | output validation error |

For the outlived case, the lifecycle error is primary. It may unwrap/join the delegated cause for `errors.Is`/`errors.As`, but `Error()` must contain only the host-owned lifecycle text and must not include delegated or callback error text. Add assertions for precedence, redacted formatting, and cause discovery.

## Work package B: canonical typed point identity

Change `extension/types.go`, `extension/registry.go`, `extension/dispatch.go`, `session/extensions.go`, and affected tests/call sites.

Introduce exported `extension.HandlerKind` with the five current semantic values. Use it in:

- private registration and dispatch entries;
- proposed `extension.HandlerIdentity` returned with a mounted component or dispatch plan;
- `session.RegistrationIdentity.Kind` and durable validation.

Replace pointer-only `pointKey` with a comparable private value containing:

- validated `Contract`;
- `HandlerKind`;
- private callback signature identity, captured with `reflect.Type` by each generic constructor.

Define a separate private durable point key containing only `Contract` and `HandlerKind`. Add a registry-lifetime private `pointSignatures map[durablePointKey]reflect.Type`, initialized with the registry. During `Registry.CommitMount`, while holding the publication lock:

- compare candidate entries with one another and the registry-lifetime signature catalog;
- allow the same durable point key only when callback signatures match;
- reject a mismatch with `ErrInvalidContract` before publication;
- add candidate signatures to the catalog only after every validator succeeds and immediately before publication;
- never remove a signature on deactivation or close, so a later incompatible remount cannot reuse the same durable identity.

Update duplicate registration checks to compare durable point key, registration ID, and scope, not constructor pointer identity. Update dispatch matching to compare the full comparable point value so independently constructed equivalent points match safely.

Replace `PlanEntryDiagnostic` with proposed `HandlerIdentity { ID, Contract, Order, Scope, Kind }` and proposed `ComponentHandlers { Component, Handlers }`. During `Registry.snapshot`, build scope-filtered handler identities alongside each selected mounted value and store the same grouped, immutable records in the resulting `Plan`. Add `MountedValue.Handlers() []HandlerIdentity` and `Plan.HandlerComponents() []ComponentHandlers`; both return deep defensive copies and only expose entries that passed `scopeApplies`. Delete `Plan.Diagnostics` and `PlanEntryDiagnostic`. No kind-to-string-to-kind conversion may remain.

Tests must prove:

- independently constructed notification, hook, transform, gate, and around points with identical contract and generic signature interoperate;
- same contract and kind with incompatible generic signatures fails atomically during mount;
- close followed by an incompatible same-contract/same-kind remount still fails;
- duplicate registration identity is rejected even when two equivalent point values came from separate constructors;
- different kinds may use the same contract only when durable validation and fingerprints retain the kind distinction;
- failed publication rolls back without making any handler dispatchable.
- selected and non-selected session scopes expose only the handler identities actually frozen into that snapshot.

## Risks and exclusions

- Go aliases and named types have different reflection identities. Treat that as a signature mismatch; do not add coercion.
- Do not export reflection details or serialize them.
- Do not change handler ordering, scope selection, callback cloning, protected-mutation validation, or reporter redaction.

## Verification

```text
go test -race ./extension ./session
```

Acceptance: no callback can outlive `InvokeAround`, and semantic point identity crosses independently constructed values without admitting unsafe signatures.
