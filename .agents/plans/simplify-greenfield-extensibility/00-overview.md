# Simplify Greenfield Extensibility And Request Lifecycle

Tracking issue: `eino-agent-auz`

## Operating context

- The project has no users.
- Backward compatibility, deprecation aliases, dual readers, and feature-flagged
  migration paths are explicitly out of scope.
- Preserve durable fencing, deterministic restart fingerprints, extension
  ordering, model-visible tool behavior, and Wasm ABI behavior.
- Prefer deleting representational choices over adding adapters around them.

## Planned design

### 1. Make interceptor delegation synchronous

`extension.Around` callbacks may call `next` synchronously at most once and only
before the callback returns. Replace the atomic four-state delegation protocol
and join channel in `extension.Invoke` with invocation-local state for:

- whether the callback is still open;
- whether `next` was called;
- whether delegation succeeded;
- the delegated output or delegated failure needed by existing validators.

Keep sequential duplicate-call rejection, late-call rejection, required
delegation, delegated error unwrapping, protected input/output validation, and
returned-stream identity checks. Delete tests whose only purpose is supporting
`next` from a goroutine; retain a sequential escaped-`next` test and document
that concurrent delegation is unsupported.

Primary files: `extension/dispatch.go`, `extension/extension_test.go`,
`extension/types.go`, and `docs/architecture/extension-points.md`. State in API
Godoc that concurrent `next` use is unsupported and is not required to be
race-safe.

### 2. Make registrars own component identity

Remove `InstanceID` from caller-provided `extension.Registration` and all four
`composition` registration inputs. The staging registrar already owns the
mounted `extension.Component`; keep that component as the one identity source
through validation and publication. Do not copy the instance ID into another
private registration field.

Expose the read-only mounted identity through `extension.Registrar.InstanceID`
for adapters that need a stable derived key, notably Wasm context contribution
sources. Composition should derive mounted capability identities from its
private component field instead of copying a caller-provided value through
`ToolRegistration`, `PromptRegistration`, `GuardRegistration`, and
`RestrictionRegistration`.

Update native examples, AG-UI/eino-tools/session adapters, tests, diagnostics,
and plan construction directly. Add focused tests for the registrar accessor
and component-derived plan/diagnostic identity. Do not retain deprecated
fields, accepting constructors, or duplicate private identity fields.

Primary files: `extension/types.go`, `extension/registry.go`,
`composition/registry.go`, adapter packages under `tools/`, `wasmext/points.go`,
and affected examples/tests.

### 3. Make the model-request ledger canonical

Remove `WithModelRequestLedger`, the orchestrator boolean, and the disabled
mode. `session.Store` and `session.ExecutionStore` already require the reader
and writer contracts, and SQLite already owns the table, so every provider
attempt should follow one lifecycle:

1. audit and hash the canonical credential-free provider payload;
2. create the prepared record through the fenced execution store;
3. set the request idempotency key and transition to `dispatch_started`;
4. dispatch the provider request;
5. transition to `completed` or `failed` before lifecycle notification.

Make the prepared record non-optional in `streamModel`; stop returning a
nullable writer from `prepareModelRequest`; update through `execution.store`;
and remove record-presence branches from model lifecycle notifications. Keep
`model.IdempotentStreamer` as an adapter capability because provider transports
may or may not support the key.

Preserve durable failure semantics: if `dispatch_started` fails, deferred
cleanup must still move the prepared record to `failed`, while provider dispatch
and lifecycle notices remain skipped. Emit `ModelCompletedPoint` only after the
terminal durable transition succeeds. Extend the shared `admissionStore` fake
with transactional, fenced model-request persistence so all runtime tests obey
the canonical path.

Replace the disabled-ledger characterization with a default-on persistence
test. Remove explicit enablement from tests/examples and update architecture,
storage, and consumer documentation to describe retention as mandatory.

Primary files: `runtime/orchestrator.go`, `runtime/options.go`,
`runtime/ledger.go`, `runtime/model_stream.go`, `runtime/ledger_test.go`, and
ledger documentation.

### 4. Require deterministic settlement timestamps

Change `session.ToolSettlement.Apply` to reject a zero `CompletedAt` for both
new and idempotent terminal applications. Remove its hidden `time.Now` fallback
while keeping exact repeated-settlement matching and claim fencing. Update the
session tests to separately assert rejection against a running call and an
otherwise identical terminal call; runtime's canonical settlement builder
already supplies and validates the timestamp.

Primary files: `session/tool_settlement.go` and
`session/tool_settlement_test.go`.

### 5. Keep Wasm ownership and projections canonical

Delete the five production `open*Default` helpers. Successful real-engine tests
must load through `wasmext.Loader` and close through `Loader.Close`; white-box
factory tests may continue using the private factory-taking helpers.

Delete the full `session.Run`/`runtime.TurnSnapshot` hook adapters and
`LoadedContextSource.loadContext` that exist only for tests. Test the bounded
registration path used in production. Simplify settled-hook dispatch to
atomically pop cached bounded metadata once by run ID before guest calls, pass
the same metadata to both after hooks, and retain the minimal run-ID fallback
for resumed plans that did not observe fresh admission. Remove
`turnMetadata`, `partialTurnMetadata`, and their now-unused helpers.

Primary files: `wasmext/wrappers.go`, `wasmext/projections.go`,
`wasmext/points.go`, `wasmext/wasmext_test.go`, and
`wasmext/phase_b_test.go`.

## Implementation order

1. Remove duplicated registration identity, repair extension/composition/Wasm
   adapters, and run focused tests.
2. Simplify synchronous dispatch independently and run its focused tests.
3. Collapse the request ledger into the single runtime path, repair the shared
   fake store, and update its tests/documentation.
4. Tighten settlement timestamp validation.
5. Remove Wasm test-only production paths and rewrite integration tests through
   `Loader`/bounded adapters.
6. Format, run focused tests after each boundary change, then run `make check`.

## Verification

- `go test ./extension ./composition ./runtime ./session ./wasmext ./tools/...`
- `go test -race ./extension ./composition ./runtime ./session ./wasmext`
- `make check`
- `git diff --check`
- Searches show no current hits for `WithModelRequestLedger`,
  `modelRequestLedger`, caller-supplied registration `InstanceID`,
  `delegationOpen`/`delegationRunning`, or `open*Default`.
- Production Go files remain below 1,000 lines.

## Delivery

Apply agreed plan-review feedback, close `eino-agent-auz`, commit only the
related changes, pull with rebase, push Beads data, push Git, and verify the
branch is clean and synchronized with origin.
