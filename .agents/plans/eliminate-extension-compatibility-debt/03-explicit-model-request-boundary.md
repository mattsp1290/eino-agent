# Explicit Model Request Boundary

## Goal and prerequisite state

Replace generic reflection cloning and silent JSON failure with checked cloning and one canonical request projection before provider dispatch.

## Repository evidence

- `model/provider.go` implements `cloneMutable` and `cloneReflectValue` for arbitrary graphs.
- `runtime/extension_model.go` has separate JSON clone helpers that return `nil` on error.
- `runtime/model_stream.go` computes a notification hash with ignored marshal errors when the ledger is disabled.
- `runtime/ledger.go:AuditModelRequest` already owns the bounded, credential-free persisted projection.

## Exact change surface

- `model/provider.go`
  - Change `Request.Clone` to proposed `Clone() (Request, error)`.
  - Replace `cloneMutable`/`cloneReflectValue` with explicit typed deep-copy functions for every supported Eino message and content-part field plus the existing checked schema clone.
  - Preserve `Observer` by direct assignment and clone identity/options containers explicitly.
  - Reject nonzero fields that cannot be faithfully copied, including `MessageOutputPart.StreamingMeta`. Reject all nonempty message, part, and tool `Extra` maps at this boundary; extension/tool provenance remains in runtime-owned metadata rather than provider-visible `Extra`.
- `extension/types.go`, `extension/dispatch.go`, extension point constructors/callers
  - Change `CloneFunc[T]` to `func(T) (T, error)` and propagate clone errors through interceptor invocation and notification dispatch.
  - Interceptors fail before calling the handler or `next`. Notifications report the clone failure through their contained-error path and skip the affected callback.
- `providers/fake/provider.go` and all model request consumers
  - Propagate clone errors before starting observer callbacks or goroutines.
- `runtime/ledger.go`
  - Replace reflective `findNonEmptyExtra` with validation of the serialized Eino message shape or explicit message-part traversal.
  - Keep all `Extra` fields excluded from the audited projection.
  - Expose a package-local canonicalization result containing the audited input, content hash, and checked defensive request copy.
- `runtime/model_stream.go`
  - Canonicalize once before `dispatch_started`, model-request notices, or `ModelStreamPoint` invocation.
  - Reuse the same content hash for the optional ledger and `ModelRequestedNotice`.
  - Propagate canonicalization failure before provider dispatch.
- `runtime/extension_model.go`
  - Replace `ModelStreamInput`'s callable-bearing `model.Resolved` and `model.Request` graph with a proposed explicit data-only request view: provider/model identity, canonical audited input, and content hash.
  - Keep required delegation and returned stream identity validation.
  - Delete `cloneMessageDeep`, `cloneProtectedMessages`, and `modelRequestContentHash` silent fallbacks.
- `runtime/extension_context.go`, `runtime/extension_tool.go`
  - Reuse checked typed clone helpers and propagate failures before callbacks; do not translate clone failure into a mutated `nil` field.
- Tests in `model`, `providers/fake`, and `runtime`.

## Intended behavior and invariants

- Provider request data is either defensively copied in full or rejected with an error.
- The canonical metadata domain contains no `Extra` values and no nonzero `StreamingMeta`; request validation, cloning, hashing, extension views, ledger input, and provider dispatch use that same rule.
- No clone or hash path ignores serialization failure.
- Unsupported `Extra` values fail before provider dispatch and before ledger state changes to `dispatch_started`.
- Extension callbacks see data-only canonical request evidence and no client, streamer, observer, executor, or approval callable.
- Ledger and lifecycle notices report the same content hash for one provider attempt.
- Provider adapters still receive the authoritative checked request and observer.

## Tests and acceptance criteria

- Replace arbitrary-graph clone tests with explicit supported-part copy tests and unsupported-value errors. Cover nonzero `StreamingMeta`, nested multimodal `Extra`, numeric metadata types, custom marshalers, and tool-schema clone failures.
- Test requests that differ only by rejected metadata both fail rather than hash to the same provider-visible operation.
- Test fake provider clone failure returns before observer start and stream goroutine creation.
- Test ledger-enabled and ledger-disabled extension notices report identical hashes for the same request.
- Test a channel/function in message metadata fails before `ModelRequestedPoint` and model dispatch.
- Test extension model-stream inputs contain only the proposed data view and required delegation remains enforced.
- `rg -n 'cloneMutable|cloneReflectValue|modelRequestContentHash|raw, _ := json.Marshal|json.Marshal\([^)]*\)\s*$' model runtime --glob '*.go'` is manually inspected so no relevant ignored serialization error remains.

## Dependencies, risks, and exclusions

- Can begin after the sealed plan API is stable; it does not depend on atomic settlement internals.
- Do not persist credentials, endpoints, clients, observers, trace attributes, or disallowed `Extra` metadata.
- Do not change provider request ordering or idempotency-key behavior.
- A checked clone signature is intentionally breaking because the application has no users.
