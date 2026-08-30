# Model Request Canonical Boundary

## Goal and prerequisites

Produce the provider request and its credential-free ledger representation from one canonical clone. Remove the duplicate validation visitors without weakening fail-closed request safety.

## Existing evidence

- `runtime/model_stream.go:streamModel` builds a request, calls `request.Clone`, then calls `AuditModelRequest`.
- `model/provider.go:Request.Clone` deep-copies request messages, tools, maps, identity, provider, and trace state.
- `model/provider.go:cloneMessages` rejects deprecated `MultiContent`, streaming metadata, and recursive JSON fields named `extra`.
- `model/provider.go:cloneToolInfos` rejects tool `Extra` and clones JSON schemas.
- `runtime/ledger.go:validateAuditSafeMessage` repeats the message checks and performs another recursive `extra` traversal.
- The ledger projection intentionally excludes credentials and includes only canonical messages, system text, tool name/description/schema, and allowlisted call options.

## Exact change surface

- `runtime/ledger.go`
  - Replace exported `AuditModelRequest` with proposed private `auditModelRequest`.
  - Change its signature to return the canonical cloned `model.Request` before `AuditedModelInput`, content hash, and error.
  - Call `request.Clone` as the first operation.
  - Build every audit field from that clone, including messages, tools, system text, and safe options.
  - Delete `validateAuditSafeMessage`, `rejectCanonicalExtra`, and the now-unused `einoschema` import.
  - Keep the audit-size limit and SHA-256 calculation over the serialized `AuditedModelInput` unchanged.
- `runtime/model_stream.go:streamModel`
  - Replace the separate `request.Clone` and `AuditModelRequest` calls with one `auditModelRequest` call.
  - Pass the current turn message slice into `ProviderRequest` and remove the overwritten `request.Messages = cloneMessages(messages)` assignment.
  - Continue assigning the ledger-derived idempotency key only after `prepareModelRequest` succeeds.
- `runtime/provider.go`
  - Change `TurnSnapshot.ProviderRequest` to accept the current turn messages explicitly.
  - Assemble the pre-canonical request without defensively cloning messages, option maps, or trace attributes that `auditModelRequest` immediately clones.
  - Keep the tool-info projection needed to translate `[]Tool` into `[]*einoschema.ToolInfo`; do not deep-clone tool schemas there.
- `runtime/provider_test.go`
  - Test raw request assembly separately from canonical ownership.
  - Route mutation-isolation assertions through `auditModelRequest`, the actual canonical boundary.
- `runtime/ledger_test.go`
  - Rename direct calls and adapt to the four return values.
  - Assert the returned request is independent of the caller-owned request.
  - Assert audited canonical messages and tool schemas derive from the returned clone.
  - Keep rejection coverage for deprecated `MultiContent`, streaming metadata, nested `extra`, tool `Extra`, invalid schema, nil messages/tools, and oversized audit input.
- Search all Go and Markdown references to `AuditModelRequest`; remove stale public API references rather than retaining an alias.

## Required behavior and invariants

- The provider receives the exact canonical request whose safe projection was hashed and persisted.
- `auditModelRequest` performs the only defensive clone of the assembled provider request graph.
- Caller mutation after `auditModelRequest` returns cannot alter the provider request or audited projection.
- Credentials, provider secrets, environment, auth maps, and non-allowlisted options remain absent from `AuditedModelInput`.
- Deprecated `MultiContent`, streaming metadata, recursive message `extra`, and tool `Extra` remain fail-closed at `model.Request.Clone`.
- The audit hash remains deterministic for equivalent canonical input and changes when a persisted safe field changes.
- `session.ErrModelRequestTooLarge` remains the size-limit result.
- No exported compatibility wrapper remains for `AuditModelRequest`.

## Tests and acceptance criteria

Run at minimum:

```bash
go test ./model ./runtime -run 'Test(RequestClone|AuditModelRequest|ModelRequest|StreamingOrchestrator.*Model)'
go test -race ./model ./runtime
rg -n 'AuditModelRequest|validateAuditSafeMessage|rejectCanonicalExtra|request.Messages = cloneMessages' --glob '*.go' --glob '*.md'
```

The final search may find test names that describe audit behavior. It must not find the deleted exported function or duplicate validators.

## Dependencies, risks, and exclusions

- This package is independent of run settlement and extension error composition, except for overlapping `runtime` test execution.
- Keep `AuditedModelInput`, `AuditedMessage`, and `AuditedToolSchema` public because public runtime extension inputs expose those value types.
- Do not move credentials into the ledger to make the canonical request serializable.
- Do not relax deprecated-field rejection under the greenfield compatibility decision; the rejection is a safety boundary.
- Do not change the ledger schema or persisted hash algorithm.
- Do not delete `runtime/config_snapshot.go:cloneMessages`; fresh-run snapshot and context-assembly paths still use it.
