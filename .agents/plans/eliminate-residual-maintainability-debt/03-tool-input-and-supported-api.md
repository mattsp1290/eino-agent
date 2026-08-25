# Canonical Tool Input And Supported Runtime API

## Repository evidence

- `normalizedToolArguments` accepts any valid JSON, including null, arrays, and
  strings; a test names these forms compatibility shapes.
- `toolPattern` probes generic JSON for `permission_pattern` and legacy
  `pattern`, coupling runtime permission authority to tool-specific field names.
- Session subagent and skill normalizers inject `permission_pattern` into the
  model's arguments instead of declaring how permission identity is derived.
- `runtime.Handle.FollowUp` is public but its only implementation always returns
  `ErrUnsupportedOperation`.

## Exact change surface

- `runtime/orchestrator.go`, `runtime/tool_preparation.go`
  - Introduce one canonical non-null-object normalizer: decode with explicit JSON
    number preservation and deterministically remarshal. Apply it to raw
    provider arguments, decoder output, successful `ToolPreparePoint` output,
    and persisted input before resume execution. Its returned bytes are the sole
    resolver, persistence, event, and execution input.
  - Return boundary-specific errors: provider rejection at model ingress,
    malformed tool input after decoding, and invalid/protected extension output
    after middleware.
  - Delete `toolPattern` and all magic-key parsing.
  - Initialize and validate the fallback pattern from the tool name before
    `ToolPreparePoint`. After successful preparation, ask the materialized
    tool's explicit resolver and replace the fallback. When preparation fails,
    retain the fallback and do not invoke the resolver, so the original error
    remains authoritative. Reject empty or oversized patterns before creating a
    call record or emitting a tool-call event.
- `runtime/types.go`, clone/projection helpers
  - Add `PermissionPatternResolver` to `Tool`, with a function adapter for host
    implementations.
  - Treat the resolver like other executable capabilities: clone it by identity,
    strip it from data-only extension projections, and reject it in protected
    callback outputs.
  - Remove `FollowUp` from `Handle`; delete its implementation and
    `ErrUnsupportedOperation` if it has no other caller.
- `tools/registry.go`, `composition/registry.go`
  - Add a typed `Definition.PermissionPattern` callback.
  - Materialize a resolver that decodes final canonical JSON again at the tools
    boundary and invokes the typed callback.
  - Wrap mounted permission callbacks with the same `mount.CallbackContext`
    lease and self-close protection as decode/normalize/encode/execute. Include
    the callback in nonempty executable provenance requirements.
- `tools/session/session.go`
  - Stop injecting permission fields during normalization.
  - Declare subagent task and skill name pattern callbacks explicitly.
- `session/types.go`, runtime fresh/resume paths
  - Persist `Pattern` in the JSON-backed `session.ToolCall` record.
  - Copy the prepared pattern into create/claim/settlement runtime calls and use
    the persisted value during pending or running resume; never re-derive it.
  - Validate nonempty bounded patterns on creation and resume. Claim and
    settlement must preserve the exact value, and duplicate creation conflicts
    when pattern identity drifts.
- `transport/http_test.go`, `docs/consumer-guide.md`
  - Remove dead fake `FollowUp` methods and consumer instructions for the
    unsupported operation.

## Invariants and tests

- Model arguments, decoder output, final prepared input, and resumed persisted
  input are JSON objects.
- Permissions, approval requests, execution, events, observation, and resume see
  the same persisted input. Permissions, approval, execution/settlement,
  tool-call observation, and resume see the same persisted pattern. Public event
  payloads continue to expose name/arguments only; pattern is not added because
  it is permission-sensitive metadata, not model output.
- A prepare interceptor may rewrite input, after which the typed resolver sees
  that final input. It cannot inject a separate protected pattern.
- Session tool normalized JSON contains only schema-declared fields.
- Unknown tools fall back to their name without parsing input.
- An AST/public-surface or repository-wide structural check proves `Handle`
  exposes no unsupported method.
- Close waits for an active permission-pattern callback; callback self-close is
  rejected consistently with other mounted tool callbacks.
- Pending resume uses the stored pattern even if a current resolver would
  differ; running interruption and settlement preserve it unchanged.
- A prepare failure persists and settles with the validated tool-name fallback,
  never invokes the resolver, and preserves the original prepare error.
- Equivalent object inputs with different whitespace or key order normalize to
  identical durable bytes; duplicate keys follow the chosen JSON decoder's
  last-value semantics and tests document that behavior.

## Dependencies and risks

- The SQLite store persists the complete tool-call JSON record, so adding
  `Pattern` requires fixture updates but no SQL column or migration.
- Pattern callbacks must be deterministic and side-effect free; document that
  contract in GoDoc and validate the result before call creation.
