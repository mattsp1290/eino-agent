# Eliminate Residual Maintainability Debt

Status: Implemented. Reviewed by two independent reviewers and one fresh
adversarial reviewer; accepted corrections applied.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "5a103722a881d753dad5dc19a7477507a63908b97e76558af273f811356ef23a",
    "confirmed_at": "2026-08-25T00:33:30Z"
  }
}
```

The user explicitly confirmed that this code has no users. Delete undeployed
public and persisted shapes instead of adding adapters, migrations, feature
flags, aliases, or deprecation paths.

## Change classification

- Change type: breaking architectural simplification and validation hardening.
- Affected areas: `model`, `providers/fake`, `runtime`, `tools`, `tools/session`,
  `session`, `composition`, `wasmext`, examples, tests, and architecture docs.
- Tracking issue: `eino-agent-05m`.
- Finding 1 is excluded from implementation here and captured as the upstream
  `eino-tools` request in
  `../eino-tools/.agents/requests/eino-agent-composition-tool-registration/`.

## Requested outcome

Implement residual thermo-nuclear review findings 2–6:

1. Replace the model resolver's client/streamer fork with one built streamer.
2. Replace the loose durable extension entry with an explicit kind-specific
   identity union and remove always-true `Required` semantics.
3. Remove the public `Handle.FollowUp` operation that can only fail.
4. Require model tool arguments to be JSON objects and move permission-pattern
   derivation from magic JSON fields into the tool-definition boundary.
5. Make `wasmext.Loader` the sole module owner and remove independent public
   `Open*`/`Close` lifetime paths.

## Success criteria

- A resolved model exposes exactly one nonnil streaming transport; runtime does
  not import Eino model options or choose between transport paths.
- Each durable plan entry contains exactly one kind payload. Tool name and
  registration ID are distinct fields, artifact config/source identity and
  kind-specific hashes are validated, and resume has no optional-entry branch.
- `Handle` contains only supported operations.
- Tool-call arguments are empty-or-object JSON. Permission patterns are derived
  by an explicit resolver after tool preparation and persisted with the call;
  runtime never parses `permission_pattern` or `pattern` keys.
- Wasm modules are opened and closed only through a loader-owned lifecycle.
- Focused tests, `make check`, and `git diff --check` pass; the bead is closed;
  changes are committed and pushed.

## Key decisions

1. **One model transport.** `model.Adapter.Build` returns `model.Streamer`.
   `model.Resolved` contains only that streamer. A model-package Eino adapter
   binds tools once, prepends the system message once, and forwards normalized
   observer callbacks for provider adapters that wrap an Eino chat model.
2. **A serialized tagged union.** Keep one descriptor entry container for JSON,
   but replace generic `Kind`, `CapabilityID`, hashes, and `Required` fields with
   exactly one of handler/tool/prompt/guard/restriction payloads. Only instance
   and artifact are common: capability payloads own their scope, while every
   handler registration owns its scope and notification/interceptor kind.
   Fingerprinting is the mandatory validation choke point and rejects zero or
   multiple payloads, duplicate identities, or malformed fields.
3. **Permission identity is executable metadata.** Add an explicit resolver to
   tool definitions and materialized runtime tools. It re-decodes final prepared
   canonical input at the tools boundary, returns a pattern, and runtime stores
   that pattern in `session.ToolCall` for execution and resume.
4. **Loader owns Wasm.** Private open helpers fully build return values before a
   transactional ownership transfer to `Loader`. The loader tracks an internal
   resource containing module shutdown and adapter-specific cleanup; only
   `Loader.Close` finalizes successfully loaded resources.

## Scope and constraints

- Preserve model-visible tool names, schemas, output envelopes, permission
  actions, extension ordering, WIT ABI, and durable claim/settlement behavior.
- Do not implement the upstream standard-tool registration request in this
  repository during this work.
- Do not add compatibility parsing for old descriptors or argument shapes.
- The in-place pre-release descriptor change has no dual reader. Rollback means
  reverting source and recreating local databases containing incompatible
  extension descriptors.
- Keep modified production Go files below 1,000 lines.
- No blocking operating-context decisions remain.

## Document map

- [01-single-model-transport.md](01-single-model-transport.md)
- [02-typed-plan-identities.md](02-typed-plan-identities.md)
- [03-tool-input-and-supported-api.md](03-tool-input-and-supported-api.md)
- [04-loader-owned-wasm.md](04-loader-owned-wasm.md)
- [05-execution-handoff.md](05-execution-handoff.md)
