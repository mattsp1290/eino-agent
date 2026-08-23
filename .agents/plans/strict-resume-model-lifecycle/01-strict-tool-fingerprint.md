# Work Package 1: Strict Tool Fingerprint

## Goal and prerequisites

Make strict resume reject a tool remount when any deterministic, serialized field that affects tool exposure or runtime policy changes. This package has no code prerequisite.

## Evidence

- `composition/registry.go` function `buildDescriptor` assigns `toolSchemaHash(entry.Definition)` to `session.ExtensionPlanEntry.SchemaHash`.
- `composition/registry.go` function `toolSchemaHash` omits `tools.Definition.Concurrency`, `Retention`, and `Metadata`.
- `tools/registry.go` function `materialize` copies these values into `runtime.Tool`; `Retention.Redact` changes persisted output privacy behavior.
- `composition/registry_test.go` function `TestStrictResumeRejectsChangedConvertedToolSchema` is the nearest remount/mismatch pattern.

## Exact change surface

- Modify existing `composition/registry.go` function `toolSchemaHash`.
- Add tests to existing `composition/registry_test.go`, near the converted-schema strict-resume test.
- Do not modify `session.ExtensionPlanEntry`, `session.ExtensionPlanSchemaVersion`, or public tool types.

## Intended behavior and invariants

- Hash a JSON object containing `Name`, `Description`, converted `Parameters`, `Permissions`, `RetrySafe`, `Concurrency`, `Retention`, and `Metadata`.
- Preserve slice order for `Permissions` because the current hash treats permission order as declared identity.
- Preserve deterministic metadata hashing regardless of map insertion order.
- A change in any included field must change the tool `SchemaHash` and the containing plan fingerprint.
- An unchanged definition, including an equivalent metadata map built in a different insertion order, must keep the same hash.
- Continue returning parameter-conversion and JSON-encoding errors from `toolSchemaHash`.

## Tests and acceptance criteria

Add a table-driven strict-resume regression that persists a plan, remounts the same component/tool identity, mutates one field, and expects `runtime.ErrExtensionPlanMismatch` for at least:

- `Retention.Redact` from `true` to `false`;
- `Retention.MaxInlineBytes` change;
- `Retention.StoreExternal` change;
- `Concurrency` from `parallel` to `sequential`;
- `Metadata` value change.

Also verify equivalent metadata maps with different insertion order produce the same `SchemaHash` or allow strict resume. This prevents accidental nondeterminism.

Run:

```bash
go test ./composition -run 'TestStrictResume.*Tool'
```

Acceptance requires each of the three retention-field mutations, the concurrency mutation, and the metadata mutation to change the descriptor fingerprint and make strict resume fail before plan execution.

## Dependencies, risks, and exclusions

- This package can be implemented independently of Work Package 2.
- Do not include function-valued fields in the JSON input.
- Do not normalize empty concurrency to `parallel` inside the fingerprint unless repository tests or validation establish that declaration-level equivalence is required.
- Do not rely on `Artifact.ConfigHash` to cover definition policy because callers are not required to derive it from those fields.
