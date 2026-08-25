# Typed Durable Plan Identities

## Repository evidence

- `session.ExtensionPlanEntry` combines `Kind`, always-true `Required`, generic
  `CapabilityID`, handler registrations, order, and two optional hashes.
- `runtime.validatePlanIdentity` checks only common nonempty fields and accepts
  missing config hashes, arbitrary source kinds, or wrong hash combinations.
- `composition.Registry.AcquireResumePlan` branches on `Required` even though all
  constructors set it true.
- Tool name and registration ID are encoded as `name + "/" + id`, then recovered
  with `strings.SplitN`; prompts use the same delimiter convention.

## Exact change surface

- `session/extensions.go`
  - Introduce payloads for handlers, tool, prompt, guard, and restriction.
  - Give tools explicit `Name`, `RegistrationID`, `SchemaHash`, and
    `ExecutorHash`; prompts explicit `Name`, `RegistrationID`, and `Order`;
    guards explicit `RegistrationID` and `Order`; restrictions explicit
    `RegistrationID` and `RulesHash`; handlers retain registrations.
  - Make `ExtensionPlanEntry` common instance/artifact identity plus exactly one
    optional payload. Tool/prompt/guard/restriction payloads own their scope.
    Handler registrations each own scope and an explicit handler kind
    (`notification` or `interceptor`) so mixed-scope components remain
    representable and dispatch-kind drift changes the fingerprint. Remove
    generic `Kind`, `Required`, `CapabilityID`, order, hashes, and entry scope.
  - Add checked `Kind()` and descriptor/entry validation. Make
    `FingerprintExtensionPlan` call validation before hashing so admission,
    resume, and plan construction share one mandatory choke point.
  - Validate schema version, exactly-one payload, allowed source/handler kinds,
    fields and scopes, duplicate semantic identities, duplicate registrations,
    and consistent session keys. Include every explicit field in comparison.
- `runtime/extension_plan.go`
  - Use kind-specific plan identity records instead of accepting an arbitrary
    generic entry for each behavior.
  - Validate nonempty artifact name/version/hash/config hash, source kind exactly
    `native` or `wasm`, valid scope, registration IDs, names, hashes, and order.
  - Build descriptor entries internally from kind-specific constructors.
  - Cross-check every identity against its attached behavior: prompt
    name/order/instance, guard ID/order/instance, materialized tool name, and a
    canonically recomputed restriction rules hash. Reject duplicate semantic
    identities and release the plan exactly once on every mismatch.
  - Resolve tools by the explicit tool name; delete delimiter parsing.
- `composition/registry.go`
  - Populate the typed identities from mounted records.
  - Resume every persisted entry as required, recover session identity from
    payload-specific scopes and every handler registration, recover tool
    selection from explicit fields, and validate before snapshotting.
  - Reconstruct prompts, guards, restrictions, and handlers from the persisted
    instance set, then compare the sealed fingerprint before returning the plan
    or permitting durable mutation. Any added, removed, or changed capability
    releases the acquired plan exactly once and returns mismatch.
- Update `session`, `runtime`, `composition`, `wasmext`, admission, and descriptor
  tests. Stored schema stays at version 1 because it has no users; no migration
  or legacy decoder is added.

## Invariants and tests

- Every entry has exactly one payload and every payload matches its behavior.
- Handler entries have registrations with valid handler kinds; non-handler
  entries cannot. One instance may contain global and session registrations.
- Config hashes and allowed source kinds are mandatory for all component-backed
  entries. Tool schema/executor hashes and restriction rules hashes are
  mandatory only for their kinds.
- Global scope has no key; session scope has one key.
- Resume rejects malformed or drifted entries before durable run mutation.
- Names and registration IDs containing `/` round-trip without parsing.
- Fingerprints change for every behaviorally relevant identity field and remain
  order-independent.
- Self-consistent but malformed fingerprints are rejected by admission/resume.
- Added/removed prompt, guard, restriction, and handler registrations all fail
  resume before mutation; no broader reconstructed plan can escape comparison.
- Table tests mutate each identity/behavior field independently and prove
  `ErrExtensionPlanMismatch` plus exactly-once release.

## Dependencies and risks

- Implement before the permission-pattern persistence update so session fixtures
  are migrated once.
- Generated WIT is unchanged; `SourceKind` validation uses the native/wasm values
  already defined by `extension`.
- Schema version remains 1 only because there are no users. No old-shape reader
  exists; recreate pre-release local databases after upgrade or source rollback.
