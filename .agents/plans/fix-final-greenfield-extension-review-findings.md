# Fix Final Greenfield Extension Review Findings

## Change Information

### Change Type

Refactor with correctness fixes. This is a breaking, greenfield simplification of
the durable extension-plan model and the one supported runtime construction path.

### Description

Resolve the four open thermo-nuclear review findings:

- replace `session.ExtensionPlanEntry`'s five-way nullable payload union with
  statically typed per-capability identities;
- require `runtime.RunPlanProvider` and remove the synthesized no-provider path;
- reject duplicate guard and restriction identities during atomic mount staging;
- canonicalize tool restriction sets so behaviorally equivalent rules have one
  durable fingerprint.

Tracking issues: `eino-agent-twc`, `eino-agent-qm9`, `eino-agent-uuk`, and
`eino-agent-3ch`.

### Links to Relevant Documentation

- `docs/architecture/extensibility.md`
- `docs/architecture/extension-points.md`
- `docs/architecture/runtime.md`
- `docs/consumer-guide.md`
- `.agents/plans/simplify-greenfield-extensibility/00-overview.md`

### Affected Areas

- `session/extensions.go` and its tests: durable descriptor types, validation,
  cloning, canonical sorting, and fingerprinting.
- `runtime/extension_plan.go`, orchestration call sites, constructor validation,
  and runtime tests: typed executable evidence, canonical restrictions, and one
  required plan-provider path.
- `composition/registry.go` and its tests: typed plan construction/resume,
  restriction normalization, and complete mount-time identity validation.
- `examples/minimal-server`, runtime test helpers, and any other constructor call
  sites that currently rely on the implicit empty plan.
- architecture and consumer documentation describing construction, durable
  identity, restriction semantics, and strict resume.

### Success Criteria

- Durable extension descriptors expose separate typed slices for handlers,
  tools, prompts, guards, and restrictions; no mutually-exclusive nullable
  payload union, `Kind()` counter, or downcast switch remains.
- `runtime.PlanTool`, `PlanPrompt`, `PlanGuard`, and `PlanRestriction` each accept
  only their matching session identity type.
- Descriptor validation and fingerprinting remain current-only, deterministic,
  restart-stable, and strict about artifact/scope/registration identity.
- Resume validates the persisted descriptor's self-integrity before using any of
  its fields for selection or lease acquisition, then separately compares the
  reconstructed live plan fingerprint.
- Duplicate guard or restriction identities fail inside the installer/mount
  transaction and roll back cleanup without publishing any capability.
- Allowed/denied restriction names are validated, deduplicated, sorted, and
  hashed canonically; equivalent reorderings produce identical fingerprints.
- `NewStreamingOrchestrator` rejects a missing plan provider. Every production
  constructor installs a `composition.Registry`, including the zero-capability
  case. The synthesized empty-provider helper and provider-nil branches are gone.
- Extension invocation call sites use the canonical `extension.Invoke`/`Notify`
  behavior directly instead of branching on whether dispatch is nil.
- Focused tests, `make check`, `git diff --check`, and structural searches pass;
  all production Go files remain below 1,000 lines.

### Constraints

- The project has no users. Backward compatibility, descriptor migrations,
  deprecated aliases, dual readers, and feature flags are dead code and must not
  be added.
- Preserve current extension ordering, scope behavior, mount leasing/draining,
  durable strict-resume guarantees, tool selection, and Wasm ABI behavior.
- Keep one canonical composition pipeline. Do not add adapters around the old
  descriptor shape or retain an optional plan-provider mode for tests/examples.
- Use direct, typed Go data models and explicit comparison functions; avoid
  reflection, `any`, JSON round-trips used only for type discrimination, or
  generic wrappers that merely relocate the same complexity.

## Planned Design

### 1. Replace the nullable durable union with typed collections

Redefine the session identities so each type contains its complete component and
capability identity:

- `HandlerPlanIdentity` owns `InstanceID`, `Artifact`, and registrations.
- `ToolPlanIdentity` owns `InstanceID`, `Artifact`, name, registration ID,
  scope, schema hash, and executor hash.
- `PromptPlanIdentity`, `GuardPlanIdentity`, and `RestrictionPlanIdentity`
  likewise own their common component identity and kind-specific fields.
- `ExtensionPlanDescriptor` contains `Handlers`, `Tools`, `Prompts`, `Guards`,
  and `Restrictions` slices rather than `Entries []ExtensionPlanEntry`.

Repeat `InstanceID` and `Artifact` as direct value fields in each typed identity.
Do not introduce a common embedded wrapper: the small field repetition keeps
each durable record self-contained and avoids another navigation/serialization
layer.

Delete `ExtensionKind`, `ExtensionPlanEntry`, `Kind`, nullable clone branches,
and switch-based validation/comparison. Validate each typed collection directly,
enforce one session key across all scopes, sort each collection explicitly, and
hash the resulting current descriptor. Do not accept or decode the old JSON
shape.

Use these exact duplicate keys; behavior-affecting fields named after the key
still participate in sorting/fingerprinting:

- handler: instance, registration ID, contract, version, handler kind, scope;
  order is fingerprinted but is not part of the duplicate key;
- tool: instance, registration ID, tool name, scope; schema and executor hashes
  are fingerprinted;
- prompt: instance, registration ID, prompt name, scope; order is fingerprinted;
- guard: instance, registration ID, scope; order is fingerprinted;
- restriction: instance, registration ID, scope; rules hash is fingerprinted.

Sort every typed slice with explicit comparisons over all persisted fields,
including the complete artifact identity and all behavior-affecting fields. Do
not use a JSON fallback comparator.

Descriptor validation must also enforce component coherence across collections:
every occurrence of one `InstanceID` in any typed slice must carry exactly the
same `Artifact`, and at most one `HandlerPlanIdentity` aggregate may exist per
instance. This prevents impossible mixed-artifact descriptors and prevents the
same logical handler set from acquiring multiple fingerprints merely by being
split across aggregates. Add focused rejection tests for both cases.

### 2. Bind executable behavior to matching typed evidence

Change runtime plan inputs so their `Identity` fields use the corresponding
session type. Build handler identities from dispatch diagnostics, append each
typed capability identity directly, cross-check behavior against its typed
fields, validate/fingerprint the complete descriptor once, and keep the sealed
plan opaque.

Update composition plan creation and resume selection to iterate the typed
descriptor slices directly. Recover the session scope from every typed slice,
select persisted tools from typed tool identities, and compare only the final
runtime-computed fingerprint. This removes generic kind switches and impossible
wrong-kind runtime values.

`AcquireResumePlan` must first recompute the supplied descriptor fingerprint and
reject a mismatch before reading instance IDs/scopes or acquiring any registry
lease. After reconstruction, independently require equality with the live-plan
fingerprint. Add a direct composition-registry test that tampers with a
non-selector field while retaining the old fingerprint and proves rejection
occurs before plan acquisition.

### 3. Canonicalize restrictions at the boundary

Introduce one checked canonicalization path used by registration and plan
sealing:

- reject blank names, an entirely empty rule set, and names present in both
  allowed and denied sets;
- deduplicate and lexically sort both sets;
- normalize every empty side to `nil`, so nil and empty inputs have one stored
  representation when the other side contains rules;
- compute `RulesHash` from that canonical representation;
- store and enforce the canonical slices, so hashing and behavior cannot drift.

Because the API is undeployed, change `RestrictionRulesHash` to return an error
if that produces the cleanest boundary. Do not retain an order-sensitive helper.
Test nil versus empty sides, duplicates, reorderings, blank names, overlaps, and
entirely empty policies. Strict resume must accept reordered/duplicated
set-equivalent rules and reject a genuinely changed set.

### 4. Complete mount validation before publication

Make `Registrar.Guard` and `Registrar.RestrictTools` reject repeated
registration ID plus scope within the staged component, matching the durable
identity key. Ensure failure occurs while the installer is still staging so the
existing prepared-mount rollback runs cleanup and nothing is published. Add
focused tests for guards and restrictions, including differing order/rules with
the same durable identity. Each test must stage cleanup and another capability
before the duplicate, then prove cleanup ran, both composition and extension
diagnostics expose nothing from the rejected mount, no acquired plan contains
the staged capability, and the same component identity can mount successfully
afterward.

### 5. Require one plan-provider path

Add `RunPlanProvider` to constructor-required dependencies and update every
constructor call. Production examples should create a `composition.Registry`
even when no capabilities are mounted. Test-only orchestrator helpers should
provide an explicit empty static provider or registry rather than weakening the
production invariant.

Delete the `o.plans == nil` branches and `emptyExtensionPlanDescriptor`. Remove
dispatch-presence conditionals around extension calls; `extension.Invoke` and
`extension.Notify` already define the empty-plan behavior. Retain nil handling
only where a terminal resumed run genuinely has no acquired executable plan.

### 6. Verification and documentation

Update descriptor, constructor, mount atomicity, strict resume, and restriction
tests. Update architecture/consumer docs to state that the plan provider is
required, the descriptor is typed/current-only, and restriction lists are
canonical sets. Run focused package tests during each phase, then `make check`,
`git diff --check`, line-count checks, and searches for removed symbols and
provider-nil branches.

## Implementation Order

1. Change session descriptor types, clone/validation/fingerprint logic, and
   session tests.
2. Migrate runtime plan evidence and tests to typed identities; add canonical
   restriction validation/hashing.
3. Migrate composition construction/resume and add atomic duplicate tests.
4. Require the provider, update all constructors, and delete empty/nil dispatch
   branches.
5. Update documentation and run the full verification matrix.

## Completion Sequence

1. Keep all four tracking issues claimed and update their notes if implementation
   diverges from this reviewed plan.
2. Run focused tests after each boundary migration, followed by `make check`,
   `git diff --check`, structural searches, and production line counts.
3. Close `eino-agent-twc`, `eino-agent-qm9`, `eino-agent-uuk`, and
   `eino-agent-3ch` only after their acceptance criteria are satisfied.
4. Stage only related files and create one coherent commit explaining the
   greenfield simplification.
5. Run `git pull --rebase`, `bd dolt push`, and `git push` as separately checked
   commands.
6. Verify `git status --short --branch` is clean and up to date with
   `origin/feat/deeper-extensibility`.

## Review Questions

- Are the proposed restriction validation rules semantically sound for all
  current call sites?
- Does requiring a provider leave any legitimate terminal-resume or zero-tool
  path uncovered?
- Are all fallible validations positioned before mount publication and plan
  lease ownership transfer?
- Is there any remaining compatibility-only branch that can be deleted in the
  same pass without broadening scope?
