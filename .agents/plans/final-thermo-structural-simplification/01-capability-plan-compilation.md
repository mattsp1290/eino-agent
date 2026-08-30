# Capability Plan Compilation

## Goal

Reduce `composition.Registry.acquire` to an ownership coordinator, establish one canonical scope-applicability rule, and move tool freezing/identity work to mount time without moving runtime-specific capability knowledge into `extension`.

Prerequisite: the application context in [00-overview.md](00-overview.md) remains valid.

## Existing evidence

- `extension/registry.go:523` contains the private canonical predicate used for handler and mounted-payload selection.
- `composition/registry.go:498` contains a second predicate with the same global/session rule plus resume-instance membership.
- `composition.Registry.acquire` performs two snapshot modes, builds prompt winners, filters four capability collections, hashes and clones tools, and manually releases on three local errors.
- `runtime.NewRunPlan` already releases `RunPlanSpec.Dispatch` on every compilation or sealing error.
- `extension.Registry.SnapshotInstances` removes disallowed mounts before producing dispatch entries or mounted values, so post-snapshot resume-instance checks are redundant.
- `Registrar.Tool` already validates and clones each definition before commit; `tools.Materialize` performs a defensive clone again at the true execution ownership boundary.
- Existing composition tests cover scope selection, resume identity, mount-order stability, and release-on-plan-failure behavior.

## Exact change surface

### `extension/registry.go`

- Rename existing private `scopeApplies` to proposed exported `ScopeApplies`.
- Document that the function assumes validated scopes and answers only whether a registration scope participates in a target snapshot.
- Use it internally for handler entries and mounted payload selection.
- Do not add an alternate predicate or policy object.

### `composition/registry.go`

- Add proposed private `snapshotForPlan(target extension.Scope, instances map[string]bool)` or an equivalent focused method. It performs only fresh/resume snapshot acquisition and deterministic instance-ID extraction.
- Add proposed private `planSelection` with:
  - target scope;
  - optional tool selector;
  - selected components;
  - precomputed prompt winners.
- Treat `Snapshot`/`SnapshotInstances` as the sole component-instance selection authority. Do not retain an instance allow-set in `planSelection` or capability projection.
- Move prompt-winner computation into proposed `newPlanSelection` or `selectPromptWinners`.
- Add private frozen schema and executor hash fields to the existing `ToolRegistration` internal mounted value.
- During `Registrar.Tool`, after validation and cloning:
  - compute the composed schema hash from the frozen definition and source identity;
  - compute the composed executor hash from source identity and `r.component.Artifact.Hash`;
  - store both hashes with the frozen registration;
  - return any clone/hash failure from mount preparation so extension rollback owns cleanup.
- Simplify `composition/tool_identity.go` helpers if the now-concrete hash input permits infallible encoding; do not keep an `error` return for a structurally infallible string-only hash.
- Add one proposed substantive `planSelection.components()` or equivalent selection/compiler boundary. It projects selected component payloads into `[]runtime.PlanComponent`, calls `extension.ScopeApplies` directly, applies prompt winners and the tool selector, and omits behaviorless components exactly as today.
- Extract a tool projector only if it owns the substantive callback wrapping and frozen-hash projection. Keep prompt/guard/restriction loops direct when separate helpers would only forward fields.
- Do not stack both singular/plural component coordinators or add an applicability wrapper that delegates one predicate call. Projection is non-failing.
- Delete existing `capabilityApplies`.
- Make `Registry.acquire` perform only: context check, target selection, snapshot acquisition, selected-value projection, and `runtime.NewRunPlan` construction.

The proposed private names are conceptual and may change if an existing local term produces a clearer implementation. No new file is required unless `composition/registry.go` would become harder to scan; if split, proposed `composition/plan_selection.go` is allowed beside the existing registry.

## Intended behavior and invariants

- Global registrations apply to global and session targets.
- A session registration applies only to the matching session target.
- Resume acquisition includes only persisted component instance IDs.
- Resume tool selection includes only the persisted complete tool identity.
- No capability projection path checks component-instance membership after snapshot acquisition.
- Session-scoped prompts override same-name global prompts exactly as today.
- Prompt winners are deterministic across mount-order permutations.
- Tool definitions are validated, cloned, and hashed once during mount; acquisition only wraps callbacks around the mount-frozen definition.
- Tool schema and executor hashes remain byte-for-byte identical for the same input.
- A component with no selected capabilities is omitted unless its dispatch handlers independently make it part of the runtime descriptor.
- Projection after snapshot acquisition is non-failing. `runtime.NewRunPlan` receives the snapshot dispatch immediately after projection and owns release on success and failure.
- No generic `any`, callback casts, or new capability interface is introduced.
- The refactor reduces the combined concept count and complexity across `acquire`, selection construction, and component projection; it does not merely make `acquire` short by distributing each loop into a thin wrapper.

## Error paths and ownership

Use one direct ownership handoff:

```text
snapshot acquired
  -> non-failing composition projection
  -> runtime.NewRunPlan called: runtime owns dispatch immediately
       -> error: NewRunPlan releases once
       -> success: RunPlan releases at execution completion
```

Do not add a local release guard for non-failing projection. Mount-time clone/hash failures occur before a snapshot exists and follow the existing preparation rollback path. Do not preserve the current repeated `snapshot.Release()` acquisition branches.

## Tests and acceptance criteria

- Add a table test for `extension.ScopeApplies` covering valid global/global, global/session, matching session/session, mismatched session/session, and session/global pairs.
- Preserve existing snapshot selection tests; update them to exercise the exported predicate indirectly.
- Preserve and strengthen composition tests for:
  - fresh global-only acquisition;
  - session-over-global prompt precedence;
  - resume instance and exact tool-identity filtering;
  - each capability family filtered by target scope;
  - mount-order-independent plan fingerprints;
  - a component owning multiple capability families;
  - invalid/uncloneable tool definitions rolling back earlier deferred mount cleanup, publishing no mount, and leaving later acquisition empty;
  - no post-snapshot instance filtering, with resume exclusion proven through `SnapshotInstances`;
  - `runtime.NewRunPlan` failure releasing the dispatch once.
- A `runtime.NewRunPlan` failure test must prove that the affected published mount can close after dispatch release; do not assert only a private counter.
- A mount-preparation failure test must prove prior deferred cleanup ran, no mount was published, and later acquisition is empty.
- Supplemental static analysis must not report `composition.Registry.acquire` under default `funlen` or `gocognit` thresholds.
- The same analysis must not report the new selection constructor, selection/compiler boundary, or substantive tool projector. Review the final helper graph for pass-through wrappers even when each helper individually passes.
- `rg -n 'capabilityApplies|func scopeApplies' composition extension --glob '*.go'` must show no duplicate private predicate.
- Acquisition tests must not require corrupting an immutable mounted payload to manufacture clone/hash errors.

## Risks and exclusions

- Do not move tool hashing, prompt precedence, guards, or restriction knowledge into `extension`.
- Do not change public runtime plan types.
- Do not merge prompt precedence with tool enable/disable selection; they have different identity rules.
- Do not broaden resume selection when a persisted component is mounted with new capabilities.
- Do not retain an optional instances map after snapshot acquisition.
- Do not repeat definition cloning or durable hash computation during acquisition.
- Do not invent a schema-hash failure fixture after definition validation/freezing makes that path structurally unreachable; test the attainable validation/clone rollback boundary instead.
- Do not redesign `extension.Registry.snapshot`; its current lock/refcount boundary is cohesive and was not a blocker finding.
