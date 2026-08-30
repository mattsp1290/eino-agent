# Consumer-resolvable module graph

Status: Ready

Planning is complete. Implementation has not occurred.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "f549d0ad80f7f16390c26ed062f19d96f258e77a0a0a37eecd736cffb9cb8147",
    "confirmed_at": "2026-08-30T15:46:46Z"
  }
}
```

The user confirmed that `eino-agent` has no active users or external consumers
and that this change does not need backward compatibility. Feature flags,
dual module layouts, compatibility aliases, and migration shims are therefore
not applicable.

## Change classification

- Change type: Go module packaging and release correction with a regression
  gate.
- Affected areas: the root and generated-bindings module manifests, the
  repository's TinyGo example module, Makefile and CI verification, public
  consumer documentation, version tags, and the external request response.
- Runtime APIs and behavior are not affected.

## Requested outcome

Publish a root `github.com/mattsp1290/eino-agent` revision whose full module
graph resolves from an unrelated Go module without a consumer `replace`,
vendoring, a workspace, a proxy override, or access to this checkout.

Success requires all of the following:

1. A fresh external module can require the proposed root tag `v0.1.3` and
   resolve every dependency with the standard configured Go proxy and checksum
   database.
2. That module imports and builds `runtime`, `store/sqlite`, `stream`,
   `composition`, `model`, and `providers/fake`.
3. In that module, `go mod tidy`, `go list -m all`, `go mod verify`,
   `go test ./...`, and `go build ./...` succeed with no `replace` directive.
4. Published verification rejects module-selection settings that could provide
   a private path, checksum bypass, alternate manifest, workspace, or vendor
   tree, and records a sanitized effective Go environment profile.
5. Repository CI runs a synthetic external-consumer fixture in which the root
   module's own `replace` directive is ignored.
6. Public documentation identifies the supported root pin and Go 1.26.3
   requirement.
7. The response record identifies the published tag and full commit SHA only
   after an independent no-replacement verification succeeds.

## Scope

- Publish the existing `wasmext/gen` nested module under the proposed
  submodule tag `wasmext/gen/v0.1.0`.
- Replace the unresolved `v0.0.0` requirements with `v0.1.0` while preserving
  the local replacement used to develop the root and generated bindings
  together.
- Add local and CI verification that exercises `eino-agent` as a dependency,
  not as the main module.
- Publish the proposed root patch tag `v0.1.3` and verify it from a fresh module.
- Record completion at the external response location described in
  [04-release-and-response.md](04-release-and-response.md).

## Non-goals

- Do not change TUI behavior, provider semantics, runtime APIs, terminal code,
  WIT semantics, or Wasm host behavior.
- Do not add a consumer-side workaround to `eino-tui`.
- Do not add vendoring, a committed workspace, a private proxy, or release
  automation.
- Do not fold `wasmext/gen` into the root module. That would couple the TinyGo
  guest bindings to the root's newer Go toolchain.
- Do not implement or respond to the separate `eino-agent-extensions` request
  in this work. The same graph correction will be relevant to it, but it owns
  a separate acceptance surface and response record.

## Constraints

- Keep the root module at Go 1.26.3.
- Keep `wasmext/gen` and `examples/wasm-extensions` at Go 1.24.6 so the existing
  TinyGo guest build remains isolated from the root toolchain.
- Treat both proposed tags as immutable after publication. Never move a tag to
  repair a failed release.
- Make tag steps resumable: reuse a tag when its peeled commit matches the
  recorded intended commit, and choose a new patch version only when the tag
  points elsewhere or its published artifact is proven defective.
- Preserve the generated source layout and import paths under `wasmext/gen`.
- Do not claim a usable consumer pin until the tag and its target commit are on
  `origin` and the published-mode fixture passes without a replacement.

## Repository findings

### Verified facts

- `go.mod` requires `github.com/mattsp1290/eino-agent/wasmext/gen v0.0.0` and
  replaces it with `./wasmext/gen`.
- `go mod download -json github.com/mattsp1290/eino-agent/wasmext/gen@v0.0.0`
  fails with `unknown revision wasmext/gen/v0.0.0`.
- `wasmext/gen/go.mod` declares the nested module and Go 1.24.6; no
  `wasmext/gen/v*` tag exists locally or on `origin` at planning time.
- `examples/wasm-extensions/go.mod` also requires the nested module at
  `v0.0.0`, replaces it with the local nested directory, and uses Go 1.24.6.
- The root imports generated types from the nested module through `wasmext`.
  The six requested TUI packages do not directly depend on the generated
  bindings, but Go still resolves every required module in the root graph.
- `Makefile` uses `GUEST_GOTOOLCHAIN := go1.24.6` for nested tidy and TinyGo
  fixture builds, but its current `wit` target invokes `go` without the pinned
  guest toolchain. The implementation must close that reproducibility gap.
- Existing root tags stop at `v0.1.2`, and all three existing root tags predate
  the generated-bindings module.
- CI validates only the checkout as the main module today. The root's local
  replacement therefore hides the unresolved version.
- `internal/deps/deps_test.go` already protects the core package boundary from
  Wasmtime and generated-binding dependencies; this invariant must remain.

### Assumptions

- `origin` remains the authoritative public VCS source for both module paths.
- Standard Go submodule tag naming remains available through
  `wasmext/gen/v<semver>`.
- The existing generated bindings at the selected submodule tag need no source
  changes for this packaging correction.

## Key decisions

1. Version the nested module instead of merging it into the root. This keeps
   the guest-facing Go 1.24.6 boundary and avoids forcing the root's Go 1.26.3
   directive into TinyGo builds.
2. Use the proposed nested version `v0.1.0`. The nested module has no published
   versions, and the generated WIT API is already identified as 0.1.0.
3. Retain repository-local replacements after changing their required version
   to `v0.1.0`. They preserve coordinated local generation; the external
   fixture proves that consumers do not depend on those replacements.
4. Use one fixture in two modes. Local mode replaces only the root module with
   the checkout; published mode requires the proposed root tag and permits no
   replacements.
5. Publish the proposed root patch version `v0.1.3`. Existing `v0.1.2` predates
   the affected code, and the correction changes packaging without changing
   runtime semantics.

### Rejected alternatives

- Removing `wasmext/gen/go.mod` would make the graph single-module, but it would
  collapse the root and TinyGo toolchain boundaries.
- Removing only the root replacement would not make `v0.0.0` resolvable and
  would make coordinated local host/binding development harder.
- Keeping `v0.0.0` with a consumer replacement, vendoring, or a workspace would
  move the defect into each consumer and violates the request.
- Testing only the repository checkout would continue to let the dependency's
  local replacement hide the failure.

## Change model

```text
Before
external consumer
  -> eino-agent@commit
       -> wasmext/gen@v0.0.0     X no VCS tag
       -> replace ./wasmext/gen  ignored outside the main module

After
external consumer
  -> eino-agent@v0.1.3
       -> wasmext/gen@v0.1.0
            -> resolved by tag wasmext/gen/v0.1.0

Repository checkout only
  -> root replace ./wasmext/gen remains for coordinated development
  -> external-consumer gate makes that dependency-local replace inoperative
```

## Compatibility, rollout, migration, and rollback

- Compatibility: no backward-compatibility layer is required. Package import
  paths and runtime APIs remain unchanged because the correction supplies a
  real version for the existing nested module path.
- Stored data and configuration: no schemas, configuration keys, or data files
  change.
- Workflows: repository contributors retain local generation through the
  checked-in replacement. External consumers gain a normal versioned workflow.
- Rollout: publish and verify the nested tag first, then merge the root graph
  and test changes, then publish and verify the root tag.
- Rollback: stop before the next publication gate when verification fails. If
  a published tag is defective, leave it immutable, fix forward, publish the
  next patch tag, update documentation, and name only the verified tag in the
  response.

## Risks and stop/go gates

- Stop before changing the root requirement if `wasmext/gen/v0.1.0` already
  exists at a different peeled commit. Reuse it when its peeled commit matches
  the recorded intended commit; otherwise the implementation owner must select
  a new nested patch version and update every plan-specified pin consistently.
- Stop before merging if the synthetic consumer resolves `wasmext/gen` through
  any replacement. That would leave the original masking path intact.
- Stop before publishing `v0.1.3` if that tag exists at a different peeled
  commit or the documentation does not name the same proposed pin. Reuse a
  matching tag after an interrupted run; select the next unused patch tag and
  update documentation only for a collision or proven defective artifact.
- Stop before writing a completion response if the published-mode fixture uses
  a `replace`, a workspace, vendoring, nonstandard proxy settings, or a warm
  module cache.
- Stop published verification if the effective Go environment has nonempty
  `GOFLAGS`, `GOPRIVATE`, `GONOSUMDB`, or `GONOPROXY`, or does not use the
  documented public proxy and checksum database profile. Report only pass/fail
  for private-pattern variables; never print their values.
- Network and module-proxy propagation can delay post-tag verification. A
  delay is not evidence that the tag is defective or that a new tag is needed.
- `/.agents/plans/` is ignored for new files, while earlier reviewed plans are
  tracked. This planning session must force-stage only this plan directory as
  the requested deliverable, leave `.gitignore` unchanged, commit it, close the
  planning bead, and push both repository and Beads state before handoff.

## Decisions and open questions

- Resolved: there are no active-user or backward-compatibility gates.
- Resolved: feature flags are not applicable because the change has no runtime
  behavior branch.
- Resolved: use nested-module versioning rather than module consolidation.
- Blocking open questions: none.
- Non-blocking open questions: none. Version-collision handling is an explicit
  execution gate, not an unresolved design choice.

## Document map

- [01-publish-bindings-module.md](01-publish-bindings-module.md): publish and
  verify the generated-bindings submodule contract.
- [02-correct-module-manifests.md](02-correct-module-manifests.md): replace the
  invalid requirements while preserving local generation.
- [03-external-consumer-gate.md](03-external-consumer-gate.md): add the local,
  CI, and published-mode external-consumer verification.
- [04-release-and-response.md](04-release-and-response.md): document, publish,
  independently verify, and report the supported root pin.
- [05-execution-handoff.md](05-execution-handoff.md): execute the work in
  dependency order with stop/go and final completion gates.
