# Execution handoff

## Operating context

- Status: implementation-ready.
- Active users: no.
- Backward compatibility required: no.
- Feature flags: not applicable.
- Migration: none.
- Runtime rollout: none; this is a dependency publication sequence.

## Dependency-ordered work packages

### 0. Claim implementation tracking

Result: repository work and release failures have a dedicated Beads record.

Change surface:

- `eino-agent-orb` (existing open Beads feature): implementation status,
  selected nested/root versions, verification evidence, and failure notes.

Prerequisites and parallelization:

- `eino-agent-vy4`, the planning bead, must be closed after this plan is
  force-staged and committed. `/.agents/plans/` ignores new files, so
  the planning session must add only this exact directory with Git's force-add
  option, matching the repository's existing tracked-plan precedent without
  changing `.gitignore`; it must then push both Git and Beads state before
  handoff.
- Claim `eino-agent-orb` atomically before changing source, tags, or external
  response state.
- No implementation package may start before this step.

Verification:

- `bd show eino-agent-orb` identifies this plan and its acceptance criteria.
- `bd update eino-agent-orb --claim` succeeds.

Acceptance: the implementation bead is in progress and records the selected
versions plus intended tag-target commits before the first tag or code change.

### 1. Publish the bindings module

Result: the existing nested module is available at the proposed
`wasmext/gen/v0.1.0` tag and resolves as module version `v0.1.0`.

Change surface:

- `wasmext/gen/go.mod` and generated tree (existing, verification only unless a
  prerequisite correction is required).
- `refs/tags/wasmext/gen/v0.1.0` (proposed tag).

Prerequisites and parallelization:

- Must use a commit already reachable from `origin/main`.
- Blocks packages 2 through 4.
- Do not run tag publication in parallel with changes to `wasmext/gen`.

Verification:

- Nested tidy and tests with Go 1.24.6.
- `GOTOOLCHAIN=go1.24.6 make wit-check` with no diff.
- Fresh-cache download of the nested version with no replacement.

Acceptance: all gates in
[01-publish-bindings-module.md](01-publish-bindings-module.md) pass.

### 2. Correct the checked-in requirements

Result: root and guest-example manifests require the verified nested version
while local coordinated-development replacements remain intact.

Change surface:

- `go.mod` and conditionally `go.sum` (existing).
- `examples/wasm-extensions/go.mod` and conditionally its `go.sum` (existing).
- `Makefile` guest-toolchain use in the existing `wit` recipe.

Prerequisites and parallelization:

- Depends on package 1's published version.
- Must precede the consumer fixture's positive acceptance run.

Verification:

- Tidy checks for all four modules named by `Makefile`.
- `go test ./internal/deps ./wasmext`.
- Nested-module tests with Go 1.24.6.

Acceptance: every manifest requirement uses the same real version and the
existing dependency-boundary tests remain intact.

### 3. Add the external-consumer regression gate

Result: local and CI runs exercise the checkout as a dependency whose own
replacement directives are ignored.

Change surface:

- `testdata/external-consumer/consumer.go` (new under existing `testdata/`).
- `testdata/external-consumer/check.sh` (new under existing `testdata/`).
- `Makefile` with `external-consumer-check` (proposed target at the existing
  phony-target section).
- `.github/workflows/ci.yml` with an external-consumer step (proposed step in
  the existing `test` job).

Prerequisites and parallelization:

- Depends on package 2.
- Documentation can be drafted in parallel, but it must not merge with an
  unverified or mismatched version.

Verification:

- Observe the old invalid requirement fail in the uncommitted negative check.
- Run `make external-consumer-check` after the fix.
- Run the CI-equivalent command with an empty module cache.

Acceptance: the nested module reports its real version without a replacement,
and all requested imports and Go commands succeed.

### 4. Document, merge, publish, verify, and respond

Result: the proposed root `v0.1.3` tag is public, independently consumable, and
recorded in the response that unblocks `eino-tui`.

Change surface:

- `README.md`, `docs/consumer-guide.md`, and `docs/dependency-status.md`
  (existing).
- `refs/tags/v0.1.3` (proposed tag).
- `$HOME/.agents/projects/eino-agent/responses/2026-08-30-consumable-module-graph.md`
  (proposed external response).

Prerequisites and parallelization:

- Candidate documentation may join package 3's pull request after version
  gates are confirmed.
- Tag publication, published verification, the supported-pin documentation
  update, and response writing are serial post-merge actions.

Verification:

- Full `make check` before merge and on the final commit.
- Published-pin fixture mode with no replacement and a fresh module cache.
- Sanitized effective Go environment preflight with the documented public
  proxy/checksum profile and no module-selection flags or private exemptions.
- Remote tag-to-SHA comparison.
- Manual comparison of response evidence to command output.
- Post-verification documentation commit and push before the response.

Acceptance: all gates in
[04-release-and-response.md](04-release-and-response.md) pass and the response
contains no placeholder or unverified claim.

## Integration and regression gates

Run these gates after all repository changes are assembled and before the root
tag is created:

```text
make wit-check
make fmt-check
make vet
make test
make race
make mod-tidy-check
make lint
make windows-compile
make external-consumer-check
git status --short
```

The final status command must show no generated, manifest, sum, fixture, or
format drift. Existing unrelated user changes must not be included in the
implementation commits.

## Stop/go decisions

1. Nested version gate: create the tag when absent; reuse it when its peeled ref
   matches the recorded intended commit; select the next patch only for a
   different peeled commit or a proven defective published artifact.
2. Root version gate: create the tag when absent; reuse it when its peeled ref
   matches the recorded release-candidate commit; select the next patch and
   update documentation only for a different commit or proven defect.
3. External-resolution gate: if the nested module shows a replacement or an
   unresolved version in the synthetic consumer, do not merge.
4. Publication gate: if published mode needs any workaround, do not create a
   completion response.
5. Support-claim gate: public docs may name the candidate before publication
   but must not call it supported until published-mode verification passes.

These gates do not require a feature-flag decision.

## Final definition of done

- Both documented tags exist on `origin` and resolve to the recorded commits.
- Root and example manifests require the same published nested version.
- The checkout-local replacement remains only a contributor convenience and
  is proven irrelevant to external consumers.
- CI runs the external-consumer regression fixture.
- A fresh published-pin consumer has no replacement, workspace, vendor tree,
  proxy override, or checkout access.
- The published-pin preflight observes
  `GOPROXY=https://proxy.golang.org,direct`, `GOSUMDB=sum.golang.org`, Go 1.26.3,
  empty `GOFLAGS`, and empty `GOPRIVATE`, `GONOSUMDB`, and `GONOPROXY` without
  printing private-pattern values.
- `go mod tidy`, `go list -m all`, `go mod verify`, `go test ./...`, and
  `go build ./...` pass for that consumer.
- The six requested public packages import and build.
- Public docs identify the verified root pin and Go 1.26.3 requirement.
- The external response names the exact root tag and full SHA and marks the
  request completed only after verification.
- `eino-agent-orb` records release evidence and is closed only after the
  response exists.
- The implementer runs `git pull --rebase`, `bd dolt push`, `git push`, pushes
  the exact new tags, and then verifies that `git status` shows the branch up to
  date with `origin`.

## Deferred work and follow-up

- The separate `eino-agent-extensions` request must run its own package-import
  acceptance set and receive its own response record. It should reuse the same
  published graph rather than request another implementation.
- General release automation remains out of scope.
- Future WIT changes require a new immutable nested-module version before a
  root release can depend on them.
