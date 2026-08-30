# Release and response

## Goal and prerequisite state

Document a concrete supported pin, publish it at the verified final commit,
repeat the consumer gate with no replacement, and then write the external
completion record that unblocks `eino-tui`.

Prerequisites:

- The nested tag is publicly resolvable.
- The corrected manifests and external-consumer gate are merged to `main`.
- All repository quality gates pass on the exact final commit.
- The proposed root tag `v0.1.3` is unused locally and on `origin`, or its
  peeled ref already equals the recorded final release-candidate commit from an
  interrupted attempt.

## Repository evidence

- `README.md` states the module and Go 1.26.3 baseline but does not name a
  consumable post-extensibility root version.
- `docs/consumer-guide.md` describes the public package surface but has no
  installation section with a supported pin.
- `docs/dependency-status.md` records dependency pins and validation evidence
  but predates the nested module.
- Existing root releases end at `v0.1.2` and predate the current public package
  state.
- The request requires a response file outside the repository only after a
  committed tag or full SHA is independently consumable.

## Exact change surface

- `README.md` (existing): in `Module Baseline`, first name `v0.1.3` as a release
  candidate that is not supported until post-tag verification passes; after
  verification, change that wording to the supported root pin, its Go 1.26.3
  requirement, and the standard `go get` form.
- `docs/consumer-guide.md` (existing): add a concise installation section with
  the same candidate status before tag publication; after verification, change
  it to require the supported tag, name Go 1.26.3, and state that consumers do
  not need a replacement, workspace, vendoring, or checkout access.
- `docs/dependency-status.md` (existing): record the generated-bindings module
  pin `v0.1.0`, its submodule tag form, and the external-consumer verification
  gate.
- `refs/tags/v0.1.3` (proposed annotated root tag): create on the exact merged
  commit containing the manifest, fixture, CI, and documentation changes.
- `$HOME/.agents/projects/eino-agent/responses/2026-08-30-consumable-module-graph.md`
  (proposed external response file): create only after published verification.
  Resolve `$HOME` through the implementation environment; do not copy a
  checkout-specific absolute path into repository artifacts.

If either proposed version is changed at a stop/go gate, update every document,
fixture expectation, command, and response entry to the chosen version before
merge or publication.

## Documentation contract

The public documentation must distinguish:

- root module pin: proposed `github.com/mattsp1290/eino-agent@v0.1.3`;
- minimum Go toolchain: 1.26.3;
- internal generated-bindings pin: proposed
  `github.com/mattsp1290/eino-agent/wasmext/gen@v0.1.0`;
- consumer workflow: standard Go module commands with no workaround.

Before tag publication, every mention of `v0.1.3` must say `release candidate`
and must not call it usable or supported. After published verification, replace
candidate wording with the supported-pin contract. Do not promise
semantic-version compatibility beyond the named pin. Do not claim that the
older root tags contain the current public runtime surface.

## Publication and verification

1. Merge candidate documentation with the manifest, fixture, and CI changes.
   The candidate text must state that the tag is not yet a supported pin.
2. Run `make check`, including the new local external-consumer target, on that
   exact final release-candidate commit.
3. Synchronize `main`; confirm the working tree is clean and `main` is up to
   date with `origin/main`.
4. Inspect the local and remote tag plus peeled ref. Create and push the
   annotated `v0.1.3` tag at `HEAD` only when absent. Reuse it and resume
   verification when its peeled commit equals the recorded release-candidate
   commit. If it points elsewhere, select the next unused patch version and
   update candidate documentation before publication.
5. Resolve the tag from `origin` and record its full commit SHA.
6. Run the published-mode environment preflight from
   [03-external-consumer-gate.md](03-external-consumer-gate.md). Reject a
   nonstandard effective proxy/checksum profile, any private-module bypass,
   nonempty `GOFLAGS`, a Go version other than 1.26.3, or a `GOMOD` path outside
   the temporary consumer. Record sanitized values only.
7. Run `EINO_AGENT_CONSUMER_VERSION=v0.1.3
   testdata/external-consumer/check.sh` from a clean checkout. The script must
   use a fresh module cache, `GOWORK=off`, no replacement, no vendor directory,
   and the standard configured proxy and checksum database.
8. Inspect the temporary manifest or retained command output and confirm that
   no `replace` directive was present.
9. Confirm that the resolved root module version and commit match the pushed
   tag and that the nested module resolves at the documented nested version.
10. Change README and consumer-guide wording from release candidate to
    supported pin, record the verified tag, full SHA, Go version, and sanitized
    environment profile in `docs/dependency-status.md`, then commit and push
    that documentation update.
11. Confirm the support-documentation commit is on `origin/main` before writing
    the external completion response.

If proxy propagation delays the standard path, wait and retry the same tag. Do
not weaken the final gate with `GOPROXY=direct`, a checksum bypass, or checkout
access. Publish a new patch only when the resolved artifact itself is proven
defective; interruption or transient resolution failure is not a defect.

## Response record

After every publication check passes, create the proposed response with:

- request title and status `completed`;
- root tag and full 40-character commit SHA;
- nested module tag and resolved version;
- Go 1.26.3 requirement;
- explicit statement that the temporary consumer had no `replace`, workspace,
  vendor tree, proxy override, or target-checkout dependency;
- imported package list;
- exact successful commands and verification date;
- links or repository-relative references to the manifest, fixture, CI gate,
  and public documentation;
- a statement that `eino-tui` may pin the named root tag.

Do not pre-create a completion response with a placeholder SHA. If final
verification fails, leave the request blocked and record the failure in the
implementation bead rather than claiming completion.

## Tests and observable acceptance

- `make check` passes at the exact root tag target.
- `git ls-remote --tags origin refs/tags/v0.1.3 'refs/tags/v0.1.3^{}'` returns
  the annotated tag object and a peeled ref equal to the recorded commit.
- Published-pin mode passes all five requested Go commands and `go build`.
- The response's tag, SHA, Go version, nested version, command evidence, and
  package list agree with actual output.
- Published verification records the accepted public proxy/checksum profile,
  Go version, empty `GOFLAGS`, and empty-status results for private-module
  variables without exposing their values.
- README and the consumer guide use candidate wording before tag publication
  and name the same supported pin only after published verification succeeds.
- The post-verification documentation commit is pushed before the completion
  response is written.

## Dependencies, risks, and exclusions

- This work package cannot run in parallel with manifest or fixture work.
- Tag creation, the support-claim documentation update, and response writing
  are serial release operations. Keep them behind the explicit post-merge
  gates.
- A bad published tag is fixed forward with a new patch version. Never rewrite
  public history or silently change the documented pin.
- Do not add general release automation or modify `eino-tui` in this work.
