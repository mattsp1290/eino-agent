# Publish the generated-bindings module

## Goal and prerequisite state

Publish the existing `github.com/mattsp1290/eino-agent/wasmext/gen` module as
the proposed version `v0.1.0` before any root manifest depends on that version.

Prerequisites:

- The selected commit is reachable from `origin/main`.
- `wasmext/gen/v0.1.0` is absent locally and on `origin`, or its peeled ref
  already equals the recorded selected commit from an interrupted attempt.
- The selected commit contains the intended generated WIT 0.1.0 bindings.
- The checkout is clean before generation verification.

## Repository evidence

- `wasmext/gen/go.mod` is an existing nested module with Go 1.24.6 and a direct
  requirement on `go.bytecodealliance.org/cm v0.3.0`.
- `wasmext/gen/generate.go` contains the six pinned `wit-bindgen-go@v0.7.0`
  generation directives and writes packages beneath `wasmext/gen/eino-agent`.
- `Makefile` runs generation from the nested module and checks the generated
  tree for a diff.
- The existing generated package and WIT package versions are 0.1.0.
- No nested-module version exists at planning time.

## Exact change surface

This work package does not require a source-file change when the generated tree
is clean. It creates one proposed external VCS ref:

- `refs/tags/wasmext/gen/v0.1.0` (proposed annotated tag) at the selected
  `origin/main` commit.

The tag must expose the existing module root at `wasmext/gen/go.mod`. Do not
create a root-style `v0.1.0` alias for the nested module.

## Intended behavior and invariants

- Standard Go version resolution maps nested module version `v0.1.0` to the
  repository tag `wasmext/gen/v0.1.0`.
- The tagged module builds with Go 1.24.6 independently of the root module.
- Generation remains reproducible before publication.
- The tag target is immutable after push.
- No secrets, local paths, or checkout-only configuration enter the tag.

## Execution and failure paths

1. Record the selected `origin/main` commit in `eino-agent-orb`.
2. Fetch tags and inspect both the tag ref and its peeled commit. If absent,
   continue toward creation. If present at the recorded selected commit, reuse
   it and resume verification. If present elsewhere, select the next unused
   nested patch version and update every downstream pin before proceeding.
3. Check out or identify that exact commit before verification.
4. Run `GOTOOLCHAIN=go1.24.6 go -C wasmext/gen mod tidy -diff`.
5. Run `GOTOOLCHAIN=go1.24.6 go -C wasmext/gen test ./...`.
6. Run `GOTOOLCHAIN=go1.24.6 make wit-check` and confirm that it leaves no
   generated diff. This explicit environment override is necessary until work
   package 2 pins the toolchain inside the `wit` recipe.
7. Create and push the annotated nested-module tag only when step 2 found it
   absent. Otherwise retain the matching existing ref unchanged.
8. Use a new temporary module cache to download
   `github.com/mattsp1290/eino-agent/wasmext/gen@v0.1.0` through the standard
   configured Go module path.
9. Confirm that the downloaded module reports the expected tag, checksum, and
   target commit.

If steps 3 through 5 change the tree, stop. Commit and review the generated
change as a separate prerequisite before choosing the tag target. If download
verification fails after a tag push, classify the failure first. Retry a
transient proxy, checksum-service, DNS, or network failure against the same
immutable tag. Fix forward with the next nested patch version only when the
published module contents, manifest, checksum, or build are proven defective,
and propagate that version through the remaining work packages.

## Verification and acceptance

- `git ls-remote --tags origin refs/tags/wasmext/gen/v0.1.0
  'refs/tags/wasmext/gen/v0.1.0^{}'` returns the annotated tag object and peeled
  ref after publication; the peeled ref equals the recorded selected commit.
- A download using an empty temporary `GOMODCACHE` succeeds without a
  replacement or filesystem access to the tagged checkout.
- The downloaded module's `go.mod` declares
  `github.com/mattsp1290/eino-agent/wasmext/gen` and Go 1.24.6.
- The nested module's tidy and test commands pass.
- `GOTOOLCHAIN=go1.24.6 make wit-check` passes and the selected commit is clean.

## Dependencies, risks, and exclusions

- This package blocks every later work package because the root regression
  gate must resolve the real nested version.
- Tag propagation can be delayed. Retry read-only resolution; never retarget
  the published version.
- Do not publish the proposed root `v0.1.3` tag in this package.
- Do not change generated APIs, WIT definitions, or TinyGo behavior here.
