# Correct module manifests

## Goal and prerequisite state

Make every checked-in requirement for the generated-bindings module name a
real, externally resolvable version while preserving the repository's local
generation workflow.

Prerequisite: [01-publish-bindings-module.md](01-publish-bindings-module.md)
has published and verified the chosen nested-module version.

## Repository evidence

- `go.mod` currently requires the nested module at the nonexistent `v0.0.0`
  and replaces it with `./wasmext/gen`.
- `examples/wasm-extensions/go.mod` repeats that pattern with a replacement to
  `../../wasmext/gen`.
- `Makefile` tidies the root, generated-bindings, guest example, and tooling
  modules separately.
- Root `wasmext` files import generated DTOs by the nested module path.
- `internal/deps/deps_test.go` asserts that core packages remain independent of
  generated bindings and Wasmtime.

## Exact change surface

- `go.mod` (existing): change the nested requirement from `v0.0.0` to the
  verified `v0.1.0`; retain the existing local replacement and add a concise
  comment that it is for coordinated repository development and is ignored by
  consumers.
- `examples/wasm-extensions/go.mod` (existing): change its nested requirement
  from `v0.0.0` to the same verified version; retain its local replacement so
  fixture generation uses the checked-out bindings.
- `Makefile` (existing): run the `wit` target's `go -C wasmext/gen generate .`
  command with `GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN)` so regeneration uses the
  nested module's declared toolchain boundary.
- `go.sum` (existing, only if changed by Go tooling): accept only checksum
  changes explained by the manifest correction.
- `examples/wasm-extensions/go.sum` (existing, only if changed by Go tooling):
  accept only checksum changes explained by its manifest correction.
- `wasmext/gen/go.mod` and `wasmext/gen/go.sum` (existing): no planned content
  changes.

No import path, exported symbol, runtime option, database schema, or
configuration value changes.

## Intended behavior and invariants

- A repository checkout continues to compile against its checked-out generated
  bindings through the local replacement.
- An external main module ignores that dependency-local replacement and can
  resolve the required nested version from VCS or its standard module proxy.
- The root remains Go 1.26.3; both guest-facing nested modules remain Go 1.24.6.
- Core packages listed in `internal/deps/deps_test.go` remain independent of the
  Wasm runtime and generated binding packages.
- `go mod tidy` must not remove the nested requirement because root `wasmext`
  imports it.

## Error paths and lifecycle concerns

- If the real nested tag resolves but root compilation fails without the local
  replacement, treat that as a host/bindings version mismatch. Publish a new
  nested patch version after correcting it; do not add a consumer workaround.
- If tidy changes unrelated dependency pins, separate or revert those changes
  before this package proceeds.
- The change has no runtime lifecycle, stored-data, migration, or feature-flag
  path.

## Tests and observable acceptance

Run the existing module checks after editing:

```text
go mod tidy -diff
GOTOOLCHAIN=go1.24.6 go -C wasmext/gen mod tidy -diff
GOTOOLCHAIN=go1.24.6 go -C examples/wasm-extensions mod tidy -diff
go -C internal/tools mod tidy -diff
go test ./internal/deps ./wasmext
GOTOOLCHAIN=go1.24.6 go -C wasmext/gen test ./...
make wit-check
```

Acceptance criteria:

- No checked-in `go.mod` requires
  `github.com/mattsp1290/eino-agent/wasmext/gen v0.0.0`.
- The root and example manifests name the same verified nested version.
- The nested module still builds independently with Go 1.24.6.
- `make wit-check` uses the pinned guest toolchain and leaves no generated diff.
- Core dependency-boundary tests pass without weakening their forbidden list.
- The repository working tree contains no unexplained `go.sum` drift.

## Dependencies, risks, and exclusions

- This work package depends on the nested tag, not merely on a proposed tag
  name.
- Local root commands still show the replacement. They are necessary but do
  not prove external consumption; [03-external-consumer-gate.md](03-external-consumer-gate.md)
  supplies that proof.
- Do not remove the nested module boundary or upgrade any toolchain or unrelated
  dependency.
