# Add the external-consumer gate

## Goal and prerequisite state

Add one repeatable fixture that exercises the root module from a separate main
module and proves that the root's local replacement cannot satisfy or hide the
nested requirement.

Prerequisites:

- The real nested-module version is published.
- The manifests require that version as specified in
  [02-correct-module-manifests.md](02-correct-module-manifests.md).

## Repository evidence

- `.github/workflows/ci.yml` runs only checkout-local generation, format, vet,
  test, race, tidy, and lint commands.
- `Makefile` has no target that builds `eino-agent` as a dependency module.
- `testdata/` is an existing parent for fixtures that should not join the root
  `./...` package pattern.
- The request names six import paths and five standard Go commands that must
  work from a clean external module.

## Exact change surface

- `testdata/external-consumer/` (new directory under existing `testdata/`):
  store the dependency-consumer fixture.
- `testdata/external-consumer/consumer.go` (new): define a buildable package
  that imports `runtime`, `store/sqlite`, `stream`, `composition`, `model`, and
  `providers/fake` from `github.com/mattsp1290/eino-agent`.
- `testdata/external-consumer/check.sh` (new executable): create an isolated
  temporary main module, copy the fixture source, construct its manifest with
  Go commands, run the complete verification sequence, and clean up through a
  trap.
- `Makefile` (existing): add the proposed phony target
  `external-consumer-check` and include it in the proposed `check` dependency
  list.
- `.github/workflows/ci.yml` (existing): add a named external-consumer step that
  runs `make external-consumer-check` so CI does not depend on callers choosing
  the aggregate target.

## Fixture modes

### Local checkout mode

This is the default for `make external-consumer-check`.

1. Create a temporary module outside the repository.
2. Require the root module at a test-only placeholder version.
3. Add exactly one main-module replacement from the root module path to the
   repository checkout.
4. Add no replacement for `wasmext/gen`.
5. Set `GOWORK=off` and use a new temporary `GOMODCACHE`.
6. Leave the effective module-download environment unmodified. Local mode is a
   regression test, not the evidence for the public-proxy release claim.

Go ignores replacement directives from dependency modules. The fixture's one
root replacement therefore makes uncommitted root source available while the
root's nested replacement is inoperative. The nested module must resolve at
its real version through standard module resolution.

### Published-pin mode

When the proposed environment input `EINO_AGENT_CONSUMER_VERSION` is nonempty,
the script requires that exact root version and adds no replacement at all.
It must fail before running Go commands if another option would provide a local
root path. This mode is the final release proof and is not part of pull-request
CI because an unmerged commit has no final public tag.

Before constructing the module, published mode must inspect effective values
from `go env` and enforce this standard verification profile:

- `GOPROXY` is exactly `https://proxy.golang.org,direct`.
- `GOSUMDB` is exactly `sum.golang.org`.
- `GOPRIVATE`, `GONOSUMDB`, and `GONOPROXY` are empty.
- `GOFLAGS` is empty, so `-modfile`, `-mod=vendor`, and other module-selection
  flags cannot redirect the commands.
- `go version` reports Go 1.26.3.

Print the accepted public proxy, checksum database, Go version, and boolean
empty/nonempty results for private-pattern variables. Never print private
pattern values. Do not set these variables inside the script to force a pass;
an operator must enter a clean standard environment and rerun.

## Intended behavior and invariants

- The temporary module contains no workspace or vendor directory.
- Both modes use the same fixture imports and verification command sequence.
- Local mode permits only the root replacement and asserts that
  `github.com/mattsp1290/eino-agent/wasmext/gen` reports the required version
  with no replacement.
- Published mode asserts that its generated `go.mod` contains no replacement.
- Every Go command runs from the temporary module with `GOWORK=off`; immediately
  before the sequence, `go env GOMOD` must equal that module's exact `go.mod`
  path.
- The script uses non-interactive file operations, quotes all paths, and cleans
  only the exact directory returned by `mktemp -d`.
- Failures preserve command output needed to identify module resolution,
  checksum, compilation, or import errors without exposing environment secrets.

## Verification sequence

From the temporary consumer, run in this order:

1. `go mod tidy`
2. `go list -m all`
3. Inspect the selected nested module and fail if it has a replacement or an
   unexpected version.
4. `go mod verify`
5. `go test ./...`
6. `go build ./...`

The fixture source must use regular imports in a non-test Go file so both tidy
and build retain and compile the requested packages. It must not depend on
runtime credentials, databases, network services, or TUI source.

## Tests and observable acceptance

- With the old `v0.0.0` nested requirement, local mode fails while resolving
  `wasmext/gen`; this negative check should be observed during implementation
  before finalizing the fix, without committing the broken state.
- With the corrected real version, `make external-consumer-check` passes from
  a clean checkout and in CI.
- The temporary `go list -m all` output contains the real nested version and no
  nested replacement arrow.
- All six requested import paths compile under both `go test ./...` and
  `go build ./...`.
- Published mode cannot succeed through checkout access because it has no root
  replacement and uses a fresh module cache.
- Published mode fails on a nonstandard effective proxy, checksum, private-path,
  or `GOFLAGS` profile and records only sanitized preflight evidence.

## Dependencies, risks, and exclusions

- The fixture downloads public dependencies and can distinguish module defects
  from transient network failures in its output.
- Do not use a committed `go.work`, vendor tree, alternate proxy, or checksum
  bypass.
- Do not make the fixture import Wasm packages merely to exercise the nested
  edge. Full module graph resolution and the root package build provide the
  requested regression signal without expanding the consumer surface.
- Do not treat local mode as proof that the proposed root tag exists; published
  mode in [04-release-and-response.md](04-release-and-response.md) is required.
