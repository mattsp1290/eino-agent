# Verification, dependency, and documentation

## Goal and prerequisites

Pin the completed source, prove production-path execution and strict identity, and make the supported setup discoverable. This follows the identity and adapter work packages.

## Exact change surface

- `go.mod`, `go.sum`
  - Advance `github.com/mattsp1290/eino-tools` to exact commit `63a3c99272c2359e24484698f2bd62e6fac849b6` using pseudo-version `v0.1.1-0.20260825160656-63a3c99272c2`.
- `README.md`
  - Replace the old dependency pin and state that standard tools mount through `tools/einotools.MountStandard` into `composition.Registry`.
- `docs/dependency-status.md`
  - Record completed catalog commits `cc35e50` and `63a3c99`, the new package, and the exact adopted pin.
- `docs/consumer-guide.md`
  - Replace the warning that standard tools are disconnected with a complete global-mount example.
  - Show an explicit component artifact/config identity, a catalog options value, composition registry, mount lifetime, and `runtime.WithRunPlanProvider`.
  - State that the component config hash covers URL client policy, user surface/I/O policy, tracker writer configuration, and executable-lifetime guarantees.
- `docs/architecture/tools.md`
  - Replace “future integration” language with the catalog-to-composition ownership boundary and locking rules.
- `docs/architecture/runtime.md`
  - Describe the canonical standard adapter as composition-backed, not a low-level registry adapter.
- `docs/architecture/security.md`
  - Replace the deleted `TestFileReadWrapperPreservesEinoToolsContract` evidence with the new composition-backed runtime contract test.
  - Document lexical workspace-relative operation patterns, the 4096-byte host limit, workspace symlink ownership, and default MCP pending-envelope behavior.
- `runtime/orchestrator.go`, `runtime/interrupt.go`
  - Preserve existing snapshot order followed by composition-plan order instead of alphabetically re-sorting tools.
- `runtime/admission.go` and its existing test files
  - Canonicalize non-empty `workspace_root` metadata before the run is durably admitted and persist the resolved path in `session.Run.Config`.
  - Add a symlink-retarget resume test that proves the stored canonical root remains authoritative.
- `runtime/tool_preparation.go` and its existing test files
  - Reject duplicate top-level tool-call JSON keys before `canonicalToolObject` collapses them.
  - Preserve non-null object normalization for valid input and test that duplicates never persist or execute.
- `runtime/orchestrator_test.go` and a proposed focused standard-catalog integration test at an existing runtime test insertion point
  - Exercise `MountStandard` through `runtime.WithRunPlanProvider`, a fake streamer, durable admission, permission evaluation, execution, settlement, and model-facing tooling.
- `tools/einotools/einotools_test.go`, `composition/registry_test.go`
  - Supply the acceptance coverage from the previous work files.

## Verification matrix

Run from the repository root in this order:

1. `go test ./composition ./tools/einotools ./session`
2. `go test ./runtime`
3. `go test -race ./composition ./tools/einotools ./runtime`
4. `go mod tidy -diff`
5. `make fmt`
6. `make check`
7. Cross-compile the adapter package with a temporary output path: `adapter_cross_dir=$(mktemp -d)` then `GOOS=windows GOARCH=amd64 go test -c ./tools/einotools -o "$adapter_cross_dir/einotools.test.exe"`, then remove that exact temporary directory.
8. `rg -n 'RegisterDefaults|TestFileReadWrapperPreservesEinoToolsContract|standard .*not yet composition-connected|legacy tools/einotools' . --glob '*.go' --glob '*.md' --glob '!.agents/**' --glob '!docs/prompts/**'`
9. `git diff --check`

The search gate must return no supported-code or current-documentation references. Historical prompt and plan artifacts are excluded.

## Acceptance criteria

- `go.mod` resolves the completed commit, not an earlier catalog implementation or a local `replace`.
- All catalog tools are acquired through `composition.Registry` in tests and documentation.
- A production-path runtime test proves provider-visible order, final persisted permission pattern, settlement, and leaf output.
- Admission/resume tests prove the canonical workspace root is stable after symlink retargeting, and duplicate-key tests prove leaf rejection semantics survive runtime normalization.
- The source hashes affect final durable identities independently.
- Full quality gates pass with race coverage.
- Current docs contain one supported setup and no disconnected-helper guidance.
- No generated WIT diff remains after `make check`.
- Windows compilation proves the adapter builds; a platform-independent acquisition-error test proves the unsupported-platform error and no-publication behavior.
- The sibling repository stays clean and unchanged.

## Risks and exclusions

- `make check` may expose pre-existing platform/toolchain failures. Confirm causality before changing unrelated packages.
- Do not rewrite historical `.agents` plans or `docs/prompts` records to hide the old state.
- Do not add a local module replacement for the sibling checkout.
