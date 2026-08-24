# Verification And Documentation

## Goal and prerequisite state

Remove stale compatibility claims, prove the simplified architecture, and leave no hidden old path after work packages 1 through 3.

## Exact change surface

- `docs/architecture/extension-points.md`, `docs/architecture/extensibility.md`, `docs/architecture/runtime.md`, `docs/architecture/tools.md`, `docs/consumer-guide.md`
  - Document runtime-sealed plans, current-only descriptors, composition-only extension mounting, unconditional configured system prompts, atomic settlement, derived permission patterns, and checked request boundaries.
  - Remove partial-legacy, schema-v1, direct runtime extension option, compatibility-flag, and reconciliation language.
- `docs/architecture/permissions.md`, `docs/architecture/security.md`, `wit/README.md`, example READMEs
  - Update only references affected by the removed paths.
- `docs/prompts/**` and `.agents/plans/**`
  - Historical prompt and plan artifacts are not production contracts. Do not rewrite old artifacts merely to make `rg` clean; production/documentation assertions must scope searches accordingly.
- `internal/deps/deps_test.go`
  - Preserve package boundaries and add an assertion only if the sealed-plan constructor introduces a new dependency risk.
- Delete obsolete tests and helpers rather than renaming them to “strict.”

## Verification

Run focused gates after each work package:

```text
go test ./extension ./composition ./runtime ./session ./store/sqlite ./model ./providers/fake ./wasmext
go test -race ./extension ./composition ./runtime ./session ./store/sqlite
```

Run repository gates after all edits:

```text
make fmt
make check
git diff --check
go list -deps ./runtime ./composition ./tools ./wasmext
```

Inspect structure:

```text
wc -l runtime/*.go composition/*.go extension/*.go model/*.go
rg -n 'PlanPartialLegacy|PlanLegacy|partial-legacy|hasLegacyExtensions|RequiresToolSettlement|ListUnreconciledToolSettlements|SystemPromptMaterialization|ContextSourceFunc|HookFuncs|ToolMiddlewareFuncs|OrderCompatibility' runtime composition session store model wasmext docs/architecture docs/consumer-guide.md --glob '*.go' --glob '*.md'
rg -n 'cloneReflectValue|cloneMutable' model runtime --glob '*.go'
```

## Acceptance criteria

- All focused and repository gates pass without cached-only assumptions; final `make check` includes race, lint, tidy, formatting, and WIT regeneration checks.
- No modified production Go file reaches 1,000 lines.
- Public APIs retained after the breaking cleanup have accurate GoDoc.
- Architecture and consumer docs describe one executable extension pipeline and one settlement path.
- Public runtime compatibility contracts and interface-returning Wasm loader methods are absent; typed `Register*` adapters and configuration records remain.
- `git status` contains only files required by this plan before commit.

## Risks and exclusions

- `make wit-check` regenerates bindings. Accept no generated diff because WIT is unchanged.
- Do not rewrite historical planning records except this plan's status after implementation.
- Do not bundle unrelated working-tree drift into the commit.
