# Sealed Plan And Single Extension Pipeline

## Goal and prerequisites

Make one runtime-owned plan the only executable extension state. This work precedes tool and request-boundary simplification because those packages must stop branching on compatibility modes first.

## Repository evidence

- `runtime/extension_plan.go` defines public `RunPlan` behavior and descriptor fields.
- `composition/registry.go:AcquireRunPlan` and `AcquireResumePlan` construct those fields directly.
- `runtime/orchestrator.go`, `runtime/interrupt.go`, and `runtime/admission.go` execute direct extension fields alongside the frozen plan.
- `session/extensions.go` fingerprints legacy modes and schema versions.

## Exact change surface

- `runtime/extension_plan.go`
  - Replace public-field `RunPlan` construction with proposed `RunPlanSpec` and `NewRunPlan`.
  - Make executable plan fields and descriptor private. Expose only behavior-specific package accessors and `Descriptor() session.ExtensionPlanDescriptor`, which returns a defensive clone for composition persistence and runtime resume comparison.
  - Define `RunPlanSpec` as per-capability records that contain identity and concrete behavior together. Do not accept an arbitrary `ToolRegistry` or parallel identity arrays.
  - Have `NewRunPlan` build a private frozen tool registry from concrete definitions and derive tool/restriction/prompt/guard descriptor entries from those same records.
  - Derive callback entries from `extension.Plan.Diagnostics` instead of accepting caller-authored handler entries.
  - Validate artifact, scope, capability, ordering, and kind fields before fingerprinting.
  - Remove `RequiresToolSettlement`, compatibility mode checks, ordering-version checks, and `hasLegacyExtensions`.
- `composition/registry.go`
  - Build a `runtime.RunPlanSpec` from selected mounted capabilities.
  - Stop building or fingerprinting `session.ExtensionPlanDescriptor` locally.
  - On constructor failure, release the dispatch plan before returning.
  - Acquire resume plans using only current-schema persisted identities and compare the runtime-sealed fingerprint.
- `session/extensions.go`
  - Remove `PlanMode`, `PlanStrict`, `PlanPartialLegacy`, `PlanLegacy`, and `ExtensionPlanDescriptor.Mode`.
  - Remove schema-v1 ordering normalization. Require the current schema for validation and acquisition.
- `runtime/orchestrator.go`, `runtime/interrupt.go`, `runtime/admission.go`, `runtime/options.go`, `runtime/types.go`
  - Remove direct `Tools`, `Context`, `Hooks`, and `Middleware` execution.
  - Remove `WithToolRegistry`, `WithContextSource`, `WithHook`, and `WithToolMiddleware`.
  - Remove `Admitter.Hooks` and run lifecycle hooks only through typed extension points.
  - Remove `SystemPromptMaterialization` and `WithSystemPromptMaterialization`; always include a nonempty configured system prompt.
  - Keep `WithRunPlanProvider`; nil provider yields a runtime-sealed empty plan.
  - Delete compatibility-only public contracts `ContextSource`, `ContextSourceFunc`, `Hook`, `HookFuncs`, `ToolMiddleware`, and `ToolMiddlewareFuncs`, plus their legacy orchestration helpers.
- `wasmext/wrappers.go`, `wasmext/points.go`
  - Keep concrete loaded adapters and registration helpers.
  - Make loader methods return concrete loaded types, make callback methods private where only typed `Register*` adapters call them, and remove interface-returning compatibility methods/comments.
  - Remove or rename `OrderCompatibility`; current ordering has no compatibility mode.
- Tests in `runtime`, `composition`, `session`, `wasmext`, and examples.

## Intended behavior and invariants

- `RunPlanProvider` supplies identity-bound behavior records, not a trusted descriptor or independently mutable registry.
- `NewRunPlan` is the only production constructor that computes a descriptor fingerprint.
- A nonnil tool registry requires at least one tool identity; each mounted prompt and guard carries exactly one matching identity; handler identities exactly match dispatch diagnostics.
- Resume seals a new plan from live evidence and compares it to the persisted descriptor before mutating run or tool state.
- An empty plan has a current-schema fingerprint and no special mode.
- Plan release closes all acquired leases exactly once on constructor failure, acquisition mismatch, execution completion, and panic recovery.
- `Start` and `Resume` install a scoped release guard immediately after acquisition and disarm it only when the run goroutine takes ownership; panics are rethrown after release.
- Configured system prompts are always inserted before durable history messages at the provider boundary.

## Tests and acceptance criteria

- Add constructor tests that reject invalid tools, prompts, guards, restrictions, or dispatch diagnostics; prove a registry whose resolution later changes cannot alter a sealed plan.
- Add a regression test proving a provider cannot attach executable tools to an empty descriptor because providers no longer supply descriptors.
- Keep fresh/resume fingerprint mismatch tests using current schema only.
- Delete schema-v1, partial-legacy, legacy-mode, and matching-live-field tests.
- Update orchestrator tests to mount behavior through `composition.Registry` or focused sealed-plan test helpers.
- Test that a configured system prompt reaches both `model.Streamer` and Eino client paths without an opt-in flag.
- Add table-driven fresh/resume lease tests for constructor failure, persisted mismatch, provider error, execution error, and panic; deactivate the mount and prove close succeeds with exactly one release/cleanup.
- `rg -n 'PlanPartialLegacy|PlanLegacy|hasLegacyExtensions|RequiresToolSettlement|WithToolRegistry|WithContextSource|WithHook|WithToolMiddleware|SystemPromptMaterialization|ContextSourceFunc|HookFuncs|ToolMiddlewareFuncs|OrderCompatibility' --glob '*.go' --glob '!config/**'` returns no production compatibility matches. `config.Hooks` is a configuration record and is not the removed runtime hook pipeline.

## Dependencies, risks, and exclusions

- Do not move `composition` into `runtime`; preserve the current dependency direction.
- Do not change typed extension contract IDs or callback ordering.
- Do not remove the infrastructure `EventSink`, model resolver, permissions policy, store, transactor, or observer options.
- Update test helpers instead of adding compatibility constructors.
