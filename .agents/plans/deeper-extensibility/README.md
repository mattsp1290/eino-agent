# Deeper Runtime Extensibility Plan

<!-- markdownlint-disable MD013 -->

Date: 2026-08-20

Status: implementation plan

## Change Type

Architecture refactor plus new feature, delivered as a sequence of compatible
changes rather than one replacement release.

## Description

Push the functional-options and WIT extensibility work beyond a fixed set of
constructor seams. Add a typed extension registry for runtime interception,
observation, scoped capability contribution, deterministic ordering, and
reversible lifecycle ownership. Keep durable session facts, live transport
events, and in-flight extension points as separate planes with explicit
bridges between them.

The target is a Go-native embeddable runtime, not a port of DeepSeek Harness or
Cordis. Existing `runtime.Hook`, `runtime.ToolMiddleware`,
`runtime.ContextSource`, `runtime.EventSink`, functional options, struct-literal
construction, and published WIT `@0.1.0` contracts remain supported.

## Links to Relevant Documentation

- Existing proposal:
  [`docs/prompts/eino-agent-functional-options-wasm-extensibility.md`](../../../docs/prompts/eino-agent-functional-options-wasm-extensibility.md)
- Current architecture:
  [`docs/architecture/extensibility.md`](../../../docs/architecture/extensibility.md)
- DeepSeek Harness at the reviewed commit:
  [deepseek-harness@141eb6f](https://github.com/deepseek-ai/deepseek-harness/tree/141eb6fef83422698aef7a981029e843e8161534)
- Harness architecture:
  [docs/architecture.md](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/docs/architecture.md)
- Harness event catalog:
  [docs/event-producer-consumer.md](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/docs/event-producer-consumer.md)
- Harness tool pipeline:
  [docs/tool-execution-pipeline.md](https://github.com/deepseek-ai/deepseek-harness/blob/141eb6fef83422698aef7a981029e843e8161534/docs/tool-execution-pipeline.md)

## Affected Areas

- New generic registration and lifecycle primitives in `extension/` and a
  composition coordinator in `composition/`.
- Runtime construction, admission, resume, model streaming, context assembly,
  tool execution, and event publication in `runtime/`.
- Reversible tool registrations in `tools/`.
- Durable extension-plan identity and model-request audit records in `session/`,
  `store/`, and `store/sqlite/`.
- Compatibility adapters and the curated Wasm surface in `wasmext/` and
  `wit/`.
- AG-UI, stream tail, observability, config lifecycle, examples, and
  architecture documentation where they consume runtime events or components.

## Success Criteria

- A host can mount a named/versioned native extension instance, register multiple typed
  observers and interceptors, scope them globally or to one session, and
  unmount them without races or affecting a dispatch already in progress. The
  same artifact can be mounted more than once, and a mount owns arbitrary
  cleanup effects as well as registrations.
- Ordering is deterministic and documented. Around interceptors form an onion;
  immutable observations run after authoritative state is known.
- A run captures one immutable extension/capability plan with a canonical
  fingerprint. New durable runs record artifact, mount-instance, effective
  configuration, ordered registration, and capability identities; strict
  resume rejects a mismatch instead of silently running different code.
- Existing extension interfaces and options keep their current behavior and
  pass compatibility tests. No published WIT `@0.1.0` package is mutated.
- Tool policy remains monotonic: input rewriting cannot skip core permissions
  or mounted deny-only guards, around middleware cannot execute a call twice or
  fabricate success without execution, and result rewriting cannot alter the
  protected outcome disposition.
- Every ledger-enabled runtime-to-adapter request projection has a
  durable, bounded prepared record containing its exact canonical messages,
  system prompt, and tool schemas before dispatch begins. The record lifecycle
  distinguishes prepared, dispatch-started, and terminal states. Credentials,
  endpoints, opaque provider options, clients, callbacks, and observer objects
  are never recorded.
- Failure behavior, cancellation, backpressure, reentrancy, scope routing,
  shutdown, resume, race behavior, and performance budgets have automated
  coverage. `go test -race ./...` and `make check` pass at the end of each
  implementation slice.

## Constraints

- Backwards compatible by default. The new registry is additive and existing
  public fields are not removed or deprecated in this series.
- Keep core runtime model execution native and keep Wasmtime/generated bindings
  isolated to `wasmext`.
- No arbitrary secrets or host objects cross the Wasm boundary or enter
  durable request-audit payloads.
- No plugin marketplace, package installer, directory discovery, config patch
  language, general-purpose dependency-injection container, or automatic hot
  reload in this series.
- Do not use `Agent.Name` as a unique scope key. Until a distinct durable agent
  identity exists, the only runtime scopes are registry-global and exact
  session.
- Do not make transport reliability depend on best-effort extension observers.
  `EventSink` remains the infrastructure path for AG-UI/tails; extension
  notices are contained observations.
- Do not activate the currently inert `TurnSnapshot.SystemPrompt` as an
  incidental refactor. Prompt materialization is an explicit, opt-in behavior
  slice with ordering and compatibility tests.
- Split delivery into the work packages in
  [`04-implementation-roadmap.md`](04-implementation-roadmap.md). Do not land a
  cross-repo architectural rewrite in one PR.

## Decision Summary

1. Keep three planes: durable facts, live transport events, and live typed
   interception/observation.
2. Add a small generic `extension` package for typed points, immutable dispatch
   plans, scopes, mounting, ordering, effects, and teardown. Runtime owns the
   concrete event vocabulary; a separate `composition` package coordinates
   tools and extensions without creating a package cycle.
3. Freeze a plan per run and persist a versioned plan descriptor/fingerprint.
   Legacy seams use compatibility adapters and retain legacy resume semantics.
4. Add narrow runtime points, not a stringly catch-all bus. Security invariants
   remain non-reorderable core code.
5. Add reversible scoped tool/prompt/context registration and monotonic mounted
   guards; leave stores, model resolvers, IDs, and config loading as explicit
   host dependencies.
6. Add a prepared-request ledger as a later slice. It audits the exact
   runtime-to-adapter content/system/schema projection, not credentials,
   transport internals, or an adapter's final wire encoding.
7. Keep Wasm curated and versioned. First complete the already-published Phase
   B wrappers; only design a new WIT version after the native point has proven
   stable.

## Reading Order

1. [`01-current-state-and-reference-research.md`](01-current-state-and-reference-research.md)
2. [`02-target-architecture.md`](02-target-architecture.md)
3. [`03-extension-point-contracts.md`](03-extension-point-contracts.md)
4. [`04-implementation-roadmap.md`](04-implementation-roadmap.md)
5. [`05-testing-migration-and-operations.md`](05-testing-migration-and-operations.md)
6. [`06-agent-handoff.md`](06-agent-handoff.md)
7. [`07-review-disposition.md`](07-review-disposition.md)

The roadmap is the execution order. The contract catalog is authoritative when
an API sketch elsewhere is abbreviated.
