# Testing, Migration, And Operations

<!-- markdownlint-disable MD013 -->

## Compatibility Matrix

| Existing path | Required result |
| --- | --- |
| Struct-literal `StreamingOrchestrator` | Continues to compile and behaves as today. |
| `NewStreamingOrchestrator` without extension registry | No new dispatch overhead beyond a cheap nil/empty check. |
| `WithContextSource` | Same append order and error propagation. |
| `WithHook` / direct `Hooks` | Same phase timing, broad snapshot mutation, and current ignored after-hook errors. |
| `WithToolMiddleware` / direct `Middleware` | Same before order, reverse after order, resume exactly-once rules, and settlement behavior. |
| `WithEventSink` / direct `Events` | Same queue, backpressure, and call-site error behavior. |
| Existing `tools.Registry.Register` / `Replace` | Same signatures and stale-generation checks. |
| WIT `eino-agent:extensions@0.1.0` | Byte-for-byte contract compatibility; wrappers keep old constructors. |
| Runs with empty/legacy `Components` | Resume with current legacy semantics. |
| Runs created with a strict versioned extension plan | Exact plan fingerprint match before resume. |
| Runs mixing versioned and anonymous legacy seams | Strict match for described entries plus an explicit partial-legacy warning. |

Do not claim backwards compatibility by changing tests to the new semantics.
Add characterization tests before refactoring any ambiguous current behavior.

## Generic Kernel Tests

Cover at minimum:

- validation of component, point, scope, ID, version, and digest;
- atomic mount visibility and full rollback on installer failure/panic;
- reverse-order non-registration cleanup on rollback/close and cleanup error retry;
- deterministic order independent of registration map and goroutine timing;
- global-before-session order at equal registration order;
- onion entry/exit order and short-circuit behavior;
- exactly-once `next` under sequential and concurrent misuse;
- rejection of protected-field mutation in every candidate passed to `next`;
- per-listener containment for notification errors and panics;
- interceptor panic classification and fixed point failure policy;
- snapshot isolation from later mount/deactivate/close;
- idempotent close, close timeout/retry, in-flight drain, and no goroutine leak;
- callback self-deactivation without deadlock;
- rejection of duplicate IDs and stale handles;
- defensive copies and absence of callback/function values in diagnostics;
- point-specific clone/validation behavior for nested mutable payloads.

Use fuzzing for registration sequences, nested invoke chains, scope resolution,
and close/interleave operations. Run the relevant packages under `-race` on
every slice.

## Runtime Sequence Tests

Build table-driven traces whose entries are stable stage names. Assert the
complete order for:

- successful model-only run;
- context source plus legacy hook plus new context interceptor;
- immutable around-stream wrappers;
- successful tool call with legacy and new prepare/execute/result middleware;
- permission deny and ask, mounted guard abstain/deny, and immutable disposition;
- with all guards abstaining, exact legacy policy/approval callback order,
  count, interruption timing, and output;
- before-transform error;
- around-execute short-circuit and timeout;
- executor error plus result transform error;
- cancellation during model stream and tool execution;
- provider retry and tool-follow-up model step;
- pending call resume and running-call interrupted settlement;
- run finish with observer errors/panics;
- crash/restart after each legacy tool-settlement write, followed by idempotent
  reconciliation before final observation.
- construction failure for strict tool plans on a store without
  `ToolSettlementStore`, while the legacy path remains compatible.

Each trace must assert both callback order and durable store state at the
callback boundary. This prevents an apparently correct sequence from moving an
extension before the fact it is supposed to observe.

## Resume And Provenance Tests

- Matching strict plan fingerprint resumes.
- Missing instance, wrong artifact/config hash, reordered registration,
  changed schema/executor/restriction/guard identity, and duplicate component identities fail
  before changing run/tool state.
- A registry change after admission does not affect the active plan.
- A restart that allocates unrelated local generations in another order still
  matches the same semantic descriptor.
- An unrelated global mount added after admission is ignored by
  descriptor-driven resume; a conflict with a persisted selected capability
  still fails.
- Close waits for the active plan and blocks new plans.
- Legacy runs still resume with legacy semantics and emit a diagnostic that
  reproducibility is not guaranteed.
- Partial-legacy runs strictly validate described entries but make no claim
  about anonymous legacy callback identity.
- A pending tool call resumes with the original normalized input and does not
  rerun prepare transforms.
- Around execution and result transforms use the same persisted component plan
  as the original run; if that cannot be guaranteed, resume fails closed.

## Request-Ledger Tests

- Exact canonical message/system/tool-schema/safe-call-config equality with the
  `AuditedModelInput` subset of the recording runtime-to-adapter request for
  every attempt; opaque identity/options/observer fields remain outside the
  equality and privacy claim.
- Stable attempt/step ordinals across retry and tool-follow-up paths.
- A ledger write failure prevents the provider call.
- Every adapter call has a prior durable prepared record. Orphaned `prepared` or
  `dispatch_started` rows after a crash are retained and queryable; they are not
  falsely reported as confirmed provider receipt.
- Context contributions and legacy snapshot mutations appear exactly once.
- Tool-result transformations appear in the next model step.
- Empty model responses and zero-tool requests are represented.
- `model-completed` fires only after the terminal ledger transition commits;
  transition failure produces a diagnostic and failed run, not completion.
- Nonserializable/disallowed message or tool-schema extras fail before adapter
  dispatch, and `ParamsOneOf` is preserved through canonical JSON Schema.
- Oversized records fail before provider execution; no truncation masquerades
  as exact audit.
- Pagination and store contract behavior match SQLite and test stores.
- Credential-like sentinels in `model.Runtime.Auth`, provider options, agent
  options, trace attributes, endpoints, environment, and resolved clients do
  not appear in encoded records or hashes unless a field is explicitly
  classified model-visible.

## Wasm And Security Tests

- Preserve all original WIT wrapper trap, timeout, memory, size, hash, path,
  wrong-world, close, and concurrent-use cases.
- Prove each adapter exposes only its existing DTO fields to the guest.
- A guest cannot register a new core point by string, request session-global
  authority, replace provider identity, or receive a Go callback/client.
- Guest and native callback error strings are bounded and prefixed with
  host-owned component/point identity.
- Mounted deny-only guards execute after final input transform and before the
  unchanged permission/approval loop and tool body, including on resume.
- A result transformer cannot erase disposition, protected permission data, or
  an executor error.

## Performance Budgets

Record baseline benchmarks before runtime integration. Initial acceptance
budgets, measured on the same machine/toolchain, are:

- empty-plan notification/invocation: no heap allocation and no more than 5%
  regression in the model-delta queue benchmark;
- ten contained observers: linear scaling with no goroutine per callback;
- plan snapshot: bounded by registered applicable entries, not total historical
  registrations;
- mount close with no in-flight plan: no blocking scheduler handoff;
- model/tool hot paths: no reflection after registration and no JSON
  serialization solely for native dispatch.

If a budget cannot be met, retain correctness and document measured numbers;
do not add hidden asynchronous queues. High-frequency delta observation must be
explicitly opt-in if it is the source of the regression.

## Failure And Observability Policy

Use stable classified errors:

- `extension.ErrInvalidRegistration`
- `extension.ErrDuplicateRegistration`
- `extension.ErrNextCalledTwice`
- `extension.ErrMountClosed`
- `extension.ErrCloseTimeout`
- `runtime.ErrExtensionPlanMismatch`
- `runtime.ErrExtensionRejected`
- `session.ErrModelRequestTooLarge`

Names may follow existing error conventions, but callers must be able to use
`errors.Is` and inspect bounded structured details. Observer output should
include component name/version, point name, run/session identity when present,
duration, outcome, and panic/error classification. It must not include full
messages, JSON tool input/output, config maps, or credentials by default.

## Rollout

The feature is opt-in through `WithRunPlanProvider` until compatibility and
performance gates pass. No feature flag inside the run loop is needed; a nil
registry selects the legacy fast path.

Recommended rollout sequence:

1. Ship the generic package for external review.
2. Dogfood observation-only points in examples and tests.
3. Move one internal cross-cutting concern, preferably metrics timing, to a new
   around point while keeping its old path available for comparison.
4. Enable scoped capability mounting in an example host.
5. Add the request ledger behind an explicit option and optional
   `ModelRequestStore` capability check. Keep it opt-in for external-store
   compatibility; deployments needing the audit invariant must enable it.
6. Only then evaluate new Wasm worlds.

## Documentation Gates

Before each slice merges, update exported API docs and the affected architecture
page. At final convergence, documentation must include:

- extension point catalog with producer, mode, consumers, failure, durability,
  resume, and Wasm status;
- lifecycle and shutdown examples;
- exact tool and model pipeline diagrams;
- scope/security warning;
- legacy adapter ordering;
- component provenance/resume policy;
- request-ledger retention and privacy guidance;
- one native plugin and one curated Wasm example.
