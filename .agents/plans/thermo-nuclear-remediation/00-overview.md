# Thermo-Nuclear Remediation

Status: Implemented and verified.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "d57d6b12029403b9a32aa8f556ef719e8bd0ab586b8017e5f732e176712c6898",
    "confirmed_at": "2026-08-28T16:36:34Z"
  }
}
```

The user explicitly confirmed that the project has no users and that backward compatibility is dead code. Implementation must change APIs, durable descriptor JSON, WIT contracts, fixtures, and tests directly. It must not add aliases, dual schemas, migrations, feature flags, or compatibility adapters.

## Change type and affected areas

This is a greenfield architectural refactor plus a durability correctness fix.

- `extension` and `runtime`: replace one configurable interceptor mechanism with semantic point primitives.
- `session`, `runtime`, and `composition`: normalize persisted run plans around component ownership.
- `session`, `runtime`, `store/sqlite`, and `store/storetest`: commit tool state and durable transition events atomically.
- `wit`, `wasmext`, examples, and generated bindings: remove misleading turn hooks and inert guest configuration.
- `tools/session`, documentation, and tests: remove remaining no-op surfaces and document the direct contracts.

Tracked Beads work: `eino-agent-5p9`, `eino-agent-8sl`, `eino-agent-kp3`, `eino-agent-cow`, and `eino-agent-ghr`.

## Requested outcome

Deliver a smaller extension architecture in which each point exposes one clear execution semantic, each persisted component identity appears once, every tool lifecycle transition is atomically replayable, and the published Wasm/configuration surface contains only behavior the runtime actually executes.

## Measurable success criteria

- Ordered data transformations and fail-fast hooks do not receive or implement `extension.Next`.
- Only true around-execution points retain synchronous delegation machinery.
- `session.ExtensionPlanDescriptor` contains one top-level record per component, with typed nested capability lists and no repeated artifact identity.
- Tool pending, running, and terminal state transitions commit their corresponding `EventToolCallUpdated` record in the same fenced transaction.
- External event sink failures cannot roll back an already committed tool transition.
- The hook WIT exposes no `before-turn` or `after-turn` operation unless real per-turn runtime boundaries are added in this implementation. This plan chooses deletion.
- `wasmext.ModuleConfig.GuestConfig` and the unused `tools/session.sessionScope` argument are absent.
- `make check`, `make wasm-fixtures`, and `git diff --check` pass.

## Repository findings

- `extension.Interceptor` stores cloning, input validation, output validation, result validation, delegated-output validation, and `requireNext`; five exported constructors select combinations of those fields.
- Runtime uses the same mechanism for a gate, three waterfalls, an immutable hook, and two true around-execution points.
- `session.ExtensionPlanDescriptor` repeats `InstanceID` and `Artifact` in handlers, tools, prompts, guards, and restrictions; validation and fingerprinting repeat a loop and comparator per kind.
- `composition.Registry.AcquireResumePlan` rebuilds the same component set by walking all five descriptor collections.
- Runtime creates, claims, and settles a tool call before separately calling `emitToolCall`; all three call sites discard the returned error.
- SQLite already gives each execution-store method a transaction and fence, and `SettleRun` already demonstrates an atomic state-plus-final-event contract.
- WIT declares `before-turn` and `after-turn`, but runtime calls the former once during fresh snapshot preparation and calls the latter only beside `after-run` at settlement.
- `ModuleConfig.GuestConfig` is byte-counted and then discarded. `sessionScope` accepts a string that is ignored.

## Key decisions

1. **Model semantics explicitly.** Add separate point types for contained notifications, fail-fast hooks, ordered transforms, gates, and required around execution. Share private dispatch helpers where useful, but do not expose a policy-option matrix.
2. **Make transform state homogeneous.** Change tool-result transformation to carry and return `ToolResultTransform`; callbacks mutate only its `Result`, while validation protects tool and call identity.
3. **Persist plans by owner.** Replace five top-level capability collections with `Components []ComponentPlan`; each component stores its artifact once and retains typed nested lists.
4. **Put atomicity in the store contract.** Require each tool create, claim, and settlement call to receive one typed phase request from which state and the durable event are derived. The store validates and writes the complete phase under one fence and transaction.
5. **Delete misleading pre-release surfaces.** Remove WIT turn hooks instead of inventing lifecycle behavior, remove inert guest configuration, and remove ignored parameters.

Rejected alternatives:

- Do not keep the interceptor matrix and add more constructor combinations.
- Do not normalize the descriptor only during hashing while retaining repeated persisted identity.
- Do not rely on callers to compose `ExecutionStore.WithinTx` around separate state and event calls; the mutation API itself must enforce the invariant.
- Do not add compatibility decoding for the old descriptor or WIT world.
- Do not wire new per-turn hooks in this change; retries, tool loops, and resume require a separate lifecycle design and are not needed by a current consumer.

## Target architecture

```text
extension registration
  -> notification | hook | transform | gate | required-around
  -> one deterministic plan ordered by registration identity
  -> semantic dispatcher with no unused policies

mounted component
  -> ComponentPlan{identity, artifact, handlers, tools, prompts, guards, restrictions}
  -> canonical fingerprint
  -> exact component-scoped resume selection

tool transition
  -> build authoritative state + durable event
  -> ExecutionStore atomic fenced commit
  -> best-effort external publication and contained extension notification
```

## Scope and non-goals

In scope:

- All five open review beads.
- Direct breaking changes across this repository.
- Generated WIT bindings and checked-in Wasm fixtures.
- Store contract, SQLite implementation, fakes, tests, examples, and documentation needed to prove the new behavior.

Out of scope:

- Compatibility with existing persisted descriptors or databases.
- A new feature-flag or migration framework.
- New provider, permission, or tool capabilities.
- A full runtime turn-lifecycle design.
- Changes to external repositories.

## Risks, assumptions, and gates

- **Stop/go gate:** semantic point tests must prove ordering, cloning, failure containment, fail-fast behavior, and exactly-once delegation before runtime call sites migrate.
- **Stop/go gate:** descriptor tests must prove fingerprint order independence and strict identity sensitivity before resume code migrates.
- **Stop/go gate:** store contract tests must prove rollback of both state and event when either write fails before runtime starts using the new signatures.
- `make wasm-fixtures` requires the pinned TinyGo and `wasm-tools` toolchain. If either is unavailable, installation is an environment blocker rather than permission to retain stale fixtures.
- The existing `.agents/plans/fix-thermo-review-findings/` directory is preserved as unrelated prior planning history and is not an implementation source of truth.
- There are no unresolved blocking product decisions. Proposed private symbol names may change during implementation if their contracts remain explicit.

## Document map

- [01-semantic-extension-points.md](01-semantic-extension-points.md): replace the interceptor policy matrix and migrate runtime point semantics.
- [02-component-owned-run-plans.md](02-component-owned-run-plans.md): normalize descriptor identity and resume selection.
- [03-atomic-tool-events.md](03-atomic-tool-events.md): make tool state and durable events one fenced store operation.
- [04-wasm-and-dead-surfaces.md](04-wasm-and-dead-surfaces.md): remove false turn semantics and inert public configuration.
- [05-verification-and-docs.md](05-verification-and-docs.md): integration coverage, generated artifacts, and documentation gates.
- [06-execution-handoff.md](06-execution-handoff.md): dependency order, commands, and definition of done.
- [07-review-disposition.md](07-review-disposition.md): synthesized review decisions and the pre-implementation gate.
