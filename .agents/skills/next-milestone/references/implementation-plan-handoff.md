# Resolved Milestone Brief and Planning Handoff

Use this reference only after selection. An unresolved upstream request stops before this phase. Material decisions should be resolved; when the user cannot decide, preserve the selection in a decision-blocked brief.

## Brief schema

Keep this brief in conversation; do not create a separate artifact. Populate every field and use `None` only when evidence proves irrelevance.

```markdown
# Resolved milestone brief

## Selection
- Selected milestone:
- Planning status: ready | blocked
- Suggested safe kebab-case plan name:
- Primary embedder and observable outcome:
- Candidate type: embedder journey | runtime foundation
- Selection rationale or accepted non-recommended trade-off:

## Scope
- Bounded journey:
- Out of scope:
- Transport/platform boundary:
- Trusted/untrusted boundary:
- Explicit Pi/DeepSeek parity-claim limit:

## Evidence
- Eino-agent facts and exact paths/symbols/tests:
- Existing plan, tracker, and incoming-demand relationship:
- Local dependency facts, revisions, public contracts, and pins:
- External facts, direct URLs, revisions/dates, publishers, and access dates:
- Inferences:
- Proposals:

## Architecture
- Relevant capability gaps:
- Pi/DeepSeek lessons and intentional differences:
- Current embedder control/data flow:
- Candidate before/after flow:
- Proposed runtime, sibling, and host ownership:
- Proposed APIs/events/configuration and persistence:
- Identity, scope, order, provenance, and capability grants:

## Requirements
- Functional requirements:
- Non-functional requirements:
- Dependencies and execution order:
- Deep-dive component:

## Gates
- Cancellation, backpressure, concurrency, and error behavior:
- Recovery, replay, resume, and ordering behavior:
- Teardown, cleanup, and failure behavior:
- Permission, trust, security, redaction, and credential behavior:
- Input/output/resource bounds and external effects:
- Supply-chain behavior:
- Rollback, removal, or exit seam:

## Decisions and requests
- User decisions:
- Assumptions:
- Unresolved upstream requests: none
- Resolved request/response evidence and verified pin:
- Blocking questions, owner, and exact unblock action:
- Non-blocking questions:

## Verification
- Unit, integration, race, fuzz, Wasm, consumer, and example tests as applicable:
- Credential-free acceptance path:
- Optional configured smoke path:
- Observable acceptance criteria:
- Mapping from every explicit user requirement to acceptance evidence:
```

A brief is `ready` only when no material decision or upstream request remains open. Preserve non-recommended choices and their trade-offs.

## Composed planning handoff

For `$implementation-plan $next-milestone`:

1. Ask the normal application-context questions about active users/external consumers, backward compatibility, and feature flags; do not infer them from repository evidence or unrelated runs.
2. Derive the plan name only after selection and normalize it to one safe kebab-case segment.
3. Create exactly one direct child under `.agents/plans/`.
4. Incorporate this brief into the standard overview, cohesive work files, and execution handoff; do not write a hidden extra brief.
5. Preserve the normal two independent reviews and adversarial review. Reviewers inspect the incorporated plan rather than receiving separate hidden context.
6. A decision-blocked brief produces a blocked plan naming owner/action. An upstream-blocked brief produces no plan.

The resulting implementation plan must include:

- an affected-lane capability-gap table;
- current and target embedder control/data flow;
- current Pi and DeepSeek evidence with direct URLs, revisions, and access dates;
- verified Eino versions and public owner contracts;
- explicit runtime, sibling, and host ownership;
- API/configuration, identity, scope, ordering, side-effect, cancellation, concurrency, recovery, teardown, and lifecycle boundaries;
- relevant security, redaction, permission, credential, path/network/process/Wasm, and supply-chain decisions;
- relationships to planned-but-unimplemented work and incoming demand;
- bounded verification with no broad parity claim;
- an end-to-end public-contract acceptance path for every embedder-journey claim.

## Standalone handoff

Return the ready or blocked brief in conversation and create no plan. A ready brief ends by telling the user to invoke `$implementation-plan` with this already resolved milestone; do not ask them to rerun `$next-milestone`.
