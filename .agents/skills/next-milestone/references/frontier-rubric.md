# Eino Agent Frontier Rubric

Use this reference after repository, local-dependency, and official external evidence is current.

## Capability matrix

Assess only lanes relevant to the next one or two milestones. Record state, repository evidence, local owner/consumer evidence, Pi evidence, DeepSeek Harness evidence, gap, and relevance.

| Lane | Questions |
| --- | --- |
| Embedder construction | Can a TUI/server construct the runtime with explicit dependencies, validate readiness, choose ownership, and shut down without internal imports or globals? |
| Agent/session control | Which long-lived and per-run handles cover create/start/status/interrupt/resume, queued input, follow-up/steering, and session lifecycle? |
| Durable state and recovery | What is admitted atomically, replayable, resumable, branchable, compactable, and inspectable after crash or disconnect? |
| Observable state/events | Can a host obtain a consistent snapshot and gap-aware bounded subscription, distinguish durable/live deltas, and recover after overflow? |
| Composition/customization | Which tools, prompts, hooks, guards, policies, models, stores, sinks, and context sources are replaceable/scoped, and how are order, identity, fingerprints, and teardown enforced? |
| Models/providers | Can hosts enumerate, resolve, configure, and change providers/models at safe boundaries without credential leakage or in-flight mutation? |
| Tools/human interaction | Are execution, permissions, approvals, questions, progress, cancellation, timeouts, bounds, settlement, and resume at the correct boundary? |
| Context/prompts/skills | How do hosts or extensions contribute instructions/resources with ordering, bounds, provenance, and next-turn semantics? |
| Concurrency/background work | What ownership, queued work, parallel tools, lanes/subagents, jobs, leases, and isolation contracts exist or are intentionally absent? |
| Transport/presentation neutrality | Can in-process TUI, HTTP/SSE, JSON-RPC/ACP, and future hosts share core behavior without runtime-owned UI/routes or adapter-invented state? |
| Security/operations | Redaction, trust, sandbox/capability boundaries, limits, observability, health/readiness, shutdown, and supply chain. |
| Delivery/consumer proof | Public module graph, examples, external-consumer fixtures, conformance/race/fuzz/platform gates, releases, and upgrade evidence. |

Absence is not automatically a defect; record intentional host ownership and deferral.

## Current and target flows

Build three grounded views:

1. `Current flow`: host construction/config → orchestrator/registry → durable admission → frozen run plan/model/tools → execution/settlement → durable replay plus live output → host projection → interrupt/resume → host-orchestrated cleanup.
2. `Reference lessons`: relevant Pi harness/application behavior and DeepSeek composition/adapter behavior, preserving their differences.
3. `Candidate delta`: smallest before/after flow per option, labeling existing, proposed, mocked, host-owned, sibling-owned, and blocked seams.

For affected flows, verify durable identity before visible execution; explicit snapshot/subscription gap, duplication, and backpressure contracts; cancellation through model/tool/extension/transport work with terminal settlement; future-boundary configuration; bounded deactivation/drain/close ordering; exact resume identity; and default exclusion of secrets, provider-private state, raw reasoning, and unsafe payloads.

Do not collapse cleanup into a nonexistent universal API. Inventory current owners such as per-run `Handle.Interrupt`, `composition.Mount.Deactivate`/`Close`, `wasmext.Loader.Close`, `stream.Tail.Close`, concrete store close methods, and host ordering. Record that `StreamingOrchestrator` has no public `Close` unless current code proves otherwise. A lifecycle candidate must say whether it preserves host orchestration or proposes a bounded public boundary with drain/failure criteria.

## Candidate contract

Offer two to four candidates, recommendation first. Each candidate type is exactly:

- `embedder journey`: a bounded TUI/server/automation-consumable path with observable end-to-end behavior; or
- `runtime foundation`: the nearest verified public runtime/customization prerequisite for such a journey.

Every candidate includes:

- concise name and type;
- primary embedder and observable outcome;
- explicit TUI, server, and runtime-customization impact, or evidence of irrelevance;
- exact current repository evidence or proposed insertion point;
- relevant Pi and DeepSeek lessons and intentional differences;
- local owner/API/version evidence and upstream-request likelihood;
- readiness: `Ready`, `Foundation first`, `Decision needed`, `Upstream request likely`, `Blocked upstream`, `Discovery`, or `Planned`;
- before/after flow delta;
- public API, persistence, lifecycle, security, and host-ownership implications;
- concrete replaceable runtime/composition seam advanced, or proof that a prerequisite embedding foundation comes first;
- largest dependency or material decision;
- why it is one coherent plan;
- explicit scope and parity-claim boundary;
- credential-free verification approach;
- one-sentence rank rationale.

Show fully planned outcomes outside the selectable list. Exclude cosmetic, duplicate, request-only, plan-maintenance, broad-refactor, comparator-port, and already fully planned options.

A usable `embedder journey` must traverse host construction through public runtime/composition APIs, durable admission, model/tool execution, ordered observable output, cancellation and error behavior, and bounded cleanup. A scripted provider is valid only through the real public path and proves no live-provider behavior.

## Ranking

Rank qualitatively:

1. Nearest complete repeatable embedder journey, or removal of its only hard blocker.
2. Correct public Eino ownership and less special-case work for both TUI and server consumers.
3. Real construction → admission → execution → observation → recovery/cleanup path.
4. Credential-free verification without uncontrolled effects.
5. Preserved durability, cancellation, scope, bounds, redaction, determinism, and resume safety.
6. Reusable contract without premature framework construction.
7. No unsupported parity claim or literal port.

Do not use unexplained numeric scores. Preserve a user's different selection and state its trade-off.

## Post-selection architecture pass

Resolve or explicitly defer:

1. Primary consumer and bounded journey.
2. Functional and non-functional success requirements.
3. In-process/transport boundary and supported platforms.
4. Current/proposed ownership and public contracts.
5. Current and target construction, run, observation, and cleanup flow.
6. Durable identities, scope, order, configuration, capability grants, and persistence.
7. One deep-dive component.
8. Cancellation, backpressure, recovery, concurrency, teardown, security/redaction, and supply-chain risks.
9. Tests and measurements proving only the bounded outcome.

Ask at most three short questions per interaction and only when the answer changes the design. Do not invent performance or capacity numbers. An unanswered material decision keeps the selection but makes its brief `blocked` with owner and exact unblock action.
