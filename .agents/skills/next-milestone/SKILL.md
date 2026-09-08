---
name: next-milestone
description: Choose the next eino-agent milestone toward embeddability and runtime customization by comparing the live repository, local Eino consumers and dependencies, and current Pi and DeepSeek Harness behavior. Use when asked what eino-agent should build next or when invoked as $implementation-plan $next-milestone; do not use for generic roadmaps or implementation.
---

# Next Eino Agent Milestone

Choose one bounded increment for this repository, then let `$implementation-plan` plan it. Rebuild the frontier from current evidence on every invocation; neither plans nor comparator behavior are a frozen roadmap.

## Ownership and ordering

When composed as `$implementation-plan $next-milestone`:

1. Let `$implementation-plan` resolve the Git root and applicable guidance, but do not name or create a plan yet.
2. Run this skill's outbound-request resume gate.
3. If unblocked, refresh repository, local-ecosystem, and comparator evidence; present candidates and stop for selection unless the conversation already contains a still-valid resolved brief.
4. Resolve the selected milestone's material decisions and ownership.
5. Reuse verified request records read-only or obtain separate authorization for exact new destinations. Stop on every unresolved upstream dependency.
6. Otherwise build the resolved milestone brief in conversation, then resume `$implementation-plan` with its application-context questions, one normal plan, and its required reviews.

`$next-milestone` owns selection and upstream gating. `$implementation-plan` owns plan naming, plan files, reviews, and delivery. Never create an interim plan, second planning format, Beads issue, roadmap, or implementation change. Standalone mode returns a resolved or blocked brief in conversation and creates no plan.

## Workflow

### 1. Gate on outbound requests

Before repository research, read [the upstream request protocol](references/upstream-request-protocol.md) and run its helper's `inspect` command against an explicitly supplied projects root. Gate only on records whose exact `Blocker consumer` is `eino-agent`; incoming requests stored for the `eino-agent` project are demand evidence, not outbound blockers.

Report all open, malformed, incomplete-set, or unverifiably resolved outbound records together and stop before candidate research. A resolved record clears only after its response, committed public code, tests, and consumable pin are independently verified.

### 2. Establish the live frontier

Read [the research playbook](references/research-playbook.md). Resolve applicable guidance and inspect current code, tests, history, plans, tracker state, project records, release state, and unique local `eino-*` Git roots without modifying or cleaning them.

Refresh official Pi and DeepSeek Harness evidence on every unblocked run that reaches fresh candidate selection. Record direct URLs, inspected revisions or releases, and access dates. If mandatory official coverage is unavailable, label volatile claims `unverified-current`, name the missing lanes, and stop before a plan-ready recommendation.

### 3. Compare and present candidates

Read [the frontier rubric](references/frontier-rubric.md). Keep `Repo fact`, `Local dependency fact`, `External fact`, `Inference`, `Proposal`, and `User decision` distinct.

Present two to four coherent candidates with the recommendation first. Each is exactly an `embedder journey` or `runtime foundation`, advances an evidenced TUI/server journey or its nearest required customization foundation, and states TUI, server, and runtime-customization impact or evidenced irrelevance. Put fully planned outcomes outside the selectable list.

Always stop after displaying fresh candidates. This checkpoint applies even when the initial prompt says to choose. Only after options are visible may the user delegate the choice. Preserve a non-recommended selection and its trade-off.

### 4. Resolve the selected milestone

Ask no more than three short questions per interaction, and only when answers materially change public behavior, ownership, persistence, recovery, security, lifecycle, host integration, platform support, or scope. Trace each capability to verified runtime, sibling, or host ownership.

If a required sibling-owned public contract is missing, reuse the already-read request protocol. Complete the ownership map, show every target and exact destination, and obtain explicit pre-write authorization for those destinations. Use `scripts/request_records.py`; do not hand-write or overwrite records. Creation or reuse of an unresolved request marks the milestone `blocked upstream` and ends the run before planning. Never hide the gap with a private import, local duplicate, speculative adapter, or mock claimed as completion.

### 5. Hand off

When no upstream request remains, read [the implementation-plan handoff](references/implementation-plan-handoff.md) and populate its complete brief in conversation. Mark it `ready` only when no material decision or dependency is unresolved.

In standalone mode, return the brief and direct the user to `$implementation-plan` with this already resolved milestone. In composed mode, resume the normal planning workflow; ask its mandatory active-users, backward-compatibility, and feature-flag questions rather than inferring them.

## Invariants

- Require an executable public path plus meaningful tests or observed behavior for `implemented`; plans, docs, names, generated bindings, examples, and mocks do not prove it.
- Use local committed public Eino contracts as implementation authority. Comparator behavior supplies clean-room lessons, not source, package structure, prompts, schemas, or API specifications.
- Preserve host ownership of TUI presentation, routes, authentication, tenant mapping, credentials, deployment, and product policy unless current public contracts prove otherwise.
- Treat identity, scope, ordering, durability, cancellation, backpressure, recovery, teardown, bounds, permissions, redaction, and supply chain as first-class when affected.
- A usable embedder-journey claim must prove host construction through public composition/runtime APIs, durable admission, model/tool execution, ordered observable output, cancellation/error behavior, and bounded cleanup.
- Do not invent a universal shutdown API: inventory the current separate close/drain seams and explicitly note that `StreamingOrchestrator` has no public `Close` unless current code proves otherwise.
- Never inspect or reproduce credentials, environment-file values, private prompt/session content, raw reasoning, or sensitive tool payloads.
- Narrow broad parity requests to one journey; return a scope blocker if the user declines.
