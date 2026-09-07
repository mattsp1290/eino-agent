# Repository and Current-Research Playbook

Use this reference after the outbound-request gate and before candidate generation. Its output is an evidence set, not a roadmap.

## Repository survey

Resolve the `eino-agent` Git root and inspect evidence proportional to the frontier:

1. Read applicable `AGENTS.md`, `CLAUDE.md`, contributor guidance, root documentation, architecture notes, and consumer guidance.
2. Record worktree state, revision, recent commit metadata, tags/releases, relevant branches, and path inventories without cleaning or modifying anything.
3. Inspect `.agents/plans/`, `.agents/skills/`, Beads state, and project requests/responses. Treat plan status and tracker entries as claims, not executable facts.
4. Trace public constructors, interfaces, configuration lifecycle, orchestration, session/store contracts, composition and extension points, transport adapters, model/tool boundaries, examples, tests, CI, release pins, and external-consumer fixtures.
5. Run or identify proportional quality commands and separate existing unrelated failures.

Never read `.env` values, auth exports, credential stores, private keys, shell history, raw session transcripts, prompts, reasoning, or sensitive tool payloads. Learn configuration from public types, docs, and variable names.

Classify each capability as exactly one of:

- `implemented`: a public executable path plus a meaningful test or observed behavior proves it;
- `partial`: real code exists but an end-to-end host, lifecycle, recovery, or public seam is missing;
- `planned`: a plan claims it but executable evidence does not;
- `absent`: grounded search finds no relevant path or contract;
- `blocked`: a named request, decision, contract, or external gate prevents safe planning;
- `unknown`: evidence is insufficient.

A README claim, plan status, type name, generated binding, registration-only mock, or example sketch is insufficient for `implemented`.

## Incoming consumer demand

Inspect records under the supplied projects root's `eino-agent/requests/` as demand directed at this repository. Parse structured status when present, match same-named responses, and verify completion against committed code and consumable versions.

- Open incoming demand influences the matrix and ranking but does not block unrelated selection or override the user.
- Legacy records remain evidence; do not mutate or invent metadata.
- Beads issues and sibling plans inform the frontier but do not prove behavior or force selection.

This is separate from outbound records stored under other project directories whose exact `Blocker consumer` is `eino-agent`.

## Local Eino ecosystem

Enumerate every unique resolved `${HOME}/git/eino-*` Git root, deduplicating aliases and worktrees. Discover the roster; likely relevant projects include `eino-agent-extensions`, `eino-agui`, `eino-obs`, `eino-providers`, `eino-tools`, and `eino-tui`.

For each relevant sibling:

1. Read its applicable guidance.
2. Record revision and worktree status without mutation.
3. Identify module/repository identity, public packages, consumer docs, requests/responses, and tag/release state.
4. Deep-inspect only contracts connected to affected lanes.
5. Treat uncommitted code, plans, and examples as provisional.
6. Record a public version or commit that a future `eino-agent` change can consume; never use a local `replace` as a planning shortcut.

Start with this ownership hypothesis and correct it from current code:

| Concern | Expected owner |
| --- | --- |
| Durable admission, orchestration, session/store interfaces, composition, permissions, extension points, resume/replay, and embedding primitives | `eino-agent` |
| Reusable native or Wasm extensions and extension-specific schemas/adapters | `eino-agent-extensions` |
| Coding leaf tools and filesystem/process safety | `eino-tools` |
| Provider-specific model construction and continuation behavior | `eino-providers` |
| AG-UI conversion, emission, stream tapping, and client-tool binding | `eino-agui` |
| Agent/model/tool observability and exporters | `eino-obs` |
| Terminal UI, route topology, auth, tenant mapping, credential storage, deployment, and product policy | consuming host |

## Mandatory current external research

Every unblocked fresh selection must browse official current sources. Resolve each default branch and revision before using seed paths; search for moved files, implementation-status notes, releases, and migrations.

| Lane | Required coverage | Seed authorities |
| --- | --- | --- |
| Pi embeddable harness | Current harness/lane/session/storage APIs, operation lifecycle, hooks/events, runtime configuration, tools, cancellation, recovery, snapshot/watch semantics, tests, and implementation status | Official `https://github.com/earendil-works/pi`, current harness docs/types/tests/releases |
| Pi application customization | Current coding-agent SDK, extensions, tools/resources/skills, host UI seams, and reusable-harness versus product-only ownership | Official Pi coding-agent SDK and extension docs/examples |
| DeepSeek composition/runtime | Current Cordis/plugin composition, service/event/effect lifecycle, Agent handle, session log, scoped registration, profiles/presets/bundles, tools, interaction, cleanup, and extension guidance | Official `https://github.com/deepseek-ai/deepseek-harness`, architecture and core subsystem/package docs/tests |
| DeepSeek embedding adapters | Current headless, SDK/JSON-RPC, ACP, and web ownership; create/resume/prompt/cancel/update/shutdown semantics and transport-specific behavior | Official DeepSeek Harness bundle, SDK, ACP, and web docs/tests |
| Candidate dependencies | Any Eino module, Go package, protocol, database, Wasm contract, executable, API, or service named in an option | Current official source, docs, releases, and security guidance |

For each material source record publisher/repository, title, direct URL, revision or release/update date, and access date. If a mandatory lane is unavailable, complete local research, label affected claims `unverified-current`, list the missing lanes, and stop before candidate recommendation.

## Evidence hierarchy and clean-room boundary

Use implementation authority in this order:

1. Current `eino-agent` code, tests, public contracts, and accepted decisions.
2. Committed public contracts and verified pins in local Eino siblings and consumers.
3. Official Eino, Go, library, protocol, storage, and security sources.
4. Official Pi and DeepSeek Harness behavior and architecture.

Label every material statement `Repo fact`, `Local dependency fact`, `External fact`, `Inference`, `Proposal`, or `User decision`. Never copy comparator code, prompts, internal schemas, private endpoints, package layouts, or undocumented behavior. Check licenses before adapting code; compare observable outcomes and generic flows, then design independently against Go and public Eino contracts.
