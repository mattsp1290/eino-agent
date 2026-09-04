# Delegated Web-Search Extension Ownership

Date: 2026-09-04

Status: Accepted

## Decision

`github.com/mattsp1290/eino-agent-extensions` is the sole owner of the
canonical, reusable model-facing `web_search` schema, its query-semantic
validation, its bounded source-result records, and its backend-neutral adapter
to a host-supplied search callback. The supported target contract pin for this
decision is `github.com/mattsp1290/eino-agent@v0.3.3`.

This repository remains generic. It owns JSON-object validation and canonical
encoding, immutable extension composition, host permission enforcement,
durable tool execution and settlement, output retention, replay, and strict
resume. The embedding host owns the search backend, credentials, egress,
freshness, rate limits, presentation, and backend lifecycle.

`eino-tools` remains a non-owner because web search is outside its coding-tool
catalog. Its earlier recommendation that the downstream runtime own the schema
is superseded by this decision. Comparator implementations are demand evidence
only; they are not authoritative contracts and must not be copied into either
module.

No production web-search package, provider client, credential source, cache,
generated-answer feature, or exported search type is added here. Any host
`Searcher` interface mentioned below is conceptual: `eino-agent-extensions`
owns the final exported Go names and package placement.

## Stable Interoperation Identities

- Tool name and `composition.ToolRegistration.ID`: `web_search`.
- Permission name: `network.web.search`.
- Permission pattern: the constant `web_search`; queries never enter
  permission metadata.

## Responsibility Matrix

| Concern | Owner | Required contract |
| --- | --- | --- |
| Model schema | `eino-agent-extensions` | One object with one bounded query; no batching, backend selection, or credentials. |
| Generic JSON-object validation | `eino-agent` | Before the extension callback, reject invalid JSON, non-object or null values, duplicate top-level keys, and trailing content, then canonicalize the object. Invalid original UTF-8 is not observable by the extension after this stage. |
| Query-semantic validation | `eino-agent-extensions` | Use a raw `tools.InputNormalizer` with strict decoding. Reject unknown fields, NUL, blank-after-trim, and over-limit queries as `tools.ErrMalformedInput`; emit canonical JSON for the one allowed field. |
| Source records | `eino-agent-extensions` | Return bounded title, absolute HTTP(S) URL, and snippet strings plus its result envelope; no provider payloads or generated answer. |
| Tool identity | `eino-agent-extensions` | Export value `web_search`; use it for `composition.ToolRegistration.ID`. |
| Permission identity | `eino-agent-extensions` | Export `network.web.search`; use constant pattern `web_search` so policy metadata does not contain the query. |
| Input/output bounds | `eino-agent-extensions` | Own query bytes, result count, per-field bytes, encoded-result bytes, and timeout; bound deterministically before returning. |
| Runtime retention | extension configures; `eino-agent` enforces | Compute the worst-case encoded result overflow-safely after JSON escaping, then set `MaxInlineBytes` to at least twice that size because typed execution retains text and structured copies. Use `StoreExternal: false` and `Redact: false`; valid output must not truncate. |
| Cancellation/deadline | `eino-agent`, then extension, then host | Runtime supplies context; extension adds a finite timeout without detaching the parent and forwards it unchanged; backend stops and releases work. |
| Retry safety | extension declares; `eino-agent` recovers | Use `RetrySafe: false`; a pending call may be claimed once, while a running call is interrupted rather than repeated. |
| Durable settlement | `eino-agent` | Own pending, claim, and terminal transitions, output encoding, result message and part, events, and cancellation-free settlement. |
| Strict resume | `eino-agent` | Obtain the fingerprinted `runtime.RunPlan.Descriptor`, release the plan, call `session.VerifyExtensionPlanForSession`, then pass the returned `session.SealedExtensionPlan` in `runtime.ResumePlanRequest` to `AcquireResumePlan`. Require an exact persisted/live fingerprint before durable mutation and release every returned plan. Use `session.SealExtensionPlanForSession` only for a newly reconstructed fingerprintless descriptor. |
| Configuration identity | extension content; `eino-agent` enforcement | Extension `ConfigHash` includes schema, result, and permission versions, limits, timeout, and non-secret host callback behavior identity. Exclude credentials and client pointers. The target fingerprints the supplied value but does not derive it. |
| Component artifact identity | host supplies and rotates; `eino-agent` enforces | Host supplies stable artifact name, package/release version, and behavior-bearing hash, and rotates version or hash for every executor/code behavior change. The target validates and fingerprints these values; it cannot prove that a supplied hash matches code. |
| Backend failures | extension classifies; `eino-agent` settles | Return stable bounded classes. Never include or wrap provider bodies, credentials, endpoints, query text, or raw backend errors in visible output. |
| Credentials/network | host | Construct the backend, resolve secrets, authorize egress, and keep secrets out of schemas, hashes, metadata, logs, durable input, and errors. |
| Freshness/backend/rate limits | host | Apply deployment policy behind the callback; do not expose backend selection to model input. |
| Backend lifecycle | host | Own startup, pooling, shutdown, and cleanup. Mount close quiesces callbacks but does not close a host-owned backend. |
| Presentation/citations | host | Render title, URL, and snippet sources. The extension returns sources only. |

## Public Target Integration Map

The delegated extension composes only through public target APIs:

- `tools.Definition`, raw `tools.InputNormalizer`,
  `tools.TypedPermissionPattern`, and `tools.TypedExecutor` define and adapt the
  JSON-native tool. Ordinary `tools.TypedNormalizer` is appropriate only when
  the request type itself provides equivalently strict unmarshalling.
- `composition.Registry.Mount`, `composition.ToolRegistration`,
  `AcquireRunPlan`, and `AcquireResumePlan` publish and freeze it.
- `extension.Component`, `extension.Artifact`, `extension.SourceNative`, and
  global or exact-session scopes establish ownership and identity.
- `runtime.WithRunPlanProvider`, `runtime.RetentionPolicy`,
  `runtime.ToolCall`, `runtime.ToolContext`, and the executor's
  `context.Context` provide execution and bounds.
- `runtime.RunPlan.Descriptor`, `runtime.RunPlan.Release`,
  `runtime.ResumePlanRequest`, `runtime.ErrExtensionPlanMismatch`,
  `session.ExtensionPlanDescriptor`, `session.SealedExtensionPlan`, and
  `session.VerifyExtensionPlanForSession` provide the complete strict-resume
  path. `session.SealExtensionPlanForSession` is only the constructor for a
  newly reconstructed fingerprintless descriptor.
- `permissions.Policy` or `permissions.StaticPolicy` applies host policy.
- `composition.Mount.Deactivate`, then bounded `Close`, removes the capability
  from future plans and quiesces retained callbacks.

The executable, deliberately non-normative proof is
[`testdata/external-consumer/delegated_web_search_fixture_test.go`](../../testdata/external-consumer/delegated_web_search_fixture_test.go).
It uses a fresh unrelated module, deterministic fake model and search backends,
SQLite settlement, strict resume, and no network or credentials. General tool
composition and settlement pipelines are documented in
[`tools.md`](tools.md) and [`extension-points.md`](extension-points.md).

## Lifecycle And Failure Invariants

- Normalized queries are durable model input; callers must not put secrets in
  them.
- Permission checks occur after normalization and before backend invocation.
  Denial or approval-required settlement never invokes the backend.
- Raw backend diagnostics remain host-internal. Only a stable bounded class can
  reach the durable call, output, result records, events, observability, or a
  public run error.
- Cancellation and deadline sentinel identity is preserved only when the
  actual execution context owns it.
- Escape-heavy maximum output must fit both runtime copies, remain structured
  JSON, report `InlineSize == 2 * len(encodedResult)`, and not truncate.
- Deactivation affects future plans. Already acquired leases remain valid
  until release, and close waits for active callbacks.
- Schema, permission, result shape, bounds, timeout, callback identity, scope,
  order, configuration hash, and honest host-supplied artifact identity affect
  the plan fingerprint. Reusing an artifact hash across behavior changes
  violates the host contract and cannot be detected by the target.

## Compatibility And Release

There are no active users or external consumers requiring compatibility. This
decision replaces the conflicting ownership statement directly: there is no
feature flag, compatibility alias, dual contract, stored-data migration, or
transition period.

The local proof must pass through `make external-consumer-check` and the full
repository must pass `make check`. After `v0.3.3` is published, the same proof
must select that tag through the public Go proxy without a root `replace`, Go
workspace, vendor tree, checkout access, or live network call by the fixture.
The release tag is immutable; a correction requires a later patch release.
