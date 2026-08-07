# Implement A Minimal Extism Catalog Extension Foundation

## Objective

Extend `eino-agent` with the smallest provider-neutral foundation needed for
external developers to build refreshable model catalogs with Extism and connect
those catalogs to separately registered native Eino provider adapters.

Do not implement a concrete remote model-provider integration,
provider-specific authentication, provider-specific model curation, or a
model-refresh extension in this repository. The result should establish
reusable contracts and a constrained Extism host while leaving endpoint,
authentication, protocol, and product UX behavior to external packages and
embedding applications.

This repository is already provider-neutral in intent. It has catalog,
resolver, adapter, and request abstractions, but no production provider. Treat
this work as completing and extending those boundaries, not as removing an
existing provider integration.

## Design Principles

- Keep model execution native. Do not stream model traffic through Wasm.
- Use Extism only as a catalog discovery and transformation control plane.
- Keep credentials, privileged headers, endpoints, TLS policy, and HTTP clients
  under host control.
- Treat the complete provider, model, and variant tuple as model identity.
- Validate complete catalog candidates before publishing one immutable
  generation.
- Preserve the last successful catalog when refresh fails.
- Make every capability opt-in and deny filesystem, process, and network access
  by default.
- Do not serialize internal Go structs, interfaces, Eino messages, callbacks, or
  secrets across the Wasm boundary.
- Preserve existing static catalogs and native adapters.

## Required Work

### 1. Correct Provider Configuration And Credential Boundaries

Update `config`, `model`, and `runtime` so provider connection configuration and
per-request inference options are separate.

- In `StreamingOrchestrator.Start`, select zero or one
  `config.ProviderConfig` matching the chosen provider. Reject duplicates
  before model resolution and durable admission, even when callers construct a
  `config.Snapshot` without using the normal config lifecycle.
- Pass provider connection options only through `model.Runtime` during adapter
  resolution.
- Continue passing agent inference options through `model.Request.Options`.
- Remove `model.Runtime.Env` and `model.Runtime.Auth`. Rename
  `model.Runtime.Options` to `ProviderOptions`, or document and enforce that it
  contains provider options only.
- Replace or deprecate `AuthResolver` with an opaque host-owned credential
  resolver carried by `model.Runtime` and suitable for capture by an immutable
  resolved adapter.
- Require native adapters to invoke the credential resolver immediately before
  every orchestrator-initiated provider request, including stream retries and
  later requests in a tool loop. SDK-internal retry behavior remains owned by
  the adapter, which must refresh credentials when its transport semantics
  require it. Resolving once at run admission is insufficient for expiring
  credentials.
- Give credential resolution the complete model selection and current
  `model.Request.Identity`, but not prompt or tool payloads.
- Core must never inspect, serialize, persist, or export resolved credentials.
  Native adapters are trusted components and are contractually responsible for
  keeping them out of config snapshots, catalog data, durable records, plugin
  inputs or outputs, errors, logs, and observations.
- Preserve providers that require no credentials.

### 2. Make Streaming Resolution-Scoped

`Adapter.Build` returns a selection-bound Eino client, while the current
resolver obtains `model.Streamer` from the shared adapter. Correct that
lifetime mismatch.

- Allow the client returned by `Adapter.Build` to implement `model.Streamer`.
- Prefer the client-level streamer when constructing `model.Resolved`.
- Retain adapter-level streaming only as a documented stateless and
  concurrency-safe compatibility fallback, or remove it if the module's
  compatibility policy permits.
- Document that the raw Eino client fallback receives messages and tools but
  not `model.Request.Identity` or `model.Request.Options`. An adapter that needs
  request-time credentials, request identity, or inference options must return
  a built client that also implements `model.Streamer`.
- Refactor the fake provider and tests so no shared adapter retains mutable
  selection, endpoint, or credential state.
- Prove that concurrent resolutions with different selections and credentials
  cannot cross-contaminate each other.

### 3. Make Model Variants First-Class

`model.Selection` already contains `Variant`, but catalog lookup and durable
identity currently discard it. Make provider, model, and variant the complete
selection key.

- Add variant identity to `model.Descriptor`, `model.Identity`, and
  `context.Identity`.
- Change catalog lookup to accept a complete `model.Selection`.
- Validate uniqueness by provider ID, model ID, and variant.
- Add variant identity to `session.Run`, `session.Message`,
  `session.ContextEpoch`, `session.EventRecord`, `runtime.Event`, model fallback
  payloads, admission payloads, provider requests, resume reconstruction, and
  AG-UI replay wherever provider/model identity is currently retained.
- Emit variant observability metadata under the stable `model.variant` key;
  do not require changes to external observation types that currently expose
  provider and model only.
- Update the static catalog, validation, cloning, equality, storage contract,
  replay, and resume tests.
- Define deterministic default-selection behavior when variants exist.
- Do not synthesize false upstream model IDs to represent variants.

SQLite stores these records as JSON blobs, so add fields compatibly without an
unnecessary schema migration unless investigation reveals a separate indexed
column requirement.

### 4. Add A Refreshable Atomic Catalog Store

Extend the existing `catalog` package rather than introducing a parallel model
registry.

- Define a small source contract that refreshes one authoritative catalog
  contribution, plus `RefreshSource` and `RefreshAll` host operations.
- Provide a compatibility adapter that wraps the existing `catalog.Static` as
  a source without breaking current users.
- Add one immutable store that implements `model.Catalog` and internally
  composes configured sources.
- Use explicit host-configured source ordering.
- Reject conflicting provider metadata and duplicate provider/model/variant
  keys instead of silently overriding entries. Identical provider metadata may
  be shared by multiple sources only if equality is exact after normalization.
- Validate a complete candidate and the resulting merged catalog before one
  atomic swap.
- Keep the previous successful contribution and merged generation when a
  source refresh fails.
- Expose bounded source status: revision, refresh time, stale state, and stable
  error classification. Do not expose raw response bodies.
- Serialize or coalesce concurrent refreshes for the same source.
- Before a source has a successful snapshot, it contributes no entries and its
  status is unavailable rather than stale.
- Treat an authoritative empty snapshot as clearing that source's contribution.
- Reject `unchanged` when the source has no prior successful revision.
- Treat a guest-declared error like any other refresh failure: retain the prior
  contribution and mark the source stale.
- For `RefreshAll`, attempt every configured source, retain prior contributions
  for failures, combine all valid successful candidates, validate the complete
  result, and publish at most one merged generation. A failed source does not
  prevent valid independent sources from advancing.
- Resolve the default in this order: explicit host default, then the first
  valid source suggestion in configured source order, otherwise return a
  default-not-configured error.
- Ensure each catalog method call observes one complete immutable generation.
  If callers need multiple reads from one generation, expose an immutable
  snapshot-view API rather than promising cross-call consistency.
- Ensure a refresh affects only later run admissions. Already admitted runs
  retain their frozen resolved model and metadata.
- Keep persistence outside the initial scope; an embedding host may trigger
  refresh during startup or persist higher-level configuration separately.

### 5. Define A Strict Catalog-Only Extism ABI

Create a dedicated wire package with explicit JSON DTOs and validation. Do not
marshal `config.Snapshot`, `model.Runtime`, Eino schema values, or arbitrary Go
types directly.

The first ABI must define:

- The guest export name `eino_agent_catalog_refresh_v1`.
- The exact ABI identifier `eino-agent.catalog/v1` in every request and
  response.
- A refresh request containing a host-assigned source ID, refresh reason,
  optional previous revision, and bounded non-secret configuration.
- A tagged response whose result is exactly one of authoritative snapshot,
  unchanged, or structured error.
- Provider fields limited to ID and display name.
- Model fields limited to provider ID, model ID, variant, display name, family,
  context/input/output limits, and a bounded set of named boolean
  capabilities.
- An authoritative snapshot containing those provider and model fields,
  source-local revision, and an optional complete default selection.
- No endpoint URLs, auth references, credentials, executable commands,
  privileged headers, transport configuration, or plugin-supplied provenance.
- No direct mapping of `model.Provider.Environment`, `model.Provider.Options`,
  or `model.Descriptor.Options`. ABI v1 has no arbitrary extension metadata.

For ABI v1, reject unknown fields and add explicit bounds for input and output
bytes, provider and model counts, ID lengths, nesting, numeric limits, duplicate
keys, and error strings. Add a new ABI version for incompatible evolution
rather than guessing at unknown security-sensitive fields.

The host must generate provenance from trusted configuration: configured source
identity, canonical load path, digest of canonical host source configuration,
Wasm SHA-256, ABI version, refresh time, and accepted revision. Store this on
the catalog contribution and source status, not on guest descriptors or durable
runs. Never trust guest-provided source identity or provenance.

### 6. Add A Constrained Extism Catalog Source

Add a leaf package such as `catalog/extism` backed by the explicitly pinned
`github.com/extism/go-sdk` v1.7.1. Core `model`, `config`, and `runtime`
packages must not import Extism.

The source must:

- Load only explicitly configured local Wasm files and reject modules larger
  than a host maximum before compilation.
- Resolve the canonical path under an allowed root, read the module exactly
  once, verify SHA-256 over those bytes, and compile with Extism `WasmData` to
  avoid a verification-to-load race.
- Reject URL-loaded modules, escaping symlinks, and modules from disallowed or
  writable locations according to an explicit host policy. Do not assume all
  owner-writable development files are invalid.
- Verify the expected ABI and required export before accepting the source.
- Compile once and create an isolated plugin instance per call, or otherwise
  serialize access. Do not concurrently call one Extism plugin instance.
- Apply nonzero host maximums for execution time, memory pages, variables,
  input and output bytes, and catalog entry counts. Guest configuration may
  tighten but never relax host limits.
- Set a nonzero Extism manifest timeout and use context-aware calls so wazero
  cancellation is enabled. Enforce ABI input/output and catalog limits in host
  code in addition to Extism's memory and variable limits.
- Disable WASI, filesystem mappings, direct network access, and unapproved host
  functions by default.
- Convert traps, timeouts, malformed JSON, invalid exit codes, and oversized
  output into bounded source errors.
- Implement deterministic shutdown: stop accepting refreshes, cancel active
  work, wait for in-flight calls, close instances and compiled state, and avoid
  racing existing catalog readers.

### 7. Add A Narrow Host-Mediated Catalog Fetch Capability

A catalog extension may need host-authorized access to protected remote
metadata. It must not receive credentials or select credential-bearing
destinations. Provide one narrow generic broker rather than direct guest
network or credential access.

- Embedding hosts register named fetch profiles outside Wasm.
- A profile owns an HTTPS origin, permitted methods and relative paths, safe
  static request headers, credential injection, redirect policy, HTTP client,
  timeout, and response-size limit.
- A profile explicitly lists the combinations of host source ID and verified
  module hash that may use it.
- Expose the Extism host import namespace `eino_agent_host` and function
  `catalog_fetch_v1`.
- Its JSON request contains ABI identifier, profile ID, method, relative path,
  query parameters, optional profile-approved headers, and an optional
  base64-encoded bounded body. It never contains an origin or credential.
- Its JSON response contains the ABI identifier and either a bounded status,
  approved headers, and base64-encoded body, or a structured bounded error.
- Define exact path normalization, query, header, method, body, encoding, and
  size rules in the wire package so independently authored guests interoperate.
- The broker validates the request, resolves and injects credentials without
  returning them to the guest, rejects cross-origin redirects, and prevents the
  guest from overriding protected headers.
- Return only the bounded status, approved response headers, and body needed for
  catalog transformation. Strip authentication, cookie, and other sensitive
  response metadata. The broker cannot prevent an upstream service from
  echoing sensitive data in its response body, so trusted fetch profiles must
  target services with an appropriate response contract.
- Default configuration exposes no fetch profiles.
- Do not provide a generic shell, environment, filesystem, socket, or raw
  credential host function.

### 8. Keep Provider Execution Native And Host-Approved

Do not add plugin-defined endpoints or a declarative transport registry in this
milestone.

- Catalog sources may contribute models only under provider IDs explicitly
  allowed for that source by host configuration.
- The embedding host separately registers a trusted native Go `model.Adapter`
  for each executable provider ID.
- Native adapters retain ownership of endpoint and protocol selection, TLS and
  HTTP clients, privileged headers, credentials, request serialization,
  streaming, usage extraction, and error normalization.
- An unknown or unregistered provider remains unavailable even if it appears in
  a catalog.
- A catalog plugin must never be able to pair a host credential with a
  guest-selected endpoint.

## Tests And Acceptance Criteria

Add focused tests proving all of the following:

- Provider connection options and agent inference options reach only their
  intended boundaries.
- Duplicate provider configurations fail before durable admission.
- Sentinel credentials resolve independently for orchestrator retries and tool
  turns; an adapter-owned simulated internal retry verifies the adapter
  contract. Sentinel scans cover every core-owned serialized snapshot, catalog,
  durable record, event, observation, and bounded core error.
- Concurrent model resolutions use isolated client-level streamers.
- Legacy stateless adapter streaming remains compatible if retained.
- Full provider/model/variant selection works through catalog validation,
  resolution, persistence, interruption, resume, and observability identity.
- Catalog refresh is atomic, cancellable, deterministic, and retains the last
  successful generation after failure. Exercise concurrent refresh, read, and
  close behavior under `go test -race`.
- ABI version, export, hash, source identity, count, size, duplicate, and unknown
  field validation fails closed.
- Extism timeout, cancellation, trap, malformed output, oversized output, and
  close behavior are covered.
- One small checked-in fixture Wasm module can publish a catalog snapshot.
- A fixture guest can use a fake fetch profile. The fake upstream must not echo
  the injected sentinel, and the broker must never return injected request
  headers or credentials to guest input, response metadata, or core errors.
- A black-box external test package using only exported APIs can register a
  fake native adapter, resolve, and stream a model contributed by the fixture
  catalog.
- Existing static catalog, fake provider, runtime, store, and example tests
  remain green under `go test ./...` and the repository's standard checks.

## Documentation

Update the architecture and consumer documentation to explain:

- The difference between catalog discovery and native provider execution.
- The variant-aware model identity contract.
- Provider options, inference options, and request-time credential ownership.
- The Extism ABI, trust model, capability defaults, resource bounds, and
  lifecycle.
- How an embedding host registers a fetch profile, catalog source, and native
  adapter using neutral placeholder values.
- Why catalog refresh changes only future admissions.

At minimum, review and update `docs/architecture/providers.md`,
`config-lifecycle.md`, `storage.md`, `observability.md`, `security.md`, and
`docs/consumer-guide.md`. Update package documentation for `context`, `session`,
`catalog`, `providers/fake`, and the minimal example where their public
contracts change. Verify through dependency tests that only the Extism leaf
package imports the SDK.

## Explicit Non-Goals

- No concrete remote model-provider integration.
- No provider-specific authentication.
- No curated model metadata, presets, or provider aliases.
- No local model discovery or background model downloads.
- No slash commands, TUI, package installer, marketplace, or file watcher.
- No arbitrary config mutation from Wasm.
- No arbitrary shell execution or environment interpolation.
- No OAuth UI or credential persistence.
- No Wasm model execution or token streaming.
- No guest-selected credential-bearing destination.

Deliver the smallest coherent implementation satisfying these contracts. Avoid
unrelated runtime, durability, AG-UI, tool, or observability redesigns.
