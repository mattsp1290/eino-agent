# Plan 03 — acceptance response write-ups + issue tracking

Both requests' **Acceptance** sections require a response file under
`~/.agents/projects/eino-agent/responses/` (the dir does not exist yet — create
it). Write each response **after** the corresponding change is implemented and
verified, so the response states what actually shipped (field names, JSON keys,
commit), not what was merely planned.

## Beads issues (do this first, per project convention)

`bd` is the required tracker (no TodoWrite / markdown TODOs). Suggested:

```bash
bd create "einotools: thread url_fetch options through RegisterDefaults" \
  -d "Add URLFetchOptions *urlfetch.Options to einotools.Options; thread into the url_fetch static factory in staticSpecs (tools/einotools/einotools.go). Mirrors ShellOptions precedent. Resolves consumer gap T1. Plan: .agents/plans/consumer-requests-2026-06-29/01-*.md" \
  -p 2 -l impl -t feature --silent

bd create "runtime: add EventModelFallbackEngaged event kind + payload" \
  -d "Add EventModelFallbackEngaged EventKind + ModelFallbackPayload{from_model_id,to_model_id,reason} + NewModelFallbackEvent helper to runtime/types.go. Wire agui/bridge.go Custom emit and upgrade examples/ensemble-adapter mapping from Omit to Durable. Resolves consumer gaps G6/R4. Plan: .agents/plans/consumer-requests-2026-06-29/02-*.md" \
  -p 2 -l impl -t feature --silent
```

(Optionally a third `docs` issue for the response write-ups, or fold them into
the two above.)

## Response file 1 — url_fetch options passthrough

Path: `~/.agents/projects/eino-agent/responses/2026-06-29-einotools-registerdefaults-options-passthrough.md`

Must state (per the request's Acceptance):

- **Decision:** yes — `einotools.Options` now exposes a `url_fetch` options
  passthrough; the `Replace`-after-`RegisterDefaults` dance is no longer needed.
- **Exact field/shape:**
  ```go
  type Options struct {
      // ... existing ...
      URLFetchOptions *urlfetch.Options // optional; nil ⇒ tool default client
  }
  ```
  Nil leaves the url_fetch default (30s-timeout client); non-nil is passed to
  `urlfetch.New(*URLFetchOptions)`, so `URLFetchOptions: &urlfetch.Options{
  HTTPClient: allowlistClient}` injects the host-allowlist client (with its
  `CheckRedirect`) directly.
- **Migration note for the consumer:** drop the capture-Registration +
  `Registry.Replace` workaround; set `URLFetchOptions` on the `RegisterDefaults`
  `Options` instead.
- **Scope note:** a generic per-tool options map / post-register hook was
  considered and deferred — shell already has `ShellOptions`, so a typed field
  per tool is the current convention; revisit a generic mechanism only when a
  third leaf tool needs config.
- **Commit:** record the actual SHA that landed the change (`git rev-parse
  HEAD` after commit).

## Response file 2 — model fallback event kind

Path: `~/.agents/projects/eino-agent/responses/2026-06-29-runtime-fallback-event-kind.md`

Must state (per the request's Acceptance):

- **Decision:** a new `EventKind` was added (Option 1), **and** a `Payload`
  convention is documented (Option 2) — the from→to pair rides in `Payload`
  rather than in new top-level `Event` fields. **Do not state schema migration
  as the reason** (the sqlite store persists `EventRecord` as a JSON blob, so
  field additions need no DDL — this was a false premise in the draft plan). The
  accurate reason: top-level fields would sit dead on every other event kind and
  require threading through the field-by-field `runtimeEvent` mapping plus the
  host-owned `Event→EventRecord` projection; the signal is host-driven, so a
  typed `Payload` keeps the core envelope unchanged. `Event.ModelID` is set to
  the to-model.
- **Exact kind:** `EventModelFallbackEngaged EventKind = "model_fallback_engaged"`.
- **Exact Payload JSON shape** (the contract `j8la.20` targets):
  ```json
  { "from_model_id": "string", "to_model_id": "string",
    "from_provider_id": "string (optional)", "to_provider_id": "string (optional)",
    "reason": "string (optional)" }
  ```
  Backed by `runtime.ModelFallbackPayload` (`json:"from_model_id"`,
  `json:"to_model_id"`, provider fields + `reason` `,omitempty`); build via
  `runtime.NewModelFallbackEvent(from, to, reason)`.
- **Metric-label mapping for the projector** (`j8la.20`): `from_model_id →
  ensemble.model.fallback_engaged_total{model.from}`, `to_model_id →
  {model.to}`. State this explicitly so there is zero ambiguity.
- **ProviderID decision:** the runtime helper is model-centric — it sets
  `Event.ModelID = to` and leaves `Event.ProviderID` (and the optional
  `from_provider_id`/`to_provider_id` payload fields) for the host to populate
  when a fallback crosses providers. The metric is model-labeled, so model-only
  is the default; provider fields are there for forward-compat.
- **Disposition:** durable (`LiveOnly = false`) — persisted and re-emitted on
  replay/reconnect as an AG-UI `Custom("model_fallback_engaged", …)` event.
- **Plumbing delivered:** AG-UI bridge emits `Custom("model_fallback_engaged",
  …)`; the `examples/ensemble-adapter` sketch now maps the event
  `DispositionDurable` (was `DispositionOmit`). The library owns the
  **record→event** reconstruction (`runtimeEvent`); the durable
  **event→record** projection is host-owned (e.g.
  `examples/minimal-server/main.go:409`, whose sink at `:398` already persists
  and replays this kind unchanged). Because `ModelID`+`Payload` are plain copied
  fields, that projection is lossless — but do not describe it as a
  library-guaranteed *bidirectional* round-trip.
- **Out of scope (unchanged):** fallback *selection* stays consumer-side; the
  `ensemble.model.fallback_engaged_total` metric stays consumer-owned.
- **Commit:** record the actual SHA.

## Closeout

After both changes are merged, verified (`make check`), and responses written:

```bash
bd close <id-1> <id-2> -r "shipped; responses written under ~/.agents/projects/eino-agent/responses/"
git pull --rebase && bd dolt push && git push && git status
```

`git status` must show "up to date with origin" — work is not done until pushed
(project session-completion protocol).
</content>
