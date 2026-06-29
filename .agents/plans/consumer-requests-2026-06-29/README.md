# Plan: local-symphony (ensemble) consumer requests — 2026-06-29

Two host-integration requests from the `ensemble` / local-symphony migration
(epic `local-symphony-j8la`), both against pinned commit
`a2ee5bbcda43d1fcb599cf94834c0fcf8dc48a6c`. Source requests live in
`~/.agents/projects/eino-agent/requests/`.

| # | Request | Plan file | Risk | Blast radius |
|---|---------|-----------|------|--------------|
| 1 | Thread per-tool options through `einotools.RegisterDefaults` (url_fetch `HTTPClient`) | [01-einotools-urlfetch-options-passthrough.md](01-einotools-urlfetch-options-passthrough.md) | Low | 1 package |
| 2 | Add a model-fallback event kind to `runtime.Event` | [02-runtime-model-fallback-event.md](02-runtime-model-fallback-event.md) | Low–Med | runtime + agui + sketch |
| — | Write the two acceptance response files | [03-response-writeups.md](03-response-writeups.md) | Trivial | docs only |

## Recommended decisions (the short version)

- **Request 1 — yes, add a typed passthrough.** Add `URLFetchOptions
  *urlfetch.Options` to `einotools.Options`, mirroring the existing
  `ShellOptions *shell.Options` precedent. Thread it into the `url_fetch`
  static factory. This removes the `Replace`-after-`RegisterDefaults` dance.

- **Request 2 — add the EventKind *and* document a Payload convention
  (hybrid).** Add `EventModelFallbackEngaged`. Carry the from→to pair in a
  documented `Payload` JSON shape (`ModelFallbackPayload`) rather than adding
  two new top-level fields to the flat `Event`/`EventRecord` structs. The reason
  is **not** schema cost — `store/sqlite/store.go:319` persists the entire
  `EventRecord` as a JSON blob, so adding fields needs no DDL migration. The
  reason is **envelope leanness + wiring surface**: top-level fields would sit
  dead on every other event kind and would have to be threaded through the
  field-by-field `runtimeEvent` mapping (`agui/replay.go:127`) and the
  host-owned `Event→EventRecord` projection. The signal is host-driven, so a
  typed `Payload` keeps the core envelope unchanged while still giving adapters
  one shared encoding. Set `Event.ModelID = ToModelID` so existing
  `ModelID`-reading observability stays meaningful; `ProviderID` is a documented
  decision (see plan 02 — model-centric helper, optional provider payload
  fields for the cross-provider case).

Both requests explicitly note they are **Medium / not hard blockers** (the
consumer has a workaround today), so neither needs to break API compatibility —
both designs below are purely additive.

## Grounding (verified against source, not assumed)

- `einotools.Options` — `tools/einotools/einotools.go:42`. Already carries
  `ShellOptions *shell.Options` (the pattern to copy).
- `url_fetch` registration — `tools/einotools/einotools.go:122`, currently
  `urlfetch.New()` with no options, inside `staticSpecs`.
- `urlfetch.New(opts ...Options)` is **variadic** and `urlfetch.Options` has
  exactly one field, `HTTPClient *http.Client`
  (`~/git/eino-tools/urlfetch/options.go:14`, vendored copy under `pkg/mod`).
- `runtime.EventKind` — `runtime/types.go:186`, 6 kinds. `runtime.Event` —
  `runtime/types.go:206`, flat envelope with single `ModelID string` plus a
  `Payload json.RawMessage` slot.
- Durable mirror `session.EventRecord` — `session/types.go:251`, has `ModelID
  string` + `Payload json.RawMessage` (no per-model-pair fields).
- Event consumers that switch on `Kind`: `agui/bridge.go:67`,
  `runtime/observability.go:29`, `agui/replay.go` (round-trips via
  `runtimeEvent`, `:127`). The `default`-less switches mean an unknown kind is
  silently ignored, not an error — additive is safe.
- The future adapter already names this event:
  `examples/ensemble-adapter/sketch.go:30` defines
  `EventModelFallbackEngaged` and currently maps it to `DispositionOmit`
  (dropped). This plan upgrades that mapping.

## Quality gates (run for every change below)

From repo root (`/Users/punk1290/git/eino-agent`), per `Makefile`:

```bash
make fmt          # gofmt + goimports -local github.com/mattsp1290/eino-agent
make vet
make test         # go test ./...
make lint         # golangci-lint
```

`make check` runs the full set (`fmt-check vet test race mod-tidy-check lint`).
Do not declare a request done on compile-success alone — run `make test` and,
for request 1, the new HTTPClient-injection test that actually exercises the
passthrough.

## Suggested execution order

1. Request 1 (self-contained, single package) → review → verify → commit.
2. Request 2 (touches runtime + agui + the example sketch) → review → verify →
   commit.
3. Response write-ups (file 03) alongside or immediately after each, so the
   acceptance criterion (a response under
   `~/.agents/projects/eino-agent/responses/`) is satisfied.

Track each as a `bd` issue per project convention (see file 03 for suggested
`bd create` commands). Follow the plan→review→implement→fix→verify→commit
workflow; request 2 earns its own review pass because it crosses the
serialization boundary (Event ⇄ EventRecord ⇄ AG-UI).
</content>
</invoke>
