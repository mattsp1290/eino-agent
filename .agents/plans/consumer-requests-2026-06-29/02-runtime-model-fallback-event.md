# Plan 02 — model-fallback event kind for `runtime.Event`

**Request:** `~/.agents/projects/eino-agent/requests/2026-06-29-runtime-fallback-event-kind.md`
**Decision (hybrid of the request's Option 1 + Option 2):** Add
`EventModelFallbackEngaged` **and** define a documented, typed `Payload`
convention (`ModelFallbackPayload`) carrying the from→to pair. Do **not** add
new top-level fields to the flat `Event`/`EventRecord` structs.
**Scope:** `runtime` (types + helpers + 1 switch), `agui/bridge.go`, and the
`examples/ensemble-adapter` sketch. Additive, no API break.

## Why Payload-carried, not new struct fields

> **Corrected rationale (both plan reviewers flagged the original).** The
> sqlite store does **not** require a schema migration for new `EventRecord`
> fields: `store/sqlite/store.go:319` marshals the *entire* `EventRecord` as a
> JSON blob into a single `record` column (only `id/session_id/run_id/kind/
> created_at` are real columns). So "schema migration" is **not** a reason to
> avoid top-level fields — do not repeat that claim in the response. The real,
> accurate reasons follow.

Adding `FromModelID`/`ToModelID` as top-level fields to `runtime.Event` is still
the worse choice, but for these reasons:

- **Dead fields on every other kind.** The flat `Event`/`EventRecord` envelope
  is shared by all 6 (soon 7) kinds; a model-pair that only one kind uses bloats
  every event and every place that constructs one.
- **Wiring surface.** `agui/replay.go:127` (`runtimeEvent`) maps record→event
  field-by-field; new fields must be threaded there, plus through the
  host-owned `Event→EventRecord` projection (there is **no** generic library
  converter — `runtime/admission.go:289` is `run_started`-specific and the
  generic mapping lives in `examples/minimal-server/main.go:409`).
- **Host-owned signal.** The request explicitly keeps fallback *selection*
  logic consumer-side (`model.Resolver` resolves exactly one `Selection`), so
  the host already builds these events; a typed `Payload` lets it do so without
  touching the core envelope.

The flat envelope already has a `Payload json.RawMessage` slot that round-trips
through `EventRecord.Payload`. So:

- Carry the transition in `Payload` using a typed, exported shape so adapters
  share one encoding (this is the request's Option 2, satisfied concretely).
- Also set `Event.ModelID = ToModelID` (the now-active model) so the existing
  `ModelID`-reading observability/replay paths stay meaningful without parsing
  payload JSON.

Net: durable, replayable, AG-UI-emittable, no new envelope fields.

### Round-trip honesty

Do **not** claim a library-guaranteed *bidirectional* `Event ⇄ EventRecord`
round-trip. The library owns only the **record→event** direction
(`runtimeEvent`, `agui/replay.go:127`). The durable **event→record** mapping
(copying `Kind` + `ModelID` + `Payload` into the record) is **host-owned** —
e.g. `examples/minimal-server/main.go:409`. Because `Payload` and `ModelID` are
plain copied fields on both sides, the projection is lossless, but the test
below proves only the `runtimeEvent` reconstruction half (it hand-builds an
`EventRecord`). State it that way in the response.

### Disposition: durable, not live-only

`model_fallback_engaged` is **durable** (`LiveOnly = false`) — the helper never
sets `LiveOnly`, and the adapter maps it `DispositionDurable`. Consequence to
document for the consumer: it is persisted and **re-emitted on every
replay/reconnect** as an AG-UI `Custom("model_fallback_engaged", …)` event.
Verified end-to-end path: `examples/minimal-server/main.go:398`
(`if !event.LiveOnly && event.Kind != runtime.EventRunStarted { … persist … }`)
already persists this kind and replays it — **no code change needed there**;
that sink is the verification vehicle for the durable path.

### ProviderID: explicit decision (model-centric)

The consumer's metric (`ensemble.model.fallback_engaged_total`) is labeled by
`model.from`/`model.to` only — providers are not a metric label, and the request
lists model labels exclusively. **Decision:** the runtime helper is
**model-centric** — it sets `Event.ModelID = ToModelID` and leaves
`Event.ProviderID` for the caller to set (the host builds the event and knows
the provider). To stay forward-compatible for a cross-provider fallback without
a second API change, `ModelFallbackPayload` includes **optional**
`from_provider_id`/`to_provider_id` (`omitempty`); the minimal helper does not
populate them. Document this so a provider+model observer
(`runtime/observability.go:365` pairs `ProviderID`+`ModelID`) is not surprised
by a populated `ModelID` next to an empty `ProviderID`.

## Changes

### 1. `runtime/types.go` — new kind + typed payload (near `:201`)

Add the kind to the `const (...)` block:

```go
	// EventModelFallbackEngaged reports that the host swapped the primary model
	// for a fallback at a turn boundary (e.g. circuit-breaker trip). The
	// from→to transition is carried in Payload as ModelFallbackPayload; Event
	// .ModelID is set to the now-active (to) model. Selection of the fallback
	// is owned by the host/consumer, not the eino-agent runtime.
	EventModelFallbackEngaged EventKind = "model_fallback_engaged"
```

Add the documented payload type + helpers (place after the `Event` struct or in
a small `runtime/events.go` — match where similar payload structs live; e.g.
`runFinishedPayload` is defined in `agui/bridge.go`, but this one is shared, so
`runtime` is the right home):

```go
// ModelFallbackPayload is the documented JSON shape carried in
// Event.Payload when Kind == EventModelFallbackEngaged. Field names are the
// stable wire contract that adapters (e.g. the ensemble local-symphony
// projector) target: from_model_id → metric label model.from,
// to_model_id → model.to.
type ModelFallbackPayload struct {
	FromModelID string `json:"from_model_id"`
	ToModelID   string `json:"to_model_id"`
	// FromProviderID/ToProviderID are optional; populate only when the
	// fallback crosses providers. The minimal helper leaves them empty.
	FromProviderID string `json:"from_provider_id,omitempty"`
	ToProviderID   string `json:"to_provider_id,omitempty"`
	// Reason is an optional, host-defined cause (e.g. "circuit_breaker").
	Reason string `json:"reason,omitempty"`
}

// NewModelFallbackEvent builds a model_fallback_engaged Event with ModelID set
// to the to-model and the model transition encoded in Payload. It is
// model-centric: ProviderID and the optional provider payload fields are left
// for the caller to set when the fallback crosses providers. Callers fill the
// remaining envelope fields (IDs, Time) as usual.
//
// The error return is intentionally omitted: json.Marshal of a struct of
// strings cannot fail, so a (Event) signature avoids dead caller boilerplate.
func NewModelFallbackEvent(from, to, reason string) Event {
	payload, _ := json.Marshal(ModelFallbackPayload{
		FromModelID: from, ToModelID: to, Reason: reason,
	})
	return Event{
		Kind:    EventModelFallbackEngaged,
		ModelID: to,
		Payload: payload,
	}
}
```

`encoding/json` is already imported by `runtime/types.go:5` (the `Event.Payload`
field is `json.RawMessage`) — confirmed, no new import.

### 2. `agui/bridge.go` — surface it to AG-UI (`Emit` switch, `:67`)

Add a case so live/replay subscribers see the swap as a custom event. Re-emit
the durable `Payload` object verbatim so the live AG-UI event carries the **exact
same key set** as the persisted payload (omitempty and all) — a single wire
shape across both surfaces, and nil-safe for hosts that bypass the helper:

```go
	case runtime.EventModelFallbackEngaged:
		value := map[string]any{}
		if len(event.Payload) > 0 {
			_ = json.Unmarshal(event.Payload, &value)
		}
		b.emit.Custom("model_fallback_engaged", value)
```

Mirrors the existing `EventContextEpochChanged` → `b.emit.Custom(...)` case
(`bridge.go:74`). The switch has no `default`, so this is additive and other
kinds are unaffected.

> **Why the map round-trip, not a hand-listed key map** (both implementation
> reviewers flagged this): a hand-built `map[string]any` with five fixed keys
> always emits `from_provider_id:""`/`to_provider_id:""`/`reason:""`, which
> diverges from the durable payload's `omitempty` shape. Re-emitting the payload
> object keeps the live and durable surfaces byte-for-byte consistent.

### 3. `runtime/observability.go` — optional observation (`Emit` switch, `:29`)

Optional but recommended for parity with the consumer's
`ensemble.model.fallback_engaged_total` metric intent. Add a case that records
a model-swap observation if `einoobs.Observer` exposes a suitable method
(check the `einoobs` surface first — `Compaction`/`Error` are used today; if
there is no natural fit, **skip this** and note it in the response rather than
forcing an awkward mapping). The metric itself stays consumer-owned (out of
scope per the request).

### 4. `examples/ensemble-adapter/sketch.go` — upgrade the mapping

Today `EventModelFallbackEngaged` maps to `DispositionOmit` (dropped,
`sketch.go:101`). Change it to a durable mapping using the new kind + payload:

```go
	case EventModelFallbackEngaged:
		base.Kind = runtime.EventModelFallbackEngaged
		base.ModelID = event.ToModel
		base.Payload = payload(runtime.ModelFallbackPayload{
			FromModelID: event.FromModel,
			ToModelID:   event.ToModel,
		})
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionDurable, Observation: "model"}
```

Remove `EventModelFallbackEngaged` from the combined `case EventOtherMessage,
EventModelFallbackEngaged:` omit-branch (`sketch.go:101`), leaving
`EventOtherMessage` there. The sketch's `RunEvent` already has `FromModel` /
`ToModel` fields (`sketch.go:55-56`), so no new adapter fields are needed.

## Tests

- **`runtime`** (add `runtime/events_test.go` or extend an existing runtime
  test): `NewModelFallbackEvent("a","b","circuit_breaker")` yields
  `Kind == EventModelFallbackEngaged`, `ModelID == "b"`, and
  `Payload` unmarshals to `ModelFallbackPayload{FromModelID:"a", ToModelID:"b",
  Reason:"circuit_breaker"}`. Assert the raw JSON keys are exactly
  `from_model_id`/`to_model_id`/`reason` (the wire contract; provider keys
  absent because `omitempty`).
- **Reconstruction (record→event only)** (extend `agui/replay_test.go`):
  hand-build an `EventRecord{Kind:"model_fallback_engaged", ModelID:"b",
  Payload: <marshaled ModelFallbackPayload>}` and assert `runtimeEvent(record)`
  reconstructs `Kind`, `ModelID`, and `Payload`. This validates the **library's
  record→event half only** — there is no generic `Event→EventRecord` converter
  in the tree (that projection is host-owned, e.g.
  `examples/minimal-server/main.go:409`). Do not write the test as a
  bidirectional round-trip through a nonexistent helper.
- **`agui` bridge** (extend `agui/bridge_test.go`): emitting an
  `EventModelFallbackEngaged` produces a `Custom("model_fallback_engaged", ...)`
  AG-UI event with the keys. Follow the existing bridge-test harness (there is
  already coverage for the `context_epoch_changed` custom event — reuse that
  shape). **Caution:** `TestBridgeEmitsFullSurfaceGolden` is a golden test —
  add the new kind in a *separate* focused test; do **not** hand-edit the golden
  fixture, and only regenerate it if you deliberately add the new kind to its
  input.
- **`examples/ensemble-adapter`** (`sketch_test.go` exists): assert
  `MapRunEvent(RunEvent{Kind: EventModelFallbackEngaged, FromModel:"a",
  ToModel:"b"})` now returns `Disposition == DispositionDurable`,
  `RuntimeEvent.Kind == runtime.EventModelFallbackEngaged`, `ModelID == "b"`,
  and a payload decoding to the pair.

## Verification

```bash
cd /Users/punk1290/git/eino-agent
make fmt
go test ./runtime/... ./agui/... ./examples/ensemble-adapter/...
make vet && make test && make lint
```

Done means: new kind round-trips Event⇄EventRecord losslessly, AG-UI bridge
emits the custom event, the adapter sketch no longer drops the signal, and full
`make test` is green.

## Acceptance deliverable

Write the response stating "new EventKind added" + the exact `Payload` JSON
shape (`from_model_id`, `to_model_id`, `reason`) so the local-symphony projector
(`j8la.20`) can target it — see file `03`.
</content>
