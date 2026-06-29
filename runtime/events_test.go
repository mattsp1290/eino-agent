package runtime

import (
	"encoding/json"
	"testing"
)

func TestNewModelFallbackEventEncodesTransition(t *testing.T) {
	t.Parallel()

	event := NewModelFallbackEvent("primary-model", "fallback-model", "circuit_breaker")

	if event.Kind != EventModelFallbackEngaged {
		t.Fatalf("Kind = %q, want %q", event.Kind, EventModelFallbackEngaged)
	}
	if event.ModelID != "fallback-model" {
		t.Fatalf("ModelID = %q, want the to-model", event.ModelID)
	}
	if event.ProviderID != "" {
		t.Fatalf("ProviderID = %q, want empty (model-centric helper)", event.ProviderID)
	}

	var payload ModelFallbackPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	want := ModelFallbackPayload{
		FromModelID: "primary-model",
		ToModelID:   "fallback-model",
		Reason:      "circuit_breaker",
	}
	if payload != want {
		t.Fatalf("payload = %+v, want %+v", payload, want)
	}
}

func TestModelFallbackPayloadWireKeys(t *testing.T) {
	t.Parallel()

	// The JSON keys are a stable wire contract the local-symphony projector
	// targets; optional provider keys must be omitted when empty.
	event := NewModelFallbackEvent("a", "b", "")
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &raw); err != nil {
		t.Fatalf("unmarshal raw payload: %v", err)
	}
	for _, key := range []string{"from_model_id", "to_model_id"} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("payload missing required key %q: %s", key, event.Payload)
		}
	}
	for _, key := range []string{"reason", "from_provider_id", "to_provider_id"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("payload should omit empty optional key %q: %s", key, event.Payload)
		}
	}
}
