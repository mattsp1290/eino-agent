package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestProviderStatePayloadRoundTripPreservesRawBytes(t *testing.T) {
	want := json.RawMessage("{\n  \"z\": 1, \"a\":\"SENTINEL\"\n}")
	payload, err := EncodeProviderStatePayload(ProviderStateEnvelope{
		CodecID: "example.test/reasoning", Version: 1, ProviderID: "provider", SourceModelID: "model",
		CompatibilityKey: "reasoning-v1", ItemIndex: 0, Data: want,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, want) || strings.Contains(string(payload), "SENTINEL") {
		t.Fatalf("payload embeds raw state: %s", payload)
	}
	got, err := DecodeProviderStatePayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data, want) {
		t.Fatalf("decoded = %q, want %q", got.Data, want)
	}
	got.Data[0] = '['
	if want[0] != '{' {
		t.Fatal("decode did not return owned bytes")
	}
}

func TestProviderStatePayloadRejectsNonCanonicalAndMalformedInput(t *testing.T) {
	valid, err := EncodeProviderStatePayload(ProviderStateEnvelope{CodecID: "codec", Version: 1, ProviderID: "provider", SourceModelID: "model", CompatibilityKey: "compat", ItemIndex: 0, Data: json.RawMessage(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	tests := [][]byte{
		append([]byte(" "), valid...),
		bytes.Replace(valid, []byte(`"codec_id":"codec"`), []byte(`"codec_id":"codec","codec_id":"codec"`), 1),
		bytes.Replace(valid, []byte(`"version":1`), []byte(`"unknown":1,"version":1`), 1),
		bytes.Replace(valid, []byte(`"data_base64":"eyJ4IjoxfQ=="`), []byte(`"data_base64":"eyJ4IjoxfQ"`), 1),
		[]byte(`{"codec_id":"codec","version":1,"provider_id":"provider","source_model_id":"model","compatibility_key":"compat","item_index":0,"data_base64":"bnVsbA=="}`),
	}
	for _, raw := range tests {
		if _, err := DecodeProviderStatePayload(raw); !errors.Is(err, ErrProviderStateInvalid) || strings.Contains(err.Error(), "SENTINEL") {
			t.Fatalf("raw %q error = %v", raw, err)
		}
	}
}

func TestProviderStatePayloadMetadataBoundaries(t *testing.T) {
	valid := ProviderStateEnvelope{
		CodecID: strings.Repeat("c", ProviderStateMaxCodecIDBytes), Version: 1,
		ProviderID: strings.Repeat("p", ProviderStateMaxProviderIDBytes), SourceModelID: strings.Repeat("m", ProviderStateMaxModelIDBytes),
		CompatibilityKey: strings.Repeat("k", ProviderStateMaxCompatibilityKeyBytes), ItemIndex: ProviderStateHardMaxItems - 1,
		Data: json.RawMessage(`{"x":1}`),
	}
	if _, err := EncodeProviderStatePayload(valid); err != nil {
		t.Fatalf("maximum metadata rejected: %v", err)
	}
	for name, mutate := range map[string]func(*ProviderStateEnvelope){
		"codec":         func(e *ProviderStateEnvelope) { e.CodecID += "c" },
		"provider":      func(e *ProviderStateEnvelope) { e.ProviderID += "p" },
		"model":         func(e *ProviderStateEnvelope) { e.SourceModelID += "m" },
		"compatibility": func(e *ProviderStateEnvelope) { e.CompatibilityKey += "k" },
		"index":         func(e *ProviderStateEnvelope) { e.ItemIndex++ },
	} {
		t.Run(name, func(t *testing.T) {
			envelope := valid
			mutate(&envelope)
			if _, err := EncodeProviderStatePayload(envelope); !errors.Is(err, ErrProviderStateInvalid) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
