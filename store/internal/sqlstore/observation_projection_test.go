package sqlstore

import (
	"encoding/json"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func FuzzObservationText(f *testing.F) {
	for _, seed := range []string{`{"text":"hello"}`, `{"text":42}`, `{"text":null}`, `{"text":""}`, "{\"text\":\"\xff\"}"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			return
		}
		value, valid := ObservationText(session.Part{Kind: session.PartText, Payload: raw})
		if valid {
			if !json.Valid(raw) {
				t.Fatal("accepted malformed JSON")
			}
			var decoded struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(raw, &decoded); err != nil || decoded.Text != value {
				t.Fatal("text changed")
			}
		}
		hidden, ok := ObservationText(session.Part{Kind: session.PartProviderState, Payload: raw})
		if hidden != "" || !ok {
			t.Fatal("hidden state inspected")
		}
	})
}
