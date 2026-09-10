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

func FuzzObservationText_ContentEnvelope(f *testing.F) {
	for _, seed := range []string{
		`{"schema":1,"block_id":"b1","kind":"user_input_text","text":{"text":"hello"}}`,
		`{"schema":1,"block_id":"b1","kind":"assistant_gen_text","text":{"text":"hi","refusal":"","annotations":[]}}`,
		`{"schema":1,"block_id":"b1","kind":"user_input_text","text":{"text":42}}`,
		`{"schema":1,"block_id":"b1","kind":"user_input_text","text":null}`,
		`{"schema":1,"block_id":"b1","kind":"user_input_text"}`,
		`{"schema":1,"block_id":"b1","kind":"user_input_text","text":{}}`,
		"{\"schema\":1,\"kind\":\"user_input_text\",\"text\":{\"text\":\"\xff\"}}",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			return
		}
		for _, kind := range []session.PartKind{session.PartUserInputText, session.PartAssistantGenText} {
			value, valid := ObservationText(session.Part{Kind: kind, Payload: raw})
			if valid {
				if !json.Valid(raw) {
					t.Fatal("accepted malformed JSON")
				}
				var decoded struct {
					Text *struct {
						Text string `json:"text"`
					} `json:"text"`
				}
				if err := json.Unmarshal(raw, &decoded); err != nil || decoded.Text == nil || decoded.Text.Text != value {
					t.Fatal("text changed")
				}
			}
		}
		// Other new kinds (function calls, media, provider state, ...) never
		// expose payload text, regardless of what the payload contains.
		for _, kind := range []session.PartKind{
			session.PartReasoning, session.PartFunctionToolCall, session.PartServerToolCall,
			session.PartMCPToolApprovalRequest, session.PartResponseMeta, session.PartProviderState,
		} {
			hidden, ok := ObservationText(session.Part{Kind: kind, Payload: raw})
			if hidden != "" || !ok {
				t.Fatal("non-text kind exposed payload text")
			}
		}
	})
}

func TestObservationText_ContentEnvelopeTable(t *testing.T) {
	cases := []struct {
		name    string
		kind    session.PartKind
		payload string
		want    string
		ok      bool
	}{
		{"user_input_text ok", session.PartUserInputText, `{"schema":1,"block_id":"b1","kind":"user_input_text","text":{"text":"hello"}}`, "hello", true},
		{"assistant_gen_text with annotations", session.PartAssistantGenText, `{"schema":1,"block_id":"b1","kind":"assistant_gen_text","text":{"text":"hi","refusal":"","annotations":[{"type":"url_citation"}]}}`, "hi", true},
		{"missing text field is malformed", session.PartUserInputText, `{"schema":1,"block_id":"b1","kind":"user_input_text"}`, "", false},
		{"null text field is malformed", session.PartUserInputText, `{"schema":1,"block_id":"b1","kind":"user_input_text","text":null}`, "", false},
		{"empty text object yields empty text", session.PartUserInputText, `{"schema":1,"block_id":"b1","kind":"user_input_text","text":{}}`, "", true},
		{"invalid JSON is malformed", session.PartUserInputText, `not json`, "", false},
		{"non-object top level is malformed", session.PartUserInputText, `[1,2,3]`, "", false},
		{"non-string text.text is malformed", session.PartUserInputText, `{"schema":1,"kind":"user_input_text","text":{"text":42}}`, "", false},
		{"other kinds never inspect payload", session.PartFunctionToolCall, `{"schema":1,"kind":"function_tool_call","function_call":{"call_id":"c1","name":"n"}}`, "", true},
		{"provider_state is never inspected", session.PartProviderState, `{"anything":"goes"}`, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ObservationText(session.Part{Kind: tc.kind, Payload: json.RawMessage(tc.payload)})
			if got != tc.want || ok != tc.ok {
				t.Fatalf("ObservationText(%s) = (%q, %v), want (%q, %v)", tc.kind, got, ok, tc.want, tc.ok)
			}
		})
	}
}
