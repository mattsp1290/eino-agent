package sqlstore

import (
	"encoding/json"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

// contentTextEnvelope decodes just enough of a durable content block envelope
// payload (see session.EncodeContentParts) to reach the nested text.text
// field for PartUserInputText and PartAssistantGenText parts, without pulling
// in the full session.DecodeContentParts machinery.
type contentTextEnvelope struct {
	Text *struct {
		Text string `json:"text"`
	} `json:"text"`
}

// Hidden payloads are never copied into observation columns.
func ObservationText(p session.Part) (string, bool) {
	switch p.Kind {
	case session.PartText:
		var value struct {
			Text *string `json:"text"`
		}
		if !utf8.Valid(p.Payload) || json.Unmarshal(p.Payload, &value) != nil || value.Text == nil {
			return "", false
		}
		return *value.Text, utf8.ValidString(*value.Text)
	case session.PartUserInputText, session.PartAssistantGenText:
		var envelope contentTextEnvelope
		if !utf8.Valid(p.Payload) || json.Unmarshal(p.Payload, &envelope) != nil || envelope.Text == nil {
			return "", false
		}
		return envelope.Text.Text, utf8.ValidString(envelope.Text.Text)
	default:
		return "", true
	}
}
