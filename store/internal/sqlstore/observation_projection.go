package sqlstore

import (
	"encoding/json"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

// Hidden payloads are never copied into observation columns.
func ObservationText(p session.Part) (string, bool) {
	if p.Kind != session.PartText {
		return "", true
	}
	var value struct {
		Text *string `json:"text"`
	}
	if !utf8.Valid(p.Payload) || json.Unmarshal(p.Payload, &value) != nil || value.Text == nil {
		return "", false
	}
	return *value.Text, utf8.ValidString(*value.Text)
}
