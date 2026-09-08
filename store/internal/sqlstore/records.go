package sqlstore

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// SameRecord compares durable records using their serialized JSON form.
func SameRecord[T any](left, right T) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

// SameModelRequestIdentity compares the immutable identity fields of requests.
func SameModelRequestIdentity(left, right session.ModelRequestRecord) bool {
	left.State, right.State = "", ""
	left.ErrorCode, right.ErrorCode = "", ""
	left.UpdatedAt, right.UpdatedAt = time.Time{}, time.Time{}
	return SameRecord(left, right)
}

// DecodeAuthoritativeMessage decodes a message and verifies its indexed fields.
func DecodeAuthoritativeMessage(id, sessionID, runID, role, createdAt string, raw []byte) (session.Message, error) {
	var message session.Message
	if err := json.Unmarshal(raw, &message); err != nil ||
		message.ID != session.MessageID(id) || message.SessionID != session.ID(sessionID) ||
		message.RunID != session.RunID(runID) || message.Role != session.Role(role) ||
		TimeText(message.CreatedAt) != createdAt {
		return session.Message{}, session.ErrConflict
	}
	return message, nil
}

// DecodeAuthoritativePart decodes a part and verifies its indexed fields.
func DecodeAuthoritativePart(id, messageID, sessionID, runID string, ordinal int64, raw []byte) (session.Part, error) {
	var part session.Part
	if err := json.Unmarshal(raw, &part); err != nil ||
		part.ID != session.PartID(id) || part.MessageID != session.MessageID(messageID) ||
		part.SessionID != session.ID(sessionID) || part.RunID != session.RunID(runID) || part.Ordinal != ordinal {
		return session.Part{}, session.ErrConflict
	}
	return part, nil
}
