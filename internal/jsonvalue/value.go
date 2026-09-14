// Package jsonvalue provides small predicates for JSON values at durable
// storage boundaries.
package jsonvalue

import (
	"bytes"
	"encoding/json"
)

// IsAbsent reports whether raw represents the repository's two durable
// absent-value shapes: no bytes in memory, or JSON null after SQL round-trip.
// Nonempty whitespace is not absent; callers retain responsibility for
// rejecting malformed JSON.
func IsAbsent(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
