// Package providerstatewire defines the dependency-neutral wire invariants
// shared by the public model boundary and durable session encoding.
package providerstatewire

import (
	"bytes"
	"encoding/json"
)

const (
	MaxItems              = 64
	MaxItemBytes          = 10 * 1024 * 1024
	MaxMessageBytes       = 16 * 1024 * 1024
	MaxEnvelopeBytes      = 13_985_112
	MaxStoredMessageBytes = 22_632_024
	MaxCodecIDBytes       = 128
	MaxCompatibilityBytes = 256
	MaxExtraKeyBytes      = 256
	MaxProviderIDBytes    = 128
	MaxModelIDBytes       = 256
)

// IsJSONObject reports whether raw is exactly one syntactically valid JSON
// object. It does not materialize numbers, so the accepted syntax is not
// constrained by float64 range and the original bytes remain untouched.
func IsJSONObject(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) >= 2 && trimmed[0] == '{' && json.Valid(trimmed)
}

// ValidASCIIToken validates the identity alphabet shared by the two package
// boundaries.
func ValidASCIIToken(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes {
		return false
	}
	for i := 0; i < len(value); i++ {
		if !tokenByte(value[i]) {
			return false
		}
	}
	return true
}

func tokenByte(value byte) bool {
	if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' {
		return true
	}
	return bytes.ContainsRune([]byte("!#$%&'*+-.^_`|~:/@"), rune(value))
}
