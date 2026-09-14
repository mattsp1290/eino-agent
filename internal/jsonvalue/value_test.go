package jsonvalue

import (
	"encoding/json"
	"testing"
)

func TestIsAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  json.RawMessage
		want bool
	}{
		{name: "nil", want: true},
		{name: "empty", raw: json.RawMessage{}, want: true},
		{name: "null", raw: json.RawMessage("null"), want: true},
		{name: "padded null", raw: json.RawMessage(" \nnull\t"), want: true},
		{name: "whitespace is malformed not absent", raw: json.RawMessage(" \n\t")},
		{name: "object", raw: json.RawMessage(`{}`)},
		{name: "invalid", raw: json.RawMessage(`{"`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsAbsent(tt.raw); got != tt.want {
				t.Fatalf("IsAbsent(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}
