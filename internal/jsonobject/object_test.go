package jsonobject

import (
	"encoding/json"
	"testing"
)

func TestDecodeRequiresUnambiguousObject(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "object", raw: `{"outer":{"value":1},"list":[1,2]}`},
		{name: "duplicate nested keys remain leaf-owned", raw: `{"outer":{"value":1,"value":2}}`},
		{name: "duplicate top-level key", raw: `{"value":1,"value":2}`, wantErr: true},
		{name: "null", raw: `null`, wantErr: true},
		{name: "array", raw: `[]`, wantErr: true},
		{name: "invalid", raw: `{`, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object, err := Decode(json.RawMessage(test.raw))
			if test.wantErr {
				if err == nil {
					t.Fatalf("Decode(%s) = %#v, want error", test.raw, object)
				}
				return
			}
			if err != nil || object == nil {
				t.Fatalf("Decode(%s) = %#v, %v", test.raw, object, err)
			}
		})
	}
}
