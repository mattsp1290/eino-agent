package sqlite

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestDiscoveryCursorRoundTripAndMaximum(t *testing.T) {
	store := strings.Repeat("a", 32)
	for _, identity := range []string{"ordinary", strings.Repeat("\x00", 1024), strings.Repeat("é", 512), strings.Repeat("界", 341) + "x"} {
		for _, at := range []time.Time{{}, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2026, 9, 8, 1, 2, 3, 123456789, time.FixedZone("offset", 3600))} {
			encoded := encodeDiscoveryCursor(store, identity, session.SessionSummary{ID: session.ID(identity), CreatedAt: at})
			if len(encoded) > 8192 {
				t.Fatal("maximum legal cursor exceeds query bound", len(encoded))
			}
			c, err := decodeDiscoveryCursor(encoded, identity)
			if err != nil || c.storeID != store || c.id != session.ID(identity) || !c.createdAt.Equal(at) {
				t.Fatal(c, err)
			}
		}
	}
}

func TestDiscoveryCursorCanonicalEncoding(t *testing.T) {
	const wire = `{"version":1,"store":"YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE","workspace":"QQ","id":"aWQ","created":""}`
	want := base64.RawURLEncoding.EncodeToString([]byte(wire))
	got := encodeDiscoveryCursor(strings.Repeat("a", 32), "A", session.SessionSummary{ID: "id"})
	if got != want {
		t.Fatal("canonical encoding changed")
	}
}
func TestDiscoveryCursorRejectsNoncanonical(t *testing.T) {
	valid := encodeDiscoveryCursor(strings.Repeat("a", 32), "A", session.SessionSummary{ID: "id"})
	raw, _ := base64.RawURLEncoding.DecodeString(valid)
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	cases := []string{"", "PRIVATE_BAD", valid + "=", strings.Repeat("x", 8193)}
	encode := func(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }
	cases = append(cases, encode(append(raw, ' ')), encode(append(raw, raw...)), encode([]byte(strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1))))
	for _, key := range []string{"version", "store", "workspace", "id", "created"} {
		clone := map[string]any{}
		for k, v := range fields {
			clone[k] = v
		}
		delete(clone, key)
		b, _ := json.Marshal(clone)
		cases = append(cases, encode(b))
	}
	for _, change := range []struct {
		k string
		v any
	}{
		{"version", 2}, {"unknown", "field"}, {"id", ""}, {"id", base64.RawURLEncoding.EncodeToString([]byte{255})},
		{"id", base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", 1025)))},
		{"workspace", base64.RawURLEncoding.EncodeToString([]byte("B"))}, {"store", base64.RawURLEncoding.EncodeToString([]byte("corrupt"))},
		{"created", "2026-01-01T00:00:00Z"}, {"created", "2026-02-30T00:00:00.000000000Z"}, {"created", "0001-01-01T00:00:00.000000000Z"},
	} {
		clone := map[string]any{}
		for k, v := range fields {
			clone[k] = v
		}
		clone[change.k] = change.v
		b, _ := json.Marshal(clone)
		cases = append(cases, encode(b))
	}
	for i, bad := range cases {
		if _, err := decodeDiscoveryCursor(bad, "A"); !errors.Is(err, session.ErrDiscoveryCursor) {
			t.Fatal(i, err)
		}
	}
}
