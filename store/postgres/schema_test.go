package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func TestInspectSchemaRequiresPinnedConnection(t *testing.T) {
	if _, err := inspectSchema(context.Background(), nil); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("nil connection: %v", err)
	}
}

func TestCatalogFingerprint(t *testing.T) {
	first := map[string]string{"column:b": "bytea", "column:a": "bigint"}
	reordered := map[string]string{"column:a": "bigint", "column:b": "bytea"}
	if catalogFingerprint(first) != catalogFingerprint(reordered) {
		t.Fatal("catalog iteration order changed its fingerprint")
	}
	for _, changed := range []map[string]string{
		{"column:a": "integer", "column:b": "bytea"},
		{"column:a": "bigint"},
		{"column:a": "bigint", "column:b": "bytea", "column:c": "text"},
	} {
		if catalogFingerprint(first) == catalogFingerprint(changed) {
			t.Fatal("catalog change did not change its fingerprint")
		}
	}
}
