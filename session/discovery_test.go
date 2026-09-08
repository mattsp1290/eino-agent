package session

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestDiscoveryQueryBounds(t *testing.T) {
	for _, limit := range []int{0, 1, 100, -1, 101} {
		err := (SessionDiscoveryQuery{WorkspaceID: "a", Limit: limit}).Validate()
		if (err == nil) != (limit >= 0 && limit <= 100) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	for _, ws := range []string{"", "\xff", strings.Repeat("x", 1025)} {
		if err := (SessionDiscoveryQuery{WorkspaceID: ws}).Validate(); !errors.Is(err, ErrDiscoveryQuery) {
			t.Fatal(err)
		}
	}
	for _, ws := range []string{" ", "*", "a\x00b", strings.Repeat("é", 512)} {
		if err := (SessionDiscoveryQuery{WorkspaceID: ws, Cursor: strings.Repeat("x", 8192)}).Validate(); err != nil {
			t.Fatal(err)
		}
	}
	if err := (SessionDiscoveryQuery{WorkspaceID: "a", Cursor: strings.Repeat("x", 8193)}).Validate(); !errors.Is(err, ErrDiscoveryCursor) {
		t.Fatal(err)
	}
}

func TestDiscoveryJSONAllowlist(t *testing.T) {
	for _, tc := range []struct {
		value any
		keys  []string
	}{
		{SessionSummary{}, []string{"created_at", "id", "title", "updated_at", "workspace_id"}},
		{SessionDiscoveryQuery{}, []string{"cursor", "limit", "workspace_id"}},
		{SessionDiscoveryPage{}, []string{"next_cursor", "sessions"}},
	} {
		raw, err := json.Marshal(tc.value)
		if err != nil {
			t.Fatal(err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			t.Fatal(err)
		}
		var keys []string
		for key := range object {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		if !reflect.DeepEqual(keys, tc.keys) {
			t.Fatal(keys)
		}
	}
}
