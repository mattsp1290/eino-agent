//go:build postgres_integration

package postgres_test

import (
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

func TestPostgresIndexes(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("lifecycle", func(t *testing.T) { testWideLifecycle(t, server) })
	t.Run("discovery_plans", func(t *testing.T) { testDiscoveryIndexPlans(t, server) })
	t.Run("non_c_ordering", func(t *testing.T) { testNonCIndexOrdering(t, server) })
}

// Public record codecs require UTF-8. Keep the deterministic random bytes in
// ASCII, including NUL, rather than repeating a short prefix to reach the bound.
func wideIdentity(size int, seed uint32) string {
	value := incompressibleBytes(size, seed)
	for i := range value {
		value[i] &= 0x7f
	}
	return string(value)
}

func testNonCIndexOrdering(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	var locale string
	var textOrder bool
	if err := f.db.QueryRowContext(f.ctx, "SELECT datcollate FROM pg_catalog.pg_database WHERE datname = current_database()").Scan(&locale); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(f.ctx, "SELECT 'z'::text > 'é'::text").Scan(&textOrder); err != nil {
		t.Fatal(err)
	}
	if locale != "en_US.utf8" || !textOrder {
		t.Fatalf("fixture did not establish differing non-C text order: locale=%q comparison=%t", locale, textOrder)
	}
	at := time.Date(2026, 9, 9, 1, 2, 3, 4, time.UTC)
	workspace := "ordering\x00workspace"
	// Descending byte order differs from this database's descending text order.
	cases := []struct {
		id session.ID
		at time.Time
	}{{"a", at.Add(time.Nanosecond)}, {"é", at}, {"z", at}}
	for i := len(cases) - 1; i >= 0; i-- {
		item := cases[i]
		if _, err := f.store.CreateSession(f.ctx, session.Session{ID: item.id, WorkspaceID: workspace, CreatedAt: item.at, UpdatedAt: item.at}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.store.(session.SessionDiscoveryReader).ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: workspace})
	if err != nil || len(page.Sessions) != len(cases) || page.NextCursor != "" {
		t.Fatalf("non-C discovery: count=%d err=%v", len(page.Sessions), err)
	}
	for i, item := range cases {
		if page.Sessions[i].ID != item.id || !page.Sessions[i].CreatedAt.Equal(item.at) {
			t.Fatalf("non-C byte/nanosecond order mismatch at %d", i)
		}
	}
}
