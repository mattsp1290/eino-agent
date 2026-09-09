//go:build postgres_integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

func TestPostgresReplay(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("messages_parts", func(t *testing.T) { testReplayMessagesParts(t, server) })
	t.Run("events_models", func(t *testing.T) { testReplayEventsModels(t, server) })
	t.Run("provider_bytes", func(t *testing.T) { testReplayProviderBytes(t, server) })
	t.Run("private_bounds", func(t *testing.T) { testReplayPrivateBounds(t, server) })
	t.Run("canonical_time", func(t *testing.T) { testReplayCanonicalTime(t, server) })
}

type replayFixture struct {
	ctx      context.Context
	database *testpostgres.Database
	db       *sql.DB
	store    session.Store
	now      time.Time
}

func newReplayFixture(t *testing.T, server *testpostgres.Server) *replayFixture {
	t.Helper()
	return newReplayFixtureWithTimeout(t, server, 30*time.Second)
}

func newReplayFixtureWithTimeout(t *testing.T, server *testpostgres.Server, timeout time.Duration) *replayFixture {
	t.Helper()
	database := server.Database(t)
	db, store := migratePostgresDatabase(t, database)
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	t.Cleanup(cancel)
	return &replayFixture{ctx: ctx, database: database, db: db, store: store, now: time.Now().UTC()}
}

func (f *replayFixture) seed(t *testing.T, id session.ID, runID session.RunID) session.Run {
	t.Helper()
	if _, err := f.store.CreateSession(f.ctx, session.Session{ID: id, CreatedAt: f.now, UpdatedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	run, err := f.store.AdmitRun(f.ctx, session.Run{ID: runID, SessionID: id, OwnerID: "owner", ClaimToken: "token", Status: session.RunPending, CreatedAt: f.now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func (f *replayFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db = f.database.Open(t)
	store, err := postgres.New(f.ctx, f.db)
	if err != nil {
		t.Fatal(err)
	}
	f.store = store
}

func testReplayCanonicalTime(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	run := f.seed(t, "session", "run")
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	cases := []struct {
		id  session.MessageID
		at  time.Time
		key string
	}{
		{"zero", time.Time{}, ""},
		{"year-zero", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), "0000-01-01T00:00:00.000000000Z"},
		{"year-one-nano", time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), "0001-01-01T00:00:00.000000001Z"},
		{"offset", time.Date(2026, 9, 8, 3, 2, 1, 123456789, time.FixedZone("offset", 3600)), "2026-09-08T02:02:01.123456789Z"},
		{"year-max", time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), "9999-12-31T23:59:59.999999999Z"},
	}
	for i := len(cases) - 1; i >= 0; i-- {
		item := cases[i]
		if _, err := execution.AppendMessage(f.ctx, session.Message{ID: item.id, SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: item.at, UpdatedAt: item.at}); err != nil {
			t.Fatalf("append boundary time %s: %v", item.id, err)
		}
	}
	f.reopen(t)
	batch, err := f.store.ListMessages(f.ctx, run.SessionID, session.ReplayCursor{})
	if err != nil || len(batch.Messages) != len(cases) || batch.Next != (session.ReplayCursor{}) {
		t.Fatalf("boundary time replay count=%d err=%v", len(batch.Messages), err)
	}
	for i, item := range cases {
		if got := batch.Messages[i]; got.ID != item.id || !got.CreatedAt.Equal(item.at) {
			t.Fatalf("boundary time order/value mismatch at %d", i)
		}
		var key string
		if err := f.db.QueryRowContext(f.ctx, "SELECT created_at FROM public.messages WHERE id = $1", []byte(item.id)).Scan(&key); err != nil || key != item.key {
			t.Fatalf("canonical time projection mismatch for %s: %v", item.id, err)
		}
	}
}
