//go:build postgres_integration

package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

func TestPostgresReaders(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("consistency", func(t *testing.T) { testReadersConsistency(t, server) })
	t.Run("privacy", func(t *testing.T) { testReadersPrivacy(t, server) })
	t.Run("bounds", func(t *testing.T) { testReadersBounds(t, server) })
	t.Run("availability", func(t *testing.T) { testReadersAvailability(t, server) })
	t.Run("admission primary", func(t *testing.T) { testAdmissionReaderPrimaryCheck(t, server) })
}

func testAdmissionReaderPrimaryCheck(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	db := database.Open(t)
	defer func() { _ = db.Close() }()
	if err := postgres.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	var queries []string
	reader, err := postgres.NewReaderTestStore(db, nil, func(query string, _ []any) { queries = append(queries, query) })
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.LookupAdmission(t.Context(), "missing-session", "missing-key")
	if !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("lookup error=%v", err)
	}
	if len(queries) < 2 || !strings.Contains(queries[0], "pg_is_in_recovery") || !strings.Contains(queries[1], "admission_receipts") {
		t.Fatalf("authoritative lookup queries=%q", queries)
	}
}

func readerLimits() session.ObservationLimits {
	return session.ObservationLimits{MaxMessages: 10, MaxParts: 20, MaxTools: 10, MaxTextBytes: 4096, MaxSnapshotBytes: 65536}
}

func testReadersAvailability(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	f.seed(t, "session", "run")
	assertErrors := func(ctx context.Context, reader session.ObservationReader, discovery session.SessionDiscoveryReader, observationErr, discoveryErr error) {
		t.Helper()
		if w, err := reader.ReadObservationRevision(ctx, "session"); !errors.Is(err, observationErr) || w != (session.ObservationWatermark{}) {
			t.Fatalf("revision availability: watermark=%+v err=%v", w, err)
		}
		if s, err := reader.ReadObservationSnapshot(ctx, "session", readerLimits()); !errors.Is(err, observationErr) || !reflect.DeepEqual(s, session.ObservationSnapshot{}) {
			t.Fatalf("snapshot availability: err=%v", err)
		}
		if p, err := discovery.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "workspace"}); !errors.Is(err, discoveryErr) || len(p.Sessions) != 0 || p.NextCursor != "" {
			t.Fatalf("discovery availability: err=%v", err)
		}
	}
	if err := f.store.WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
		assertErrors(ctx, tx.(session.ObservationReader), tx.(session.SessionDiscoveryReader), session.ErrObservationReader, session.ErrDiscoveryReader)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if inUse := f.db.Stats().InUse; inUse != 0 {
		t.Fatalf("transaction reader rejection leaked %d pool connections", inUse)
	}
	var nilStore *postgres.Store
	assertErrors(f.ctx, nilStore, nilStore, session.ErrObservationReader, session.ErrDiscoveryReader)
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	// Valid root readers preserve cancellation ahead of backend access.
	assertErrors(canceled, f.store.(session.ObservationReader), f.store.(session.SessionDiscoveryReader), context.Canceled, context.Canceled)
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	assertErrors(f.ctx, f.store.(session.ObservationReader), f.store.(session.SessionDiscoveryReader), session.ErrObservationStore, session.ErrDiscoveryStore)
	if _, err := nilStore.ReadObservationSnapshot(f.ctx, "session", session.ObservationLimits{}); !errors.Is(err, session.ErrObservationLimits) {
		t.Fatalf("invalid observation limit precedence: %v", err)
	}
	if _, err := nilStore.ListSessions(canceled, session.SessionDiscoveryQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled discovery precedence: %v", err)
	}
}
