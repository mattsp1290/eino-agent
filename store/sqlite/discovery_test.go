package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func discoveryStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := openSQLiteFixture(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.db.Close() })
	return s
}
func putDiscovery(t *testing.T, s session.Store, record session.Session) {
	t.Helper()
	if _, err := s.CreateSession(t.Context(), record); err != nil {
		t.Fatal(err)
	}
}
func listDiscovery(t *testing.T, s *Store, q session.SessionDiscoveryQuery) session.SessionDiscoveryPage {
	t.Helper()
	p, err := s.ListSessions(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func requireDiscoveryError(t *testing.T, s session.SessionDiscoveryReader, ctx context.Context, q session.SessionDiscoveryQuery, want error) {
	t.Helper()
	if s == nil {
		s = (*Store)(nil)
	}
	p, err := s.ListSessions(ctx, q)
	if err != want || !reflect.DeepEqual(p, session.SessionDiscoveryPage{}) {
		t.Fatalf("page=%+v err=%v want=%v", p, err, want)
	}
}
func TestDiscoveryProjectionWritesAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	s := discoveryStore(t, path)
	at := time.Date(2026, 9, 8, 1, 2, 3, 123456789, time.FixedZone("offset", 3600))
	record := session.Session{ID: "b", WorkspaceID: "A", Title: "initial", CreatedAt: at, UpdatedAt: at}
	putDiscovery(t, s, record)
	putDiscovery(t, s, record)
	putDiscovery(t, s, session.Session{ID: "a", WorkspaceID: "A"})
	first := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1})
	if first.Sessions[0].ID != "b" || !first.Sessions[0].CreatedAt.Equal(at) {
		t.Fatal(first)
	}
	rollback := errors.New("rollback")
	err := s.WithinTx(t.Context(), func(ctx context.Context, tx session.Store) error {
		changed := record
		changed.Title = "uncommitted"
		if err := tx.UpdateSession(ctx, changed); err != nil {
			return err
		}
		if _, err := tx.CreateSession(ctx, session.Session{ID: "rollback", WorkspaceID: "A"}); err != nil {
			return err
		}
		requireDiscoveryError(t, tx.(session.SessionDiscoveryReader), ctx, session.SessionDiscoveryQuery{WorkspaceID: "A"}, session.ErrDiscoveryReader)
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if got := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1}); !reflect.DeepEqual(got, first) {
		t.Fatal(got)
	}
	record.Title = "edited"
	record.WorkspaceID = "B"
	record.CreatedAt = at.Add(time.Hour)
	record.UpdatedAt = at.Add(2 * time.Hour)
	if err = s.UpdateSession(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err = s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = reopenSQLiteFixture(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.db.Close() })
	next := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A", Cursor: first.NextCursor})
	if len(next.Sessions) != 1 || next.Sessions[0].ID != "a" || next.NextCursor != "" {
		t.Fatal(next)
	}
	got := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "B"}).Sessions[0]
	stored, err := s.GetSession(t.Context(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != stored.Title || got.WorkspaceID != stored.WorkspaceID || !got.CreatedAt.Equal(stored.CreatedAt) || !got.UpdatedAt.Equal(stored.UpdatedAt) {
		t.Fatal("projection parity", got, stored)
	}
	fresh := discoveryStore(t, filepath.Join(t.TempDir(), "fresh.db"))
	requireDiscoveryError(t, fresh, t.Context(), session.SessionDiscoveryQuery{WorkspaceID: "A", Cursor: first.NextCursor}, session.ErrDiscoveryCursor)
}
func TestDiscoveryInvalidWrites(t *testing.T) {
	s := discoveryStore(t, filepath.Join(t.TempDir(), "store.db"))
	base := session.Session{ID: "id", WorkspaceID: "A"}
	putDiscovery(t, s, base)
	for _, mutate := range []func(*session.Session){
		func(s *session.Session) { s.ID = "\xff" }, func(s *session.Session) { s.WorkspaceID = "\xff" }, func(s *session.Session) { s.Title = "\xff" },
		func(s *session.Session) { s.CreatedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC) }, func(s *session.Session) { s.UpdatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		func(s *session.Session) { s.CreatedAt = time.Date(0, 1, 1, 0, 0, 0, 0, time.FixedZone("offset", 3600)) },
	} {
		record := base
		mutate(&record)
		if _, err := s.CreateSession(t.Context(), record); err != session.ErrConflict {
			t.Fatal(err)
		}
		if err := s.UpdateSession(t.Context(), record); err != session.ErrConflict {
			t.Fatal(err)
		}
	}
	if p := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A"}); len(p.Sessions) != 1 || p.Sessions[0].Title != "" {
		t.Fatal(p)
	}
}
func TestDiscoveryOversizeLookaheadAndCorruption(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		value     any
		want      error
	}{
		{"large title", "UPDATE sessions SET title = ? WHERE id=x'61'", []byte(strings.Repeat("t", 16385)), session.ErrDiscoveryTooLarge},
		{"invalid utf8", "UPDATE sessions SET title = ? WHERE id=x'61'", []byte{255}, session.ErrDiscoveryInvalid},
		{"text scalar", "UPDATE sessions SET title = ? WHERE id=x'61'", "text", session.ErrDiscoveryInvalid},
		{"timestamp", "UPDATE sessions SET updated_at = ? WHERE id=x'61'", "PRIVATE_BAD_TIME", session.ErrDiscoveryInvalid},
		{"long timestamp", "UPDATE sessions SET updated_at = ? WHERE id=x'61'", strings.Repeat("x", 31), session.ErrDiscoveryInvalid},
		{"empty identity", "UPDATE sessions SET id = ? WHERE id=x'61'", []byte{}, session.ErrDiscoveryInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := discoveryStore(t, filepath.Join(t.TempDir(), "store.db"))
			for _, id := range []session.ID{"a", "z"} {
				putDiscovery(t, s, session.Session{ID: id, WorkspaceID: "A"})
			}
			if _, err := s.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(tc.sql, tc.value); err != nil {
				t.Fatal(err)
			}
			first := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1})
			if len(first.Sessions) != 1 || first.Sessions[0].ID != "z" || first.NextCursor == "" {
				t.Fatal(first)
			}
			requireDiscoveryError(t, s, t.Context(), session.SessionDiscoveryQuery{WorkspaceID: "A", Cursor: first.NextCursor}, tc.want)
			requireDiscoveryError(t, s, t.Context(), session.SessionDiscoveryQuery{WorkspaceID: "A"}, tc.want)
		})
	}
}
func TestDiscoveryReaderFailuresAndCancellation(t *testing.T) {
	q := session.SessionDiscoveryQuery{WorkspaceID: "A"}
	requireDiscoveryError(t, nil, t.Context(), q, session.ErrDiscoveryReader)
	requireDiscoveryError(t, &Store{}, t.Context(), q, session.ErrDiscoveryReader)
	s := discoveryStore(t, filepath.Join(t.TempDir(), "store.db"))
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	requireDiscoveryError(t, s, ctx, q, context.DeadlineExceeded)
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	listDiscovery(t, s, q) // Cancellation released the waiting read, not a leaked tx.
	if _, err = s.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE observation_store SET incarnation = ?", strings.Repeat("x", 10000)); err != nil {
		t.Fatal(err)
	}
	requireDiscoveryError(t, s, t.Context(), q, session.ErrDiscoveryInvalid)
	if _, err = s.db.Exec("DELETE FROM observation_store"); err != nil {
		t.Fatal(err)
	}
	requireDiscoveryError(t, s, t.Context(), q, session.ErrDiscoveryInvalid)
	if err = s.db.Close(); err != nil {
		t.Fatal(err)
	}
	requireDiscoveryError(t, s, t.Context(), q, session.ErrDiscoveryStore)
}

//go:embed testdata/pre_discovery_schema.sql
var preDiscoverySchema string

func TestMigrateRejectsPreDiscoverySchemaWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err = db.Exec(preDiscoverySchema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO sessions(id,record,updated_at) VALUES ('preserved','private record','')`); err != nil {
		t.Fatal(err)
	}
	before, err := migrationSchemaCatalog(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	s, err := openSQLiteFixture(t.Context(), path)
	if s != nil {
		_ = s.db.Close()
	}
	if !errors.Is(err, session.ErrConflict) {
		t.Fatal(err)
	}
	after, err := migrationSchemaCatalog(t.Context(), db)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("schema modified", err)
	}
	var record string
	if err = db.QueryRow("SELECT record FROM sessions WHERE id='preserved'").Scan(&record); err != nil || record != "private record" {
		t.Fatal("data modified", err)
	}
}

func TestDiscoveryMaximumScalarsProduceUsablePages(t *testing.T) {
	s := discoveryStore(t, filepath.Join(t.TempDir(), "store.db"))
	ws := strings.Repeat("\x00", 1024)
	title := strings.Repeat("é", 8192)
	for _, id := range []session.ID{session.ID(strings.Repeat("\x00", 1023) + "a"), session.ID(strings.Repeat("\x00", 1023) + "b")} {
		putDiscovery(t, s, session.Session{ID: id, WorkspaceID: ws, Title: title, CreatedAt: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)})
	}
	first := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: ws, Limit: 1})
	if len(first.NextCursor) > 8192 || first.NextCursor == "" || first.Sessions[0].Title != title {
		t.Fatal("maximum scalar page failed")
	}
	next := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: ws, Limit: 1, Cursor: first.NextCursor})
	if next.NextCursor != "" || len(next.Sessions) != 1 || next.Sessions[0].ID == first.Sessions[0].ID || next.Sessions[0].Title != title {
		t.Fatal("maximum scalar continuation failed")
	}
}

func TestDiscoveryNullIdentityRejectedBeforeRead(t *testing.T) {
	s := discoveryStore(t, filepath.Join(t.TempDir(), "store.db"))
	putDiscovery(t, s, session.Session{ID: "a", WorkspaceID: "A"})
	before := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A"})
	if _, err := s.db.Exec("UPDATE sessions SET id=NULL WHERE id=?", []byte("a")); err == nil {
		t.Fatal("NULL public identity accepted")
	}
	if after := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A"}); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected NULL identity changed discovery", before, after)
	}
}
