package consumer

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
)

func openTestSQLite(ctx context.Context, path string) (*sqlite.Store, *sql.DB, error) {
	return sqliteFixturePool(ctx, path, true)
}
func reopenTestSQLite(ctx context.Context, path string) (*sqlite.Store, *sql.DB, error) {
	return sqliteFixturePool(ctx, path, false)
}
func sqliteFixturePool(ctx context.Context, path string, initialize bool) (*sqlite.Store, *sql.DB, error) {
	uri := url.URL{Scheme: "file", Path: path, OmitHost: true}
	if strings.HasPrefix(path, "file:") {
		parsed, err := url.Parse(path)
		if err != nil {
			return nil, nil, err
		}
		uri = *parsed
	}
	q := uri.Query()
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	uri.RawQuery = q.Encode()
	dsn := uri.String()
	if path == ":memory:" {
		dsn = ":memory:?" + uri.RawQuery
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if initialize {
		if err := sqlite.Migrate(ctx, db); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
	}
	st, err := sqlite.New(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return st, db, nil
}

func TestPublicSQLiteHostPoolSurvivesStoreAbandonmentAndReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("named_memory", func(t *testing.T) {
		st, db, err := openTestSQLite(ctx, "file:consumer-memory?mode=memory&cache=shared")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		if _, err := st.Execution(session.RunFence{}).AppendMessage(ctx, session.Message{}); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("invalid execution error = %v, want conflict", err)
		}
		if err := db.PingContext(ctx); err != nil {
			t.Fatalf("host pool after invalid operation: %v", err)
		}
		if _, err := sqlite.New(ctx, db); err != nil {
			t.Fatalf("new store after abandoning prior instance: %v", err)
		}
	})

	t.Run("file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "consumer.db")
		st, db, err := openTestSQLite(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		at := time.Now().UTC()
		if _, err := st.CreateSession(ctx, session.Session{ID: "sqlite-reopen", CreatedAt: at, UpdatedAt: at}); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, reopenedDB, err := reopenTestSQLite(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reopenedDB.Close() })
		got, err := reopened.GetSession(ctx, "sqlite-reopen")
		if err != nil || got.ID != "sqlite-reopen" {
			t.Fatalf("reopened session = %+v, err=%v", got, err)
		}
	})
}
