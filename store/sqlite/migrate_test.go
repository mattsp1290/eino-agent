package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/mattsp1290/eino-agent/session"
)

func TestMigrationLifecycle(t *testing.T) {
	for _, state := range []migrationSchemaState{migrationSchemaEmpty, migrationSchemaBootstrap, migrationSchemaCurrent} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			db := migrationSchemaDB(t, state)
			before := migrationCatalog(t, db)
			if state != migrationSchemaCurrent {
				if _, err := New(t.Context(), db); !errors.Is(err, session.ErrConflict) {
					t.Fatalf("uninitialized constructor: %v", err)
				}
			}
			if !reflect.DeepEqual(before, migrationCatalog(t, db)) {
				t.Fatal("constructor changed catalog")
			}
			if err := Migrate(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			identity := baselineStrings(t, db, "SELECT incarnation FROM observation_store")
			before = migrationCatalog(t, db)
			history := migrationHistory(t, db)
			if _, err := New(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			if err := Migrate(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, migrationCatalog(t, db)) || !reflect.DeepEqual(history, migrationHistory(t, db)) || !reflect.DeepEqual(identity, baselineStrings(t, db, "SELECT incarnation FROM observation_store")) {
				t.Fatal("current lifecycle mutated database")
			}
			if err := db.PingContext(t.Context()); err != nil {
				t.Fatal("host pool closed", err)
			}
		})
	}
	t.Run("rejection", func(t *testing.T) {
		for _, change := range []string{"CREATE TABLE foreign_app(id TEXT)", currentSchema} {
			db := migrationSchemaDB(t, migrationSchemaEmpty)
			execBaseline(t, db, change)
			before := migrationCatalog(t, db)
			if err := Migrate(t.Context(), db); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("foreign migration: %v", err)
			}
			if _, err := New(t.Context(), db); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("foreign constructor: %v", err)
			}
			if !reflect.DeepEqual(before, migrationCatalog(t, db)) {
				t.Fatal("rejection wrote schema/history")
			}
		}
		db := migrationSchemaDB(t, migrationSchemaCurrent)
		execBaseline(t, db, "DROP INDEX events_replay_idx")
		before := migrationCatalog(t, db)
		history := migrationHistory(t, db)
		if err := Migrate(t.Context(), db); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("corrupt current: %v", err)
		}
		if !reflect.DeepEqual(before, migrationCatalog(t, db)) || !reflect.DeepEqual(history, migrationHistory(t, db)) {
			t.Fatal("corrupt rejection mutated database")
		}
	})
	t.Run("rollback_retry", func(t *testing.T) {
		db := migrationSchemaDB(t, migrationSchemaEmpty)
		fixture := fstest.MapFS{"00001_initial.sql": {Data: append(append([]byte(nil), baselineSQL...), []byte("\nINSERT INTO nonexistent_table VALUES (1);\n")...)}}
		if err := migrate(t.Context(), db, fixture); err == nil {
			t.Fatal("failing baseline succeeded")
		}
		assertMigrationSchema(t, db, migrationSchemaBootstrap)
		if err := Migrate(t.Context(), db); err != nil {
			t.Fatal("retry", err)
		}
		assertMigrationSchema(t, db, migrationSchemaCurrent)
	})
	t.Run("invalid_pool", func(t *testing.T) {
		if _, err := New(t.Context(), nil); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("nil constructor: %v", err)
		}
		if err := Migrate(t.Context(), nil); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("nil migration: %v", err)
		}
		db := openBaselinePool(t, filepath.Join(t.TempDir(), "closed.db"))
		_ = db.Close()
		if _, err := New(t.Context(), db); err == nil {
			t.Fatal("closed constructor succeeded")
		}
		if err := Migrate(t.Context(), db); err == nil {
			t.Fatal("closed migration succeeded")
		}
		off, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "foreign-keys-off.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = off.Close() }()
		if err := Migrate(t.Context(), off); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("missing host pragma: %v", err)
		}
		if len(migrationCatalog(t, off)) != 0 {
			t.Fatal("migration repaired host settings/schema")
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		db := migrationSchemaDB(t, migrationSchemaEmpty)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := Migrate(ctx, db); !errors.Is(err, context.Canceled) {
			t.Fatalf("migration cancellation: %v", err)
		}
		if len(migrationCatalog(t, db)) != 0 {
			t.Fatal("canceled migration wrote history")
		}
	})
}

func TestMemoryPoolLifecycle(t *testing.T) {
	for _, dsn := range []string{":memory:?_pragma=foreign_keys(1)", "file:" + t.Name() + "?mode=memory&cache=shared&_pragma=foreign_keys(1)"} {
		t.Run(dsn[:4], func(t *testing.T) {
			open := func() *sql.DB {
				db, err := sql.Open("sqlite", dsn)
				if err != nil {
					t.Fatal(err)
				}
				db.SetMaxOpenConns(1)
				db.SetMaxIdleConns(1)
				t.Cleanup(func() { _ = db.Close() })
				return db
			}
			db := open()
			if err := Migrate(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			st, err := New(t.Context(), db)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.CreateSession(t.Context(), session.Session{ID: "retained"}); err != nil {
				t.Fatal(err)
			}
			if _, err := st.GetSession(t.Context(), "retained"); err != nil {
				t.Fatal(err)
			}
			if dsn[0:5] == "file:" {
				keeper := open()
				if _, err := New(t.Context(), keeper); err != nil {
					t.Fatal(err)
				}
				_ = db.Close()
				kept, err := New(t.Context(), keeper)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := kept.GetSession(t.Context(), "retained"); err != nil {
					t.Fatal(err)
				}
				_ = keeper.Close()
			} else {
				_ = db.Close()
			}
			fresh := open()
			if _, err := New(t.Context(), fresh); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("last-close should lose memory db: %v", err)
			}
		})
	}
}
