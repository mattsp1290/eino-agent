package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/fstest"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"

	"github.com/mattsp1290/eino-agent/session"
)

var updateMigrationReference = flag.Bool("update-sqlite-schema", false, "regenerate the reviewed SQLite migration catalog fingerprints")

func TestMigrationSchemaReference(t *testing.T) {
	hashes := make([]string, 0, 2)
	for _, state := range []migrationSchemaState{migrationSchemaBootstrap, migrationSchemaCurrent} {
		db := migrationSchemaDB(t, state)
		hashes = append(hashes, migrationSchemaFingerprint(migrationCatalog(t, db)))
	}
	encoded, err := json.MarshalIndent(struct{ Bootstrap, Current string }{hashes[0], hashes[1]}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if *updateMigrationReference {
		if err := os.WriteFile("schema_fingerprints.json", encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if string(encoded) != string(migrationSchemaFingerprints) {
		t.Fatal("SQLite reference differs; review baseline/catalog changes before explicit regeneration")
	}
}

func TestMigrationSchemaStates(t *testing.T) {
	for _, state := range []migrationSchemaState{migrationSchemaEmpty, migrationSchemaBootstrap, migrationSchemaCurrent} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			db := migrationSchemaDB(t, state)
			before, history := migrationCatalog(t, db), migrationHistory(t, db)
			for range 2 {
				assertMigrationSchema(t, db, state)
			}
			if !reflect.DeepEqual(before, migrationCatalog(t, db)) || !reflect.DeepEqual(history, migrationHistory(t, db)) {
				t.Fatal("inspection changed catalog or history")
			}
			if err := db.PingContext(t.Context()); err != nil {
				t.Fatal("inspection closed the host pool", err)
			}
		})
	}
	t.Run("maintenance_and_reopen", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "reopen.db")
		db := openBaselinePool(t, path)
		prepareMigrationSchema(t, db, migrationSchemaCurrent)
		identity := baselineStrings(t, db, "SELECT incarnation FROM main.observation_store")
		execBaseline(t, db, "ANALYZE")
		assertMigrationSchema(t, db, migrationSchemaCurrent)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened := openBaselinePool(t, path)
		assertMigrationSchema(t, reopened, migrationSchemaCurrent)
		if !reflect.DeepEqual(identity, baselineStrings(t, reopened, "SELECT incarnation FROM main.observation_store")) {
			t.Fatal("inspection changed incarnation")
		}
	})
	t.Run("history_id_gaps", func(t *testing.T) {
		db := migrationSchemaDB(t, migrationSchemaCurrent)
		execBaseline(t, db, "UPDATE eino_agent_goose_version SET id=id+10")
		assertMigrationSchema(t, db, migrationSchemaCurrent)
	})
}

func TestMigrationSchemaRejection(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  migrationSchemaState
		change string
	}{
		{"foreign", migrationSchemaEmpty, "CREATE TABLE foreign_app(id INTEGER)"},
		{"legacy", migrationSchemaEmpty, currentSchema},
		{"baseline_without_history", migrationSchemaEmpty, string(baselineSQL)},
		{"history_without_row", migrationSchemaBootstrap, "DELETE FROM eino_agent_goose_version"},
		{"malformed_history", migrationSchemaBootstrap, "ALTER TABLE eino_agent_goose_version ADD COLUMN unexpected TEXT"},
		{"history_extra_index", migrationSchemaBootstrap, "CREATE INDEX unexpected ON eino_agent_goose_version(version_id)"},
		{"unknown_version", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET version_id=9"},
		{"negative_id", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET id=-1"},
		{"version_blob", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET version_id=zeroblob(1000000)"},
		{"applied_blob", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET is_applied=x'31'"},
		{"timestamp_blob", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET tstamp=x'31'"},
		{"timestamp_null", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET tstamp=NULL"},
		{"timestamp_invalid", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET tstamp='invalid'"},
		{"timestamp_nul_suffix", migrationSchemaBootstrap, "UPDATE eino_agent_goose_version SET tstamp='0000-01-01 00:00:00' || char(0) || printf('%1000000s', 'x')"},
		{"partial", migrationSchemaBootstrap, "CREATE TABLE sessions(id BLOB)"},
		{"missing_current_row", migrationSchemaCurrent, "DELETE FROM eino_agent_goose_version WHERE version_id=1"},
		{"extra_history", migrationSchemaCurrent, "INSERT INTO eino_agent_goose_version(version_id,is_applied) VALUES(1,1)"},
		{"reversed_history", migrationSchemaCurrent, "UPDATE eino_agent_goose_version SET version_id=1-version_id"},
		{"unapplied", migrationSchemaCurrent, "UPDATE eino_agent_goose_version SET is_applied=0 WHERE version_id=1"},
		{"newer", migrationSchemaCurrent, "UPDATE eino_agent_goose_version SET version_id=2 WHERE version_id=1"},
		{"missing_table", migrationSchemaCurrent, "DROP TABLE events"},
		{"missing_index", migrationSchemaCurrent, "DROP INDEX events_replay_idx"},
		{"altered_index", migrationSchemaCurrent, "DROP INDEX events_replay_idx; CREATE INDEX events_replay_idx ON events(session_key,created_at)"},
		{"missing_trigger", migrationSchemaCurrent, "DROP TRIGGER sessions_observation_update"},
		{"altered_trigger", migrationSchemaCurrent, "DROP TRIGGER sessions_observation_update; CREATE TRIGGER sessions_observation_update AFTER UPDATE ON sessions BEGIN SELECT 1; END"},
		{"extra_column", migrationSchemaCurrent, "ALTER TABLE sessions ADD COLUMN unexpected TEXT"},
		{"extra_view", migrationSchemaCurrent, "CREATE VIEW unexpected AS SELECT id FROM sessions"},
		{"missing_identity", migrationSchemaCurrent, "DELETE FROM observation_store"},
		{"invalid_identity", migrationSchemaCurrent, "PRAGMA ignore_check_constraints=ON; UPDATE observation_store SET incarnation='not-valid'"},
		{"identity_blob", migrationSchemaCurrent, "PRAGMA ignore_check_constraints=ON; UPDATE observation_store SET incarnation=zeroblob(1000000)"},
		{"identity_nul_suffix", migrationSchemaCurrent, "PRAGMA ignore_check_constraints=ON; UPDATE observation_store SET incarnation='0123456789abcdef0123456789abcdef' || char(0) || printf('%1000000s', 'x')"},
		{"extra_identity", migrationSchemaCurrent, "PRAGMA ignore_check_constraints=ON; INSERT INTO observation_store VALUES(2,lower(hex(randomblob(16))))"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := migrationSchemaDB(t, test.state)
			execBaseline(t, db, test.change)
			before, history := migrationCatalog(t, db), migrationHistory(t, db)
			conn, err := db.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			_, inspectErr := inspectMigrationSchema(t.Context(), conn)
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(inspectErr, session.ErrConflict) {
				t.Fatalf("rejection = %v", inspectErr)
			}
			if !reflect.DeepEqual(before, migrationCatalog(t, db)) || !reflect.DeepEqual(history, migrationHistory(t, db)) {
				t.Fatal("rejection changed catalog or history")
			}
		})
	}
}

func TestMigrationSchemaProjectionBounds(t *testing.T) {
	t.Run("timestamp", func(t *testing.T) {
		db := migrationSchemaDB(t, migrationSchemaBootstrap)
		execBaseline(t, db, "UPDATE eino_agent_goose_version SET tstamp='0000-01-01 00:00:00' || char(0) || printf('%1000000s', 'x')")
		var id, version, applied sql.NullInt64
		var timestamp sql.NullString
		var idType, versionType, appliedType, timeType string
		if err := db.QueryRowContext(t.Context(), migrationHistoryQuery).Scan(
			&id, &version, &applied, &timestamp, &idType, &versionType, &appliedType, &timeType); err != nil {
			t.Fatal(err)
		}
		if timestamp.Valid {
			t.Fatal("malformed timestamp was materialized by SQL projection")
		}
	})
	t.Run("incarnation", func(t *testing.T) {
		db := migrationSchemaDB(t, migrationSchemaCurrent)
		execBaseline(t, db, "PRAGMA ignore_check_constraints=ON; UPDATE observation_store SET incarnation='0123456789abcdef0123456789abcdef' || char(0) || printf('%1000000s', 'x')")
		var singleton sql.NullInt64
		var incarnation sql.NullString
		var singletonType, incarnationType string
		if err := db.QueryRowContext(t.Context(), migrationIncarnationQuery).Scan(
			&singleton, &incarnation, &singletonType, &incarnationType); err != nil {
			t.Fatal(err)
		}
		if incarnation.Valid {
			t.Fatal("malformed incarnation was materialized by SQL projection")
		}
	})
}

func TestMigrationSchemaReadOnly(t *testing.T) {
	for _, journal := range []string{"DELETE", "WAL"} {
		t.Run(journal, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "readonly.db")
			db := openBaselinePool(t, path)
			prepareMigrationSchema(t, db, migrationSchemaCurrent)
			execBaseline(t, db, "PRAGMA journal_mode="+journal)
			writer, err := db.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _, _ = writer.ExecContext(context.Background(), "ROLLBACK"); _ = writer.Close() }()
			if _, err := writer.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
				t.Fatal(err)
			}
			uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&_txlock=immediate&_pragma=query_only(1)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"}
			reader, err := sql.Open("sqlite", uri.String())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			reader.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			conn, err := reader.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			state, inspectErr := inspectMigrationSchema(ctx, conn)
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if inspectErr != nil || state != migrationSchemaCurrent {
				t.Fatalf("read-only inspection took write intent: %d, %v", state, inspectErr)
			}
			for pragma, want := range map[string]string{"query_only": "1", "foreign_keys": "1", "busy_timeout": "5000"} {
				if got := baselineStrings(t, reader, "PRAGMA "+pragma); len(got) != 1 || got[0] != want {
					t.Fatalf("host %s changed: %v", pragma, got)
				}
			}
		})
	}
	t.Run("temporary_shadow", func(t *testing.T) {
		db := migrationSchemaDB(t, migrationSchemaCurrent)
		db.SetMaxOpenConns(1)
		execBaseline(t, db, "CREATE TEMP TABLE eino_agent_goose_version(id TEXT); CREATE TEMP TABLE observation_store(singleton TEXT)")
		assertMigrationSchema(t, db, migrationSchemaCurrent)
	})
	t.Run("canceled_and_closed", func(t *testing.T) {
		db := migrationSchemaDB(t, migrationSchemaCurrent)
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := inspectMigrationSchema(ctx, conn); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled inspection: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := inspectMigrationSchema(t.Context(), conn); !errors.Is(err, sql.ErrConnDone) {
			t.Fatalf("closed connection: %v", err)
		}
		if _, err := inspectMigrationSchema(t.Context(), nil); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("nil connection: %v", err)
		}
		assertMigrationSchema(t, db, migrationSchemaCurrent)
	})
}

func migrationSchemaDB(t *testing.T, state migrationSchemaState) *sql.DB {
	t.Helper()
	db := openBaselinePool(t, filepath.Join(t.TempDir(), "migration.db"))
	prepareMigrationSchema(t, db, state)
	return db
}

func prepareMigrationSchema(t *testing.T, db *sql.DB, state migrationSchemaState) {
	t.Helper()
	if state == migrationSchemaEmpty {
		return
	}
	// Generate the history shape through the actual pinned Goose store, not a
	// hand-maintained SQL copy. These fixture-only writes never run in inspection.
	history, err := database.NewStore(database.DialectSQLite3, "eino_agent_goose_version")
	if err != nil {
		t.Fatal(err)
	}
	if err := history.CreateVersionTable(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := history.Insert(t.Context(), db, database.InsertRequest{Version: 0}); err != nil {
		t.Fatal(err)
	}
	if state == migrationSchemaBootstrap {
		return
	}
	files := fstest.MapFS{"00001_initial.sql": &fstest.MapFile{Data: baselineSQL}}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, files, goose.WithTableName("eino_agent_goose_version"), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ApplyVersion(t.Context(), 1, true); err != nil {
		t.Fatal(err)
	}
	// Provider.Close would close the fixture's host-owned pool.
}

func migrationCatalog(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	got, err := migrationSchemaCatalog(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func assertMigrationSchema(t *testing.T, db *sql.DB, want migrationSchemaState) {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, inspectErr := inspectMigrationSchema(t.Context(), conn)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if inspectErr != nil || got != want {
		t.Fatalf("schema state=%d want=%d: %v", got, want, inspectErr)
	}
}

func migrationHistory(t *testing.T, db *sql.DB) []string {
	t.Helper()
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM main.sqlite_schema WHERE type='table' AND name='eino_agent_goose_version'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		return nil
	}
	rows, err := db.QueryContext(t.Context(), "SELECT * FROM main.eino_agent_goose_version ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprintf("%#v", values))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
