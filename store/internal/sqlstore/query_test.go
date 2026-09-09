package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	_ "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
)

type queryBindTrace struct {
	calls    int
	values   [][]byte
	nonBytes int
}

// numberedDialector makes a raw GORM expression observably use the dialect
// binder. SQLite accepts both ? and $N, so the call trace is part of the test.
type numberedDialector struct {
	gorm.Dialector
	trace *queryBindTrace
}

func (d numberedDialector) Initialize(db *gorm.DB) error {
	if err := d.Dialector.Initialize(db); err != nil {
		return err
	}
	// The SQLite dialector's INSERT clause builder writes only stmt.Table and
	// drops a schema-qualified TableExpr. The standard GORM builder preserves
	// it, matching the PostgreSQL path this shared store also supports.
	delete(db.ClauseBuilders, "INSERT")
	return nil
}

func (d numberedDialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, value interface{}) {
	d.trace.calls++
	if b, ok := value.([]byte); ok {
		d.trace.values = append(d.trace.values, append([]byte(nil), b...))
	} else {
		d.trace.nonBytes++
	}
	_ = writer.WriteByte('$')
	_, _ = writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

type queryDialectTrace struct {
	reads     int
	begins    int
	commits   int
	rollbacks int
}

// queryDialect is deliberately complete: embedding an uninitialized Dialect
// would make an accidental call to an unrelated policy panic in this test.
type queryDialect struct{ trace *queryDialectTrace }

func (d queryDialect) Begin(ctx context.Context, db *sql.DB) (Transaction, error) {
	d.trace.begins++
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &queryTestTransaction{Tx: tx, trace: d.trace}, nil
}

func (d queryDialect) Read(ctx context.Context, db *sql.DB, fn func(SQLReader) error) error {
	d.trace.reads++
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return fn(conn)
}

func (queryDialect) ClockSQL() string                  { return "CURRENT_TIMESTAMP" }
func (queryDialect) LockRows(db *gorm.DB) *gorm.DB     { return db }
func (queryDialect) MapError(err error) error          { return err }
func (queryDialect) IndexHint(string) string           { return "" }
func (queryDialect) ByteLength(column string) string   { return "length(CAST(" + column + " AS BLOB))" }
func (queryDialect) InvalidScalar(string, bool) string { return "0" }

type queryTestTransaction struct {
	*sql.Tx
	trace *queryDialectTrace
}

func (t *queryTestTransaction) Commit(context.Context) error {
	t.trace.commits++
	return t.Tx.Commit()
}
func (t *queryTestTransaction) Rollback(context.Context) error {
	t.trace.rollbacks++
	return t.Tx.Rollback()
}
func (t *queryTestTransaction) Close() error { return nil }

func openQueryTestStore(t *testing.T, naming schema.NamingStrategy, bind *queryBindTrace, dialect *queryDialectTrace) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	orm, err := gorm.Open(numberedDialector{
		Dialector: gormsqlite.New(gormsqlite.Config{Conn: db, DriverName: "sqlite"}),
		trace:     bind,
	}, &gorm.Config{DisableAutomaticPing: true, Logger: logger.Discard, NamingStrategy: naming})
	if err != nil {
		t.Fatal(err)
	}
	return New(orm, db, queryDialect{trace: dialect}), db
}

func execQueryTestSQL(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestQualifiedTableNamesIgnoreTemporaryShadows(t *testing.T) {
	bind := &queryBindTrace{}
	trace := &queryDialectTrace{}
	st, db := openQueryTestStore(t, schema.NamingStrategy{TablePrefix: "main.", SingularTable: true}, bind, trace)
	const schemaSQL = `
CREATE TABLE main.observation_store (singleton INTEGER PRIMARY KEY, incarnation TEXT NOT NULL);
CREATE TABLE main.observation_revisions (session_id BLOB PRIMARY KEY, revision INTEGER NOT NULL);
CREATE TABLE main.sessions (row_key INTEGER PRIMARY KEY, id BLOB NOT NULL UNIQUE, record BLOB NOT NULL, workspace_id BLOB NOT NULL, title BLOB NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE INDEX main.sessions_workspace_created_idx ON sessions(workspace_id, created_at, id);`
	for _, statement := range strings.Split(strings.TrimSpace(schemaSQL), ";") {
		execQueryTestSQL(t, db, statement)
	}
	execQueryTestSQL(t, db, `INSERT INTO main.observation_store(singleton, incarnation) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`)
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	mainID := session.ID("created")
	execQueryTestSQL(t, db, `INSERT INTO main.observation_revisions(session_id, revision) VALUES (?, 1)`, []byte(mainID))
	record := session.Session{ID: mainID, WorkspaceID: "workspace", Title: "main", CreatedAt: now, UpdatedAt: now}
	shadow := record
	shadow.Title = "shadow"
	raw, err := json.Marshal(shadow)
	if err != nil {
		t.Fatal(err)
	}
	execQueryTestSQL(t, db, `CREATE TEMP TABLE observation_store (singleton INTEGER PRIMARY KEY, incarnation TEXT NOT NULL)`)
	execQueryTestSQL(t, db, `INSERT INTO temp.observation_store(singleton, incarnation) VALUES (1, 'shadow')`)
	execQueryTestSQL(t, db, `CREATE TEMP TABLE sessions (row_key INTEGER PRIMARY KEY, id BLOB NOT NULL UNIQUE, record BLOB NOT NULL, workspace_id BLOB NOT NULL, title BLOB NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`)
	execQueryTestSQL(t, db, `INSERT INTO temp.sessions(id, record, workspace_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, []byte(mainID), raw, []byte(shadow.WorkspaceID), []byte(shadow.Title), TimeText(now), TimeText(now))

	created, err := st.CreateSession(t.Context(), record)
	if err != nil || !SameRecord(created, record) {
		t.Fatalf("qualified create = %#v, %v", created, err)
	}
	got, err := st.GetSession(t.Context(), mainID)
	if err != nil || !SameRecord(got, record) {
		t.Fatalf("qualified get = %#v, %v", got, err)
	}
	watermark, err := st.ReadObservationRevision(t.Context(), mainID)
	if err != nil || watermark.SessionID != mainID || watermark.Revision != 1 || watermark.StoreID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("qualified observation revision = %#v, %v", watermark, err)
	}
	page, err := st.ListSessions(t.Context(), session.SessionDiscoveryQuery{WorkspaceID: record.WorkspaceID, Limit: 10})
	if err != nil || len(page.Sessions) != 1 || !SameRecord(page.Sessions[0], session.SessionSummary{ID: mainID, WorkspaceID: record.WorkspaceID, Title: record.Title, CreatedAt: now, UpdatedAt: now}) {
		t.Fatalf("qualified discovery = %#v, %v", page, err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM main.sessions WHERE id = ?`, []byte(mainID)).Scan(&count); err != nil || count != 1 {
		t.Fatalf("main row count = %d, %v", count, err)
	}
}

func TestRawQueriesUseDialectBindingOnPinnedReaderAndTransaction(t *testing.T) {
	bind := &queryBindTrace{}
	trace := &queryDialectTrace{}
	st, _ := openQueryTestStore(t, schema.NamingStrategy{SingularTable: true}, bind, trace)
	wantFirst := []byte{0, 1, 255, 2}
	wantSecond := []byte("second-value")
	readErr := st.read(t.Context(), func(tx *Store) error {
		rows, err := tx.query(t.Context(), "SELECT ? AS first, ? AS second", wantFirst, wantSecond)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		if !rows.Next() {
			return errors.New("raw reader returned no row")
		}
		var first, second []byte
		if err := rows.Scan(&first, &second); err != nil {
			return err
		}
		if !bytes.Equal(first, wantFirst) || !bytes.Equal(second, wantSecond) {
			return errors.New("raw reader changed bound bytes")
		}
		return rows.Err()
	})
	if readErr != nil {
		t.Fatal(readErr)
	}
	txErr := st.WithinTx(t.Context(), func(ctx context.Context, view session.Store) error {
		tx := view.(*Store)
		var first, second []byte
		if err := tx.queryRow(ctx, "SELECT ? AS first, ? AS second", wantFirst, wantSecond).Scan(&first, &second); err != nil {
			return err
		}
		if !bytes.Equal(first, wantFirst) || !bytes.Equal(second, wantSecond) {
			return errors.New("raw transaction changed bound bytes")
		}
		return nil
	})
	if txErr != nil {
		t.Fatal(txErr)
	}
	if trace.reads != 1 || trace.begins != 1 || trace.commits != 1 || trace.rollbacks != 0 {
		t.Fatalf("reader/transaction lifecycle = %#v", *trace)
	}
	if bind.calls != 4 || bind.nonBytes != 0 || len(bind.values) != 4 {
		t.Fatalf("dialect binder calls = %#v", *bind)
	}
	for i, value := range bind.values {
		want := wantFirst
		if i%2 == 1 {
			want = wantSecond
		}
		if !bytes.Equal(value, want) {
			t.Fatalf("binder value %d = %v, want %v", i, value, want)
		}
	}
}
