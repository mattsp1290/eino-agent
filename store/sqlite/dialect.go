package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"sync"
	"time"

	"gorm.io/gorm"
	modernsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"
)

type sqliteDialect struct{}

func (sqliteDialect) IndexHint(index string) string   { return " INDEXED BY " + index }
func (sqliteDialect) ByteLength(column string) string { return "length(CAST(" + column + " AS BLOB))" }
func (sqliteDialect) InvalidScalar(column string, binary bool) string {
	kind := "text"
	if binary {
		kind = "blob"
	}
	return "typeof(" + column + ") != '" + kind + "'"
}

func (sqliteDialect) ClockSQL() string {
	return "CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)"
}
func (sqliteDialect) LockRun(db *gorm.DB) *gorm.DB { return db }
func (sqliteDialect) MapError(err error) error {
	var sqliteErr *modernsqlite.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqlite3.SQLITE_CONSTRAINT {
		return session.ErrConflict
	}
	return err
}

func (sqliteDialect) Begin(ctx context.Context, db *sql.DB) (sqlstore.Transaction, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var timeout int
	if err = conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	_, err = conn.ExecContext(ctx, "PRAGMA busy_timeout=0")
	if err == nil {
		err = retryDiscoveryRead(ctx, time.Duration(timeout)*time.Millisecond, func() error {
			_, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE")
			return err
		})
	}
	if err != nil {
		// Cancellation can race BEGIN's completion. Discard its physical connection.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return nil, errors.Join(err, conn.Close())
	}
	return &writerTransaction{Conn: conn, active: true, timeout: timeout, context: ctx}, nil
}

// A manually started SQLite transaction stays on one connection until cleanup.
type writerTransaction struct {
	*sql.Conn
	mu      sync.Mutex
	active  bool
	timeout int
	context context.Context
}

func (t *writerTransaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	err := retryDiscoveryRead(ctx, time.Duration(t.timeout)*time.Millisecond, func() error {
		_, err := t.ExecContext(ctx, "COMMIT")
		return err
	})
	if err == nil {
		t.active = false
	}
	return err
}
func (t *writerTransaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.active {
		return nil
	}
	_, err := t.ExecContext(ctx, "ROLLBACK")
	if err == nil {
		t.active = false
	}
	return err
}
func (t *writerTransaction) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active {
		_ = t.Raw(func(any) error { return driver.ErrBadConn })
		return t.Conn.Close()
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(t.context), 5*time.Second)
	defer cancel()
	_, restoreErr := t.ExecContext(cleanup, "PRAGMA busy_timeout="+strconv.Itoa(t.timeout))
	if restoreErr != nil {
		_ = t.Raw(func(any) error { return driver.ErrBadConn })
	}
	return errors.Join(restoreErr, t.Conn.Close())
}
