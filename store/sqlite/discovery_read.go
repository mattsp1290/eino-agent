package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"time"

	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"

	modernsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// SQLite's busy handler can sleep through a canceled Go context. Pin the
// connection and temporarily disable that handler, retrying whole read views
// in Go instead. Other store operations retain their original busy timeout.
func (sqliteDialect) Read(ctx context.Context, pool *sql.DB, read func(sqlstore.SQLReader) error) (err error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	var timeout int
	if err = conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		return err
	}
	// Restore even if cancellation races the statement that disables the handler.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, restoreErr := conn.ExecContext(cleanup, "PRAGMA busy_timeout="+strconv.Itoa(timeout))
		if restoreErr != nil {
			// A connection with unknown settings must not return to the pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		err = errors.Join(err, restoreErr)
	}()
	if _, err = conn.ExecContext(ctx, "PRAGMA busy_timeout=0"); err != nil {
		return err
	}
	return retryDiscoveryRead(ctx, time.Duration(timeout)*time.Millisecond, func() error {
		return discoveryReadAttempt(ctx, conn, read)
	})
}

func discoveryReadAttempt(ctx context.Context, conn *sql.Conn, read func(sqlstore.SQLReader) error) (err error) {
	// Own rollback explicitly. database/sql's cancellation rollback can discard
	// the pinned connection before its original busy handler is restored.
	// ReadOnly forces modernc to use deferred BEGIN even if the host selected
	// _txlock=immediate. The busy handler stays disabled through tx cleanup.
	tx, err := conn.BeginTx(context.WithoutCancel(ctx), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			err = errors.Join(err, rollbackErr)
		}
	}()
	if err = read(tx); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		// A failed driver commit may also have failed its implicit rollback.
		// Discard rather than reuse a connection with unknown transaction state.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	return err
}

func retryDiscoveryRead(ctx context.Context, timeout time.Duration, read func() error) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	retry := time.NewTicker(5 * time.Millisecond)
	defer retry.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := read()
		var sqliteErr *modernsqlite.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_BUSY || timeout <= 0 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return err
		case <-retry.C:
		}
	}
}
