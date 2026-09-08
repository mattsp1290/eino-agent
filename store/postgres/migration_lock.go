package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3/lock"
)

const (
	acquireTimeout = 60 * time.Second
	cleanupTimeout = 5 * time.Second
)

// validatingSessionLocker validates the schema after acquiring Goose's
// session advisory lock and keeps all of the work on the supplied connection.
// A locker is created for one migration run, so one saved path is sufficient.
type validatingSessionLocker struct {
	delegate  lock.SessionLocker
	operation *migrationContext

	original sessionSettings
	saved    bool
}

type sessionSettings struct {
	searchPath          string
	quoteAllIdentifiers string
}

var _ lock.SessionLocker = (*validatingSessionLocker)(nil)

func (l *validatingSessionLocker) SessionLock(ctx context.Context, conn *sql.Conn) (err error) {
	// Goose closes the SQL connection immediately when initialization fails.
	// Disarm before returning so a late cancellation cannot touch its next borrower.
	defer func() {
		if err != nil {
			l.operation.stop()
		}
	}()
	if err := l.operation.bind(conn); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.AfterFunc(acquireTimeout, func() { l.operation.abort(context.DeadlineExceeded) })
	defer timer.Stop()
	acquireCtx := l.operation.Context

	var original sessionSettings
	if err := conn.QueryRowContext(acquireCtx, `
SELECT pg_catalog.current_setting('search_path'),
       pg_catalog.current_setting('quote_all_identifiers')`).Scan(
		&original.searchPath, &original.quoteAllIdentifiers); err != nil {
		return err
	}
	if err := setMigrationSettings(acquireCtx, conn); err != nil {
		return errors.Join(err, discardPGXConnection(conn))
	}

	if err := l.delegate.SessionLock(acquireCtx, conn); err != nil {
		// Acquisition may have succeeded before the delegate reported an error.
		// Closing the backend is the only unambiguous cleanup in that case.
		return errors.Join(err, discardPGXConnection(conn))
	}
	if _, err := inspectSchema(acquireCtx, conn); err != nil {
		l.operation.stop()
		cleanupErr := l.unlockAndRestore(conn, original, ctx)
		return errors.Join(err, cleanupErr)
	}
	l.original = original
	l.saved = true
	return nil
}

func (l *validatingSessionLocker) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	l.operation.stop()
	if !l.saved {
		return errors.New("postgres session lock has no saved search_path")
	}
	original := l.original
	l.saved = false
	return l.unlockAndRestore(conn, original, ctx)
}

func (l *validatingSessionLocker) unlockAndRestore(conn *sql.Conn, original sessionSettings, parent context.Context) error {
	cleanup := newMigrationContext(context.WithoutCancel(parent))
	defer cleanup.stop()
	if err := cleanup.bind(conn); err != nil {
		return err
	}
	timer := time.AfterFunc(cleanupTimeout, func() { cleanup.abort(context.DeadlineExceeded) })
	defer timer.Stop()
	cleanupCtx := cleanup.Context
	unlockErr := l.delegate.SessionUnlock(cleanupCtx, conn)
	if unlockErr != nil {
		// The delegate may have returned without releasing its lock. Do
		// not issue another SQL call with a possibly exhausted context: pgx can
		// turn a SafeToRetry cancellation into driver.ErrBadConn and release the
		// native connection before a later Raw callback can discard it.
		return errors.Join(unlockErr, cleanup.err(), discardPGXConnection(conn))
	}
	probeErr := probeGooseLock(cleanupCtx, conn)
	if probeErr != nil {
		return errors.Join(probeErr, cleanup.err(), discardPGXConnection(conn))
	}
	restoreErr := restoreSettings(cleanupCtx, conn, original)
	if restoreErr == nil {
		return nil
	}
	return errors.Join(restoreErr, discardPGXConnection(conn))
}

func setMigrationSettings(ctx context.Context, conn *sql.Conn) error {
	if err := setConfig(ctx, conn, "quote_all_identifiers", "off"); err != nil {
		return err
	}
	return setConfig(ctx, conn, "search_path", "pg_catalog, pg_temp")
}

func restoreSettings(ctx context.Context, conn *sql.Conn, settings sessionSettings) error {
	quoteErr := setConfig(ctx, conn, "quote_all_identifiers", settings.quoteAllIdentifiers)
	if quoteErr != nil {
		return quoteErr
	}
	return setConfig(ctx, conn, "search_path", settings.searchPath)
}

func setConfig(ctx context.Context, conn *sql.Conn, name, value string) error {
	var result string
	return conn.QueryRowContext(ctx,
		`SELECT pg_catalog.set_config($1, $2, false)`, name, value).Scan(&result)
}

func probeGooseLock(ctx context.Context, conn *sql.Conn) error {
	var count int64
	if err := conn.QueryRowContext(ctx, `
SELECT count(*)
FROM pg_catalog.pg_locks
WHERE locktype='advisory' AND granted AND objsubid=1
  AND pid=pg_catalog.pg_backend_pid()
  AND ((classid::bigint << 32) | objid::bigint) = $1`, lock.DefaultLockID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("postgres Goose advisory lock remains held")
	}
	return nil
}

func discardPGXConnection(conn *sql.Conn) error {
	closeCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	var closeErr error
	var nativeClosed bool
	rawErr := conn.Raw(func(raw any) error {
		stdlibConn, ok := raw.(*stdlib.Conn)
		if !ok {
			return errors.New("postgres session locker requires the pgx stdlib driver")
		}
		native := stdlibConn.Conn()
		if native == nil {
			return errors.New("postgres session locker has no native connection")
		}
		closeErr = native.Close(closeCtx)
		nativeClosed = native.IsClosed()
		// Returning ErrBadConn here makes database/sql discard this wrapper. The
		// underlying pgx connection was closed above for pool-backed drivers.
		return driver.ErrBadConn
	})
	if rawErr != nil && !errors.Is(rawErr, driver.ErrBadConn) {
		closeErr = errors.Join(closeErr, rawErr)
	}
	if !nativeClosed {
		closeErr = errors.Join(closeErr, errors.New("postgres native connection did not close"))
	}
	return closeErr
}
