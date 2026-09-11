package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
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
	// Disarm before returning so a late cancellation cannot touch its next
	// borrower. On success l.operation stays bound - and its parent-
	// cancellation watcher armed - through goose's migration run; only
	// SessionUnlock (via stop) may discard it after that, on this same
	// goroutine.
	defer func() {
		if err != nil {
			err = errors.Join(err, l.operation.stop())
		}
	}()
	if err := l.operation.bind(conn); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.AfterFunc(acquireTimeout, func() { l.operation.interrupt(context.DeadlineExceeded) })
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
		return errors.Join(err, l.operation.discard())
	}

	if err := l.delegate.SessionLock(acquireCtx, conn); err != nil {
		// Acquisition may have succeeded before the delegate reported an
		// error. Closing the backend is the only unambiguous cleanup in that
		// case. l.operation.discard runs on this goroutine, the one that
		// owns conn, so it can never race a concurrent interrupt (the
		// acquireTimeout timer above, or the caller's context being
		// canceled) for database/sql's grabConn/close on this *sql.Conn: an
		// interrupt only ever closes the transport and records a cause, it
		// never calls conn.Raw or cancels the pgx-visible context. See
		// migration_context.go's ownership-model doc for why that split
		// closes the panic window in both directions.
		return errors.Join(err, l.operation.discard())
	}
	if _, err := inspectSchema(acquireCtx, conn); err != nil {
		stopErr := l.operation.stop()
		cleanupErr := l.unlockAndRestore(conn, original, ctx)
		return errors.Join(err, stopErr, cleanupErr)
	}
	l.original = original
	l.saved = true
	return nil
}

func (l *validatingSessionLocker) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	// Stop l.operation first: it was bound (and its watcher armed) by
	// SessionLock and has stayed that way through goose's migration run. If
	// a parent cancellation or the acquireTimeout interrupted it in the
	// meantime, stop discards conn here, on this goroutine, before
	// unlockAndRestore binds its own cleanup context to the same conn.
	stopErr := l.operation.stop()
	if !l.saved {
		return errors.Join(errors.New("postgres session lock has no saved search_path"), stopErr)
	}
	original := l.original
	l.saved = false
	return errors.Join(l.unlockAndRestore(conn, original, ctx), stopErr)
}

func (l *validatingSessionLocker) unlockAndRestore(conn *sql.Conn, original sessionSettings, parent context.Context) (err error) {
	cleanup := newMigrationContext(context.WithoutCancel(parent))
	defer func() {
		err = errors.Join(err, cleanup.stop())
	}()
	if err := cleanup.bind(conn); err != nil {
		return err
	}
	timer := time.AfterFunc(cleanupTimeout, func() { cleanup.interrupt(context.DeadlineExceeded) })
	defer timer.Stop()
	cleanupCtx := cleanup.Context
	unlockErr := l.delegate.SessionUnlock(cleanupCtx, conn)
	if unlockErr != nil {
		// The delegate may have returned without releasing its lock. Do
		// not issue another SQL call with a possibly exhausted context: pgx
		// can turn a SafeToRetry cancellation into driver.ErrBadConn and
		// release the native connection before we discard it ourselves.
		//
		// Call discard before err: a concurrent cleanupTimeout interrupt can
		// still be recording its cause when unlockErr comes back, and
		// reading err() first could snapshot a stale cleanup.interrupted.
		// discard runs on this goroutine, the one that owns conn, so it
		// never races interrupt for access to conn.Raw - see
		// migration_context.go.
		discardErr := cleanup.discard()
		return errors.Join(unlockErr, cleanup.err(), discardErr)
	}
	probeErr := probeGooseLock(cleanupCtx, conn)
	if probeErr != nil {
		discardErr := cleanup.discard()
		return errors.Join(probeErr, cleanup.err(), discardErr)
	}
	restoreErr := restoreSettings(cleanupCtx, conn, original)
	if restoreErr == nil {
		return nil
	}
	discardErr := cleanup.discard()
	return errors.Join(restoreErr, cleanup.err(), discardErr)
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

// discardObserved, when non-nil, is called with the *sql.Conn being
// discarded and true just before this function's body runs, and with the
// same conn and false just before it returns. It exists so tests can observe
// discardPGXConnection activity, keyed by connection: this function has two
// legitimate callers in this package (migrationContext's owner-side discard
// and stop, and dialect.go's discardAndClose for a plain transaction), and
// per-connection keying keeps a test for one from miscounting the other.
// After the ownership-model fix (see migration_context.go), a migration
// lock's discard is only ever called by the goroutine that owns the
// *sql.Conn being discarded, never from an interrupting goroutine, so two
// calls for the same connection can no longer overlap in time. Regression
// tests use this hook to confirm that invariant and to count how many
// discards actually happened, so a test cannot pass without exercising the
// path it claims to test. Stored behind an atomic.Pointer so a test can set
// and clear it without racing a concurrent discard goroutine. Nil in
// production, where the hook costs one atomic load on a path that only runs
// for errors and cleanup.
var discardObserved atomic.Pointer[func(conn *sql.Conn, entering bool)]

func discardPGXConnection(conn *sql.Conn) error {
	if hook := discardObserved.Load(); hook != nil {
		(*hook)(conn, true)
		defer (*hook)(conn, false)
	}
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
		if native.IsClosed() {
			// A watcher goroutine may already have closed the raw transport
			// (see migrationContext.interrupt) before pgx observed the I/O
			// failure that would normally set this. Calling Close again
			// would attempt a graceful shutdown message over an
			// already-dead socket and report a misleading transport error
			// for what is already a successful discard.
			nativeClosed = true
			return driver.ErrBadConn
		}
		closeErr = native.Close(closeCtx)
		nativeClosed = native.IsClosed()
		// Returning ErrBadConn here makes database/sql discard this wrapper. The
		// underlying pgx connection was closed above for pool-backed drivers.
		return driver.ErrBadConn
	})
	if rawErr != nil && errors.Is(rawErr, sql.ErrConnDone) {
		// database/sql already released this *sql.Conn - typically because
		// the owner's own statement just failed with driver.ErrBadConn and
		// database/sql closed it before this call reached Raw. The callback
		// above never ran, so nativeClosed was never observed; that is not a
		// failure to discard; the connection database/sql cared about is
		// already gone, so report nothing further.
		return closeErr
	}
	if rawErr != nil && !errors.Is(rawErr, driver.ErrBadConn) {
		closeErr = errors.Join(closeErr, rawErr)
	}
	if !nativeClosed {
		closeErr = errors.Join(closeErr, errors.New("postgres native connection did not close"))
	}
	return closeErr
}
