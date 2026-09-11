package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/stdlib"
)

// migrationContext hides the parent's deadline from database/sql. A normal
// WithTimeout can make pgx return ErrBadConn and release a still-live pool
// resource before its transport is closed; this guard closes first.
//
// Ownership model. The bound *sql.Conn may be used through database/sql -
// Query, Exec, Raw, Close, anything - ONLY by the goroutine that bound it
// (the goose/locker goroutine: SessionLock, unlockAndRestore, SessionUnlock).
// database/sql's Conn.grabConn reads Conn.done before taking Conn.closemu
// for read; Conn.close sets done before taking closemu for write and nils
// out the driverConn. That gap exists between ANY grab (Query, Exec, Raw)
// and ANY close (an explicit Close, or database/sql's own release after a
// statement returns driver.ErrBadConn) on the same *sql.Conn - not just
// between two discards. If a grab and a close for the same *sql.Conn can run
// on different goroutines, one of them can resume with a nil driverConn and
// panic (github.com/mattsp1290/eino-agent issue eino-agent-f3i). A mutex
// around discardPGXConnection alone cannot prevent this: it only serializes
// discard against discard, and the owner's ordinary queries (setConfig,
// inspectSchema, probeGooseLock, the delegate's lock query, goose's own
// migration statements) never take that mutex, so they can still race a
// foreign goroutine's discard in either direction.
//
// A parent cancellation or an acquire/cleanup timeout runs on a goroutine
// that does NOT own the bound *sql.Conn: the callback registered with
// context.AfterFunc in newMigrationContext, or a time.AfterFunc timer in
// SessionLock and unlockAndRestore. That goroutine calls interrupt, which
// closes the bound *net.Conn transport (unblocking any in-flight or next
// I/O on it) and records the cause under m.mu, via closeTransportLocked. It
// never calls conn.Raw or any other database/sql method. Only two functions
// may ever call m.cancel, which makes m.Context - the pgx-visible context -
// Done: take, always safe because the owner has taken over whatever was
// bound by the time it runs; and cancelIfUnbound, interrupt's own delegate
// for the sole case where cancelling from the non-owner side is safe -
// nothing bound yet. Canceling a context pgx can still observe matters for a
// BOUND connection: pgx
// v5.10.0's pgconn returns an already-canceled context as a SafeToRetry
// error before attempting any I/O (pgconn.go:contextAlreadyDoneError),
// which stdlib turns into driver.ErrBadConn without the underlying pgx
// connection's IsClosed ever becoming true. stdlib's ResetSession, and
// pgxpool.Conn.Release for a pool-backed *sql.DB, only destroy a connection
// whose IsClosed() is true (or that is busy, or mid-transaction) - so
// canceling the context ahead of the owner's discard could hand a
// dead-socket connection back to the host pool looking reusable. Closing
// only the transport avoids that: the owner's in-flight or next I/O fails
// with a real write/read error, which pgx maps to IsClosed() == true before
// database/sql ever sees ErrBadConn. While unbound there is no pgx
// connection whose IsClosed() could lie - and bind refuses once a cause is
// recorded (see bind) - so canceling m.Context in that case is safe, and is
// the only way a parent cancellation reaches goose's own pool wait
// (p.db.Conn(m.Context), called before SessionLock ever binds anything).
//
// m.mu is held across socket.Close() inside interrupt. That is fine for the
// plain TCP transport pgx uses here: net.Conn.Close on a TCP socket evicts
// any blocked reader/writer and returns promptly. It would not be fine for
// a transport whose Close can block on its own protocol teardown (a TLS
// close-notify write can hold its deadline, up to a few seconds) - do not
// add one under this lock without moving the close outside m.mu first.
type migrationContext struct {
	context.Context
	cancel context.CancelCauseFunc
	stopFn func() bool

	mu          sync.Mutex
	socket      net.Conn
	sqlConn     *sql.Conn
	interrupted error // recorded by interrupt; the owner must discard once set
	closed      error
	stopped     bool
}

func newMigrationContext(parent context.Context) *migrationContext {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	m := &migrationContext{Context: ctx, cancel: cancel}
	m.stopFn = context.AfterFunc(parent, func() {
		m.interrupt(parent.Err())
	})
	if err := parent.Err(); err != nil {
		m.interrupt(err)
	}
	return m
}

// bind associates conn's transport with m so interrupt can close it. Raw
// runs its callback synchronously on the calling goroutine before returning,
// so m.mu below is acquired only after Raw's own internal locks are already
// held by this same goroutine.
//
// bind refuses both when m.interrupted is set and, independently, when
// m.Context is already Done. The two are not the same check: take clears
// m.interrupted once it has reported it (see take), so after a plain
// discard/stop m.interrupted alone no longer reflects that this context was
// ever interrupted, even though cancelIfUnbound already canceled m.Context.
// No current caller re-binds after a discard, so this is a structural
// guarantee rather than one a live bug depends on today: binding a *sql.Conn
// to an already-canceled m.Context would hand pgx a context it returns
// SafeToRetry for before any I/O, with IsClosed() still false - the exact
// hazard this type exists to prevent.
func (m *migrationContext) bind(conn *sql.Conn) error {
	if cause := m.interruptedCause(); cause != nil {
		return cause
	}
	if cause := context.Cause(m.Context); cause != nil {
		return cause
	}
	if conn == nil {
		return errors.New("postgres migration context requires a connection")
	}
	return conn.Raw(func(raw any) error {
		stdlibConn, ok := raw.(*stdlib.Conn)
		if !ok || stdlibConn.Conn() == nil || stdlibConn.Conn().PgConn() == nil {
			return errors.New("postgres migration context requires the pgx stdlib driver")
		}
		socket := stdlibConn.Conn().PgConn().Conn()
		if socket == nil {
			return errors.New("postgres migration context has no transport")
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.interrupted != nil {
			return m.interrupted
		}
		if cause := context.Cause(m.Context); cause != nil {
			return cause
		}
		if m.stopped || m.socket != nil {
			return errors.New("postgres migration context is already bound")
		}
		m.socket, m.sqlConn = socket, conn
		return nil
	})
}

func (m *migrationContext) interruptedCause() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.interrupted
}

// stop disarms the parent-cancellation watcher and this context's binding,
// and always cancels m.Context - the pgx-visible context - via take, since
// nothing may still be relying on it once the caller is done. The caller
// must be the goroutine that owns the bound *sql.Conn (SessionLock's own
// defer, or SessionUnlock before it hands conn back to goose). If interrupt
// ever recorded a cause while a *sql.Conn was bound, stop discards it - like
// discard - and folds that cause (and any transport-close error interrupt
// recorded) into the returned error, so a connection whose transport
// interrupt closed is never handed back, to goose or to the pool, looking
// valid. If nothing was ever interrupted, stop leaves the still-good
// connection alone for the caller to keep using or return. Calling stop or
// discard again after the first one that actually released something is a
// safe no-op that reports nothing further - the cause is reported once.
func (m *migrationContext) stop() error {
	if m.stopFn != nil {
		m.stopFn()
	}
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	socket, conn, report := m.take()
	if report == nil {
		return nil
	}
	return errors.Join(report, releaseConn(socket, conn))
}

// interrupt runs on a goroutine that does not own the bound *sql.Conn: the
// parent-context watcher registered in newMigrationContext, or an
// acquire/cleanup time.AfterFunc timer in SessionLock/unlockAndRestore. It
// records cause and closes the transport (via closeTransportLocked) for the
// owner to discard and report, then - only when closeTransportLocked found
// nothing bound at all - cancels m.Context through cancelIfUnbound. See the
// type doc for why it must never call conn.Raw, any other database/sql
// method, or cancel m.Context while a *sql.Conn is bound: that split is why
// this function delegates both of those steps to separately named,
// independently checked helpers rather than doing them inline.
func (m *migrationContext) interrupt(cause error) {
	m.mu.Lock()
	if m.stopped || m.interrupted != nil {
		m.mu.Unlock()
		return
	}
	m.interrupted = cause
	unbound := m.closeTransportLocked()
	if hook := interruptObserved.Load(); hook != nil {
		(*hook)()
	}
	m.mu.Unlock()
	m.cancelIfUnbound(cause, unbound)
}

// closeTransportLocked closes m.socket, if bound, and reports whether
// nothing was bound at all - neither a socket nor a *sql.Conn. Only that
// unbound case is safe to cancel m.Context for afterward (see
// cancelIfUnbound). Callers must hold m.mu; this never touches database/sql.
func (m *migrationContext) closeTransportLocked() (unbound bool) {
	unbound = m.socket == nil && m.sqlConn == nil
	if m.socket != nil {
		if err := m.socket.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			m.closed = errors.Join(m.closed, err)
		}
		m.socket = nil
	}
	return unbound
}

// cancelIfUnbound cancels m.Context - the pgx-visible context - only when
// unbound is true: that is the only way a parent cancellation reaches
// goose's own connection-pool wait before SessionLock has bound anything,
// and it is safe only then, because bind refuses once m.interrupted is set,
// so no *sql.Conn can subsequently be bound to a context this call
// canceled. interrupt and take are the only two places that may call
// m.cancel; this is interrupt's half of that rule, named and isolated so it
// can be checked independently of interrupt's other, non-owner-safe work.
func (m *migrationContext) cancelIfUnbound(cause error, unbound bool) {
	if unbound {
		m.cancel(cause)
	}
}

// err reports the interrupt cause and any transport-close error recorded so
// far, without consuming them the way take does: calling err before stop or
// discard is safe and does not affect what they go on to report. Once
// stop/discard has run, err returns nil for whatever it already reported.
func (m *migrationContext) err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return errors.Join(m.interrupted, m.closed)
}

// discard performs the owner-side teardown of the bound *sql.Conn
// unconditionally, regardless of whether interrupt ever fired. The caller
// must be the goroutine that called bind: SessionLock's own error paths, or
// unlockAndRestore. It is a safe no-op when nothing is currently bound.
func (m *migrationContext) discard() error {
	socket, conn, report := m.take()
	return errors.Join(report, releaseConn(socket, conn))
}

// take clears the bound socket and *sql.Conn, and the recorded interrupt
// cause and transport-close error, all under m.mu, and returns the socket
// and conn together with report: errors.Join of that cause and error. take
// clears the cause and error as well as the socket/conn - not just the
// socket/conn - so a second call (discard after stop, or stop after
// discard) sees nil cause and nil closed and returns a nil report: the
// cause is reported, and the connection discarded, exactly once no matter
// how many times stop/discard/err are called afterward. take always cancels
// m.Context - the pgx-visible context - since the owner has taken sole
// responsibility for whatever was (or was not) bound. It never touches the
// network or calls into database/sql itself: callers do that only after
// take returns, never while holding m.mu.
func (m *migrationContext) take() (socket net.Conn, conn *sql.Conn, report error) {
	m.mu.Lock()
	socket, conn = m.socket, m.sqlConn
	cause, closed := m.interrupted, m.closed
	m.socket, m.sqlConn = nil, nil
	m.interrupted, m.closed = nil, nil
	m.mu.Unlock()
	m.cancel(cause)
	return socket, conn, errors.Join(cause, closed)
}

// releaseConn closes socket, if not already closed, and discards conn, if
// bound. Callers must run this on the goroutine that owns conn - never from
// interrupt's goroutine - and never while holding migrationContext.mu.
func releaseConn(socket net.Conn, conn *sql.Conn) error {
	var err error
	if socket != nil {
		if closeErr := socket.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = closeErr
		}
	}
	if conn != nil {
		err = errors.Join(err, discardPGXConnection(conn))
	}
	return err
}

// interruptObserved, when non-nil, is called synchronously by interrupt
// immediately after it closes the bound transport, while still holding
// m.mu, before the interrupting goroutine returns. Tests use it to hold the
// interrupt path open for a controlled window while forcing the owning
// goroutine to issue SQL on the same *sql.Conn, so that SQL genuinely races
// interrupt's transport close instead of relying on scheduling luck - the
// interleaving behind github.com/mattsp1290/eino-agent issue eino-agent-f3i.
// Stored behind an atomic.Pointer so a test can set and clear it without a
// data race against a concurrent interrupt goroutine. It is a single global
// slot, not scoped per test or per connection: tests that install it must
// not run under t.Parallel with each other, and must restore the previous
// value (typically via t.Cleanup) when done. Nil in production.
var interruptObserved atomic.Pointer[func()]
