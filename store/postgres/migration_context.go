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
// does two things only: close the bound *net.Conn transport (unblocking any
// in-flight or next I/O on it) and record the cause under m.mu. It never
// calls conn.Raw or any other database/sql method, and it never cancels
// m.Context - the pgx-visible context - itself. Canceling a context pgx can
// still observe matters: pgx v5.10.0's pgconn returns an already-canceled
// context as a SafeToRetry error before attempting any I/O
// (pgconn.go:contextAlreadyDoneError), which stdlib turns into
// driver.ErrBadConn without the underlying pgx connection's IsClosed ever
// becoming true. stdlib's ResetSession, and pgxpool.Conn.Release for a
// pool-backed *sql.DB, only destroy a connection whose IsClosed() is true
// (or that is busy, or mid-transaction) - so canceling the context ahead of
// the owner's discard could hand a dead-socket connection back to the host
// pool looking reusable. Closing only the transport avoids that: the owner's
// in-flight or next I/O fails with a real write/read error, which pgx maps
// to IsClosed() == true before database/sql ever sees ErrBadConn.
//
// Only the owner may cancel m.Context, and only after it has taken over the
// bound socket/conn (see take, called by stop and discard) - by which point
// nothing else can still be relying on it.
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
func (m *migrationContext) bind(conn *sql.Conn) error {
	if cause := m.interruptedCause(); cause != nil {
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

// stop disarms the parent-cancellation watcher and this context's binding.
// The caller must be the goroutine that owns the bound *sql.Conn
// (SessionLock's own defer, or SessionUnlock before it hands conn back to
// goose). If interrupt ever recorded a cause while bound, stop discards the
// *sql.Conn - like discard - and folds that cause into the returned error,
// so a connection whose transport interrupt closed is never handed back,
// to goose or to the pool, looking valid. If nothing was ever interrupted,
// stop only clears the binding: the still-good connection is left for the
// caller to keep using or return. Safe to call more than once; only the
// first call after a bind does any work.
func (m *migrationContext) stop() error {
	if m.stopFn != nil {
		m.stopFn()
	}
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	socket, conn, cause := m.take()
	if cause == nil {
		return nil
	}
	return errors.Join(cause, releaseConn(socket, conn))
}

// interrupt runs on a goroutine that does not own the bound *sql.Conn: the
// parent-context watcher registered in newMigrationContext, or an
// acquire/cleanup time.AfterFunc timer in SessionLock/unlockAndRestore. It
// closes only the transport and records cause for the owner to discard and
// report - see the type doc for why it must never call conn.Raw, any other
// database/sql method, or cancel m.Context itself.
func (m *migrationContext) interrupt(cause error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.interrupted != nil {
		return
	}
	m.interrupted = cause
	if m.socket != nil {
		if err := m.socket.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			m.closed = errors.Join(m.closed, err)
		}
		m.socket = nil
	}
	if hook := interruptObserved.Load(); hook != nil {
		(*hook)()
	}
}

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
	socket, conn, cause := m.take()
	return errors.Join(cause, releaseConn(socket, conn))
}

// take clears the bound socket and *sql.Conn under m.mu, returns them along
// with any cause interrupt recorded, and cancels m.Context - the pgx-visible
// context - now that the owner has taken sole responsibility for what was
// bound. It never touches the network or calls into database/sql itself:
// callers do that only after take returns, never while holding m.mu.
func (m *migrationContext) take() (net.Conn, *sql.Conn, error) {
	m.mu.Lock()
	socket, conn, cause := m.socket, m.sqlConn, m.interrupted
	m.socket, m.sqlConn = nil, nil
	m.mu.Unlock()
	m.cancel(cause)
	return socket, conn, cause
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
// data race against a concurrent interrupt goroutine. Nil in production.
var interruptObserved atomic.Pointer[func()]
