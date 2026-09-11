package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"sync"

	"github.com/jackc/pgx/v5/stdlib"
)

// migrationContext hides the parent's deadline from database/sql. A normal
// WithTimeout can make pgx return ErrBadConn and release a still-live pool
// resource before its transport is closed; this guard closes first.
type migrationContext struct {
	context.Context
	cancel context.CancelCauseFunc
	stopFn func() bool

	mu      sync.Mutex
	socket  net.Conn
	sqlConn *sql.Conn
	closed  error
	stopped bool
}

func newMigrationContext(parent context.Context) *migrationContext {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	m := &migrationContext{Context: ctx, cancel: cancel}
	m.stopFn = context.AfterFunc(parent, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.stopped {
			return
		}
		m.closeLocked()
		m.cancel(parent.Err())
	})
	if err := parent.Err(); err != nil {
		m.abort(err)
	}
	return m
}

func (m *migrationContext) bind(conn *sql.Conn) error {
	if err := context.Cause(m.Context); err != nil {
		return err
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
		if err := context.Cause(m.Context); err != nil {
			return err
		}
		if m.stopped || m.socket != nil {
			return errors.New("postgres migration context is already bound")
		}
		m.socket, m.sqlConn = socket, conn
		return nil
	})
}

func (m *migrationContext) stop() {
	if m.stopFn != nil {
		m.stopFn()
	}
	m.mu.Lock()
	m.stopped = true
	m.socket, m.sqlConn = nil, nil
	m.mu.Unlock()
}

func (m *migrationContext) abort(err error) {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.closeLocked()
	m.cancel(err)
	m.mu.Unlock()
	if m.stopFn != nil {
		m.stopFn()
	}
}

func (m *migrationContext) err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return errors.Join(context.Cause(m.Context), m.closed)
}

// discard closes the transport and discards the backend connection bound via
// bind, exactly like abort's cleanup does, but for callers that need to
// force a discard themselves (e.g. after the delegate reports an error) while
// leaving the context and stopFn registration alone.
//
// It coordinates with abort through m.mu so the two paths can never both call
// discardPGXConnection on the same *sql.Conn concurrently: database/sql's
// Conn.grabConn checks Conn.done before taking Conn.closemu for read, and
// Conn.close sets done before taking Conn.closemu for write and nils out its
// driverConn. A grabConn that reads done as false just before a concurrent
// close's CAS, then blocks on closemu until that close finishes, resumes with
// a nil driverConn and a nil error — and Conn.Raw dereferences it
// unconditionally, panicking. Routing every discard of the bound connection
// through this mutex ensures only one goroutine ever races into that window;
// the other observes the connection already cleared and does nothing.
func (m *migrationContext) discard() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dropLocked()
}

// closeLocked drops the bound transport/connection for abort and folds any
// resulting error into m.closed for a later err() call. Callers must hold m.mu.
func (m *migrationContext) closeLocked() {
	m.closed = errors.Join(m.closed, m.dropLocked())
}

// dropLocked closes the bound socket and discards the backend *sql.Conn
// exactly once, then clears both fields so any later call - from abort or
// discard, on either goroutine - is a no-op. Callers must hold m.mu.
func (m *migrationContext) dropLocked() error {
	socket, sqlConn := m.socket, m.sqlConn
	m.socket, m.sqlConn = nil, nil
	if socket == nil && sqlConn == nil {
		return nil
	}
	var err error
	if socket != nil {
		if closeErr := socket.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = closeErr
		}
	}
	if sqlConn != nil {
		err = errors.Join(err, discardPGXConnection(sqlConn))
	}
	return err
}
