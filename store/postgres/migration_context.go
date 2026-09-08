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

func (m *migrationContext) closeLocked() {
	if m.socket != nil {
		if err := m.socket.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			m.closed = errors.Join(m.closed, err)
		}
	}
	if m.sqlConn != nil {
		m.closed = errors.Join(m.closed, discardPGXConnection(m.sqlConn))
	}
	m.socket, m.sqlConn = nil, nil
}
