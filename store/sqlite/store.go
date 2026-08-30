package sqlite

import (
	"context"
	"database/sql"
	_ "embed"

	_ "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
)

//go:embed schema.sql
var currentSchema string

// Store persists sessions in SQLite.
type Store struct {
	db *sql.DB
	tx *sql.Tx
}

// Open opens a current SQLite store or atomically initializes an empty one.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if err := initializeOrVerify(ctx, conn); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func initializeOrVerify(ctx context.Context, conn *sql.Conn) (err error) {
	if _, err = conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		}
	}()
	empty, err := emptySchema(ctx, conn)
	if err != nil {
		return err
	}
	if empty {
		if _, err = conn.ExecContext(ctx, currentSchema); err != nil {
			return err
		}
	}
	if err = verifySchema(ctx, conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `COMMIT`)
	return err
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// WithinTx executes fn inside a SQLite transaction.
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, session.Store) error) (err error) {
	if s == nil || fn == nil {
		return session.ErrConflict
	}
	if s.tx != nil {
		return fn(ctx, s)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	child := &Store{db: s.db, tx: tx}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback()
			panic(recovered)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()
	err = fn(ctx, child)
	return err
}
