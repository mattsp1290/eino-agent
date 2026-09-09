package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

type transactionState struct {
	sequence atomic.Uint64
	mu       sync.Mutex
	poison   error
}

func (t *transactionState) failure() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.poison
}

func (t *transactionState) fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.poison = errors.Join(t.poison, err)
}

// WithinTx reuses public nesting; only individual mutations create savepoints.
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, session.Store) error) (err error) {
	if s == nil || s.db == nil || fn == nil {
		return session.ErrConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.tx != nil {
		if err := s.tx.failure(); err != nil {
			return err
		}
		return fn(ctx, s)
	}
	tx, err := s.dialect.Begin(ctx, s.pool)
	if err != nil {
		return err
	}
	child := s.bound(ctx, tx)
	child.tx = &transactionState{}
	committed := false
	defer func() {
		if !committed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err = errors.Join(err, tx.Rollback(cleanup))
			cancel()
		}
		err = errors.Join(err, tx.Close())
	}()
	if err = fn(ctx, child); err != nil {
		return err
	}
	if err = errors.Join(ctx.Err(), child.tx.failure()); err != nil {
		return err
	}
	err = tx.Commit(ctx)
	committed = err == nil
	return err
}

// atomic rolls back a failed mutation even if its caller catches the error.
func (s *Store) atomic(ctx context.Context, fn func(*Store) error) (err error) {
	if s == nil || s.db == nil || fn == nil {
		return session.ErrConflict
	}
	if s.tx == nil {
		return s.WithinTx(ctx, func(_ context.Context, store session.Store) error { return fn(store.(*Store)) })
	}
	if err := errors.Join(ctx.Err(), s.tx.failure()); err != nil {
		return err
	}
	name := fmt.Sprintf("eino_operation_%d", s.tx.sequence.Add(1))
	conn := s.dbFor(ctx).Statement.ConnPool
	if _, err := conn.ExecContext(ctx, "SAVEPOINT "+name); err != nil {
		return err
	}
	finished := false
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var rollbackErr error
		if !finished {
			_, rollbackErr = conn.ExecContext(cleanup, "ROLLBACK TO SAVEPOINT "+name)
		}
		_, releaseErr := conn.ExecContext(cleanup, "RELEASE SAVEPOINT "+name)
		cleanupErr := errors.Join(rollbackErr, releaseErr)
		if cleanupErr != nil {
			s.tx.fail(cleanupErr)
		}
		err = errors.Join(err, cleanupErr)
	}()
	err = fn(s)
	if err == nil {
		err = ctx.Err()
	}
	finished = err == nil
	return err
}
