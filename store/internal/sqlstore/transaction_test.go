package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	_ "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
)

var errCleanupInjected = errors.New("injected savepoint cleanup failure")
var errCommitAckInjected = errors.New("injected commit acknowledgement failure")

type cleanupDialect struct {
	Dialect
	failPrefix string
}

func (d cleanupDialect) MapError(err error) error { return err }
func (d cleanupDialect) Begin(ctx context.Context, db *sql.DB) (Transaction, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &cleanupTransaction{Tx: tx, failPrefix: d.failPrefix}, nil
}

type cleanupTransaction struct {
	*sql.Tx
	failPrefix string
}

func (t *cleanupTransaction) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if strings.HasPrefix(q, t.failPrefix) {
		return nil, errCleanupInjected
	}
	return t.Tx.ExecContext(ctx, q, args...)
}
func (t *cleanupTransaction) Commit(context.Context) error   { return t.Tx.Commit() }
func (t *cleanupTransaction) Rollback(context.Context) error { return t.Tx.Rollback() }
func (t *cleanupTransaction) Close() error                   { return nil }

func TestSavepointCleanupPoisonsOuterTransaction(t *testing.T) {
	for _, prefix := range []string{"ROLLBACK TO SAVEPOINT", "RELEASE SAVEPOINT"} {
		t.Run(prefix, func(t *testing.T) {
			pool, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pool.Close() }()
			pool.SetMaxOpenConns(1)
			if _, err := pool.Exec("CREATE TABLE sessions(row_key INTEGER PRIMARY KEY,id BLOB UNIQUE,record BLOB,workspace_id BLOB,title BLOB,created_at TEXT,updated_at TEXT)"); err != nil {
				t.Fatal(err)
			}
			orm, err := gorm.Open(gormsqlite.New(gormsqlite.Config{Conn: pool, DriverName: "sqlite"}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Discard})
			if err != nil {
				t.Fatal(err)
			}
			st := New(orm, pool, cleanupDialect{failPrefix: prefix})
			calls := 0
			err = st.WithinTx(t.Context(), func(ctx context.Context, view session.Store) error {
				calls++
				tx := view.(*Store)
				// Ignore the operation error deliberately. Failed cleanup must still abort.
				_ = tx.atomic(ctx, func(op *Store) error {
					if err := op.dbFor(ctx).Table("sessions").Create(map[string]any{"id": []byte("partial")}).Error; err != nil {
						return err
					}
					return errors.New("operation failed")
				})
				if _, err := tx.CreateSession(ctx, session.Session{ID: "after-poison"}); !errors.Is(err, errCleanupInjected) {
					t.Errorf("poison lost: %v", err)
				}
				return nil
			})
			if !errors.Is(err, errCleanupInjected) || calls != 1 {
				t.Fatalf("result %v, callbacks %d", err, calls)
			}
			var count int
			if err := pool.QueryRow("SELECT count(*) FROM sessions").Scan(&count); err != nil || count != 0 {
				t.Fatalf("poisoned transaction committed %d rows: %v", count, err)
			}
		})
	}
}

type commitAckDialect struct{ Dialect }

func (commitAckDialect) MapError(err error) error { return err }
func (commitAckDialect) Begin(ctx context.Context, db *sql.DB) (Transaction, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &commitAckTransaction{Tx: tx}, nil
}

type commitAckTransaction struct{ *sql.Tx }

func (t *commitAckTransaction) Commit(context.Context) error {
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	return MarkTransactionOutcomeUnknown(errCommitAckInjected)
}
func (t *commitAckTransaction) Rollback(context.Context) error { return t.Tx.Rollback() }
func (t *commitAckTransaction) Close() error                   { return nil }

func TestWithinTxMarksLostCommitAcknowledgementWhileDataCommits(t *testing.T) {
	pool, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	pool.SetMaxOpenConns(1)
	if _, err := pool.Exec("CREATE TABLE witness(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	orm, err := gorm.Open(gormsqlite.New(gormsqlite.Config{Conn: pool, DriverName: "sqlite"}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	store := New(orm, pool, commitAckDialect{})
	err = store.WithinTx(t.Context(), func(ctx context.Context, view session.Store) error {
		_, err := view.(*Store).dbFor(ctx).Statement.ConnPool.ExecContext(ctx, "INSERT INTO witness(value) VALUES (?)", "committed")
		return err
	})
	var marked interface{ TransactionOutcomeUnknown() bool }
	if !errors.As(err, &marked) || !marked.TransactionOutcomeUnknown() || !errors.Is(err, errCommitAckInjected) {
		t.Fatalf("commit result=%v", err)
	}
	var count int
	if err := pool.QueryRow("SELECT count(*) FROM witness").Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed rows=%d err=%v", count, err)
	}
}
