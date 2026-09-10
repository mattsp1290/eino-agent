package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"
)

type postgresDialect struct{}

func (postgresDialect) IndexHint(string) string { return "" }
func (postgresDialect) ByteLength(column string) string {
	return "pg_catalog.octet_length(" + column + ")"
}
func (postgresDialect) InvalidScalar(string, bool) string {
	// PostgreSQL's fixed bytea/text schema makes malformed scalar types
	// impossible. Keep the expression typed and NULL-safe for shared guards.
	return "FALSE"
}
func (postgresDialect) ClockSQL() string {
	return "(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint"
}

func (postgresDialect) ValidateAdmissionReader(ctx context.Context, reader sqlstore.SQLReader) error {
	var recovering bool
	if err := reader.QueryRowContext(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovering); err != nil {
		return admissionUnknown(ctx)
	}
	if recovering {
		return session.ErrAdmissionUnknown
	}
	return nil
}

func admissionUnknown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(session.ErrAdmissionUnknown, err)
	}
	return session.ErrAdmissionUnknown
}

// LockRows adds a row lock for only the query's current table. Joins (such as
// the run/session lookup) therefore do not accidentally lock every relation.
// PostgreSQL requires the unqualified relation name or alias in FOR UPDATE OF.
func (postgresDialect) LockRows(db *gorm.DB) *gorm.DB {
	return db.Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate, Table: clause.Table{Name: db.Statement.Table}})
}

func (postgresDialect) MapError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) == 5 && pgErr.Code[:2] == "23" {
		return session.ErrConflict
	}
	return err
}

func (postgresDialect) Begin(ctx context.Context, db *sql.DB) (sqlstore.Transaction, error) {
	return beginTransaction(ctx, db, "BEGIN ISOLATION LEVEL READ COMMITTED")
}

func (postgresDialect) Read(ctx context.Context, pool *sql.DB, read func(sqlstore.SQLReader) error) (err error) {
	tx, err := beginTransaction(ctx, pool, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, tx.Close()) }()
	if err = read(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func beginTransaction(ctx context.Context, pool *sql.DB, statement string) (*postgresTransaction, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, errors.Join(err, ctx.Err())
	}
	if _, err = conn.ExecContext(ctx, statement); err != nil {
		return nil, errors.Join(err, ctx.Err(), discardAndClose(conn))
	}
	return &postgresTransaction{Conn: conn, ctx: ctx, active: true}, nil
}

// Writers and committed readers share one pinned connection lifecycle. A
// transaction command failure discards the physical pgx connection, including
// when the host supplied a database/sql wrapper over pgxpool.
type postgresTransaction struct {
	*sql.Conn
	ctx    context.Context
	mu     sync.Mutex
	active bool
	closed bool
}

func (t *postgresTransaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !t.active {
		return sql.ErrTxDone
	}
	tag, err := commitPGX(ctx, t.Conn)
	t.active = false
	if err != nil {
		t.closed = true
		return sqlstore.MarkTransactionOutcomeUnknown(errors.Join(err, ctx.Err(), discardAndClose(t.Conn)))
	}
	if tag.String() == "ROLLBACK" {
		return pgx.ErrTxCommitRollback
	}
	return nil
}

// commitPGX preserves PostgreSQL's COMMIT command tag. database/sql's
// ExecContext exposes only RowsAffected, losing the ROLLBACK tag PostgreSQL
// returns when a transaction was already aborted by an earlier statement.
func commitPGX(ctx context.Context, conn *sql.Conn) (tag pgconn.CommandTag, err error) {
	err = conn.Raw(func(raw any) error {
		stdlibConn, ok := raw.(*stdlib.Conn)
		if !ok || stdlibConn.Conn() == nil {
			return errors.New("postgres transaction requires the pgx stdlib driver")
		}
		tag, err = stdlibConn.Conn().Exec(ctx, "COMMIT")
		return err
	})
	return tag, errors.Join(err, ctx.Err())
}

func (t *postgresTransaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rollback(ctx)
}

// rollback is called with the lifecycle mutex held and a bounded cleanup context.
func (t *postgresTransaction) rollback(ctx context.Context) error {
	if !t.active {
		return nil
	}
	_, err := t.ExecContext(ctx, "ROLLBACK")
	t.active = false
	if err != nil {
		t.closed = true
		return errors.Join(err, ctx.Err(), discardAndClose(t.Conn))
	}
	return nil
}

func (t *postgresTransaction) Close() (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(t.ctx), cleanupTimeout)
		err = t.rollback(cleanup)
		cancel()
	}
	if !t.closed {
		err = errors.Join(err, t.Conn.Close())
		t.closed = true
	}
	return errors.Join(err, t.ctx.Err())
}

func discardAndClose(conn *sql.Conn) error {
	return errors.Join(discardPGXConnection(conn), conn.Close())
}

var _ sqlstore.Dialect = postgresDialect{}
var _ sqlstore.Transaction = (*postgresTransaction)(nil)
