package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func sameRecord[T any](left, right T) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if s.tx != nil {
		return s.tx.ExecContext(ctx, query, args...)
	}
	return s.db.ExecContext(ctx, query, args...)
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	if s.tx != nil {
		return s.tx.QueryRowContext(ctx, query, args...)
	}
	return s.db.QueryRowContext(ctx, query, args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if s.tx != nil {
		return s.tx.QueryContext(ctx, query, args...)
	}
	return s.db.QueryContext(ctx, query, args...)
}

func (s *Store) getJSON(ctx context.Context, query string, args []any, dst any) error {
	var raw []byte
	if err := s.queryRow(ctx, query, args...).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return session.ErrNotFound
		}
		return err
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("%w: %v", session.ErrConflict, err)
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

func (s *Store) getRun(ctx context.Context, query string, args ...any) (session.Run, error) {
	record, err := scanRun(s.queryRow(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return session.Run{}, session.ErrNotFound
	}
	return record, err
}

var _ session.Store = (*Store)(nil)

func scanRun(row rowScanner) (session.Run, error) {
	var (
		raw         []byte
		status      session.RunStatus
		ownerID     string
		claimToken  string
		leaseMicros int64
	)
	if err := row.Scan(&raw, &status, &ownerID, &claimToken, &leaseMicros); err != nil {
		return session.Run{}, err
	}
	var record session.Run
	if err := json.Unmarshal(raw, &record); err != nil {
		return session.Run{}, fmt.Errorf("%w: %v", session.ErrConflict, err)
	}
	record.Status = status
	record.OwnerID = ownerID
	record.ClaimToken = claimToken
	record.LeaseUntil = time.UnixMicro(leaseMicros).UTC()
	return record, nil
}

func durationMicros(duration time.Duration) int64 {
	micros := duration.Microseconds()
	if micros < 1 {
		return 1
	}
	return micros
}

func listJSON[T any](ctx context.Context, s *Store, query string, args ...any) ([]T, error) {
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []T
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%w: %v", session.ErrConflict, err)
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func rowsAffected(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return session.ErrNotFound
	}
	return nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if constraintFailed(err) {
		return session.ErrConflict
	}
	return err
}

func constraintFailed(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "constraint failed") || strings.Contains(msg, "constraint failed:")
}

func timeText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
