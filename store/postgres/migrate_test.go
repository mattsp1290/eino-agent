package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"go.uber.org/multierr"
	_ "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
)

func TestMigrationNoopErrors(t *testing.T) {
	cleanup := errors.New("cleanup failed")
	for _, test := range []struct {
		err error
		ok  bool
	}{
		{nil, false}, {cleanup, false}, {goose.ErrAlreadyApplied, true},
		{fmt.Errorf("version 1: %w", goose.ErrAlreadyApplied), true},
		{errors.Join(goose.ErrAlreadyApplied), true},
		{errors.Join(goose.ErrAlreadyApplied, cleanup), false},
		{multierr.Append(goose.ErrAlreadyApplied, cleanup), false},
		{fmt.Errorf("migration: %w", errors.Join(goose.ErrAlreadyApplied, cleanup)), false},
		{errors.Join(goose.ErrAlreadyApplied, goose.ErrAlreadyApplied), false},
	} {
		if got := soleAlreadyApplied(test.err); got != test.ok {
			t.Errorf("soleAlreadyApplied(%v) = %t, want %t", test.err, got, test.ok)
		}
	}
}

func TestMigrateNilPool(t *testing.T) {
	if err := Migrate(context.Background(), nil); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("nil pool: %v", err)
	}
}

func TestMigrateRejectsWrongDriverAndClosedPool(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := Migrate(t.Context(), db); err == nil {
		t.Fatal("accepted a non-pgx driver")
	}
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM sqlite_master").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejection mutated or closed pool: count=%d, %v", count, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(t.Context(), db); err == nil {
		t.Fatal("accepted a closed pool")
	}
}

func TestMigrateDoesNotEchoConnectionString(t *testing.T) {
	// Invalid timeout is parsed lazily by pgx, before any network connection.
	db, err := sql.Open("pgx", "host=dsn-host-marker dbname=dsn-db-marker user=dsn-user-marker connect_timeout=invalid")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	err = Migrate(t.Context(), db)
	var parseErr *pgconn.ParseConfigError
	if !errors.As(err, &parseErr) {
		t.Fatal("migration did not preserve the underlying configuration error")
	}
	for _, marker := range []string{"dsn-host-marker", "dsn-db-marker", "dsn-user-marker", "connect_timeout"} {
		if strings.Contains(err.Error(), marker) {
			t.Fatal("public migration error echoed connection-string fields")
		}
	}
}
