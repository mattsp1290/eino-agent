//go:build postgres_integration

// Package testpostgres provides the disposable PostgreSQL fixture used by
// integration tests. It is intentionally available only to Docker-backed
// integration-test builds.
package testpostgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"net/url"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresImage = "postgres:17.9-bookworm@sha256:47f917f7409eacd22fc5dfb1dee634e1b55cf0c01d1a7eb701be2227a03e0641"

	startupTimeout   = 5 * time.Minute
	operationTimeout = 30 * time.Second
	cleanupTimeout   = 30 * time.Second
)

// Server owns one PostgreSQL container and the admin pool used to provision
// disposable databases within it.
type Server struct {
	admin    *sql.DB
	adminDSN string
}

// Database is a fresh database owned by a Server. Its connection string is
// private so fixtures cannot accidentally expose credentials in test output.
type Database struct {
	dsn string
}

// Start starts a PostgreSQL 17 container and registers all cleanup needed for
// both successful and partially successful container creation.
func Start(t *testing.T) *Server {
	t.Helper()

	startupCtx, cancel := context.WithTimeout(t.Context(), startupTimeout)
	defer cancel()

	container, err := postgres.Run(
		startupCtx,
		postgresImage,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_INITDB_ARGS": "--locale=en_US.utf8 --encoding=UTF8",
		}),
		postgres.BasicWaitStrategies(),
	)

	// postgres.Run may return a usable container together with an error during
	// a partial startup. Acquire cleanup before reporting that error.
	if container != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			if err := container.Terminate(ctx); err != nil {
				t.Errorf("postgres fixture: container cleanup failed")
			}
		})
	}
	if err != nil {
		t.Fatalf("postgres fixture: container startup failed: %v", err)
	}
	if container == nil {
		t.Fatal("postgres fixture: container startup returned no container")
	}

	adminDSN, err := container.ConnectionString(startupCtx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres fixture: connection endpoint lookup failed")
	}

	admin := openPool(t, adminDSN)

	return &Server{
		admin:    admin,
		adminDSN: adminDSN,
	}
}

// Database creates and owns a fresh database for one test case. The database
// is created from template0 with the non-C locale used by the integration
// suite, and is dropped during test cleanup.
func (s *Server) Database(t *testing.T) *Database {
	t.Helper()

	name := "eino_test_" + rand.Text()
	ctx, cancel := context.WithTimeout(t.Context(), operationTimeout)
	_, err := s.admin.ExecContext(ctx, "CREATE DATABASE \""+name+"\" WITH TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'")
	cancel()
	if err != nil {
		t.Fatalf("postgres fixture: database creation failed")
	}

	// Open pools are registered after this cleanup and therefore close before
	// the database is dropped. Register it before parsing the private DSN so a
	// later setup failure still removes the acquired database.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := s.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS \""+name+"\" WITH (FORCE)"); err != nil {
			t.Errorf("postgres fixture: database cleanup failed")
		}
	})

	parsed, err := url.Parse(s.adminDSN)
	if err != nil {
		t.Fatalf("postgres fixture: database connection setup failed")
	}
	parsed.Path = "/" + name
	return &Database{dsn: parsed.String()}
}

// Open opens an independent pgx database/sql pool for this database. Each
// call creates a separate pool, allowing reopen and multiple-pool race tests.
func (d *Database) Open(t *testing.T) *sql.DB {
	t.Helper()

	return openPool(t, d.dsn)
}

func openPool(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("postgres fixture: pool open failed")
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error("postgres fixture: pool cleanup failed")
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), operationTimeout)
	err = db.PingContext(ctx)
	cancel()
	if err != nil {
		t.Fatalf("postgres fixture: pool ping failed")
	}
	return db
}
