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

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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
	admin     *sql.DB
	adminDSN  string
	container *postgres.PostgresContainer
}

// Database is a fresh database owned by a Server. Its connection string is
// private so fixtures cannot accidentally expose credentials in test output.
type Database struct {
	server *Server
	name   string
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
		// Preserve the module's two readiness checks, using this fixture's
		// startup budget instead of the strategies' one-minute defaults.
		testcontainers.WithAdditionalWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(startupTimeout),
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(startupTimeout),
		),
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

	server := &Server{
		adminDSN:  adminDSN,
		container: container,
	}
	// The current admin pool belongs to the server's test, so replacement
	// pools outlive child database cleanup after a restart.
	t.Cleanup(func() {
		if server.admin != nil {
			if err := server.admin.Close(); err != nil {
				t.Error("postgres fixture: admin pool cleanup failed")
			}
		}
	})
	server.openAdmin(t)
	return server
}

// Restart stops and starts the same container, retaining its data directory.
// Callers must close all database pools first and must not run other cases on
// this server concurrently. Existing Database handles resolve the new endpoint.
func (s *Server) Restart(t *testing.T) {
	t.Helper()
	if err := s.admin.Close(); err != nil {
		t.Fatal("postgres fixture: close admin before restart failed")
	}
	ctx, cancel := context.WithTimeout(t.Context(), startupTimeout)
	defer cancel()
	// A zero grace period exercises recovery across an abrupt container stop.
	grace := time.Duration(0)
	if err := s.container.Stop(ctx, &grace); err != nil {
		t.Fatal("postgres fixture: container stop failed")
	}
	if err := s.container.Start(ctx); err != nil {
		t.Fatal("postgres fixture: container restart failed")
	}
	dsn, err := s.container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal("postgres fixture: restarted endpoint lookup failed")
	}
	s.adminDSN = dsn
	// Startup logs survive a restart, so require a fresh successful SQL query
	// before opening the replacement admin pool.
	ready := wait.ForSQL("5432/tcp", "pgx", func(string, string) string { return dsn }).WithStartupTimeout(startupTimeout)
	if err := ready.WaitUntilReady(ctx, s.container); err != nil {
		t.Fatal("postgres fixture: restarted database readiness failed")
	}
	s.openAdmin(t)
}

func (s *Server) openAdmin(t *testing.T) {
	t.Helper()
	pool, err := sql.Open("pgx", s.adminDSN)
	if err != nil {
		t.Fatal("postgres fixture: admin pool open failed")
	}
	// Assign before ping so the server cleanup owns even a partially opened pool.
	s.admin = pool
	pingPool(t, pool)
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
	// the database is dropped. Register cleanup before returning the handle so
	// a later setup failure still removes the acquired database.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := s.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS \""+name+"\" WITH (FORCE)"); err != nil {
			t.Errorf("postgres fixture: database cleanup failed")
		}
	})

	return &Database{server: s, name: name}
}

func (d *Database) connectionString(t *testing.T) string {
	t.Helper()
	parsed, err := url.Parse(d.server.adminDSN)
	if err != nil {
		t.Fatalf("postgres fixture: database connection setup failed")
	}
	parsed.Path = "/" + d.name
	return parsed.String()
}

// Open opens an independent pgx database/sql pool for this database. Each
// call creates a separate pool, allowing reopen and multiple-pool race tests.
func (d *Database) Open(t *testing.T) *sql.DB {
	t.Helper()

	return openPool(t, d.connectionString(t))
}

// OpenPGX opens an independent native pool for testing the database/sql bridge.
// Cleanup stays host-owned and runs after wrappers registered by the caller.
func (d *Database) OpenPGX(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), operationTimeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, d.connectionString(t))
	if err != nil {
		t.Fatal("postgres fixture: native pool open failed")
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal("postgres fixture: native pool ping failed")
	}
	return pool
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

	pingPool(t, db)
	return db
}

func pingPool(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), operationTimeout)
	err := db.PingContext(ctx)
	cancel()
	if err != nil {
		t.Fatalf("postgres fixture: pool ping failed")
	}
}
