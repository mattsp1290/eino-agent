package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate initializes or validates a dedicated database. The caller must stop
// store writers and use one migrator. Migrate never closes the host pool.
func Migrate(ctx context.Context, db *sql.DB) error {
	files, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return err
	}
	if err := migrate(ctx, db, files); err != nil {
		return sqliteLifecycleError{err}
	}
	return nil
}

func migrate(ctx context.Context, db *sql.DB, files fs.FS) error {
	if err := validatePool(ctx, db); err != nil {
		return err
	}
	state, err := inspectPool(ctx, db)
	if err != nil {
		return err
	}
	if state == migrationSchemaCurrent {
		return nil
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, files,
		goose.WithTableName("eino_agent_goose_version"), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return err
	}
	if _, err := provider.ApplyVersion(ctx, 1, true); err != nil {
		return err
	}
	_, err = inspectPool(ctx, db)
	return err
}
