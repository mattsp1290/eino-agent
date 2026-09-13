package sqlstore

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

// SQLReader is a pinned read transaction supplied by the backend.
type SQLReader interface{ gorm.ConnPool }

type Transaction interface {
	gorm.ConnPool
	Commit(context.Context) error
	Rollback(context.Context) error
	Close() error
}

// Dialect contains the backend policies that cannot be expressed portably.
type Dialect interface {
	Begin(context.Context, *sql.DB) (Transaction, error)
	Read(context.Context, *sql.DB, func(SQLReader) error) error
	ValidateAdmissionReader(context.Context, SQLReader) error
	ClockSQL() string
	// LockRows locks rows from the query's current table. Joined queries must
	// not accidentally lock the joined relation as well.
	LockRows(*gorm.DB) *gorm.DB
	MapError(error) error
	IndexHint(string) string
	ByteLength(string) string
	InvalidScalar(string, bool) string
}
