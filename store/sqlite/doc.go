// Package sqlite provides a local durable session store backed by SQLite.
//
// Migrations are deterministic and forward-only. The schema_version table
// records applied versions; downgrades are intentionally unsupported because
// older binaries may not understand records written by newer runtimes. Operators
// that need rollback should restore a database backup captured before opening
// it with the newer binary.
package sqlite
