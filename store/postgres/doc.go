// Package postgres provides a durable session store and explicit schema
// migration for a dedicated PostgreSQL 17 database using a borrowed pgx-backed
// pool. The host owns pool credentials, configuration, and shutdown; New and
// Migrate never close it or alter its settings. New requires the current
// migration schema and uses GORM's singular public table names.
//
// Committed observation and discovery readers use independent repeatable-read
// snapshots. Writes use read-committed transactions and lock session rows in
// the order required by the shared store. Hosts running callbacks over several
// sessions should acquire those sessions in ascending bytewise ID order so
// PostgreSQL deadlocks are avoided and can be propagated without retrying the
// callback.
//
// Goose is the sole schema authority: Migrate initializes the fresh version-1
// baseline with writers quiesced; normal startup calls verify-only New.
// Shared application schemas, legacy imports, automatic Down/reset, retention
// APIs, and arbitrary SQL backends are not supported. Hosts own backups,
// authorization, retention scheduling and database maintenance.
//
// A host may pass stdlib.OpenDBFromPool(nativePool), retaining ownership of
// both pools and closing the SQL wrapper before the native pgxpool. Nil pools
// and unsupported drivers match session.ErrConflict; closed pools and canceled
// contexts retain their underlying error classification.
package postgres
