// Package sqlite provides a durable session store using shared GORM persistence
// over a host-owned modernc SQLite pool.
//
// Migrate explicitly initializes the fresh Goose baseline or validates the
// current schema, with writers stopped. New validates without creating schema.
// Neither closes the pool or changes its configuration. The host enables foreign
// keys on every connection and retains memory-database connections until shutdown.
// Legacy and unsupported schemas are rejected without repair or upgrade.
//
// Writer transactions acquire BEGIN IMMEDIATE before checking fences. Committed
// observation/discovery readers use deferred read views outside caller transactions.
// Projection writes accept valid UTF-8 and UTC years 0000–9999.
//
// Persistence algorithms are shared with store/postgres. Public nested callbacks
// reuse the outer transaction; individual mutations use internal savepoints.
// Hosts own backup, retention and migration policy. No automatic Down/reset or
// legacy file import is provided, and unsupported files remain untouched.
package sqlite
