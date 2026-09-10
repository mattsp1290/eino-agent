# PostgreSQL store example

This example uses the public `session` and `store/postgres` packages with a
host-owned `database/sql` pool backed by pgx. It runs a small session lifecycle,
then replays the stored message and prints only its generated session ID and
replay count.

Use a dedicated PostgreSQL 17 database. Set the connection string in the
environment; the program never prints it:

```sh
export EINO_AGENT_POSTGRES_DSN='postgres://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require'
go run ./examples/postgres-store -migrate
go run ./examples/postgres-store
```

Run `-migrate` as a setup or release operation while application writers are
quiesced. Goose migration version 1 creates and validates this store's schema.
The normal command calls `postgres.New`, which verifies the existing schema and
does no DDL. There is no automatic `Down`, reset, or legacy import operation.

The host owns credentials, connection settings, backups, retention and
deletion scheduling, vacuum and other database health, migration coordination,
and closing the pool. If the host owns a `*pgxpool.Pool`, bridge it with
`stdlib.OpenDBFromPool(pool)` and keep pool shutdown in the host; the store
borrows the resulting `*sql.DB` and never closes it itself. This example opens
and closes its own `*sql.DB` to make that ownership explicit.

The migration baseline is intended for a fresh dedicated database. Existing
application schemas, arbitrary row deletion, and in-place rollback are not
supported; destructive cleanup belongs to a disposable test database. Choose
backups and retention policies with awareness of the store's durable
relationships and observation invariants.

The example requires Go 1.26.3 and is designed for PostgreSQL 17. No model
service, Docker runtime, or Ensemble integration is required.
