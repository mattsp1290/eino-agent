// Package sqlite provides a local durable session store backed by SQLite.
//
// Empty databases are initialized from one current schema. Initialized
// databases are opened only when their version and structure exactly match;
// this pre-release package does not mutate or upgrade older schemas.
package sqlite
