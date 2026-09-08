// Package sqlite provides a local durable session store backed by SQLite.
//
// Empty databases are initialized from one current schema. Initialized
// databases are opened only when their structure exactly matches;
// this pre-release package does not mutate or upgrade older schemas.
// Store implements session.SessionDiscoveryReader with indexed committed pages.
// Projection writes require valid UTF-8 and nonzero UTC years 0000–9999.
// Discovery is only available on root stores, outside caller transactions.
package sqlite
