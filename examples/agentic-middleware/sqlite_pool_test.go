package agenticmiddleware

import (
	"context"
	"database/sql"
	"net/url"
	"strings"

	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
)

// openTestSQLite mirrors examples/native-extension's helper of the same
// name: a durable sqlite-backed session.Store for a black-box example test,
// never the runtime package's own internal admissionStore.
func openTestSQLite(ctx context.Context, path string) (*sqlite.Store, *sql.DB, error) {
	uri := url.URL{Scheme: "file", Path: path, OmitHost: true}
	if strings.HasPrefix(path, "file:") {
		parsed, err := url.Parse(path)
		if err != nil {
			return nil, nil, err
		}
		uri = *parsed
	}
	q := uri.Query()
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	uri.RawQuery = q.Encode()
	dsn := uri.String()
	if path == ":memory:" {
		dsn = ":memory:?" + uri.RawQuery
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := sqlite.Migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	st, err := sqlite.New(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return st, db, nil
}
