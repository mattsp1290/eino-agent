package sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
)

func openSQLiteFixture(ctx context.Context, path string) (*Store, error) {
	return sqliteFixturePool(ctx, path, true)
}
func reopenSQLiteFixture(ctx context.Context, path string) (*Store, error) {
	return sqliteFixturePool(ctx, path, false)
}
func sqliteFixturePool(ctx context.Context, path string, initialize bool) (*Store, error) {
	uri := url.URL{Scheme: "file", Path: path, OmitHost: true}
	if strings.HasPrefix(path, "file:") {
		parsed, err := url.Parse(path)
		if err != nil {
			return nil, err
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
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if initialize {
		if err := Migrate(ctx, db); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	st, err := New(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}
