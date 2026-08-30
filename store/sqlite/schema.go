package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/mattsp1290/eino-agent/session"
)

type schemaReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type schemaObject struct {
	kind string
	sql  string
}

var expectedSchema struct {
	sync.Once
	objects map[string]schemaObject
	err     error
}

func emptySchema(ctx context.Context, db schemaReader) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'`).Scan(&count)
	return count == 0, err
}

func verifySchema(ctx context.Context, db schemaReader) error {
	want, err := currentSchemaObjects()
	if err != nil {
		return err
	}
	got, err := readSchemaObjects(ctx, db)
	if err != nil {
		return err
	}
	for name, expected := range want {
		actual, ok := got[name]
		if !ok {
			return fmt.Errorf("%w: missing sqlite schema object %s:%s", session.ErrConflict, expected.kind, name)
		}
		if actual != expected {
			return fmt.Errorf("%w: sqlite schema object %s:%s differs from current definition", session.ErrConflict, expected.kind, name)
		}
		delete(got, name)
	}
	for name, unexpected := range got {
		return fmt.Errorf("%w: unexpected sqlite schema object %s:%s", session.ErrConflict, unexpected.kind, name)
	}
	return nil
}

func currentSchemaObjects() (map[string]schemaObject, error) {
	expectedSchema.Do(func() {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			expectedSchema.err = err
			return
		}
		defer func() { _ = db.Close() }()
		if _, err = db.Exec(currentSchema); err != nil {
			expectedSchema.err = err
			return
		}
		expectedSchema.objects, expectedSchema.err = readSchemaObjects(context.Background(), db)
	})
	if expectedSchema.err != nil {
		return nil, expectedSchema.err
	}
	result := make(map[string]schemaObject, len(expectedSchema.objects))
	for name, object := range expectedSchema.objects {
		result[name] = object
	}
	return result, nil
}

func readSchemaObjects(ctx context.Context, db schemaReader) (map[string]schemaObject, error) {
	rows, err := db.QueryContext(ctx, `SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	objects := make(map[string]schemaObject)
	for rows.Next() {
		var kind, name, definition string
		if err := rows.Scan(&kind, &name, &definition); err != nil {
			return nil, err
		}
		objects[name] = schemaObject{kind: kind, sql: normalizeSchemaSQL(definition)}
	}
	return objects, rows.Err()
}

func normalizeSchemaSQL(value string) string {
	return strings.TrimSpace(value)
}
