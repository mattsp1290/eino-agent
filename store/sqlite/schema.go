package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/mattsp1290/eino-agent/session"
)

type schemaReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type indexSchema struct {
	table   string
	columns []string
	unique  bool
	partial bool
}

var currentTables = map[string][]string{
	"schema_version": {"version", "applied_at"},
	"sessions":       {"id", "record", "updated_at"},
	"runs":           {"id", "session_id", "status", "owner_id", "claim_token", "lease_until", "record", "created_at"},
	"messages":       {"id", "session_id", "run_id", "role", "record", "created_at"},
	"parts":          {"id", "message_id", "session_id", "run_id", "ordinal", "record", "created_at"},
	"events":         {"id", "session_id", "run_id", "kind", "tool_call_id", "tool_transition", "record", "created_at"},
	"tool_calls":     {"id", "session_id", "run_id", "message_id", "status", "claimed_by", "claim_token", "record"},
	"context_epochs": {"id", "session_id", "record", "closed_at"},
	"model_requests": {"id", "run_id", "state", "attempt", "step", "record", "created_at"},
}

var currentIndexes = map[string]indexSchema{
	"runs_session_active_idx":             {table: "runs", columns: []string{"session_id", "status"}},
	"runs_session_active_unique_idx":      {table: "runs", columns: []string{"session_id"}, unique: true, partial: true},
	"messages_replay_idx":                 {table: "messages", columns: []string{"session_id", "created_at", "id"}},
	"parts_replay_idx":                    {table: "parts", columns: []string{"session_id", "message_id", "ordinal", "id"}},
	"events_replay_idx":                   {table: "events", columns: []string{"session_id", "created_at", "id"}},
	"events_tool_transition_unique_idx":   {table: "events", columns: []string{"tool_call_id", "tool_transition"}, unique: true, partial: true},
	"events_run_finished_unique_idx":      {table: "events", columns: []string{"run_id", "kind"}, unique: true, partial: true},
	"tool_calls_unfinished_idx":           {table: "tool_calls", columns: []string{"run_id", "status"}},
	"model_requests_run_attempt_step_idx": {table: "model_requests", columns: []string{"run_id", "attempt", "step"}, unique: true},
	"model_requests_run_created_idx":      {table: "model_requests", columns: []string{"run_id", "created_at", "id"}},
}

func emptySchema(ctx context.Context, db *sql.DB) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`).Scan(&count)
	return count == 0, err
}

func verifySchema(ctx context.Context, db schemaReader) error {
	if err := verifySchemaVersion(ctx, db); err != nil {
		return err
	}
	if err := verifySchemaObjects(ctx, db); err != nil {
		return err
	}
	for table, columns := range currentTables {
		if err := verifyNamedColumns(ctx, db, "table", table, columns); err != nil {
			return err
		}
	}
	for name, index := range currentIndexes {
		if err := verifyNamedColumns(ctx, db, "index", name, index.columns); err != nil {
			return err
		}
		var unique, partial int
		if err := db.QueryRowContext(ctx, `SELECT "unique", partial FROM pragma_index_list(?) WHERE name = ?`, index.table, name).Scan(&unique, &partial); err != nil || (unique != 0) != index.unique || (partial != 0) != index.partial {
			return fmt.Errorf("%w: sqlite index %s flags mismatch", session.ErrConflict, name)
		}
	}
	return nil
}

func verifySchemaVersion(ctx context.Context, db schemaReader) error {
	var version int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		return fmt.Errorf("%w: sqlite schema version unavailable: %v", session.ErrConflict, err)
	}
	if version != 1 {
		return fmt.Errorf("%w: unsupported sqlite schema version %d", session.ErrConflict, version)
	}
	return nil
}

func verifySchemaObjects(ctx context.Context, db schemaReader) error {
	rows, err := db.QueryContext(ctx, `SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	want := make(map[string]bool, len(currentTables)+len(currentIndexes))
	for name := range currentTables {
		want["table:"+name] = true
	}
	for name := range currentIndexes {
		want["index:"+name] = true
	}
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			return err
		}
		key := kind + ":" + name
		if !want[key] {
			return fmt.Errorf("%w: unexpected sqlite schema object %s", session.ErrConflict, key)
		}
		delete(want, key)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(want) != 0 {
		for missing := range want {
			return fmt.Errorf("%w: missing sqlite schema object %s", session.ErrConflict, missing)
		}
	}
	return nil
}

func verifyNamedColumns(ctx context.Context, db schemaReader, kind, name string, want []string) error {
	pragma := `SELECT name FROM pragma_table_info(?) ORDER BY cid`
	if kind == "index" {
		pragma = `SELECT name FROM pragma_index_info(?) ORDER BY seqno`
	}
	rows, err := db.QueryContext(ctx, pragma, name)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return err
		}
		got = append(got, column)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("%w: sqlite %s %s columns are %v, want %v", session.ErrConflict, kind, name, got, want)
	}
	return nil
}
