package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

type migrationSchemaState uint8

const (
	migrationSchemaEmpty migrationSchemaState = iota
	migrationSchemaBootstrap
	migrationSchemaCurrent
)

// This reference is deliberately static. It is regenerated only when the
// reviewed Goose baseline or its history-table DDL changes.
//
//go:embed schema_fingerprints.json
var migrationSchemaFingerprints []byte

// inspectMigrationSchema only reads a pinned connection. In particular, the
// read-only option prevents modernc's _txlock=immediate setting from turning
// this preflight into a writer transaction.
func inspectMigrationSchema(ctx context.Context, conn *sql.Conn) (state migrationSchemaState, err error) {
	if conn == nil {
		return 0, fmt.Errorf("%w: nil sqlite connection", session.ErrConflict)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()

	catalog, err := migrationSchemaCatalog(ctx, tx)
	if err != nil {
		return 0, err
	}
	if len(catalog) == 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return migrationSchemaEmpty, tx.Commit()
	}

	var reference struct {
		Bootstrap string
		Current   string
	}
	if err := json.Unmarshal(migrationSchemaFingerprints, &reference); err != nil {
		return 0, fmt.Errorf("sqlite schema reference: %w", err)
	}
	switch migrationSchemaFingerprint(catalog) {
	case reference.Bootstrap:
		state = migrationSchemaBootstrap
	case reference.Current:
		state = migrationSchemaCurrent
	default:
		return 0, migrationSchemaConflict("unsupported sqlite catalog structure")
	}
	if err := validateMigrationHistory(ctx, tx, state); err != nil {
		return 0, err
	}
	if state == migrationSchemaCurrent {
		if err := validateMigrationIncarnation(ctx, tx); err != nil {
			return 0, err
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return state, tx.Commit()
}

// migrationSchemaCatalog returns one canonical JSON value for every schema
// row. sqlite_schema.sql is nullable for implicit autoindexes, so null is
// retained in the value rather than converted to an empty string.
func migrationSchemaCatalog(ctx context.Context, db schemaReader) (catalog map[string]string, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT type, name, tbl_name, sql
FROM main.sqlite_schema
WHERE name NOT IN ('sqlite_stat1', 'sqlite_stat4')
ORDER BY type, name, tbl_name`)
	if err != nil {
		return nil, err
	}
	catalog = make(map[string]string)
	for rows.Next() {
		var typ, name, tableName string
		var definition sql.NullString
		if err = rows.Scan(&typ, &name, &tableName, &definition); err != nil {
			break
		}
		var sqlValue any
		if definition.Valid {
			sqlValue = strings.TrimSpace(definition.String)
		}
		encoded, marshalErr := json.Marshal(struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			TableName string `json:"tbl_name"`
			SQL       any    `json:"sql"`
		}{typ, name, tableName, sqlValue})
		if marshalErr != nil {
			err = marshalErr
			break
		}
		key := typ + "\x00" + name + "\x00" + tableName
		if _, exists := catalog[key]; exists {
			err = migrationSchemaConflict("duplicate sqlite catalog object")
			break
		}
		catalog[key] = string(encoded)
	}
	if closeErr := rows.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		err = errors.Join(err, rowsErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = errors.Join(err, ctxErr)
	}
	if err != nil {
		return nil, err
	}
	return catalog, nil
}

func migrationSchemaFingerprint(catalog map[string]string) string {
	keys := make([]string, 0, len(catalog))
	for key := range catalog {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00", key, catalog[key])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func validateMigrationHistory(ctx context.Context, tx *sql.Tx, state migrationSchemaState) (err error) {
	rows, err := tx.QueryContext(ctx, `
SELECT CASE WHEN typeof(id) = 'integer' THEN id END,
       CASE WHEN typeof(version_id) = 'integer' THEN version_id END,
       CASE WHEN typeof(is_applied) = 'integer' THEN is_applied END,
       CASE WHEN typeof(tstamp) = 'text' AND length(tstamp) = 19 THEN tstamp END,
       typeof(id), typeof(version_id), typeof(is_applied), typeof(tstamp)
FROM main.eino_agent_goose_version
ORDER BY id
LIMIT 3`)
	if err != nil {
		return err
	}
	type historyRow struct {
		id, version, applied sql.NullInt64
		timestamp            sql.NullString
		idType               string
		versionType          string
		appliedType          string
		timeType             string
	}
	var history []historyRow
	for rows.Next() {
		var row historyRow
		if err = rows.Scan(&row.id, &row.version, &row.applied, &row.timestamp,
			&row.idType, &row.versionType, &row.appliedType, &row.timeType); err != nil {
			break
		}
		history = append(history, row)
	}
	if closeErr := rows.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		err = errors.Join(err, rowsErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = errors.Join(err, ctxErr)
	}
	if err != nil {
		return err
	}
	want := 1
	if state == migrationSchemaCurrent {
		want = 2
	}
	if len(history) != want {
		return migrationSchemaConflict("unsupported sqlite migration history")
	}
	var previousID int64
	for index, row := range history {
		if !row.id.Valid || !row.version.Valid || !row.applied.Valid || row.id.Int64 <= 0 || row.id.Int64 <= previousID || row.version.Int64 != int64(index) || row.applied.Int64 != 1 ||
			row.idType != "integer" || row.versionType != "integer" || row.appliedType != "integer" ||
			!row.timestamp.Valid || row.timeType != "text" {
			return migrationSchemaConflict("malformed sqlite migration history")
		}
		parsed, parseErr := time.Parse("2006-01-02 15:04:05", row.timestamp.String)
		if parseErr != nil || parsed.Format("2006-01-02 15:04:05") != row.timestamp.String {
			return migrationSchemaConflict("malformed sqlite migration timestamp")
		}
		previousID = row.id.Int64
	}
	return nil
}

func validateMigrationIncarnation(ctx context.Context, tx *sql.Tx) error {
	var singleton sql.NullInt64
	var incarnation sql.NullString
	var singletonType, incarnationType string
	rows, err := tx.QueryContext(ctx, `
SELECT CASE WHEN typeof(singleton) = 'integer' AND singleton = 1 THEN singleton END,
       CASE WHEN typeof(incarnation) = 'text' AND length(incarnation) = 32 THEN incarnation END,
       typeof(singleton), typeof(incarnation)
FROM main.observation_store
ORDER BY rowid
LIMIT 2`)
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		count++
		if count == 1 {
			err = rows.Scan(&singleton, &incarnation, &singletonType, &incarnationType)
		}
	}
	if closeErr := rows.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		err = errors.Join(err, rowsErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = errors.Join(err, ctxErr)
	}
	if err != nil {
		return err
	}
	if count != 1 || !singleton.Valid || singleton.Int64 != 1 || singletonType != "integer" || incarnationType != "text" ||
		!incarnation.Valid || len(incarnation.String) != 32 || incarnation.String != strings.ToLower(incarnation.String) ||
		strings.Trim(incarnation.String, "0123456789abcdef") != "" {
		return migrationSchemaConflict("invalid sqlite store identity")
	}
	return nil
}

func migrationSchemaConflict(message string) error {
	return fmt.Errorf("%w: %s", session.ErrConflict, message)
}
