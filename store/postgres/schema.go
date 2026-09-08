package postgres

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

	"github.com/mattsp1290/eino-agent/session"
)

type schemaState uint8

const (
	schemaEmpty schemaState = iota
	schemaBootstrap
	schemaCurrent
)

// These fingerprints describe catalog structure, never application records or
// sequence positions. Regeneration is explicit and tied to the reviewed SQL.
//
//go:embed schema_fingerprints.json
var schemaFingerprints []byte

const schemaCatalogSettings = `SET LOCAL search_path = pg_catalog, pg_temp;
SET LOCAL quote_all_identifiers = off`

// inspectSchema uses the caller's pinned connection so a later migration locker
// can retain its session lock across verification and migration dispatch.
func inspectSchema(ctx context.Context, conn *sql.Conn) (state schemaState, err error) {
	if conn == nil {
		return 0, fmt.Errorf("%w: nil postgres connection", session.ErrConflict)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	// Deparsers otherwise vary with host quoting and temporary type visibility.
	// SET LOCAL is confined to this transaction and cannot reconfigure the pool.
	if _, err = tx.ExecContext(ctx, schemaCatalogSettings); err != nil {
		return 0, err
	}
	objects, err := readSchemaCatalog(ctx, tx)
	if err != nil {
		return 0, err
	}
	state, err = classifySchema(ctx, tx, objects)
	if err != nil {
		return 0, err
	}
	return state, tx.Commit()
}

func classifySchema(ctx context.Context, tx *sql.Tx, objects map[string]string) (schemaState, error) {
	if len(objects) == 0 {
		return schemaEmpty, nil
	}
	var expected struct{ Bootstrap, Current string }
	if err := json.Unmarshal(schemaFingerprints, &expected); err != nil {
		return 0, fmt.Errorf("postgres schema reference: %w", err)
	}
	state := schemaBootstrap
	switch catalogFingerprint(objects) {
	case expected.Bootstrap:
	case expected.Current:
		state = schemaCurrent
	default:
		return 0, fmt.Errorf("%w: unsupported postgres catalog structure", session.ErrConflict)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, version_id, is_applied FROM public.eino_agent_goose_version ORDER BY id LIMIT 3`)
	if err != nil {
		return 0, err
	}
	var count int
	var previousID int64
	valid := true
	for rows.Next() {
		var id, version int64
		var applied bool
		if err := rows.Scan(&id, &version, &applied); err != nil {
			return 0, errors.Join(err, rows.Close())
		}
		valid = valid && id > previousID && version == int64(count) && applied
		previousID = id
		count++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, err
	}
	wantRows := 1
	if state == schemaCurrent {
		wantRows = 2
	}
	if !valid || count != wantRows {
		return 0, fmt.Errorf("%w: unsupported postgres migration history", session.ErrConflict)
	}
	if state == schemaCurrent {
		var incarnation string
		if err := tx.QueryRowContext(ctx, `SELECT incarnation FROM public.observation_store WHERE singleton=1`).Scan(&incarnation); err != nil {
			return 0, fmt.Errorf("%w: postgres store identity unavailable: %w", session.ErrConflict, err)
		}
		decoded, err := hex.DecodeString(incarnation)
		if err != nil || len(decoded) != 16 || strings.ToLower(incarnation) != incarnation {
			return 0, fmt.Errorf("%w: invalid postgres store identity", session.ErrConflict)
		}
	}
	return state, nil
}

func catalogFingerprint(objects map[string]string) string {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00", key, objects[key])
	}
	return hex.EncodeToString(hash.Sum(nil))
}
