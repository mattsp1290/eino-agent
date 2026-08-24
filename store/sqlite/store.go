package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
)

//go:embed migrations/001_sqlite_store.sql
var migration001 string

//go:embed migrations/002_model_requests.sql
var migration002 string

// Store persists sessions in SQLite.
type Store struct {
	db *sql.DB
	tx *sql.Tx
}

// Open opens or creates a SQLite store and applies deterministic migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, migration001); err != nil {
		_ = db.Close()
		return nil, err
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		_ = db.Close()
		return nil, err
	}
	if version > 2 {
		_ = db.Close()
		return nil, fmt.Errorf("%w: unsupported sqlite schema version %d", session.ErrConflict, version)
	}
	if version < 2 {
		if _, err := db.ExecContext(ctx, migration002); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := verifySchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// WithinTx executes fn inside a SQLite transaction.
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, session.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	child := &Store{db: s.db, tx: tx}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback()
			panic(recovered)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()
	err = fn(ctx, child)
	return err
}

func (s *Store) CreateSession(ctx context.Context, record session.Session) (session.Session, error) {
	var existing session.Session
	if err := s.getJSON(ctx, "SELECT record FROM sessions WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.Session{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Session{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Session{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO sessions(id, record, updated_at) VALUES (?, ?, ?)`, record.ID, raw, timeText(record.UpdatedAt))
	return record, mapErr(err)
}

func (s *Store) GetSession(ctx context.Context, id session.ID) (session.Session, error) {
	var record session.Session
	err := s.getJSON(ctx, "SELECT record FROM sessions WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) UpdateSession(ctx context.Context, record session.Session) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE sessions SET record = ?, updated_at = ? WHERE id = ?`, raw, timeText(record.UpdatedAt), record.ID)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

func (s *Store) AdmitRun(ctx context.Context, record session.Run) (session.Run, error) {
	var existing session.Run
	if err := s.getJSON(ctx, "SELECT record FROM runs WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.Run{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Run{}, err
	}
	active, err := s.ActiveRun(ctx, record.SessionID)
	if err == nil && active.ID != record.ID {
		return session.Run{}, session.ErrSessionBusy
	}
	if err != nil && !errors.Is(err, session.ErrNotFound) {
		return session.Run{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Run{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO runs(id, session_id, status, owner_id, record, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		record.ID, record.SessionID, record.Status, record.OwnerID, raw, timeText(record.CreatedAt))
	if constraintFailed(err) {
		if reread, getErr := s.GetRun(ctx, record.ID); getErr == nil {
			if sameRecord(reread, record) {
				return reread, nil
			}
			return session.Run{}, session.ErrConflict
		}
		if active, activeErr := s.ActiveRun(ctx, record.SessionID); activeErr == nil && active.ID != record.ID {
			return session.Run{}, session.ErrSessionBusy
		}
	}
	return record, mapErr(err)
}

func (s *Store) GetRun(ctx context.Context, id session.RunID) (session.Run, error) {
	var record session.Run
	err := s.getJSON(ctx, "SELECT record FROM runs WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) ActiveRun(ctx context.Context, sessionID session.ID) (session.Run, error) {
	var record session.Run
	err := s.getJSON(ctx, `SELECT record FROM runs WHERE session_id = ? AND status IN (?, ?) ORDER BY created_at LIMIT 1`,
		[]any{sessionID, session.RunPending, session.RunRunning}, &record)
	return record, err
}

func (s *Store) ListUnfinishedRuns(ctx context.Context) ([]session.Run, error) {
	return listJSON[session.Run](ctx, s, `SELECT record FROM runs WHERE status IN (?, ?) ORDER BY created_at, id`, session.RunPending, session.RunRunning)
}

func (s *Store) RenewRunLease(ctx context.Context, runID session.RunID, ownerID string, until time.Time) error {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.OwnerID != ownerID {
		return session.ErrConflict
	}
	run.LeaseUntil = until
	return s.FinishRun(ctx, run)
}

func (s *Store) FinishRun(ctx context.Context, record session.Run) error {
	current, err := s.GetRun(ctx, record.ID)
	if err != nil {
		return err
	}
	if current.Terminal() {
		if sameRecord(current, record) {
			return nil
		}
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE runs SET status = ?, owner_id = ?, record = ? WHERE id = ? AND status IN (?, ?)`,
		record.Status, record.OwnerID, raw, record.ID, session.RunPending, session.RunRunning)
	if err != nil {
		return mapErr(err)
	}
	if err := rowsAffected(result); err != nil {
		latest, getErr := s.GetRun(ctx, record.ID)
		if getErr != nil {
			return getErr
		}
		if latest.Terminal() && sameRecord(latest, record) {
			return nil
		}
		return session.ErrConflict
	}
	return nil
}

func (s *Store) AppendMessage(ctx context.Context, record session.Message) (session.Message, error) {
	var existing session.Message
	if err := s.getJSON(ctx, "SELECT record FROM messages WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.Message{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Message{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Message{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO messages(id, session_id, run_id, role, record, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		record.ID, record.SessionID, record.RunID, record.Role, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) AppendPart(ctx context.Context, record session.Part) (session.Part, error) {
	var existing session.Part
	if err := s.getJSON(ctx, "SELECT record FROM parts WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.Part{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Part{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Part{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO parts(id, message_id, session_id, run_id, ordinal, record, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.MessageID, record.SessionID, record.RunID, record.Ordinal, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) UpdatePart(ctx context.Context, record session.Part) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE parts SET ordinal = ?, record = ? WHERE id = ?`, record.Ordinal, raw, record.ID)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

func (s *Store) ListMessages(ctx context.Context, sessionID session.ID, cursor session.ReplayCursor) (session.ReplayBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	args := []any{sessionID}
	where := "session_id = ?"
	if cursor.AfterMessageID != "" {
		after, err := s.GetMessage(ctx, cursor.AfterMessageID)
		if err != nil {
			return session.ReplayBatch{}, err
		}
		where += " AND (created_at > ? OR (created_at = ? AND id > ?))"
		args = append(args, timeText(after.CreatedAt), timeText(after.CreatedAt), cursor.AfterMessageID)
	}
	args = append(args, limit+1)
	messages, err := listJSON[session.Message](ctx, s, `SELECT record FROM messages WHERE `+where+` ORDER BY created_at, id LIMIT ?`, args...)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	next := session.ReplayCursor{}
	if len(messages) > limit {
		last := messages[limit-1]
		next = session.ReplayCursor{AfterMessageID: last.ID, Limit: limit}
		messages = messages[:limit]
	}
	parts, err := listJSON[session.Part](ctx, s, `SELECT p.record FROM parts p JOIN messages m ON p.message_id = m.id WHERE p.session_id = ? ORDER BY m.created_at, m.id, p.ordinal, p.id`, sessionID)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	include := make(map[session.MessageID]bool, len(messages))
	for _, msg := range messages {
		include[msg.ID] = true
	}
	filtered := parts[:0]
	for _, part := range parts {
		if include[part.MessageID] {
			filtered = append(filtered, part)
		}
	}
	parts = filtered
	return session.ReplayBatch{Messages: messages, Parts: parts, Next: next}, nil
}

func (s *Store) AppendEvent(ctx context.Context, record session.EventRecord) (session.EventRecord, error) {
	var existing session.EventRecord
	if err := s.getJSON(ctx, "SELECT record FROM events WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.EventRecord{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.EventRecord{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.EventRecord{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO events(id, session_id, run_id, kind, record, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		record.ID, record.SessionID, record.RunID, record.Kind, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) ListEvents(ctx context.Context, sessionID session.ID, cursor session.EventCursor) (session.EventBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	args := []any{sessionID}
	where := "session_id = ?"
	if cursor.AfterEventID != "" {
		var after session.EventRecord
		if err := s.getJSON(ctx, "SELECT record FROM events WHERE id = ?", []any{cursor.AfterEventID}, &after); err != nil {
			return session.EventBatch{}, err
		}
		where += " AND (created_at > ? OR (created_at = ? AND id > ?))"
		args = append(args, timeText(after.CreatedAt), timeText(after.CreatedAt), cursor.AfterEventID)
	}
	args = append(args, limit+1)
	events, err := listJSON[session.EventRecord](ctx, s, `SELECT record FROM events WHERE `+where+` ORDER BY created_at, id LIMIT ?`, args...)
	if err != nil {
		return session.EventBatch{}, err
	}
	next := session.EventCursor{}
	if len(events) > limit {
		last := events[limit-1]
		next = session.EventCursor{AfterEventID: last.ID, Limit: limit}
		events = events[:limit]
	}
	return session.EventBatch{Events: events, Next: next}, nil
}

func (s *Store) CreateToolCall(ctx context.Context, record session.ToolCall) (session.ToolCall, error) {
	var existing session.ToolCall
	if err := s.getJSON(ctx, "SELECT record FROM tool_calls WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.ToolCall{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.ToolCall{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ToolCall{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO tool_calls(id, session_id, run_id, message_id, status, claimed_by, claim_token, record) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.SessionID, record.RunID, record.MessageID, record.Status, record.ClaimedBy, record.ClaimToken, raw)
	return record, mapErr(err)
}

func (s *Store) GetToolCall(ctx context.Context, id session.ToolCallID) (session.ToolCall, error) {
	var record session.ToolCall
	err := s.getJSON(ctx, "SELECT record FROM tool_calls WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) ListUnfinishedToolCalls(ctx context.Context, runID session.RunID) ([]session.ToolCall, error) {
	return listJSON[session.ToolCall](ctx, s, `SELECT record FROM tool_calls WHERE run_id = ? AND status IN (?, ?) ORDER BY id`, runID, session.ToolCallPending, session.ToolCallRunning)
}

func (s *Store) ClaimToolCall(ctx context.Context, record session.ToolCall) (session.ToolCall, error) {
	current, err := s.GetToolCall(ctx, record.ID)
	if err != nil {
		return session.ToolCall{}, err
	}
	if session.TerminalToolCall(current.Status) || current.ClaimedBy != "" {
		if current.ClaimedBy == record.ClaimedBy && current.ClaimToken == record.ClaimToken {
			return current, nil
		}
		return session.ToolCall{}, session.ErrConflict
	}
	record.Status = session.ToolCallRunning
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ToolCall{}, err
	}
	result, err := s.exec(ctx, `UPDATE tool_calls SET status = ?, claimed_by = ?, claim_token = ?, record = ? WHERE id = ? AND status = ? AND claimed_by = ''`,
		record.Status, record.ClaimedBy, record.ClaimToken, raw, record.ID, session.ToolCallPending)
	if err != nil {
		return session.ToolCall{}, mapErr(err)
	}
	if err := rowsAffected(result); err != nil {
		current, getErr := s.GetToolCall(ctx, record.ID)
		if getErr != nil {
			return session.ToolCall{}, getErr
		}
		if current.ClaimedBy == record.ClaimedBy && current.ClaimToken == record.ClaimToken {
			return current, nil
		}
		return session.ToolCall{}, session.ErrConflict
	}
	return record, nil
}

func (s *Store) FinishToolCall(ctx context.Context, record session.ToolCall) error {
	current, err := s.GetToolCall(ctx, record.ID)
	if err != nil {
		return err
	}
	if current.ClaimedBy != record.ClaimedBy || current.ClaimToken != record.ClaimToken {
		return session.ErrConflict
	}
	if session.TerminalToolCall(current.Status) {
		if sameRecord(current, record) {
			return nil
		}
		return session.ErrConflict
	}
	if !session.TerminalToolCall(record.Status) {
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE tool_calls SET status = ?, claimed_by = ?, claim_token = ?, record = ? WHERE id = ? AND claimed_by = ? AND claim_token = ? AND status IN (?, ?)`,
		record.Status, record.ClaimedBy, record.ClaimToken, raw, record.ID, record.ClaimedBy, record.ClaimToken, session.ToolCallPending, session.ToolCallRunning)
	if err != nil {
		return mapErr(err)
	}
	if err := rowsAffected(result); err != nil {
		latest, getErr := s.GetToolCall(ctx, record.ID)
		if getErr != nil {
			return getErr
		}
		if session.TerminalToolCall(latest.Status) && sameRecord(latest, record) {
			return nil
		}
		return session.ErrConflict
	}
	return nil
}

// SettleToolCall atomically commits a terminal call and its reserved result
// message/part. Repeating the identical settlement is idempotent.
func (s *Store) SettleToolCall(ctx context.Context, settlement session.ToolSettlement) error {
	if s.tx == nil {
		return s.WithinTx(ctx, func(ctx context.Context, tx session.Tx) error {
			store, ok := tx.(*Store)
			if !ok {
				return session.ErrConflict
			}
			return store.SettleToolCall(ctx, settlement)
		})
	}
	call, err := s.GetToolCall(ctx, settlement.ID)
	if err != nil {
		return err
	}
	if !validToolResultEnvelope(call, settlement) {
		return session.ErrConflict
	}
	settled, err := settlement.Apply(call)
	if err != nil {
		return err
	}
	if err := s.FinishToolCall(ctx, settled); err != nil {
		return err
	}
	if _, err := s.AppendMessage(ctx, settlement.ResultMessage); err != nil {
		return err
	}
	if _, err := s.AppendPart(ctx, settlement.ResultPart); err != nil {
		return err
	}
	return nil
}

func validToolResultEnvelope(call session.ToolCall, settlement session.ToolSettlement) bool {
	message := settlement.ResultMessage
	part := settlement.ResultPart
	return call.ResultMessageID != "" && call.ResultPartID != "" &&
		message.ID == call.ResultMessageID && message.SessionID == call.SessionID && message.RunID == call.RunID && message.ParentID == call.MessageID && message.Role == session.RoleTool &&
		part.ID == call.ResultPartID && part.MessageID == call.ResultMessageID && part.SessionID == call.SessionID && part.RunID == call.RunID && part.Kind == session.PartToolResult &&
		rawJSONEqual(part.Payload, settlement.Output)
}

func rawJSONEqual(left, right json.RawMessage) bool {
	left = bytes.TrimSpace(left)
	right = bytes.TrimSpace(right)
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right)
	}
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil {
		return reflect.DeepEqual(leftValue, rightValue)
	}
	return bytes.Equal(left, right)
}

func (s *Store) StartContextEpoch(ctx context.Context, record session.ContextEpoch) (session.ContextEpoch, error) {
	var existing session.ContextEpoch
	if err := s.getJSON(ctx, "SELECT record FROM context_epochs WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.ContextEpoch{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.ContextEpoch{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ContextEpoch{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO context_epochs(id, session_id, record, closed_at) VALUES (?, ?, ?, ?)`, record.ID, record.SessionID, raw, timeText(record.ClosedAt))
	if constraintFailed(err) {
		var reread session.ContextEpoch
		if getErr := s.getJSON(ctx, "SELECT record FROM context_epochs WHERE id = ?", []any{record.ID}, &reread); getErr == nil && sameRecord(reread, record) {
			return reread, nil
		}
	}
	return record, mapErr(err)
}

func (s *Store) FinishContextEpoch(ctx context.Context, record session.ContextEpoch) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE context_epochs SET record = ?, closed_at = ? WHERE id = ?`, raw, timeText(record.ClosedAt), record.ID)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

func (s *Store) ListContextEpochs(ctx context.Context, sessionID session.ID) ([]session.ContextEpoch, error) {
	epochs, err := listJSON[session.ContextEpoch](ctx, s, `SELECT record FROM context_epochs WHERE session_id = ? ORDER BY json_extract(record, '$.CreatedAt'), id`, sessionID)
	if err != nil {
		return nil, err
	}
	return epochs, nil
}

func (s *Store) GetMessage(ctx context.Context, id session.MessageID) (session.Message, error) {
	var record session.Message
	err := s.getJSON(ctx, "SELECT record FROM messages WHERE id = ?", []any{id}, &record)
	return record, err
}

const maxModelRequestRecordBytes = 4 << 20

func (s *Store) CreateModelRequest(ctx context.Context, record session.ModelRequestRecord) (session.ModelRequestRecord, error) {
	if record.ID == "" || record.RunID == "" || record.State != session.ModelRequestPrepared {
		return session.ModelRequestRecord{}, session.ErrConflict
	}
	var existing session.ModelRequestRecord
	if err := s.getJSON(ctx, "SELECT record FROM model_requests WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.ModelRequestRecord{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.ModelRequestRecord{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ModelRequestRecord{}, err
	}
	if len(raw) > maxModelRequestRecordBytes {
		return session.ModelRequestRecord{}, session.ErrModelRequestTooLarge
	}
	_, err = s.exec(ctx, `INSERT INTO model_requests(id, run_id, state, attempt, step, record, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, record.ID, record.RunID, record.State, record.Attempt, record.Step, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) UpdateModelRequest(ctx context.Context, record session.ModelRequestRecord) error {
	current, err := s.GetModelRequest(ctx, record.ID)
	if err != nil {
		return err
	}
	if !sameModelRequestIdentity(current, record) {
		return session.ErrConflict
	}
	if current.State == record.State {
		if sameRecord(current, record) {
			return nil
		}
		return session.ErrConflict
	}
	if !session.ValidModelRequestTransition(current.State, record.State) {
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(raw) > maxModelRequestRecordBytes {
		return session.ErrModelRequestTooLarge
	}
	result, err := s.exec(ctx, `UPDATE model_requests SET state = ?, record = ? WHERE id = ? AND state = ?`, record.State, raw, record.ID, current.State)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

func sameModelRequestIdentity(left, right session.ModelRequestRecord) bool {
	left.State, right.State = "", ""
	left.ErrorCode, right.ErrorCode = "", ""
	left.UpdatedAt, right.UpdatedAt = time.Time{}, time.Time{}
	return sameRecord(left, right)
}

func (s *Store) GetModelRequest(ctx context.Context, id session.ModelRequestID) (session.ModelRequestRecord, error) {
	var record session.ModelRequestRecord
	err := s.getJSON(ctx, "SELECT record FROM model_requests WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) ListModelRequests(ctx context.Context, runID session.RunID, cursor session.ModelRequestCursor) (session.ModelRequestBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	where := "run_id = ?"
	args := []any{runID}
	if cursor.AfterID != "" {
		after, err := s.GetModelRequest(ctx, cursor.AfterID)
		if err != nil {
			return session.ModelRequestBatch{}, err
		}
		where += " AND (created_at > ? OR (created_at = ? AND id > ?))"
		args = append(args, timeText(after.CreatedAt), timeText(after.CreatedAt), cursor.AfterID)
	}
	args = append(args, limit+1)
	records, err := listJSON[session.ModelRequestRecord](ctx, s, `SELECT record FROM model_requests WHERE `+where+` ORDER BY created_at, id LIMIT ?`, args...)
	if err != nil {
		return session.ModelRequestBatch{}, err
	}
	next := session.ModelRequestCursor{}
	if len(records) > limit {
		next = session.ModelRequestCursor{AfterID: records[limit-1].ID, Limit: limit}
		records = records[:limit]
	}
	return session.ModelRequestBatch{Records: records, Next: next}, nil
}

func sameRecord[T any](left, right T) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func verifySchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		return err
	}
	if version != 2 {
		return fmt.Errorf("%w: unsupported sqlite schema version %d", session.ErrConflict, version)
	}
	required := map[string][]string{
		"sessions":       {"id", "record", "updated_at"},
		"runs":           {"id", "session_id", "status", "owner_id", "record", "created_at"},
		"messages":       {"id", "session_id", "run_id", "role", "record", "created_at"},
		"parts":          {"id", "message_id", "session_id", "run_id", "ordinal", "record", "created_at"},
		"events":         {"id", "session_id", "run_id", "kind", "record", "created_at"},
		"tool_calls":     {"id", "session_id", "run_id", "message_id", "status", "claimed_by", "claim_token", "record"},
		"context_epochs": {"id", "session_id", "record", "closed_at"},
		"model_requests": {"id", "run_id", "state", "attempt", "step", "record", "created_at"},
	}
	for table, columns := range required {
		if err := verifyColumns(ctx, db, table, columns); err != nil {
			return err
		}
	}
	return nil
}

func verifyColumns(ctx context.Context, db *sql.DB, table string, required []string) error {
	rows, err := db.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	present := make(map[string]bool, len(required))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		present[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, column := range required {
		if !present[column] {
			return fmt.Errorf("%w: sqlite schema missing %s.%s", session.ErrConflict, table, column)
		}
	}
	return nil
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if s.tx != nil {
		return s.tx.ExecContext(ctx, query, args...)
	}
	return s.db.ExecContext(ctx, query, args...)
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	if s.tx != nil {
		return s.tx.QueryRowContext(ctx, query, args...)
	}
	return s.db.QueryRowContext(ctx, query, args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if s.tx != nil {
		return s.tx.QueryContext(ctx, query, args...)
	}
	return s.db.QueryContext(ctx, query, args...)
}

func (s *Store) getJSON(ctx context.Context, query string, args []any, dst any) error {
	var raw []byte
	if err := s.queryRow(ctx, query, args...).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return session.ErrNotFound
		}
		return err
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("%w: %v", session.ErrConflict, err)
	}
	return nil
}

func listJSON[T any](ctx context.Context, s *Store, query string, args ...any) ([]T, error) {
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []T
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%w: %v", session.ErrConflict, err)
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func rowsAffected(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return session.ErrNotFound
	}
	return nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if constraintFailed(err) {
		return session.ErrConflict
	}
	return err
}

func constraintFailed(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "constraint failed") || strings.Contains(msg, "constraint failed:")
}

func timeText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
