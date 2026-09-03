package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) appendMessage(ctx context.Context, record session.Message) (session.Message, error) {
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

func (s *Store) appendPart(ctx context.Context, record session.Part) (session.Part, error) {
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

func (s *Store) updatePart(ctx context.Context, record session.Part) error {
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
		var authoritativeID, authoritativeSessionID, authoritativeRunID, authoritativeRole, authoritativeCreatedAt string
		var raw []byte
		err := s.queryRow(ctx, `SELECT id, session_id, run_id, role, created_at, record FROM messages WHERE id = ? AND session_id = ?`, cursor.AfterMessageID, sessionID).
			Scan(&authoritativeID, &authoritativeSessionID, &authoritativeRunID, &authoritativeRole, &authoritativeCreatedAt, &raw)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return session.ReplayBatch{}, session.ErrNotFound
			}
			return session.ReplayBatch{}, err
		}
		after, err := decodeAuthoritativeMessage(authoritativeID, authoritativeSessionID, authoritativeRunID, authoritativeRole, authoritativeCreatedAt, raw)
		if err != nil {
			return session.ReplayBatch{}, err
		}
		where += " AND (created_at > ? OR (created_at = ? AND id > ?))"
		args = append(args, timeText(after.CreatedAt), timeText(after.CreatedAt), cursor.AfterMessageID)
	}
	args = append(args, limit+1)
	messageRows, err := s.query(ctx, `SELECT id, session_id, run_id, role, created_at, record FROM messages WHERE `+where+` ORDER BY created_at, id LIMIT ?`, args...)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	var messages []session.Message
	var messageIDs []session.MessageID
	for messageRows.Next() {
		var authoritativeID, authoritativeSessionID, authoritativeRunID, authoritativeRole, authoritativeCreatedAt string
		var raw []byte
		if err := messageRows.Scan(&authoritativeID, &authoritativeSessionID, &authoritativeRunID, &authoritativeRole, &authoritativeCreatedAt, &raw); err != nil {
			_ = messageRows.Close()
			return session.ReplayBatch{}, err
		}
		message, err := decodeAuthoritativeMessage(authoritativeID, authoritativeSessionID, authoritativeRunID, authoritativeRole, authoritativeCreatedAt, raw)
		if err != nil {
			_ = messageRows.Close()
			return session.ReplayBatch{}, err
		}
		messages = append(messages, message)
		messageIDs = append(messageIDs, session.MessageID(authoritativeID))
	}
	if err := messageRows.Err(); err != nil {
		_ = messageRows.Close()
		return session.ReplayBatch{}, err
	}
	if err := messageRows.Close(); err != nil {
		return session.ReplayBatch{}, err
	}
	next := session.ReplayCursor{}
	if len(messages) > limit {
		next = session.ReplayCursor{AfterMessageID: messageIDs[limit-1], Limit: limit}
		messages = messages[:limit]
		messageIDs = messageIDs[:limit]
	}
	if len(messages) == 0 {
		return session.ReplayBatch{Messages: messages, Next: next}, nil
	}
	placeholders := make([]string, len(messages))
	partArgs := make([]any, len(messages))
	for index, messageID := range messageIDs {
		placeholders[index] = "?"
		partArgs[index] = messageID
	}
	rows, err := s.query(ctx, `WITH page(id, created_at) AS (
		SELECT id, created_at FROM messages WHERE id IN (`+strings.Join(placeholders, ",")+`)
	) SELECT p.id, p.message_id, p.session_id, p.run_id, p.ordinal, p.record FROM page JOIN parts p ON p.message_id = page.id
	ORDER BY page.created_at, page.id, p.ordinal, p.id`, partArgs...)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	defer func() { _ = rows.Close() }()
	var parts []session.Part
	var owners []session.MessageID
	stateCounts := make(map[session.MessageID]int)
	stateBytes := make(map[session.MessageID]int)
	for rows.Next() {
		var authoritativeID, authoritativeMessageID, authoritativeSessionID, authoritativeRunID string
		var authoritativeOrdinal int64
		var raw []byte
		if err := rows.Scan(&authoritativeID, &authoritativeMessageID, &authoritativeSessionID, &authoritativeRunID, &authoritativeOrdinal, &raw); err != nil {
			return session.ReplayBatch{}, err
		}
		var part session.Part
		if err := json.Unmarshal(raw, &part); err != nil ||
			part.ID != session.PartID(authoritativeID) || part.MessageID != session.MessageID(authoritativeMessageID) ||
			part.SessionID != session.ID(authoritativeSessionID) || part.RunID != session.RunID(authoritativeRunID) ||
			part.Ordinal != authoritativeOrdinal {
			return session.ReplayBatch{}, session.ErrConflict
		}
		owner := session.MessageID(authoritativeMessageID)
		if part.Kind == session.PartProviderState {
			if stateCounts[owner] >= session.ProviderStateHardMaxItems ||
				len(part.Payload) > session.ProviderStateHardMaxStoredMessageBytes-stateBytes[owner] {
				return session.ReplayBatch{}, session.ErrConflict
			}
			stateCounts[owner]++
			stateBytes[owner] += len(part.Payload)
		}
		parts = append(parts, part)
		owners = append(owners, owner)
	}
	if err := rows.Err(); err != nil {
		return session.ReplayBatch{}, err
	}
	return session.ReplayBatch{Messages: messages, Parts: parts, PartOwnerMessageIDs: owners, Next: next}, nil
}

func decodeAuthoritativeMessage(id, sessionID, runID, role, createdAt string, raw []byte) (session.Message, error) {
	var message session.Message
	if err := json.Unmarshal(raw, &message); err != nil ||
		message.ID != session.MessageID(id) || message.SessionID != session.ID(sessionID) ||
		message.RunID != session.RunID(runID) || message.Role != session.Role(role) ||
		timeText(message.CreatedAt) != createdAt {
		return session.Message{}, session.ErrConflict
	}
	return message, nil
}

func (s *Store) GetMessage(ctx context.Context, id session.MessageID) (session.Message, error) {
	var record session.Message
	err := s.getJSON(ctx, "SELECT record FROM messages WHERE id = ?", []any{id}, &record)
	return record, err
}
