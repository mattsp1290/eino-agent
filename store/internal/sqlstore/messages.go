package sqlstore

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) appendMessage(ctx context.Context, record session.Message) (session.Message, error) {
	sessionKey, err := s.key(ctx, "sessions", string(record.SessionID))
	if err != nil {
		return session.Message{}, err
	}
	runKey, err := s.key(ctx, "runs", string(record.RunID))
	if err != nil {
		return session.Message{}, err
	}
	db := s.dbFor(ctx)
	var ownerCount int64
	if err := db.Table("runs").Where("row_key = ? AND session_key = ?", runKey, sessionKey).Count(&ownerCount).Error; err != nil {
		return session.Message{}, s.mapErr(err)
	}
	if ownerCount != 1 {
		return session.Message{}, session.ErrConflict
	}
	row, readErr := s.messageRowByID(ctx, string(record.ID))
	if readErr == nil {
		var existing session.Message
		if err := decodeStoredRecord(row.Record, &existing); err != nil {
			return session.Message{}, err
		}
		if !messageRowMatches(row, existing) || row.SessionKey != sessionKey || row.RunKey != runKey {
			return session.Message{}, session.ErrConflict
		}
		if !SameRecord(existing, record) {
			return session.Message{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(readErr, session.ErrNotFound) {
		return session.Message{}, readErr
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Message{}, err
	}
	created := db.Table("messages").Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"id": publicID(record.ID), "session_key": sessionKey, "run_key": runKey,
		"role": string(record.Role), "finalized": record.Role != session.RoleAssistant,
		"record": raw, "created_at": TimeText(record.CreatedAt),
	})
	if err := s.mapErr(created.Error); err != nil {
		return session.Message{}, err
	}
	if created.RowsAffected != 0 {
		return record, nil
	}
	row, err = s.messageRowByID(ctx, string(record.ID))
	if err != nil {
		return session.Message{}, err
	}
	var existing session.Message
	if err := decodeStoredRecord(row.Record, &existing); err != nil {
		return session.Message{}, err
	}
	if !messageRowMatches(row, existing) || row.SessionKey != sessionKey || row.RunKey != runKey {
		return session.Message{}, session.ErrConflict
	}
	if !SameRecord(existing, record) {
		return session.Message{}, session.ErrConflict
	}
	return existing, nil
}

func (s *Store) appendPart(ctx context.Context, record session.Part) (session.Part, error) {
	if err := s.validatePartOwner(ctx, record); err != nil {
		return session.Part{}, err
	}
	text, valid := ObservationText(record)
	messageKey, sessionKey, runKey, err := s.partOwnerKeys(ctx, record)
	if err != nil {
		return session.Part{}, err
	}
	db := s.dbFor(ctx)
	row, readErr := s.partRowByID(ctx, string(record.ID))
	if readErr == nil {
		var existing session.Part
		if err := decodeStoredRecord(row.Record, &existing); err != nil {
			return session.Part{}, err
		}
		if !partRowMatches(row, existing) || row.MessageKey != messageKey || row.SessionKey != sessionKey || row.RunKey != runKey {
			return session.Part{}, session.ErrConflict
		}
		if !SameRecord(existing, record) {
			return session.Part{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(readErr, session.ErrNotFound) {
		return session.Part{}, readErr
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Part{}, err
	}
	created := db.Table("parts").Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"id": publicID(record.ID), "message_key": messageKey, "session_key": sessionKey,
		"run_key": runKey, "ordinal": record.Ordinal, "kind": string(record.Kind),
		"display_text": []byte(text), "text_valid": valid, "record": raw,
		"created_at": TimeText(record.CreatedAt),
	})
	if err := s.mapErr(created.Error); err != nil {
		return session.Part{}, err
	}
	if created.RowsAffected != 0 {
		return record, nil
	}
	row, err = s.partRowByID(ctx, string(record.ID))
	if err != nil {
		return session.Part{}, err
	}
	var existing session.Part
	if err := decodeStoredRecord(row.Record, &existing); err != nil {
		return session.Part{}, err
	}
	if !partRowMatches(row, existing) || row.MessageKey != messageKey || row.SessionKey != sessionKey || row.RunKey != runKey {
		return session.Part{}, session.ErrConflict
	}
	if !SameRecord(existing, record) {
		return session.Part{}, session.ErrConflict
	}
	return existing, nil
}

func (s *Store) updatePart(ctx context.Context, record session.Part) error {
	if err := s.validatePartOwner(ctx, record); err != nil {
		return err
	}
	text, valid := ObservationText(record)
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	messageKey, sessionKey, runKey, err := s.partOwnerKeys(ctx, record)
	if err != nil {
		return err
	}
	db := s.dbFor(ctx).Table("parts").Where("id = ? AND session_key = ? AND run_key = ? AND message_key = ?",
		publicID(record.ID), sessionKey, runKey, messageKey).Updates(map[string]any{
		"ordinal": record.Ordinal, "kind": string(record.Kind), "display_text": []byte(text),
		"text_valid": valid, "record": raw,
	})
	if err := s.mapErr(db.Error); err != nil {
		return err
	}
	return rowsAffected(db)
}

func (s *Store) validatePartOwner(ctx context.Context, record session.Part) error {
	messageKey, sessionKey, runKey, err := s.partOwnerKeys(ctx, record)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return session.ErrConflict
		}
		return err
	}
	var owner struct {
		SessionKey int64 `gorm:"column:session_key"`
		RunKey     int64 `gorm:"column:run_key"`
	}
	result := s.dbFor(ctx).Table("messages").Select("session_key, run_key").Where("row_key = ?", messageKey).Take(&owner)
	if result.Error != nil {
		mapped := s.mapErr(result.Error)
		if errors.Is(mapped, session.ErrNotFound) {
			return session.ErrConflict
		}
		return mapped
	}
	if owner.SessionKey != sessionKey || owner.RunKey != runKey {
		return session.ErrConflict
	}
	return nil
}

func messageRowMatches(row messageRow, record session.Message) bool {
	return string(row.ID) == string(record.ID) && row.Role == string(record.Role) &&
		row.Finalized == (record.Role != session.RoleAssistant) && TimeText(record.CreatedAt) == row.CreatedAt
}

func partRowMatches(row partRow, record session.Part) bool {
	text, valid := ObservationText(record)
	return string(row.ID) == string(record.ID) && row.Ordinal == record.Ordinal && row.Kind == string(record.Kind) &&
		string(row.DisplayText) == text && row.TextValid == valid && TimeText(record.CreatedAt) == row.CreatedAt
}

func (s *Store) partOwnerKeys(ctx context.Context, record session.Part) (messageKey, sessionKey, runKey int64, err error) {
	sessionKey, err = s.key(ctx, "sessions", string(record.SessionID))
	if err != nil {
		return 0, 0, 0, err
	}
	runKey, err = s.key(ctx, "runs", string(record.RunID))
	if err != nil {
		return 0, 0, 0, err
	}
	messageKey, err = s.key(ctx, "messages", string(record.MessageID))
	return messageKey, sessionKey, runKey, err
}

func (s *Store) ListMessages(ctx context.Context, sessionID session.ID, cursor session.ReplayCursor) (session.ReplayBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	sessionKey, err := s.key(ctx, "sessions", string(sessionID))
	if err != nil {
		return session.ReplayBatch{}, err
	}
	where := "m.session_key = ?"
	args := []any{sessionKey}
	if cursor.AfterMessageID != "" {
		var after messageRow
		result := s.dbFor(ctx).Table("messages AS m").Select("m.id, m.session_key, m.run_key, m.role, m.finalized, m.record, m.created_at").Where("m.id = ? AND m.session_key = ?", publicID(cursor.AfterMessageID), sessionKey).Take(&after)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return session.ReplayBatch{}, session.ErrNotFound
		}
		if err := s.mapErr(result.Error); err != nil {
			return session.ReplayBatch{}, err
		}
		where += " AND (m.created_at > ? OR (m.created_at = ? AND m.id > ?))"
		args = append(args, after.CreatedAt, after.CreatedAt, publicID(cursor.AfterMessageID))
	}
	args = append(args, limit+1)
	messages, ids, err := s.loadReplayMessages(ctx, where, args...)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	next := session.ReplayCursor{}
	if len(messages) > limit {
		next = session.ReplayCursor{AfterMessageID: ids[limit-1], Limit: limit}
		messages, ids = messages[:limit], ids[:limit]
	}
	if len(messages) == 0 {
		return session.ReplayBatch{Messages: messages, Next: next}, nil
	}
	parts, owners, err := s.loadReplayParts(ctx, ids)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	return session.ReplayBatch{Messages: messages, Parts: parts, PartOwnerMessageIDs: owners, Next: next}, nil
}

func (s *Store) loadReplayMessages(ctx context.Context, where string, args ...any) ([]session.Message, []session.MessageID, error) {
	var rows []replayMessageRow
	limit, ok := args[len(args)-1].(int)
	if !ok {
		return nil, nil, session.ErrConflict
	}
	db := s.dbFor(ctx).Table("messages AS m").Select("m.id, s.id AS session_id, r.id AS run_id, m.role, m.record, m.created_at").Joins("JOIN sessions AS s ON s.row_key = m.session_key").Joins("JOIN runs AS r ON r.row_key = m.run_key").Where(where, args[:len(args)-1]...).Order("m.created_at, m.id").Limit(limit)
	if err := s.mapErr(db.Find(&rows).Error); err != nil {
		return nil, nil, err
	}
	messages := make([]session.Message, 0, len(rows))
	ids := make([]session.MessageID, 0, len(rows))
	for _, row := range rows {
		message, err := DecodeAuthoritativeMessage(string(row.ID), string(row.SessionID), string(row.RunID), row.Role, row.CreatedAt, row.Record)
		if err != nil {
			return nil, nil, err
		}
		messages = append(messages, message)
		ids = append(ids, message.ID)
	}
	return messages, ids, nil
}

func (s *Store) loadReplayParts(ctx context.Context, messageIDs []session.MessageID) ([]session.Part, []session.MessageID, error) {
	if len(messageIDs) == 0 {
		return nil, nil, nil
	}
	keys := make([]int64, len(messageIDs))
	for i, id := range messageIDs {
		key, err := s.key(ctx, "messages", string(id))
		if err != nil {
			return nil, nil, err
		}
		keys[i] = key
	}
	var rows []replayPartRow
	db := s.dbFor(ctx).Table("parts AS p").Select("p.id, m.id AS message_id, s.id AS session_id, r.id AS run_id, p.ordinal, p.record").Joins("JOIN messages AS m ON m.row_key = p.message_key").Joins("JOIN sessions AS s ON s.row_key = p.session_key").Joins("JOIN runs AS r ON r.row_key = p.run_key").Where("p.message_key IN ?", keys).Order("m.created_at, m.id, p.ordinal, p.id")
	if err := s.mapErr(db.Find(&rows).Error); err != nil {
		return nil, nil, err
	}
	parts := make([]session.Part, 0, len(rows))
	owners := make([]session.MessageID, 0, len(rows))
	stateCounts := make(map[session.MessageID]int)
	stateBytes := make(map[session.MessageID]int)
	for _, row := range rows {
		part, err := DecodeAuthoritativePart(string(row.ID), string(row.MessageID), string(row.SessionID), string(row.RunID), row.Ordinal, row.Record)
		if err != nil {
			return nil, nil, err
		}
		if part.Kind == session.PartProviderState {
			owner := part.MessageID
			if stateCounts[owner] >= session.ProviderStateHardMaxItems || len(part.Payload) > session.ProviderStateHardMaxStoredMessageBytes-stateBytes[owner] {
				return nil, nil, session.ErrConflict
			}
			stateCounts[owner]++
			stateBytes[owner] += len(part.Payload)
		}
		parts = append(parts, part)
		owners = append(owners, part.MessageID)
	}
	return parts, owners, nil
}

func (s *Store) GetMessage(ctx context.Context, id session.MessageID) (session.Message, error) {
	var row replayMessageRow
	result := s.dbFor(ctx).Table("messages AS m").Select("m.id, s.id AS session_id, r.id AS run_id, m.role, m.record, m.created_at").Joins("JOIN sessions AS s ON s.row_key = m.session_key").Joins("JOIN runs AS r ON r.row_key = m.run_key").Where("m.id = ?", publicID(id)).Take(&row)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return session.Message{}, session.ErrNotFound
		}
		return session.Message{}, s.mapErr(result.Error)
	}
	return DecodeAuthoritativeMessage(string(row.ID), string(row.SessionID), string(row.RunID), row.Role, row.CreatedAt, row.Record)
}

func publicID[T ~string](value T) []byte { return []byte(value) }
