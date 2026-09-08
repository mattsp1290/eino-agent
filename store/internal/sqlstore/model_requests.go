package sqlstore

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

const maxModelRequestRecordBytes = 4 << 20

func (s *Store) createModelRequest(ctx context.Context, record session.ModelRequestRecord) (session.ModelRequestRecord, error) {
	if record.ID == "" || record.RunID == "" || record.State != session.ModelRequestPrepared {
		return session.ModelRequestRecord{}, session.ErrConflict
	}
	sessionKey, runKey, err := s.modelRequestOwnerKeys(ctx, record.SessionID, record.RunID)
	if err != nil {
		return session.ModelRequestRecord{}, err
	}
	db := s.dbFor(ctx)
	validateExisting := func(row modelRequestRow) (session.ModelRequestRecord, error) {
		existing, err := modelRequestRecordFromRow(row, string(record.ID))
		if err != nil {
			return session.ModelRequestRecord{}, err
		}
		if row.SessionKey != sessionKey || row.RunKey != runKey {
			return session.ModelRequestRecord{}, session.ErrConflict
		}
		if !SameRecord(existing, record) {
			return session.ModelRequestRecord{}, session.ErrConflict
		}
		return existing, nil
	}
	row, readErr := s.modelRequestRowByID(ctx, string(record.ID))
	if readErr == nil {
		return validateExisting(row)
	} else if !errors.Is(readErr, session.ErrNotFound) {
		return session.ModelRequestRecord{}, readErr
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ModelRequestRecord{}, err
	}
	if len(raw) > maxModelRequestRecordBytes {
		return session.ModelRequestRecord{}, session.ErrModelRequestTooLarge
	}
	created := db.Table("model_requests").Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"id": publicID(record.ID), "session_key": sessionKey, "run_key": runKey,
		"assistant_message_id": publicID(record.AssistantMessageID), "state": string(record.State),
		"attempt": record.Attempt, "step": record.Step, "record": raw,
		"created_at": TimeText(record.CreatedAt),
	})
	if err := s.mapErr(created.Error); err != nil {
		return session.ModelRequestRecord{}, err
	}
	if created.RowsAffected != 0 {
		return record, nil
	}
	row, err = s.modelRequestRowByID(ctx, string(record.ID))
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return session.ModelRequestRecord{}, session.ErrConflict
		}
		return session.ModelRequestRecord{}, err
	}
	return validateExisting(row)
}

func (s *Store) updateModelRequest(ctx context.Context, record session.ModelRequestRecord) error {
	sessionKey, runKey, err := s.modelRequestOwnerKeys(ctx, record.SessionID, record.RunID)
	if err != nil {
		return err
	}
	row, err := s.modelRequestRowByID(ctx, string(record.ID))
	if err != nil {
		return err
	}
	if row.SessionKey != sessionKey || row.RunKey != runKey {
		return session.ErrConflict
	}
	current, err := modelRequestRecordFromRow(row, string(record.ID))
	if err != nil {
		return err
	}
	if !SameModelRequestIdentity(current, record) {
		return session.ErrConflict
	}
	if current.State == record.State {
		if SameRecord(current, record) {
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
	db := s.dbFor(ctx).Table("model_requests").Where("id = ? AND state = ?", publicID(record.ID), string(current.State)).Updates(map[string]any{
		"state": string(record.State), "record": raw,
	})
	if err := s.mapErr(db.Error); err != nil {
		return err
	}
	return rowsAffected(db)
}

func (s *Store) GetModelRequest(ctx context.Context, id session.ModelRequestID) (session.ModelRequestRecord, error) {
	row, err := s.modelRequestRowByID(ctx, string(id))
	if err != nil {
		return session.ModelRequestRecord{}, err
	}
	return modelRequestRecordFromRow(row, string(id))
}

func modelRequestRecordFromRow(row modelRequestRow, id string) (session.ModelRequestRecord, error) {
	var record session.ModelRequestRecord
	if err := decodeModelRequest(row, &record); err != nil {
		return session.ModelRequestRecord{}, err
	}
	if !modelRequestRowMatches(row, record) || string(record.ID) != id {
		return session.ModelRequestRecord{}, session.ErrConflict
	}
	return record, nil
}

func modelRequestRowMatches(row modelRequestRow, record session.ModelRequestRecord) bool {
	return string(row.ID) == string(record.ID) && string(row.SessionID) == string(record.SessionID) && string(row.RunID) == string(record.RunID) && string(row.AssistantMessageID) == string(record.AssistantMessageID) &&
		row.State == string(record.State) && row.Attempt == record.Attempt && row.Step == record.Step &&
		TimeText(record.CreatedAt) == row.CreatedAt
}

func (s *Store) ListModelRequests(ctx context.Context, runID session.RunID, cursor session.ModelRequestCursor) (session.ModelRequestBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	runKey, err := s.key(ctx, "runs", string(runID))
	if err != nil {
		return session.ModelRequestBatch{}, err
	}
	query := s.modelRequestQuery(ctx).Where("model_requests.run_key = ?", runKey)
	if cursor.AfterID != "" {
		after, err := s.modelRequestRowByID(ctx, string(cursor.AfterID))
		if err != nil {
			return session.ModelRequestBatch{}, err
		}
		if after.RunKey != runKey {
			return session.ModelRequestBatch{}, session.ErrNotFound
		}
		query = query.Where("(model_requests.created_at, model_requests.id) > (?, ?)", after.CreatedAt, publicID(cursor.AfterID))
	}
	var rows []modelRequestRow
	if err := query.Order("model_requests.created_at, model_requests.id").Limit(limit + 1).Find(&rows).Error; err != nil {
		return session.ModelRequestBatch{}, s.mapErr(err)
	}
	records := make([]session.ModelRequestRecord, 0, len(rows))
	for _, row := range rows {
		var record session.ModelRequestRecord
		if err := decodeModelRequest(row, &record); err != nil {
			return session.ModelRequestBatch{}, err
		}
		if !modelRequestRowMatches(row, record) {
			return session.ModelRequestBatch{}, session.ErrConflict
		}
		records = append(records, record)
	}
	next := session.ModelRequestCursor{}
	if len(records) > limit {
		next = session.ModelRequestCursor{AfterID: records[limit-1].ID, Limit: limit}
		records = records[:limit]
	}
	return session.ModelRequestBatch{Records: records, Next: next}, nil
}

func (s *Store) modelRequestOwnerKeys(ctx context.Context, sessionID session.ID, runID session.RunID) (sessionKey, runKey int64, err error) {
	sessionKey, err = s.key(ctx, "sessions", string(sessionID))
	if err != nil {
		return 0, 0, err
	}
	runKey, err = s.key(ctx, "runs", string(runID))
	if err != nil {
		return 0, 0, err
	}
	var owner struct {
		SessionKey int64 `gorm:"column:session_key"`
	}
	result := s.dbFor(ctx).Table("runs").Select("session_key").Where("row_key = ?", runKey).Take(&owner)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) || result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return 0, 0, session.ErrNotFound
		}
		return 0, 0, s.mapErr(result.Error)
	}
	if owner.SessionKey != sessionKey {
		return 0, 0, session.ErrConflict
	}
	return sessionKey, runKey, nil
}

func decodeModelRequest(row modelRequestRow, record *session.ModelRequestRecord) error {
	if row.RecordBytes > maxModelRequestRecordBytes {
		return session.ErrModelRequestTooLarge
	}
	return decodeStoredRecord(row.Record, record)
}
