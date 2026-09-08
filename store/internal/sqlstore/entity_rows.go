package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/mattsp1290/eino-agent/session"
)

// These rows deliberately contain database-owned relation keys separately
// from caller-visible byte IDs. GORM never maps the public session records.
type sessionRow struct {
	RowKey    int64  `gorm:"column:row_key"`
	ID        []byte `gorm:"column:id"`
	Record    []byte `gorm:"column:record"`
	Workspace []byte `gorm:"column:workspace_id"`
	Title     []byte `gorm:"column:title"`
	CreatedAt string `gorm:"column:created_at"`
	UpdatedAt string `gorm:"column:updated_at"`
}

type messageRow struct {
	RowKey     int64  `gorm:"column:row_key"`
	ID         []byte `gorm:"column:id"`
	SessionKey int64  `gorm:"column:session_key"`
	RunKey     int64  `gorm:"column:run_key"`
	Role       string `gorm:"column:role"`
	Finalized  bool   `gorm:"column:finalized"`
	Record     []byte `gorm:"column:record"`
	CreatedAt  string `gorm:"column:created_at"`
}

type replayMessageRow struct {
	ID        []byte `gorm:"column:id"`
	SessionID []byte `gorm:"column:session_id"`
	RunID     []byte `gorm:"column:run_id"`
	Role      string `gorm:"column:role"`
	Record    []byte `gorm:"column:record"`
	CreatedAt string `gorm:"column:created_at"`
}

type partRow struct {
	RowKey      int64  `gorm:"column:row_key"`
	ID          []byte `gorm:"column:id"`
	MessageKey  int64  `gorm:"column:message_key"`
	SessionKey  int64  `gorm:"column:session_key"`
	RunKey      int64  `gorm:"column:run_key"`
	Ordinal     int64  `gorm:"column:ordinal"`
	Kind        string `gorm:"column:kind"`
	DisplayText []byte `gorm:"column:display_text"`
	TextValid   bool   `gorm:"column:text_valid"`
	Record      []byte `gorm:"column:record"`
	CreatedAt   string `gorm:"column:created_at"`
}

type replayPartRow struct {
	ID        []byte `gorm:"column:id"`
	MessageID []byte `gorm:"column:message_id"`
	SessionID []byte `gorm:"column:session_id"`
	RunID     []byte `gorm:"column:run_id"`
	Ordinal   int64  `gorm:"column:ordinal"`
	Record    []byte `gorm:"column:record"`
}

type contextEpochRow struct {
	SessionID  []byte `gorm:"column:session_id"`
	RowKey     int64  `gorm:"column:row_key"`
	ID         []byte `gorm:"column:id"`
	SessionKey int64  `gorm:"column:session_key"`
	Record     []byte `gorm:"column:record"`
	CreatedAt  string `gorm:"column:created_at"`
	ClosedAt   string `gorm:"column:closed_at"`
}

type modelRequestRow struct {
	SessionID          []byte `gorm:"column:session_id"`
	RunID              []byte `gorm:"column:run_id"`
	RecordBytes        int64  `gorm:"column:record_bytes"`
	RowKey             int64  `gorm:"column:row_key"`
	ID                 []byte `gorm:"column:id"`
	SessionKey         int64  `gorm:"column:session_key"`
	RunKey             int64  `gorm:"column:run_key"`
	AssistantMessageID []byte `gorm:"column:assistant_message_id"`
	State              string `gorm:"column:state"`
	Attempt            int    `gorm:"column:attempt"`
	Step               int    `gorm:"column:step"`
	Record             []byte `gorm:"column:record"`
	CreatedAt          string `gorm:"column:created_at"`
}

func decodeStoredRecord(raw []byte, dst any) error {
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("%w: malformed stored record", session.ErrConflict)
	}
	return nil
}

func (s *Store) sessionRowByID(ctx context.Context, id string) (sessionRow, error) {
	var row sessionRow
	err := s.dbFor(ctx).Table("sessions").Select("row_key, id, record, workspace_id, title, created_at, updated_at").Where("id = ?", []byte(id)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return sessionRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

func (s *Store) messageRowByID(ctx context.Context, id string) (messageRow, error) {
	var row messageRow
	err := s.dbFor(ctx).Table("messages").Select("row_key, id, session_key, run_key, role, finalized, record, created_at").Where("id = ?", []byte(id)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return messageRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

func (s *Store) partRowByID(ctx context.Context, id string) (partRow, error) {
	var row partRow
	err := s.dbFor(ctx).Table("parts").Select("row_key, id, message_key, session_key, run_key, ordinal, kind, display_text, text_valid, record, created_at").Where("id = ?", []byte(id)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return partRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

func (s *Store) contextEpochRowByID(ctx context.Context, id string) (contextEpochRow, error) {
	var row contextEpochRow
	err := s.contextEpochQuery(ctx).Where("context_epochs.id = ?", []byte(id)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return contextEpochRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

func (s *Store) modelRequestRowByID(ctx context.Context, id string) (modelRequestRow, error) {
	var row modelRequestRow
	err := s.modelRequestQuery(ctx).Where("model_requests.id = ?", []byte(id)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return modelRequestRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

func (s *Store) contextEpochQuery(ctx context.Context) *gorm.DB {
	return s.dbFor(ctx).Table("context_epochs").Select("context_epochs.*, sessions.id AS session_id").Joins("JOIN sessions ON sessions.row_key = context_epochs.session_key")
}
func (s *Store) modelRequestQuery(ctx context.Context) *gorm.DB {
	size := s.dialect.ByteLength("model_requests.record")
	return s.dbFor(ctx).Table("model_requests").Select("model_requests.row_key, model_requests.id, model_requests.session_key, model_requests.run_key, model_requests.assistant_message_id, model_requests.state, model_requests.attempt, model_requests.step, model_requests.created_at, sessions.id AS session_id, runs.id AS run_id, "+size+" AS record_bytes, CASE WHEN "+size+" <= ? THEN model_requests.record END AS record", maxModelRequestRecordBytes).Joins("JOIN sessions ON sessions.row_key = model_requests.session_key").Joins("JOIN runs ON runs.row_key = model_requests.run_key AND runs.session_key = model_requests.session_key")
}
