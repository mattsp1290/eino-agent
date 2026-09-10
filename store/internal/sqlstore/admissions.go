package sqlstore

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

// LookupAdmission deliberately creates a committed reader. A transaction view
// is never authoritative enough to tell a retry that a receipt is absent.
func (s *Store) LookupAdmission(ctx context.Context, sessionID session.ID, key string) (session.AdmissionRecord, error) {
	if s == nil || s.db == nil || s.tx != nil {
		return session.AdmissionRecord{}, session.ErrAdmissionStore
	}
	if err := session.ValidateAdmissionKey(key); err != nil || sessionID == "" {
		return session.AdmissionRecord{}, session.ErrAdmissionInvalid
	}
	var result session.AdmissionRecord
	err := s.read(ctx, func(reader *Store) error {
		var err error
		result, err = reader.admission(ctx, sessionID, key)
		return err
	})
	return result, err
}

// GetAdmission is restricted to the admission write transaction.
func (s *Store) GetAdmission(ctx context.Context, sessionID session.ID, key string) (session.AdmissionRecord, error) {
	if s == nil || s.db == nil || s.tx == nil {
		return session.AdmissionRecord{}, session.ErrAdmissionStore
	}
	if err := session.ValidateAdmissionKey(key); err != nil || sessionID == "" {
		return session.AdmissionRecord{}, session.ErrAdmissionInvalid
	}
	return s.admission(ctx, sessionID, key)
}

func (s *Store) admission(ctx context.Context, sessionID session.ID, key string) (session.AdmissionRecord, error) {
	var row admissionRow
	err := s.dbFor(ctx).Table(s.tableName("admission_receipts")).
		Select("admission_receipts.admission_key, admission_receipts.fingerprint_version, admission_receipts.fingerprint, admission_receipts.created_at, sessions.id AS session_id, runs.id AS run_id, runs.status AS run_status, user_messages.id AS user_message_id, assistant_messages.id AS assistant_message_id").
		Joins("JOIN "+s.tableName("sessions")+" ON sessions.row_key = admission_receipts.session_key").
		Joins("JOIN "+s.tableName("runs")+" ON runs.row_key = admission_receipts.run_key AND runs.session_key = admission_receipts.session_key").
		Joins("JOIN "+s.tableName("messages")+" AS user_messages ON user_messages.row_key = admission_receipts.user_message_key AND user_messages.session_key = admission_receipts.session_key AND user_messages.run_key = admission_receipts.run_key AND user_messages.role = ?", string(session.RoleUser)).
		Joins("JOIN "+s.tableName("messages")+" AS assistant_messages ON assistant_messages.row_key = admission_receipts.assistant_message_key AND assistant_messages.session_key = admission_receipts.session_key AND assistant_messages.run_key = admission_receipts.run_key AND assistant_messages.role = ?", string(session.RoleAssistant)).
		Where("admission_receipts.session_key = (SELECT row_key FROM "+s.tableName("sessions")+" WHERE id = ?)", []byte(sessionID)).
		Where("admission_receipts.admission_key = ?", []byte(key)).Take(&row).Error
	if err != nil {
		return session.AdmissionRecord{}, s.mapErr(err)
	}
	createdAt, err := DiscoveryTime(row.CreatedAt)
	if err != nil || len(row.Fingerprint) != 32 {
		return session.AdmissionRecord{}, session.ErrAdmissionStore
	}
	var fingerprint [32]byte
	copy(fingerprint[:], row.Fingerprint)
	record := session.AdmissionRecord{Receipt: session.AdmissionReceipt{
		SessionID: session.ID(row.SessionID), Key: string(row.AdmissionKey), RunID: session.RunID(row.RunID),
		UserMessageID: session.MessageID(row.UserMessageID), AssistantMessageID: session.MessageID(row.AssistantMessageID), CreatedAt: createdAt,
	}, FingerprintVersion: row.FingerprintVersion, Fingerprint: fingerprint, RunStatus: session.RunStatus(row.RunStatus)}
	if err := session.ValidateAdmissionRecord(record); err != nil {
		return session.AdmissionRecord{}, session.ErrAdmissionStore
	}
	return record, nil
}

// LockAdmissionSession gives keyed admission a stable session lock before it
// checks the receipt or active-run state. Existing metadata is intentionally
// returned unchanged; duplicate resolution precedes identity comparison.
func (s *Store) LockAdmissionSession(ctx context.Context, candidate session.Session) (session.Session, error) {
	if s == nil || s.db == nil || s.tx == nil {
		return session.Session{}, session.ErrAdmissionStore
	}
	if candidate.ID == "" || !validSessionProjection(candidate) {
		return session.Session{}, session.ErrAdmissionInvalid
	}
	row, err := s.lockSessionByID(ctx, string(candidate.ID))
	if errors.Is(err, session.ErrNotFound) {
		raw, marshalErr := marshalSession(candidate)
		if marshalErr != nil {
			return session.Session{}, marshalErr
		}
		created := s.dbFor(ctx).Table(s.tableName("sessions")).Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
			"id": publicID(candidate.ID), "record": raw, "workspace_id": []byte(candidate.WorkspaceID), "title": []byte(candidate.Title),
			"created_at": TimeText(candidate.CreatedAt), "updated_at": TimeText(candidate.UpdatedAt),
		})
		if err := s.mapErr(created.Error); err != nil {
			return session.Session{}, err
		}
		row, err = s.lockSessionByID(ctx, string(candidate.ID))
	}
	if err != nil {
		return session.Session{}, err
	}
	var record session.Session
	if err := decodeStoredRecord(row.Record, &record); err != nil || !sessionRowMatches(row, record) || record.ID != candidate.ID {
		return session.Session{}, session.ErrAdmissionStore
	}
	return record, nil
}

func marshalSession(record session.Session) ([]byte, error) {
	// Reuse the existing authoritative serialization helper through the stable
	// JSON encoding rather than adding a second record format.
	return json.Marshal(record)
}
