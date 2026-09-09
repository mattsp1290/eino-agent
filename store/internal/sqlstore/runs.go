package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) AdmitRun(ctx context.Context, record session.Run, leaseDuration time.Duration) (session.Run, error) {
	if s == nil || record.ID == "" || record.SessionID == "" || record.ClaimToken == "" || leaseDuration <= 0 {
		return session.Run{}, session.ErrConflict
	}
	var result session.Run
	err := s.atomic(ctx, func(st *Store) error {
		sessionKey, err := st.key(ctx, "sessions", string(record.SessionID))
		if err != nil {
			return relationError(err)
		}
		active, err := st.activeRun(ctx, record.SessionID)
		if err != nil && !errors.Is(err, session.ErrNotFound) {
			return err
		}
		if err == nil && active.ID != record.ID {
			return session.ErrSessionBusy
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		db := st.dbFor(ctx).Table(st.tableName("runs")).Clauses(clause.OnConflict{DoNothing: true})
		row := map[string]any{
			"id": []byte(record.ID), "session_key": sessionKey, "status": string(record.Status),
			"provider_id": []byte(record.ProviderID), "model_id": []byte(record.ModelID),
			"owner_id": []byte(record.OwnerID), "claim_token": []byte(record.ClaimToken),
			"lease_until": gorm.Expr(st.dialect.ClockSQL()+" + ?", durationMicros(leaseDuration)),
			"record":      raw, "created_at": TimeText(record.CreatedAt),
		}
		created := db.Create(row)
		if err := created.Error; err != nil {
			return st.mapErr(err)
		}
		if created.RowsAffected == 0 {
			if current, getErr := st.getRun(ctx, record.ID); getErr == nil {
				if current.ID == record.ID {
					return session.ErrConflict
				}
			} else if !errors.Is(getErr, session.ErrNotFound) {
				return getErr
			}
			if current, getErr := st.activeRun(ctx, record.SessionID); getErr == nil && current.ID != record.ID {
				return session.ErrSessionBusy
			}
			return session.ErrConflict
		}
		result, err = st.getRun(ctx, record.ID)
		return err
	})
	return result, err
}

func (s *Store) GetRun(ctx context.Context, id session.RunID) (session.Run, error) {
	return s.getRun(ctx, id)
}

func (s *Store) ActiveRun(ctx context.Context, sessionID session.ID) (session.Run, error) {
	return s.activeRun(ctx, sessionID)
}

func (s *Store) runQuery(ctx context.Context) *gorm.DB {
	return s.dbFor(ctx).Table(s.tableName("runs")).Select("runs.row_key, runs.id, runs.session_key, sessions.id AS session_id, runs.status, runs.provider_id, runs.model_id, runs.owner_id, runs.claim_token, runs.lease_until, runs.record, runs.created_at").Joins("JOIN " + s.tableName("sessions") + " ON sessions.row_key = runs.session_key")
}

func (s *Store) activeRun(ctx context.Context, sessionID session.ID) (session.Run, error) {
	var row runRow
	sessionKey, err := s.key(ctx, "sessions", string(sessionID))
	if err != nil {
		return session.Run{}, err
	}
	db := s.runQuery(ctx).Where("runs.session_key = ? AND runs.status IN ?", sessionKey, []string{string(session.RunPending), string(session.RunRunning)}).Order("runs.created_at, runs.id").Limit(1)
	if err := db.Take(&row).Error; err != nil {
		return session.Run{}, translateReadError(err)
	}
	return decodeRunRow(row)
}

func (s *Store) getRun(ctx context.Context, id session.RunID) (session.Run, error) {
	var row runRow
	db := s.runQuery(ctx).Where("runs.id = ?", []byte(id)).Limit(1)
	if err := db.Take(&row).Error; err != nil {
		return session.Run{}, translateReadError(err)
	}
	return decodeRunRow(row)
}

func (s *Store) ListUnfinishedRuns(ctx context.Context) ([]session.Run, error) {
	var rows []runRow
	err := s.runQuery(ctx).Where("runs.status IN ?", []string{string(session.RunPending), string(session.RunRunning)}).Order("runs.created_at, runs.id").Find(&rows).Error
	if err != nil {
		return nil, translateReadError(err)
	}
	result := make([]session.Run, 0, len(rows))
	for _, row := range rows {
		value, err := decodeRunRow(row)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (s *Store) ClaimRun(ctx context.Context, claim session.RunClaim) (session.Run, error) {
	if s == nil || claim.RunID == "" || claim.OwnerID == "" || claim.ClaimToken == "" || claim.LeaseDuration <= 0 {
		return session.Run{}, session.ErrConflict
	}
	var result session.Run
	err := s.atomic(ctx, func(st *Store) error {
		current, err := st.getRun(ctx, claim.RunID)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		candidate := current
		candidate.Status = session.RunRunning
		candidate.OwnerID = claim.OwnerID
		candidate.ClaimToken = claim.ClaimToken
		raw, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		// The conditional update is evaluated against the database clock. The
		// prior read is informational only; it is never used to authorize claim.
		db := st.dbFor(ctx).Table(st.tableName("runs")).Where("id = ? AND status IN ? AND lease_until <= "+st.dialect.ClockSQL(), []byte(claim.RunID), []string{string(session.RunPending), string(session.RunRunning)}).Updates(map[string]any{
			"status": string(session.RunRunning), "owner_id": []byte(claim.OwnerID), "claim_token": []byte(claim.ClaimToken), "record": raw,
			"lease_until": gorm.Expr(st.dialect.ClockSQL()+" + ?", durationMicros(claim.LeaseDuration)),
		})
		if err := db.Error; err != nil {
			return st.mapErr(err)
		}
		if db.RowsAffected == 0 {
			current, err := st.getRun(ctx, claim.RunID)
			if err != nil {
				if errors.Is(err, session.ErrNotFound) {
					return session.ErrConflict
				}
				return err
			}
			if !current.Terminal() {
				return session.ErrSessionBusy
			}
			return session.ErrConflict
		}
		var getErr error
		result, getErr = st.getRun(ctx, claim.RunID)
		return getErr
	})
	return result, err
}

func (s *Store) Execution(fence session.RunFence) session.ExecutionStore {
	return &executionStore{store: s, fence: fence}
}

func (s *Store) writeRun(ctx context.Context, record session.Run) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	db := s.dbFor(ctx).Table(s.tableName("runs")).Where("id = ? AND claim_token = ? AND status IN ?", []byte(record.ID), []byte(record.ClaimToken), []string{string(session.RunPending), string(session.RunRunning)}).Updates(map[string]any{
		"status": string(record.Status), "owner_id": []byte(record.OwnerID), "claim_token": []byte(record.ClaimToken),
		"provider_id": []byte(record.ProviderID), "model_id": []byte(record.ModelID), "record": raw,
	})
	if err := db.Error; err != nil {
		return s.mapErr(err)
	}
	if db.RowsAffected == 0 {
		latest, getErr := s.getRun(ctx, record.ID)
		if getErr != nil {
			return getErr
		}
		if latest.Terminal() && SameRecord(latest, record) {
			return nil
		}
		return session.ErrConflict
	}
	return nil
}

func translateReadError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return session.ErrNotFound
	}
	return err
}
