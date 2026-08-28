package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) AdmitRun(ctx context.Context, record session.Run, leaseDuration time.Duration) (session.Run, error) {
	if record.ClaimToken == "" || leaseDuration <= 0 {
		return session.Run{}, session.ErrConflict
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
	leaseMicros := durationMicros(leaseDuration)
	_, err = s.exec(ctx, `INSERT INTO runs(id, session_id, status, owner_id, claim_token, lease_until, record, created_at)
		VALUES (?, ?, ?, ?, ?, CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER) + ?, ?, ?)`,
		record.ID, record.SessionID, record.Status, record.OwnerID, record.ClaimToken, leaseMicros, raw, timeText(record.CreatedAt))
	if constraintFailed(err) {
		if active, activeErr := s.ActiveRun(ctx, record.SessionID); activeErr == nil && active.ID != record.ID {
			return session.Run{}, session.ErrSessionBusy
		}
		return session.Run{}, session.ErrConflict
	}
	if err != nil {
		return session.Run{}, mapErr(err)
	}
	return s.GetRun(ctx, record.ID)
}

func (s *Store) GetRun(ctx context.Context, id session.RunID) (session.Run, error) {
	return s.getRun(ctx, `SELECT record, status, owner_id, claim_token, lease_until FROM runs WHERE id = ?`, id)
}

func (s *Store) ActiveRun(ctx context.Context, sessionID session.ID) (session.Run, error) {
	return s.getRun(ctx, `SELECT record, status, owner_id, claim_token, lease_until FROM runs WHERE session_id = ? AND status IN (?, ?) ORDER BY created_at LIMIT 1`, sessionID, session.RunPending, session.RunRunning)
}

func (s *Store) ListUnfinishedRuns(ctx context.Context) ([]session.Run, error) {
	rows, err := s.query(ctx, `SELECT record, status, owner_id, claim_token, lease_until FROM runs WHERE status IN (?, ?) ORDER BY created_at, id`, session.RunPending, session.RunRunning)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []session.Run
	for rows.Next() {
		record, scanErr := scanRun(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) ClaimRun(ctx context.Context, claim session.RunClaim) (session.Run, error) {
	if claim.RunID == "" || claim.OwnerID == "" || claim.ClaimToken == "" || claim.LeaseDuration <= 0 {
		return session.Run{}, session.ErrConflict
	}
	result, err := s.exec(ctx, `UPDATE runs SET status = ?, owner_id = ?, claim_token = ?,
		lease_until = CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER) + ?
		WHERE id = ? AND status IN (?, ?) AND lease_until <= CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)`,
		session.RunRunning, claim.OwnerID, claim.ClaimToken, durationMicros(claim.LeaseDuration), claim.RunID, session.RunPending, session.RunRunning)
	if err != nil {
		return session.Run{}, mapErr(err)
	}
	if err := rowsAffected(result); err != nil {
		if current, getErr := s.GetRun(ctx, claim.RunID); getErr == nil && !current.Terminal() {
			return session.Run{}, session.ErrSessionBusy
		}
		return session.Run{}, session.ErrConflict
	}
	return s.GetRun(ctx, claim.RunID)
}

func (s *Store) Execution(fence session.RunFence) session.ExecutionStore {
	return &executionStore{store: s, fence: fence}
}

func (s *Store) writeRun(ctx context.Context, record session.Run) error {
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
	result, err := s.exec(ctx, `UPDATE runs SET status = ?, owner_id = ?, claim_token = ?, record = ? WHERE id = ? AND claim_token = ? AND status IN (?, ?)`,
		record.Status, record.OwnerID, record.ClaimToken, raw, record.ID, record.ClaimToken, session.RunPending, session.RunRunning)
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
