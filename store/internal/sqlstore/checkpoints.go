package sqlstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

type checkpointRow struct {
	RowKey    int64  `gorm:"column:row_key"`
	RunKey    int64  `gorm:"column:run_key"`
	RunID     []byte `gorm:"column:run_id"`
	Revision  int64  `gorm:"column:revision"`
	Promoted  bool   `gorm:"column:promoted"`
	Bytes     []byte `gorm:"column:bytes"`
	Record    []byte `gorm:"column:record"`
	CreatedAt string `gorm:"column:created_at"`
}

func (s *Store) checkpointQuery(ctx context.Context) *gorm.DB {
	return s.dbFor(ctx).Table(s.tableName("checkpoints")).
		Select("checkpoints.row_key, checkpoints.run_key, runs.id AS run_id, checkpoints.revision, checkpoints.promoted, checkpoints.bytes, checkpoints.record, checkpoints.created_at").
		Joins("JOIN " + s.tableName("runs") + " ON runs.row_key = checkpoints.run_key")
}

func decodeCheckpointRow(row checkpointRow) (session.Checkpoint, error) {
	var value session.Checkpoint
	if err := decodeStoredRecord(row.Record, &value); err != nil {
		return session.Checkpoint{}, err
	}
	if string(row.RunID) != string(value.RunID) || row.Revision != value.Revision ||
		row.Promoted != value.Promoted || !bytes.Equal(row.Bytes, value.Bytes) {
		return session.Checkpoint{}, session.ErrConflict
	}
	return value, nil
}

func (s *Store) checkpointRowByRevision(ctx context.Context, runKey, revision int64) (checkpointRow, error) {
	var row checkpointRow
	err := s.checkpointQuery(ctx).Where("checkpoints.run_key = ? AND checkpoints.revision = ?", runKey, revision).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return checkpointRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

// createCheckpoint inserts checkpoint, or returns the existing identical
// revision when this exact (run, revision) was already staged.
func (s *Store) createCheckpoint(ctx context.Context, checkpoint session.Checkpoint) (session.Checkpoint, error) {
	runKey, err := s.key(ctx, "runs", string(checkpoint.RunID))
	if err != nil {
		return session.Checkpoint{}, relationError(err)
	}
	validateExisting := func(row checkpointRow) (session.Checkpoint, error) {
		existing, err := decodeCheckpointRow(row)
		if err != nil {
			return session.Checkpoint{}, err
		}
		if !SameRecord(existing, checkpoint) {
			return session.Checkpoint{}, session.ErrConflict
		}
		return existing, nil
	}
	if row, err := s.checkpointRowByRevision(ctx, runKey, checkpoint.Revision); err == nil {
		return validateExisting(row)
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Checkpoint{}, err
	}
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		return session.Checkpoint{}, err
	}
	created := s.dbFor(ctx).Table(s.tableName("checkpoints")).Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"run_key": runKey, "revision": checkpoint.Revision, "promoted": flagValue(checkpoint.Promoted),
		"bytes": []byte(checkpoint.Bytes), "record": raw, "created_at": TimeText(checkpoint.CreatedAt),
	})
	if err := s.mapErr(created.Error); err != nil {
		return session.Checkpoint{}, err
	}
	if created.RowsAffected == 0 {
		row, err := s.checkpointRowByRevision(ctx, runKey, checkpoint.Revision)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.Checkpoint{}, session.ErrConflict
			}
			return session.Checkpoint{}, err
		}
		return validateExisting(row)
	}
	return checkpoint, nil
}

func (e *executionStore) StageCheckpoint(ctx context.Context, request session.StageCheckpointRequest) (session.Checkpoint, error) {
	if request.Checkpoint.RunID != e.fence.RunID {
		return session.Checkpoint{}, session.ErrConflict
	}
	var result session.Checkpoint
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if _, err := session.ValidateStageCheckpoint(run, request); err != nil {
			return err
		}
		var err error
		result, err = store.createCheckpoint(ctx, request.Checkpoint)
		return err
	})
	if err != nil {
		return session.Checkpoint{}, err
	}
	return result, nil
}

// PromotePause atomically promotes a staged revision into the durable pause
// boundary. It requires the revision to exist (unpromoted or, defensively,
// already promoted by this exact call) under the current run. Because
// loadRunFence only accepts pending/running runs, a successful promotion
// makes every subsequent write on this fence (including a replay of
// PromotePause itself) fail fast with ErrConflict before reaching any
// mutation here -- that rejection, not an internal idempotent branch, is the
// durable "the old fence can no longer write" guarantee.
func (e *executionStore) PromotePause(ctx context.Context, request session.PromotePauseRequest) (session.PromotePauseResult, error) {
	var result session.PromotePauseResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if err := session.ValidatePromotePause(run, request); err != nil {
			return err
		}
		runKey, err := store.key(ctx, "runs", string(run.ID))
		if err != nil {
			return err
		}
		checkpointRow, err := store.checkpointRowByRevision(ctx, runKey, request.Revision)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		checkpoint, err := decodeCheckpointRow(checkpointRow)
		if err != nil {
			return err
		}
		if !checkpoint.Promoted {
			checkpoint.Promoted = true
			raw, err := json.Marshal(checkpoint)
			if err != nil {
				return err
			}
			db := store.dbFor(ctx).Table(store.tableName("checkpoints")).Where("row_key = ? AND promoted = ?", checkpointRow.RowKey, flagValue(false)).Updates(map[string]any{
				"promoted": flagValue(true), "record": raw,
			})
			if err := store.mapErr(db.Error); err != nil {
				return err
			}
			if err := rowsAffected(db); err != nil {
				return err
			}
		}
		tRow, err := store.turnRowByID(ctx, string(request.TurnID))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		currentTurn, err := decodeTurnRow(tRow)
		if err != nil {
			return err
		}
		if currentTurn.RunID != run.ID || currentTurn.SessionID != run.SessionID {
			return session.ErrConflict
		}
		interruptedTurn, err := session.ApplyInterruptTurn(currentTurn, session.InterruptTurnRequest{TurnID: request.TurnID, Event: request.Event})
		if err != nil {
			return err
		}
		turnRaw, err := json.Marshal(interruptedTurn)
		if err != nil {
			return err
		}
		turnDB := store.dbFor(ctx).Table(store.tableName("turns")).Where("row_key = ?", tRow.RowKey).Updates(map[string]any{
			"state": string(interruptedTurn.State), "record": turnRaw,
		})
		if err := store.mapErr(turnDB.Error); err != nil {
			return err
		}
		if err := rowsAffected(turnDB); err != nil {
			return err
		}
		sessionKey, err := store.key(ctx, "sessions", string(run.SessionID))
		if err != nil {
			return err
		}
		if err := store.interruptInboxItems(ctx, sessionKey, request.InboxIDs, request.Event.CreatedAt); err != nil {
			return err
		}
		pausedRun := run
		pausedRun.Status = session.RunPaused
		runRaw, err := json.Marshal(pausedRun)
		if err != nil {
			return err
		}
		runDB := store.dbFor(ctx).Table(store.tableName("runs")).Where("row_key = ? AND claim_token = ? AND status IN ?", runKey, []byte(run.ClaimToken), []string{string(session.RunPending), string(session.RunRunning)}).Updates(map[string]any{
			"status": string(session.RunPaused), "lease_until": 0, "record": runRaw,
		})
		if err := store.mapErr(runDB.Error); err != nil {
			return err
		}
		if err := rowsAffected(runDB); err != nil {
			return err
		}
		finalRun, err := store.getRun(ctx, run.ID)
		if err != nil {
			return err
		}
		event, err := store.appendCanonicalEvent(ctx, request.Event)
		if err != nil {
			return err
		}
		result = session.PromotePauseResult{Run: finalRun, Turn: interruptedTurn, Checkpoint: checkpoint, Event: event}
		return nil
	})
	if err != nil {
		return session.PromotePauseResult{}, err
	}
	return result, nil
}

// ReadPromotedCheckpoint reads the latest promoted revision for a run
// without a fence. It reports (Checkpoint{}, false, nil) when the run exists
// but has no promoted revision.
func (s *Store) ReadPromotedCheckpoint(ctx context.Context, runID session.RunID) (session.Checkpoint, bool, error) {
	runKey, err := s.key(ctx, "runs", string(runID))
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return session.Checkpoint{}, false, nil
		}
		return session.Checkpoint{}, false, err
	}
	var row checkpointRow
	err = s.checkpointQuery(ctx).Where("checkpoints.run_key = ? AND checkpoints.promoted = ?", runKey, flagValue(true)).Order("checkpoints.revision DESC").Limit(1).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return session.Checkpoint{}, false, nil
	}
	if err != nil {
		return session.Checkpoint{}, false, s.mapErr(err)
	}
	value, err := decodeCheckpointRow(row)
	if err != nil {
		return session.Checkpoint{}, false, err
	}
	return value, true, nil
}

func (s *Store) deleteCheckpointsUpTo(ctx context.Context, runKey, upToRevision int64) error {
	result := s.dbFor(ctx).Exec("DELETE FROM "+s.tableName("checkpoints")+" WHERE run_key = ? AND revision <= ?", runKey, upToRevision)
	if err := result.Error; err != nil {
		return s.mapErr(err)
	}
	return nil
}

// RetireCheckpoints deletes staged or promoted revisions at or below
// upToRevision for the fenced (running) run.
func (e *executionStore) RetireCheckpoints(ctx context.Context, upToRevision int64) error {
	if upToRevision <= 0 {
		return session.ErrConflict
	}
	return e.withFence(ctx, func(store *Store, run session.Run) error {
		runKey, err := store.key(ctx, "runs", string(run.ID))
		if err != nil {
			return err
		}
		return store.deleteCheckpointsUpTo(ctx, runKey, upToRevision)
	})
}

// RetireRunCheckpoints deletes staged or promoted revisions at or below
// upToRevision for a terminal run, where no fence is available.
func (s *Store) RetireRunCheckpoints(ctx context.Context, runID session.RunID, upToRevision int64) error {
	if upToRevision <= 0 {
		return session.ErrConflict
	}
	return s.atomic(ctx, func(st *Store) error {
		run, err := st.getRun(ctx, runID)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		if !run.Terminal() {
			return session.ErrConflict
		}
		runKey, err := st.key(ctx, "runs", string(runID))
		if err != nil {
			return err
		}
		return st.deleteCheckpointsUpTo(ctx, runKey, upToRevision)
	})
}
