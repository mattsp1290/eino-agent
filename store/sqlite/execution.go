package sqlite

import (
	"context"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

type executionStore struct {
	store *Store
	fence session.RunFence
}

func (e *executionStore) WithinTx(ctx context.Context, fn func(context.Context, session.ExecutionStore) error) error {
	if e == nil || e.store == nil || fn == nil || e.fence.RunID == "" || e.fence.ClaimToken == "" {
		return session.ErrConflict
	}
	if e.store.tx != nil {
		return fn(ctx, e)
	}
	return e.store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		store, ok := tx.(*Store)
		if !ok {
			return session.ErrConflict
		}
		return fn(ctx, &executionStore{store: store, fence: e.fence})
	})
}

func (e *executionStore) withFence(ctx context.Context, fn func(*Store, session.Run) error) error {
	return e.withFenceState(ctx, false, fn)
}

func (e *executionStore) withFenceState(ctx context.Context, allowTerminal bool, fn func(*Store, session.Run) error) error {
	if e == nil || e.store == nil || e.fence.RunID == "" || e.fence.ClaimToken == "" {
		return session.ErrConflict
	}
	if e.store.tx != nil {
		run, err := loadRunFence(ctx, e.store, e.fence, allowTerminal)
		if err != nil {
			return err
		}
		return fn(e.store, run)
	}
	return e.store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		store, ok := tx.(*Store)
		if !ok {
			return session.ErrConflict
		}
		run, err := loadRunFence(ctx, store, e.fence, allowTerminal)
		if err != nil {
			return err
		}
		return fn(store, run)
	})
}

func loadRunFence(ctx context.Context, store *Store, fence session.RunFence, allowTerminal bool) (session.Run, error) {
	query := `SELECT record, status, owner_id, claim_token, lease_until FROM runs WHERE id = ? AND claim_token = ?`
	args := []any{fence.RunID, fence.ClaimToken}
	if !allowTerminal {
		query += ` AND status IN (?, ?)`
		args = append(args, session.RunPending, session.RunRunning)
	}
	run, err := store.getRun(ctx, query, args...)
	if err != nil {
		return session.Run{}, session.ErrConflict
	}
	return run, nil
}

func sameRunExecution(current, candidate session.Run) bool {
	current.Status, candidate.Status = "", ""
	current.LeaseUntil, candidate.LeaseUntil = time.Time{}, time.Time{}
	current.StartedAt, candidate.StartedAt = time.Time{}, time.Time{}
	current.FinishedAt, candidate.FinishedAt = time.Time{}, time.Time{}
	current.Error, candidate.Error = "", ""
	return sameRecord(current, candidate)
}

func (e *executionStore) StartRun(ctx context.Context, startedAt time.Time) (session.Run, error) {
	var started session.Run
	err := e.withFence(ctx, func(store *Store, current session.Run) error {
		current.Status = session.RunRunning
		current.StartedAt = startedAt
		if err := store.writeRun(ctx, current); err != nil {
			return err
		}
		var err error
		started, err = store.GetRun(ctx, e.fence.RunID)
		return err
	})
	return started, err
}

func (e *executionStore) RenewRunLease(ctx context.Context, leaseDuration time.Duration) (session.Run, error) {
	if leaseDuration <= 0 {
		return session.Run{}, session.ErrConflict
	}
	var renewed session.Run
	err := e.withFence(ctx, func(store *Store, _ session.Run) error {
		result, err := store.exec(ctx, `UPDATE runs SET lease_until = CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER) + ? WHERE id = ? AND claim_token = ? AND status IN (?, ?)`, durationMicros(leaseDuration), e.fence.RunID, e.fence.ClaimToken, session.RunPending, session.RunRunning)
		if err != nil {
			return mapErr(err)
		}
		if err := rowsAffected(result); err != nil {
			return session.ErrConflict
		}
		renewed, err = store.GetRun(ctx, e.fence.RunID)
		return err
	})
	return renewed, err
}

func (e *executionStore) SettleRun(ctx context.Context, run session.Run, finalEvent *session.EventRecord) error {
	if run.ID != e.fence.RunID || run.ClaimToken != e.fence.ClaimToken || !run.Terminal() {
		return session.ErrConflict
	}
	return e.withFenceState(ctx, true, func(store *Store, current session.Run) error {
		if !sameRunExecution(current, run) {
			return session.ErrConflict
		}
		if finalEvent != nil && (finalEvent.RunID != run.ID || finalEvent.SessionID != current.SessionID) {
			return session.ErrConflict
		}
		if err := store.writeRun(ctx, run); err != nil {
			return err
		}
		if finalEvent != nil {
			_, err := store.appendEvent(ctx, *finalEvent)
			return err
		}
		return nil
	})
}

func (e *executionStore) AppendMessage(ctx context.Context, record session.Message) (session.Message, error) {
	if record.RunID != e.fence.RunID {
		return session.Message{}, session.ErrConflict
	}
	var result session.Message
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		var err error
		result, err = store.appendMessage(ctx, record)
		return err
	})
	return result, err
}

func (e *executionStore) AppendPart(ctx context.Context, record session.Part) (session.Part, error) {
	if record.RunID != e.fence.RunID {
		return session.Part{}, session.ErrConflict
	}
	var result session.Part
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		var err error
		result, err = store.appendPart(ctx, record)
		return err
	})
	return result, err
}

func (e *executionStore) UpdatePart(ctx context.Context, record session.Part) error {
	if record.RunID != e.fence.RunID {
		return session.ErrConflict
	}
	return e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		return store.updatePart(ctx, record)
	})
}

func (e *executionStore) AppendEvent(ctx context.Context, record session.EventRecord) (session.EventRecord, error) {
	if record.RunID != e.fence.RunID {
		return session.EventRecord{}, session.ErrConflict
	}
	var result session.EventRecord
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		var err error
		result, err = store.appendEvent(ctx, record)
		return err
	})
	return result, err
}

func (e *executionStore) CreateToolCall(ctx context.Context, record session.ToolCall) (session.ToolCall, error) {
	if record.RunID != e.fence.RunID {
		return session.ToolCall{}, session.ErrConflict
	}
	var result session.ToolCall
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		var err error
		result, err = store.createToolCall(ctx, record)
		return err
	})
	return result, err
}

func (e *executionStore) ClaimToolCall(ctx context.Context, record session.ToolCall, leaseDuration time.Duration) (session.ToolCall, error) {
	if record.RunID != e.fence.RunID || leaseDuration <= 0 {
		return session.ToolCall{}, session.ErrConflict
	}
	var result session.ToolCall
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		runLease, err := (&executionStore{store: store, fence: e.fence}).RenewRunLease(ctx, leaseDuration)
		if err != nil {
			return err
		}
		record.LeaseUntil = runLease.LeaseUntil
		result, err = store.claimToolCall(ctx, record)
		return err
	})
	return result, err
}

func (e *executionStore) SettleToolCall(ctx context.Context, settlement session.ToolSettlement) error {
	return e.withFence(ctx, func(store *Store, run session.Run) error {
		call, err := store.GetToolCall(ctx, settlement.ID)
		if err != nil {
			return err
		}
		if call.RunID != e.fence.RunID || call.SessionID != run.SessionID {
			return session.ErrConflict
		}
		return store.settleToolCall(ctx, settlement)
	})
}

func (e *executionStore) StartContextEpoch(ctx context.Context, record session.ContextEpoch) (session.ContextEpoch, error) {
	var result session.ContextEpoch
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		var err error
		result, err = store.startContextEpoch(ctx, record)
		return err
	})
	return result, err
}

func (e *executionStore) FinishContextEpoch(ctx context.Context, record session.ContextEpoch) error {
	return e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		return store.finishContextEpoch(ctx, record)
	})
}

func (e *executionStore) CreateModelRequest(ctx context.Context, record session.ModelRequestRecord) (session.ModelRequestRecord, error) {
	if record.RunID != e.fence.RunID {
		return session.ModelRequestRecord{}, session.ErrConflict
	}
	var result session.ModelRequestRecord
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		var err error
		result, err = store.createModelRequest(ctx, record)
		return err
	})
	return result, err
}

func (e *executionStore) UpdateModelRequest(ctx context.Context, record session.ModelRequestRecord) error {
	if record.RunID != e.fence.RunID {
		return session.ErrConflict
	}
	return e.withFence(ctx, func(store *Store, run session.Run) error {
		if record.SessionID != run.SessionID {
			return session.ErrConflict
		}
		return store.updateModelRequest(ctx, record)
	})
}

var _ session.ExecutionStore = (*executionStore)(nil)
