package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

// turnRow deliberately contains only indexed/relational columns and the
// opaque record; see the same pattern in lifecycle_rows.go.
type turnRow struct {
	RowKey     int64  `gorm:"column:row_key"`
	ID         []byte `gorm:"column:id"`
	RunKey     int64  `gorm:"column:run_key"`
	SessionKey int64  `gorm:"column:session_key"`
	RunID      []byte `gorm:"column:run_id"`
	SessionID  []byte `gorm:"column:session_id"`
	Ordinal    int64  `gorm:"column:ordinal"`
	State      string `gorm:"column:state"`
	Record     []byte `gorm:"column:record"`
	CreatedAt  string `gorm:"column:created_at"`
}

func (s *Store) turnQuery(ctx context.Context) *gorm.DB {
	return s.dbFor(ctx).Table(s.tableName("turns")).
		Select("turns.row_key, turns.id, turns.run_key, turns.session_key, runs.id AS run_id, sessions.id AS session_id, turns.ordinal, turns.state, turns.record, turns.created_at").
		Joins("JOIN " + s.tableName("runs") + " ON runs.row_key = turns.run_key").
		Joins("JOIN " + s.tableName("sessions") + " ON sessions.row_key = turns.session_key")
}

func decodeTurnRow(row turnRow) (session.Turn, error) {
	var value session.Turn
	if err := decodeStoredRecord(row.Record, &value); err != nil {
		return session.Turn{}, err
	}
	if string(row.ID) != string(value.ID) || string(row.RunID) != string(value.RunID) ||
		string(row.SessionID) != string(value.SessionID) || row.Ordinal != value.Ordinal || row.State != string(value.State) {
		return session.Turn{}, session.ErrConflict
	}
	return value, nil
}

func (s *Store) turnRowByID(ctx context.Context, id string) (turnRow, error) {
	var row turnRow
	err := s.turnQuery(ctx).Where("turns.id = ?", []byte(id)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return turnRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

func (s *Store) GetTurn(ctx context.Context, id session.TurnID) (session.Turn, error) {
	row, err := s.turnRowByID(ctx, string(id))
	if err != nil {
		return session.Turn{}, err
	}
	return decodeTurnRow(row)
}

func (s *Store) ListTurns(ctx context.Context, runID session.RunID) ([]session.Turn, error) {
	runKey, err := s.key(ctx, "runs", string(runID))
	if err != nil {
		return nil, err
	}
	var rows []turnRow
	if err := s.turnQuery(ctx).Where("turns.run_key = ?", runKey).Order("turns.ordinal").Find(&rows).Error; err != nil {
		return nil, s.mapErr(err)
	}
	out := make([]session.Turn, 0, len(rows))
	for _, row := range rows {
		value, err := decodeTurnRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

// createTurn inserts turn at the next ordinal for its run, or returns the
// existing identical row when this exact turn (by ID) was already admitted.
// Ordinal continuity is enforced against the live max ordinal for the run,
// which is safe because loadRunFence already holds an exclusive row lock on
// the run for the whole enclosing atomic savepoint.
func (s *Store) createTurn(ctx context.Context, turn session.Turn) (session.Turn, error) {
	if turn.ID == "" || turn.RunID == "" || turn.SessionID == "" || turn.Ordinal <= 0 || turn.State != session.TurnAdmitted {
		return session.Turn{}, session.ErrConflict
	}
	validateExisting := func(row turnRow) (session.Turn, error) {
		existing, err := decodeTurnRow(row)
		if err != nil {
			return session.Turn{}, err
		}
		if !SameRecord(existing, turn) {
			return session.Turn{}, session.ErrConflict
		}
		return existing, nil
	}
	if row, err := s.turnRowByID(ctx, string(turn.ID)); err == nil {
		return validateExisting(row)
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Turn{}, err
	}
	sessionKey, err := s.key(ctx, "sessions", string(turn.SessionID))
	if err != nil {
		return session.Turn{}, relationError(err)
	}
	runKey, err := s.key(ctx, "runs", string(turn.RunID))
	if err != nil {
		return session.Turn{}, relationError(err)
	}
	var owner int64
	if err := s.dbFor(ctx).Table(s.tableName("runs")).Where("row_key = ? AND session_key = ?", runKey, sessionKey).Count(&owner).Error; err != nil {
		return session.Turn{}, s.mapErr(err)
	}
	if owner != 1 {
		return session.Turn{}, session.ErrConflict
	}
	var maxOrdinal sql.NullInt64
	if err := s.queryRow(ctx, "SELECT MAX(ordinal) FROM "+s.tableName("turns")+" WHERE run_key = ?", runKey).Scan(&maxOrdinal); err != nil {
		return session.Turn{}, s.mapErr(err)
	}
	expected := int64(1)
	if maxOrdinal.Valid {
		expected = maxOrdinal.Int64 + 1
	}
	if turn.Ordinal != expected {
		return session.Turn{}, session.ErrConflict
	}
	raw, err := json.Marshal(turn)
	if err != nil {
		return session.Turn{}, err
	}
	created := s.dbFor(ctx).Table(s.tableName("turns")).Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"id": []byte(turn.ID), "run_key": runKey, "session_key": sessionKey,
		"ordinal": turn.Ordinal, "state": string(turn.State), "record": raw,
		"created_at": TimeText(turn.CreatedAt),
	})
	if err := s.mapErr(created.Error); err != nil {
		return session.Turn{}, err
	}
	if created.RowsAffected == 0 {
		row, err := s.turnRowByID(ctx, string(turn.ID))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.Turn{}, session.ErrConflict
			}
			return session.Turn{}, err
		}
		return validateExisting(row)
	}
	return turn, nil
}

func (e *executionStore) AdmitTurn(ctx context.Context, request session.AdmitTurnRequest) (session.AdmitTurnResult, error) {
	if request.Turn.RunID != e.fence.RunID {
		return session.AdmitTurnResult{}, session.ErrConflict
	}
	var result session.AdmitTurnResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if err := session.ValidateAdmitTurn(run, request); err != nil {
			return err
		}
		for _, message := range request.UserMessages {
			if _, err := store.appendMessage(ctx, message); err != nil {
				return err
			}
		}
		for _, part := range request.UserParts {
			if _, err := store.appendPart(ctx, part); err != nil {
				return err
			}
		}
		if request.AssistantPlaceholder.ID != "" {
			if _, err := store.appendMessage(ctx, request.AssistantPlaceholder); err != nil {
				return err
			}
		}
		createdTurn, err := store.createTurn(ctx, request.Turn)
		if err != nil {
			return err
		}
		if len(request.InboxIDs) > 0 {
			sessionKey, err := store.key(ctx, "sessions", string(run.SessionID))
			if err != nil {
				return err
			}
			turnKey, err := store.key(ctx, "turns", string(createdTurn.ID))
			if err != nil {
				return err
			}
			if err := store.consumeInboxForTurn(ctx, sessionKey, turnKey, createdTurn.ID, request.InboxIDs, request.Turn.CreatedAt); err != nil {
				return err
			}
		}
		event, err := store.appendCanonicalEvent(ctx, request.Event)
		if err != nil {
			return err
		}
		result = session.AdmitTurnResult{Turn: createdTurn, Event: event}
		return nil
	})
	if err != nil {
		return session.AdmitTurnResult{}, err
	}
	return result, nil
}

func (e *executionStore) CompleteTurn(ctx context.Context, request session.CompleteTurnRequest) (session.CompleteTurnResult, error) {
	var result session.CompleteTurnResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		row, err := store.turnRowByID(ctx, string(request.TurnID))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		current, err := decodeTurnRow(row)
		if err != nil {
			return err
		}
		if current.RunID != run.ID || current.SessionID != run.SessionID || current.RunID != e.fence.RunID {
			return session.ErrConflict
		}
		candidate, err := session.ApplyCompleteTurn(current, request)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		db := store.dbFor(ctx).Table(store.tableName("turns")).Where("row_key = ?", row.RowKey).Updates(map[string]any{
			"state": string(candidate.State), "record": raw,
		})
		if err := store.mapErr(db.Error); err != nil {
			return err
		}
		if err := rowsAffected(db); err != nil {
			return err
		}
		// A turn resumed via ResumeRun after a pause may have its own
		// InboxInterrupted rows (its in-flight items, interrupted by
		// PromotePause -- see runtime/turn_loop.go's finishTurnLoop): if
		// that resumed execution now completes normally, those rows must
		// still reach InboxCompleted, not stay interrupted forever.
		if err := store.settleInboxForTurn(ctx, row.RowKey, []session.InboxState{session.InboxConsumed, session.InboxInterrupted}, session.InboxCompleted, request.Event.CreatedAt); err != nil {
			return err
		}
		event, err := store.appendCanonicalEvent(ctx, request.Event)
		if err != nil {
			return err
		}
		result = session.CompleteTurnResult{Turn: candidate, Event: event}
		return nil
	})
	if err != nil {
		return session.CompleteTurnResult{}, err
	}
	return result, nil
}

func (e *executionStore) InterruptTurn(ctx context.Context, request session.InterruptTurnRequest) (session.InterruptTurnResult, error) {
	var result session.InterruptTurnResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		row, err := store.turnRowByID(ctx, string(request.TurnID))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		current, err := decodeTurnRow(row)
		if err != nil {
			return err
		}
		if current.RunID != run.ID || current.SessionID != run.SessionID || current.RunID != e.fence.RunID {
			return session.ErrConflict
		}
		candidate, err := session.ApplyInterruptTurn(current, request)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		db := store.dbFor(ctx).Table(store.tableName("turns")).Where("row_key = ?", row.RowKey).Updates(map[string]any{
			"state": string(candidate.State), "record": raw,
		})
		if err := store.mapErr(db.Error); err != nil {
			return err
		}
		if err := rowsAffected(db); err != nil {
			return err
		}
		if err := store.settleInboxForTurn(ctx, row.RowKey, []session.InboxState{session.InboxConsumed}, session.InboxInterrupted, request.Event.CreatedAt); err != nil {
			return err
		}
		// InterruptTurn is itself a typed atomic mutation method: it is
		// trusted to append its caller's event verbatim (any kind), unlike
		// the public AppendEvent path.
		event, err := store.insertEvent(ctx, request.Event, true)
		if err != nil {
			return err
		}
		result = session.InterruptTurnResult{Turn: candidate, Event: event}
		return nil
	})
	if err != nil {
		return session.InterruptTurnResult{}, err
	}
	return result, nil
}

// ReconcileInterruptedTurn implements session.ExecutionStore's crash-
// recovery reconciliation (round-four reconciliation item 1/CR-C1): exactly
// like InterruptTurn, it settles an admitted/running turn as interrupted and
// carries the turn's consumed inbox items forward as InboxInterrupted --
// never requeued to InboxQueued. The turn's user messages/parts were
// already durably committed by the AdmitTurn that admitted it (AdmitTurn
// atomically commits the turn, its UserMessages/UserParts, and the inbox
// consumption in one transaction), so requeuing the items would let a fresh
// AdmitTurn re-consume them into a SECOND turn and duplicate that content in
// provider history. The correct continuation is ResumeInterruptedTurn,
// which later redrives this SAME turn by TurnID -- never a fresh AdmitTurn
// over the same inbox items.
func (e *executionStore) ReconcileInterruptedTurn(ctx context.Context, request session.ReconcileInterruptedTurnRequest) (session.ReconcileInterruptedTurnResult, error) {
	var result session.ReconcileInterruptedTurnResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		row, err := store.turnRowByID(ctx, string(request.TurnID))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		current, err := decodeTurnRow(row)
		if err != nil {
			return err
		}
		if current.RunID != run.ID || current.SessionID != run.SessionID || current.RunID != e.fence.RunID {
			return session.ErrConflict
		}
		candidate, err := session.ApplyInterruptTurn(current, session.InterruptTurnRequest(request))
		if err != nil {
			return err
		}
		raw, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		db := store.dbFor(ctx).Table(store.tableName("turns")).Where("row_key = ?", row.RowKey).Updates(map[string]any{
			"state": string(candidate.State), "record": raw,
		})
		if err := store.mapErr(db.Error); err != nil {
			return err
		}
		if err := rowsAffected(db); err != nil {
			return err
		}
		if err := store.settleInboxForTurn(ctx, row.RowKey, []session.InboxState{session.InboxConsumed}, session.InboxInterrupted, request.Event.CreatedAt); err != nil {
			return err
		}
		// ReconcileInterruptedTurn is itself a typed atomic mutation
		// method: it is trusted to append its caller's event verbatim (any
		// kind), unlike the public AppendEvent path.
		event, err := store.insertEvent(ctx, request.Event, true)
		if err != nil {
			return err
		}
		result = session.ReconcileInterruptedTurnResult{Turn: candidate, Event: event}
		return nil
	})
	if err != nil {
		return session.ReconcileInterruptedTurnResult{}, err
	}
	return result, nil
}

// ResumeInterruptedTurn implements session.ExecutionStore's redrive of a
// TurnInterrupted turn (round-four reconciliation item 1/CR-C1): see
// session.ResumeInterruptedTurnRequest for the full contract. It transitions
// the turn to TurnRunning and its own InboxInterrupted rows back to
// InboxConsumed, atomically, under the current run fence.
// terminalizeResidualTurns forces every turn for the run identified by
// runKey still TurnAdmitted, TurnRunning, or TurnInterrupted to TurnFailed,
// carrying any of its own still-InboxConsumed/InboxInterrupted rows forward
// as InboxInterrupted -- exactly like ReconcileInterruptedTurn's own inbox
// step, since the content is already durably committed and must never be
// requeued. Called only from SettleRun's own fenced transaction (never as a
// standalone ExecutionStore method), immediately after a run's FIRST
// terminal settlement: once the run is terminal, no other path
// (ResumeInterruptedTurn, CompleteTurn, ReconcileInterruptedTurn) can ever
// reach these turns again, so leaving them TurnInterrupted would falsely
// suggest they are still resumable -- see TurnFailed's own doc comment.
func (s *Store) terminalizeResidualTurns(ctx context.Context, runKey int64, at time.Time) error {
	var rows []turnRow
	if err := s.turnQuery(ctx).Where("turns.run_key = ? AND turns.state IN ?", runKey,
		[]string{string(session.TurnAdmitted), string(session.TurnRunning), string(session.TurnInterrupted)}).Find(&rows).Error; err != nil {
		return s.mapErr(err)
	}
	for _, row := range rows {
		current, err := decodeTurnRow(row)
		if err != nil {
			return err
		}
		candidate, err := session.ApplyFailTurn(current, at)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		db := s.dbFor(ctx).Table(s.tableName("turns")).Where("row_key = ?", row.RowKey).Updates(map[string]any{
			"state": string(candidate.State), "record": raw,
		})
		if err := s.mapErr(db.Error); err != nil {
			return err
		}
		if err := rowsAffected(db); err != nil {
			return err
		}
		if err := s.settleInboxForTurn(ctx, row.RowKey, []session.InboxState{session.InboxConsumed, session.InboxInterrupted}, session.InboxInterrupted, at); err != nil {
			return err
		}
	}
	return nil
}

func (e *executionStore) ResumeInterruptedTurn(ctx context.Context, request session.ResumeInterruptedTurnRequest) (session.ResumeInterruptedTurnResult, error) {
	var result session.ResumeInterruptedTurnResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		row, err := store.turnRowByID(ctx, string(request.TurnID))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		current, err := decodeTurnRow(row)
		if err != nil {
			return err
		}
		if current.RunID != run.ID || current.SessionID != run.SessionID || current.RunID != e.fence.RunID {
			return session.ErrConflict
		}
		candidate, err := session.ApplyResumeInterruptedTurn(current, request)
		if err != nil {
			return err
		}
		if candidate.State == current.State {
			// Idempotent replay (already TurnRunning): nothing left to move.
			result = session.ResumeInterruptedTurnResult{Turn: candidate}
			return nil
		}
		raw, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		db := store.dbFor(ctx).Table(store.tableName("turns")).Where("row_key = ?", row.RowKey).Updates(map[string]any{
			"state": string(candidate.State), "record": raw,
		})
		if err := store.mapErr(db.Error); err != nil {
			return err
		}
		if err := rowsAffected(db); err != nil {
			return err
		}
		if err := store.settleInboxForTurn(ctx, row.RowKey, []session.InboxState{session.InboxInterrupted}, session.InboxConsumed, request.ResumedAt); err != nil {
			return err
		}
		result = session.ResumeInterruptedTurnResult{Turn: candidate}
		return nil
	})
	if err != nil {
		return session.ResumeInterruptedTurnResult{}, err
	}
	return result, nil
}
