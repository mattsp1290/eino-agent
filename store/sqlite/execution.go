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

func (e *executionStore) SettleRun(ctx context.Context, request session.SettleRunRequest) (session.RunSettlementResult, error) {
	if !request.Settlement.FinishedAt.IsZero() {
		request.Settlement.FinishedAt = request.Settlement.FinishedAt.UTC()
	}
	if request.Event.ID == "" {
		return session.RunSettlementResult{}, session.ErrConflict
	}
	var committed session.RunSettlementResult
	err := e.withFenceState(ctx, true, func(store *Store, current session.Run) error {
		var unfinished int
		if err := store.queryRow(ctx, `SELECT COUNT(*) FROM tool_calls WHERE run_id = ? AND status IN (?, ?)`, current.ID, session.ToolCallPending, session.ToolCallRunning).Scan(&unfinished); err != nil {
			return mapErr(err)
		}
		if unfinished != 0 {
			return session.ErrConflict
		}
		if current.Terminal() {
			if current.Status != request.Settlement.Status || !current.FinishedAt.Equal(request.Settlement.FinishedAt) || current.Error != request.Settlement.Error {
				return session.ErrConflict
			}
			expected, err := session.RunSettlementRecord(current, request.Event)
			if err != nil {
				return err
			}
			var existing session.EventRecord
			if err := store.getJSON(ctx, `SELECT record FROM events WHERE run_id = ? AND kind = ?`, []any{current.ID, session.RunSettlementEventKind}, &existing); err != nil {
				return session.ErrConflict
			}
			if !sameRecord(existing, expected) {
				return session.ErrConflict
			}
			committed = session.RunSettlementResult{Run: current, Event: existing}
			return nil
		}
		canonicalRun, err := session.ApplyRunSettlement(current, request.Settlement)
		if err != nil {
			return err
		}
		canonicalEvent, err := session.RunSettlementRecord(canonicalRun, request.Event)
		if err != nil {
			return err
		}
		if err := store.writeRun(ctx, canonicalRun); err != nil {
			return err
		}
		canonicalEvent, err = store.appendEvent(ctx, canonicalEvent)
		if err != nil {
			return err
		}
		committed = session.RunSettlementResult{Run: canonicalRun, Event: canonicalEvent}
		return nil
	})
	return committed, err
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
	if record.Kind == session.PartToolCall || record.Kind == session.PartToolResult {
		return session.Part{}, session.ErrConflict
	}
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
	if record.RunID != e.fence.RunID || record.Kind == session.RunSettlementEventKind || record.ToolTransition != "" || (record.Kind == session.ToolTransitionEventKind && record.ToolCallID != "") {
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

func (e *executionStore) CreateToolCall(ctx context.Context, request session.CreateToolCallRequest) (session.ToolTransitionResult, error) {
	record := request.Call
	if record.RunID != e.fence.RunID {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	var result session.ToolTransitionResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		part := request.RequestPart
		if record.SessionID != run.SessionID || !validToolRequestEnvelope(record, part) {
			return session.ErrConflict
		}
		event, err := session.ToolTransitionRecord(record, request.Event)
		if err != nil || event.ToolTransition != session.ToolTransitionPending {
			return session.ErrConflict
		}
		if _, err = store.appendPart(ctx, part); err != nil {
			return err
		}
		result.Call, err = store.createToolCall(ctx, record)
		if err != nil {
			return err
		}
		result.Event, err = store.appendEvent(ctx, event)
		if err != nil {
			return err
		}
		result.Call, err = store.GetToolCall(ctx, record.ID)
		return err
	})
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	return result, err
}

func (e *executionStore) ClaimToolCall(ctx context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
	if request.ID == "" || request.ClaimedBy == "" || request.ClaimToken == "" || request.StartedAt.IsZero() || request.LeaseDuration <= 0 {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	var result session.ToolTransitionResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		current, err := store.GetToolCall(ctx, request.ID)
		if err != nil || current.RunID != e.fence.RunID || current.SessionID != run.SessionID {
			return session.ErrConflict
		}
		candidate := current
		candidate.Status = session.ToolCallRunning
		candidate.ClaimedBy = request.ClaimedBy
		candidate.ClaimToken = request.ClaimToken
		candidate.StartedAt = request.StartedAt.UTC()
		candidate.CompletedAt = time.Time{}
		event, err := session.ToolTransitionRecord(candidate, request.Event)
		if err != nil || event.ToolTransition != session.ToolTransitionRunning {
			return session.ErrConflict
		}
		if current.Status == session.ToolCallRunning {
			if !session.SameToolTransitionState(current, candidate) {
				return session.ErrConflict
			}
			canonicalEvent, err := store.appendEvent(ctx, event)
			if err != nil {
				return err
			}
			result = session.ToolTransitionResult{Call: current, Event: canonicalEvent}
			return nil
		}
		if current.Status != session.ToolCallPending || current.ClaimedBy != "" || current.ClaimToken != "" {
			return session.ErrConflict
		}
		runLease, err := (&executionStore{store: store, fence: e.fence}).RenewRunLease(ctx, request.LeaseDuration)
		if err != nil {
			return err
		}
		candidate.LeaseUntil = runLease.LeaseUntil
		result.Call, err = store.claimToolCall(ctx, candidate)
		if err != nil {
			return err
		}
		result.Event, err = store.appendEvent(ctx, event)
		if err != nil {
			return err
		}
		result.Call, err = store.GetToolCall(ctx, candidate.ID)
		return err
	})
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	return result, err
}

func (e *executionStore) SettleToolCall(ctx context.Context, request session.SettleToolCallRequest) (session.ToolTransitionResult, error) {
	settlement := request.Settlement
	var result session.ToolTransitionResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		call, err := store.GetToolCall(ctx, settlement.ID)
		if err != nil {
			return err
		}
		if call.RunID != e.fence.RunID || call.SessionID != run.SessionID {
			return session.ErrConflict
		}
		settled, err := settlement.Apply(call)
		if err != nil {
			return err
		}
		event, err := session.ToolTransitionRecord(settled, request.Event)
		if err != nil || event.ToolTransition != session.ToolTransitionTerminal {
			return session.ErrConflict
		}
		if err := store.settleToolCall(ctx, settlement); err != nil {
			return err
		}
		result.Call = settled
		result.Event, err = store.appendEvent(ctx, event)
		if err != nil {
			return err
		}
		result.Call, err = store.GetToolCall(ctx, settled.ID)
		return err
	})
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	return result, nil
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
