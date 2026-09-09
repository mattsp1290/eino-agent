package sqlstore

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

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

// withFence is an operation boundary as well as the authorization check. The
// dialect lock is acquired before reading the run and held until fn returns.
func (e *executionStore) withFence(ctx context.Context, fn func(*Store, session.Run) error) error {
	return e.withFenceState(ctx, false, fn)
}

func (e *executionStore) withFenceState(ctx context.Context, allowTerminal bool, fn func(*Store, session.Run) error) error {
	if e == nil || e.store == nil || fn == nil || e.fence.RunID == "" || e.fence.ClaimToken == "" {
		return session.ErrConflict
	}
	return e.store.atomic(ctx, func(st *Store) error {
		run, err := loadRunFence(ctx, st, e.fence, allowTerminal)
		if err != nil {
			return err
		}
		return fn(st, run)
	})
}

func loadRunFence(ctx context.Context, store *Store, fence session.RunFence, allowTerminal bool) (session.Run, error) {
	row, err := store.lockRun(ctx, fence.RunID)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return session.Run{}, session.ErrConflict
		}
		return session.Run{}, err
	}
	run, err := decodeRunRow(row)
	if err != nil {
		return session.Run{}, err
	}
	if run.ClaimToken != fence.ClaimToken || (!allowTerminal && run.Status != session.RunPending && run.Status != session.RunRunning) {
		return session.Run{}, session.ErrConflict
	}
	return run, nil
}

func (e *executionStore) StartRun(ctx context.Context, startedAt time.Time) (session.Run, error) {
	if startedAt.IsZero() {
		return session.Run{}, session.ErrConflict
	}
	var result session.Run
	err := e.withFence(ctx, func(store *Store, current session.Run) error {
		current.Status = session.RunRunning
		current.StartedAt = startedAt.UTC()
		if err := store.writeRun(ctx, current); err != nil {
			return err
		}
		var err error
		result, err = store.getRun(ctx, e.fence.RunID)
		return err
	})
	return result, err
}

func (e *executionStore) RenewRunLease(ctx context.Context, leaseDuration time.Duration) (session.Run, error) {
	if leaseDuration <= 0 {
		return session.Run{}, session.ErrConflict
	}
	var result session.Run
	err := e.withFence(ctx, func(store *Store, _ session.Run) error {
		if err := renewRunLease(ctx, store, e.fence, leaseDuration); err != nil {
			return err
		}
		var err error
		result, err = store.getRun(ctx, e.fence.RunID)
		return err
	})
	return result, err
}

func renewRunLease(ctx context.Context, store *Store, fence session.RunFence, leaseDuration time.Duration) error {
	db := store.dbFor(ctx).Table(store.tableName("runs")).Where("id = ? AND claim_token = ? AND status IN ?", []byte(fence.RunID), []byte(fence.ClaimToken), []string{string(session.RunPending), string(session.RunRunning)}).Updates(map[string]any{
		"lease_until": gorm.Expr(store.dialect.ClockSQL()+" + ?", durationMicros(leaseDuration)),
	})
	if err := db.Error; err != nil {
		return store.mapErr(err)
	}
	if db.RowsAffected == 0 {
		return session.ErrConflict
	}
	return nil
}

func (e *executionStore) SettleRun(ctx context.Context, request session.SettleRunRequest) (session.RunSettlementResult, error) {
	if request.Event.ID == "" || request.Settlement.FinishedAt.IsZero() {
		return session.RunSettlementResult{}, session.ErrConflict
	}
	var result session.RunSettlementResult
	err := e.withFenceState(ctx, true, func(store *Store, current session.Run) error {
		var count int64
		runKey, err := store.key(ctx, "runs", string(current.ID))
		if err != nil {
			return err
		}
		if err := store.dbFor(ctx).Table(store.tableName("tool_calls")).Where("run_key = ? AND status IN ?", runKey, []string{string(session.ToolCallPending), string(session.ToolCallRunning)}).Count(&count).Error; err != nil {
			return store.mapErr(err)
		}
		if count != 0 {
			return session.ErrConflict
		}
		if current.Terminal() {
			if current.Status != request.Settlement.Status || !current.FinishedAt.Equal(request.Settlement.FinishedAt.UTC()) || current.Error != request.Settlement.Error {
				return session.ErrConflict
			}
			expected, err := session.RunSettlementRecord(current, request.Event)
			if err != nil {
				return err
			}
			existing, err := store.eventByRunFinished(ctx, runKey, expected.Kind)
			if err != nil || !SameRecord(existing, expected) {
				if err != nil {
					return session.ErrConflict
				}
				return session.ErrConflict
			}
			result = session.RunSettlementResult{Run: current, Event: existing}
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
		canonicalEvent, err = store.appendCanonicalEvent(ctx, canonicalEvent)
		if err != nil {
			return err
		}
		result = session.RunSettlementResult{Run: canonicalRun, Event: canonicalEvent}
		return nil
	})
	return result, err
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

func (e *executionStore) FinalizeAssistantMessage(ctx context.Context, id session.MessageID) error {
	return e.withFence(ctx, func(store *Store, run session.Run) error {
		sessionKey, err := store.key(ctx, "sessions", string(run.SessionID))
		if err != nil {
			return err
		}
		runKey, err := store.key(ctx, "runs", string(run.ID))
		if err != nil {
			return err
		}
		db := store.dbFor(ctx).Table(store.tableName("messages")).Where("id = ? AND session_key = ? AND run_key = ? AND role = ?", []byte(id), sessionKey, runKey, string(session.RoleAssistant)).Updates(map[string]any{"finalized": 1})
		if err := db.Error; err != nil {
			return store.mapErr(err)
		}
		if db.RowsAffected == 0 {
			return session.ErrNotFound
		}
		return nil
	})
}

func (e *executionStore) AppendPart(ctx context.Context, record session.Part) (session.Part, error) {
	if record.Kind == session.PartToolCall || record.Kind == session.PartToolResult || record.RunID != e.fence.RunID {
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
	call := request.Call
	if call.RunID != e.fence.RunID {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	var result session.ToolTransitionResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if call.SessionID != run.SessionID || !ValidToolRequestEnvelope(call, request.RequestPart) {
			return session.ErrConflict
		}
		event, err := session.ToolTransitionRecord(call, request.Event)
		if err != nil || event.ToolTransition != session.ToolTransitionPending {
			return session.ErrConflict
		}
		if _, err := store.appendPart(ctx, request.RequestPart); err != nil {
			return err
		}
		result.Call, err = store.createToolCall(ctx, call)
		if err != nil {
			return err
		}
		result.Event, err = store.appendCanonicalEvent(ctx, event)
		if err != nil {
			return err
		}
		result.Call, err = store.GetToolCall(ctx, call.ID)
		return err
	})
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	return result, nil
}

func (e *executionStore) ClaimToolCall(ctx context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
	if request.ID == "" || request.ClaimedBy == "" || request.ClaimToken == "" || request.StartedAt.IsZero() || request.LeaseDuration <= 0 {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	var result session.ToolTransitionResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		current, err := store.GetToolCall(ctx, request.ID)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		if current.RunID != e.fence.RunID || current.SessionID != run.SessionID {
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
			result = session.ToolTransitionResult{Call: current}
			result.Event, err = store.appendCanonicalEvent(ctx, event)
			return err
		}
		if current.Status != session.ToolCallPending || current.ClaimedBy != "" || current.ClaimToken != "" {
			return session.ErrConflict
		}
		if err := renewRunLease(ctx, store, e.fence, request.LeaseDuration); err != nil {
			return err
		}
		var leasedRun session.Run
		leasedRun, err = store.getRun(ctx, e.fence.RunID)
		if err != nil {
			return err
		}
		candidate.LeaseUntil = leasedRun.LeaseUntil
		result.Call, err = store.claimToolCall(ctx, candidate)
		if err != nil {
			return err
		}
		result.Event, err = store.appendCanonicalEvent(ctx, event)
		if err != nil {
			return err
		}
		result.Call, err = store.GetToolCall(ctx, request.ID)
		return err
	})
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	return result, nil
}

func (e *executionStore) SettleToolCall(ctx context.Context, request session.SettleToolCallRequest) (session.ToolTransitionResult, error) {
	var result session.ToolTransitionResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		settlement := request.Settlement
		call, err := store.GetToolCall(ctx, settlement.ID)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
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
		result.Event, err = store.appendCanonicalEvent(ctx, event)
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
