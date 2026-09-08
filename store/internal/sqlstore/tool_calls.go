package sqlstore

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) createToolCall(ctx context.Context, record session.ToolCall) (session.ToolCall, error) {
	if record.ID == "" || record.SessionID == "" || record.RunID == "" || record.MessageID == "" || record.RequestPartID == "" || record.Name == "" || record.Status != session.ToolCallPending {
		return session.ToolCall{}, session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ToolCall{}, err
	}
	sessionKey, err := s.key(ctx, "sessions", string(record.SessionID))
	if err != nil {
		return session.ToolCall{}, relationError(err)
	}
	runKey, err := s.key(ctx, "runs", string(record.RunID))
	if err != nil {
		return session.ToolCall{}, relationError(err)
	}
	messageKey, err := s.key(ctx, "messages", string(record.MessageID))
	if err != nil {
		return session.ToolCall{}, relationError(err)
	}
	partKey, err := s.key(ctx, "parts", string(record.RequestPartID))
	if err != nil {
		return session.ToolCall{}, relationError(err)
	}
	// Relation keys are checked together so a valid ID from another run cannot
	// be smuggled into the owning row.
	var count int64
	if err := s.dbFor(ctx).Table("runs").Where("row_key = ? AND session_key = ?", runKey, sessionKey).Count(&count).Error; err != nil || count != 1 {
		if err != nil {
			return session.ToolCall{}, s.mapErr(err)
		}
		return session.ToolCall{}, session.ErrConflict
	}
	if err := s.dbFor(ctx).Table("messages").Where("row_key = ? AND session_key = ? AND run_key = ?", messageKey, sessionKey, runKey).Count(&count).Error; err != nil || count != 1 {
		if err != nil {
			return session.ToolCall{}, s.mapErr(err)
		}
		return session.ToolCall{}, session.ErrConflict
	}
	if err := s.dbFor(ctx).Table("parts").Where("row_key = ? AND message_key = ? AND session_key = ? AND run_key = ? AND kind = ?", partKey, messageKey, sessionKey, runKey, string(session.PartToolCall)).Count(&count).Error; err != nil || count != 1 {
		if err != nil {
			return session.ToolCall{}, s.mapErr(err)
		}
		return session.ToolCall{}, session.ErrConflict
	}
	db := s.dbFor(ctx).Table("tool_calls").Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"id": []byte(record.ID), "session_key": sessionKey, "run_key": runKey,
		"request_message_key": messageKey, "request_part_key": partKey,
		"result_message_id": []byte(record.ResultMessageID), "result_part_id": []byte(record.ResultPartID),
		"status": string(record.Status), "name": []byte(record.Name), "claimed_by": []byte(record.ClaimedBy),
		"claim_token": []byte(record.ClaimToken), "record": raw,
	})
	if err := db.Error; err != nil {
		return session.ToolCall{}, s.mapErr(err)
	}
	if db.RowsAffected == 0 {
		existing, readErr := s.GetToolCall(ctx, record.ID)
		if readErr == nil && SameRecord(existing, record) {
			return existing, nil
		}
		if readErr != nil && !errors.Is(readErr, session.ErrNotFound) {
			return session.ToolCall{}, readErr
		}
		return session.ToolCall{}, session.ErrConflict
	}
	return record, nil
}

func (s *Store) toolCallSelect() string {
	return "tool_calls.row_key, tool_calls.id, tool_calls.session_key, tool_calls.run_key, sessions.id AS session_id, runs.id AS run_id, tool_calls.request_message_key, tool_calls.request_part_key, reqm.id AS request_message_id, reqm.session_key AS request_message_session_key, reqm.run_key AS request_message_run_key, reqp.id AS request_part_id, reqp.message_key AS request_part_message_key, reqp.session_key AS request_part_session_key, reqp.run_key AS request_part_run_key, tool_calls.result_message_id, tool_calls.result_part_id, tool_calls.status, tool_calls.name, tool_calls.claimed_by, tool_calls.claim_token, tool_calls.record"
}

func (s *Store) GetToolCall(ctx context.Context, id session.ToolCallID) (session.ToolCall, error) {
	var row toolCallRow
	err := s.dbFor(ctx).Table("tool_calls").Select(s.toolCallSelect()).Joins("JOIN sessions ON sessions.row_key = tool_calls.session_key").Joins("JOIN runs ON runs.row_key = tool_calls.run_key").Joins("JOIN messages AS reqm ON reqm.row_key = tool_calls.request_message_key").Joins("JOIN parts AS reqp ON reqp.row_key = tool_calls.request_part_key").Where("tool_calls.id = ?", []byte(id)).Take(&row).Error
	if err != nil {
		return session.ToolCall{}, translateReadError(err)
	}
	value, err := decodeToolCallRow(row)
	if err != nil {
		return session.ToolCall{}, err
	}
	if string(row.SessionID) != string(value.SessionID) || string(row.RunID) != string(value.RunID) || value.MessageID == "" {
		return session.ToolCall{}, session.ErrConflict
	}
	return value, nil
}

func (s *Store) ListUnfinishedToolCalls(ctx context.Context, runID session.RunID) ([]session.ToolCall, error) {
	runKey, err := s.key(ctx, "runs", string(runID))
	if err != nil {
		return nil, err
	}
	var rows []toolCallRow
	err = s.dbFor(ctx).Table("tool_calls").Select(s.toolCallSelect()).Joins("JOIN sessions ON sessions.row_key = tool_calls.session_key").Joins("JOIN runs ON runs.row_key = tool_calls.run_key").Joins("JOIN messages AS reqm ON reqm.row_key = tool_calls.request_message_key").Joins("JOIN parts AS reqp ON reqp.row_key = tool_calls.request_part_key").Where("tool_calls.run_key = ? AND tool_calls.status IN ?", runKey, []string{string(session.ToolCallPending), string(session.ToolCallRunning)}).Order("tool_calls.id").Find(&rows).Error
	if err != nil {
		return nil, s.mapErr(err)
	}
	result := make([]session.ToolCall, 0, len(rows))
	for _, row := range rows {
		value, err := decodeToolCallRow(row)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (s *Store) claimToolCall(ctx context.Context, record session.ToolCall) (session.ToolCall, error) {
	current, err := s.GetToolCall(ctx, record.ID)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return session.ToolCall{}, session.ErrConflict
		}
		return session.ToolCall{}, err
	}
	if session.TerminalToolCall(current.Status) || current.ClaimedBy != "" {
		if session.SameToolTransitionState(current, record) {
			return current, nil
		}
		return session.ToolCall{}, session.ErrConflict
	}
	if current.Status != session.ToolCallPending || record.Status != session.ToolCallRunning {
		return session.ToolCall{}, session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ToolCall{}, err
	}
	toolKey, err := s.key(ctx, "tool_calls", string(record.ID))
	if err != nil {
		return session.ToolCall{}, err
	}
	db := s.dbFor(ctx).Table("tool_calls").Where("row_key = ? AND status = ? AND claimed_by = ? AND claim_token = ?", toolKey, string(session.ToolCallPending), []byte{}, []byte{}).Updates(map[string]any{
		"status": string(record.Status), "claimed_by": []byte(record.ClaimedBy), "claim_token": []byte(record.ClaimToken), "record": raw,
	})
	if err := db.Error; err != nil {
		return session.ToolCall{}, s.mapErr(err)
	}
	if db.RowsAffected == 0 {
		latest, getErr := s.GetToolCall(ctx, record.ID)
		if getErr != nil {
			return session.ToolCall{}, getErr
		}
		if session.SameToolTransitionState(latest, record) {
			return latest, nil
		}
		return session.ToolCall{}, session.ErrConflict
	}
	return record, nil
}

func (s *Store) finishToolCall(ctx context.Context, record session.ToolCall) error {
	if !session.TerminalToolCall(record.Status) || record.ClaimedBy == "" || record.ClaimToken == "" {
		return session.ErrConflict
	}
	current, err := s.GetToolCall(ctx, record.ID)
	if err != nil {
		return err
	}
	if session.TerminalToolCall(current.Status) {
		if SameRecord(current, record) {
			return nil
		}
		return session.ErrConflict
	}
	if current.ClaimedBy != record.ClaimedBy || current.ClaimToken != record.ClaimToken {
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	toolKey, err := s.key(ctx, "tool_calls", string(record.ID))
	if err != nil {
		return err
	}
	db := s.dbFor(ctx).Table("tool_calls").Where("row_key = ? AND claimed_by = ? AND claim_token = ? AND status IN ?", toolKey, []byte(record.ClaimedBy), []byte(record.ClaimToken), []string{string(session.ToolCallPending), string(session.ToolCallRunning)}).Updates(map[string]any{
		"status": string(record.Status), "claimed_by": []byte(record.ClaimedBy), "claim_token": []byte(record.ClaimToken), "record": raw,
	})
	if err := db.Error; err != nil {
		return s.mapErr(err)
	}
	if db.RowsAffected == 0 {
		latest, getErr := s.GetToolCall(ctx, record.ID)
		if getErr == nil && session.TerminalToolCall(latest.Status) && SameRecord(latest, record) {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		return session.ErrConflict
	}
	return nil
}

// settleToolCall writes the reserved result envelope and terminal call in the
// same operation. The enclosing execution method owns the fence and atomic
// savepoint; this private method intentionally does not start another tx.
func (s *Store) settleToolCall(ctx context.Context, settlement session.ToolSettlement) error {
	call, err := s.GetToolCall(ctx, settlement.ID)
	if err != nil {
		return err
	}
	if !ValidToolResultEnvelope(call, settlement) {
		return session.ErrConflict
	}
	settled, err := settlement.Apply(call)
	if err != nil {
		return err
	}
	if session.TerminalToolCall(call.Status) {
		if !SameRecord(call, settled) {
			return session.ErrConflict
		}
		return nil
	}
	if _, err := s.appendMessage(ctx, settlement.ResultMessage); err != nil {
		return err
	}
	if _, err := s.appendPart(ctx, settlement.ResultPart); err != nil {
		return err
	}
	return s.finishToolCall(ctx, settled)
}
