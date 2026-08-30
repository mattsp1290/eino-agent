package sqlite

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mattsp1290/eino-agent/internal/jsonequal"
	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) createToolCall(ctx context.Context, record session.ToolCall) (session.ToolCall, error) {
	var existing session.ToolCall
	if err := s.getJSON(ctx, "SELECT record FROM tool_calls WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.ToolCall{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.ToolCall{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ToolCall{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO tool_calls(id, session_id, run_id, message_id, status, claimed_by, claim_token, record) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.SessionID, record.RunID, record.MessageID, record.Status, record.ClaimedBy, record.ClaimToken, raw)
	return record, mapErr(err)
}

func validToolRequestEnvelope(call session.ToolCall, part session.Part) bool {
	if call.RequestPartID == "" || part.ID != call.RequestPartID || part.MessageID != call.MessageID ||
		part.SessionID != call.SessionID || part.RunID != call.RunID || part.Kind != session.PartToolCall {
		return false
	}
	var payload struct {
		ID        session.ToolCallID `json:"id"`
		Name      string             `json:"name"`
		Arguments json.RawMessage    `json:"arguments"`
	}
	if err := json.Unmarshal(part.Payload, &payload); err != nil {
		return false
	}
	return payload.ID == call.ID && payload.Name == call.Name && jsonequal.Equal(payload.Arguments, call.Input)
}

func (s *Store) GetToolCall(ctx context.Context, id session.ToolCallID) (session.ToolCall, error) {
	var record session.ToolCall
	err := s.getJSON(ctx, "SELECT record FROM tool_calls WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) ListUnfinishedToolCalls(ctx context.Context, runID session.RunID) ([]session.ToolCall, error) {
	return listJSON[session.ToolCall](ctx, s, `SELECT record FROM tool_calls WHERE run_id = ? AND status IN (?, ?) ORDER BY id`, runID, session.ToolCallPending, session.ToolCallRunning)
}

func (s *Store) claimToolCall(ctx context.Context, record session.ToolCall) (session.ToolCall, error) {
	current, err := s.GetToolCall(ctx, record.ID)
	if err != nil {
		return session.ToolCall{}, err
	}
	if session.TerminalToolCall(current.Status) || current.ClaimedBy != "" {
		if current.ClaimedBy == record.ClaimedBy && current.ClaimToken == record.ClaimToken {
			return current, nil
		}
		return session.ToolCall{}, session.ErrConflict
	}
	record.Status = session.ToolCallRunning
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ToolCall{}, err
	}
	result, err := s.exec(ctx, `UPDATE tool_calls SET status = ?, claimed_by = ?, claim_token = ?, record = ? WHERE id = ? AND status = ? AND claimed_by = ''`,
		record.Status, record.ClaimedBy, record.ClaimToken, raw, record.ID, session.ToolCallPending)
	if err != nil {
		return session.ToolCall{}, mapErr(err)
	}
	if err := rowsAffected(result); err != nil {
		current, getErr := s.GetToolCall(ctx, record.ID)
		if getErr != nil {
			return session.ToolCall{}, getErr
		}
		if current.ClaimedBy == record.ClaimedBy && current.ClaimToken == record.ClaimToken {
			return current, nil
		}
		return session.ToolCall{}, session.ErrConflict
	}
	return record, nil
}

func (s *Store) finishToolCall(ctx context.Context, record session.ToolCall) error {
	current, err := s.GetToolCall(ctx, record.ID)
	if err != nil {
		return err
	}
	if current.ClaimedBy != record.ClaimedBy || current.ClaimToken != record.ClaimToken {
		return session.ErrConflict
	}
	if session.TerminalToolCall(current.Status) {
		if sameRecord(current, record) {
			return nil
		}
		return session.ErrConflict
	}
	if !session.TerminalToolCall(record.Status) {
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE tool_calls SET status = ?, claimed_by = ?, claim_token = ?, record = ? WHERE id = ? AND claimed_by = ? AND claim_token = ? AND status IN (?, ?)`,
		record.Status, record.ClaimedBy, record.ClaimToken, raw, record.ID, record.ClaimedBy, record.ClaimToken, session.ToolCallPending, session.ToolCallRunning)
	if err != nil {
		return mapErr(err)
	}
	if err := rowsAffected(result); err != nil {
		latest, getErr := s.GetToolCall(ctx, record.ID)
		if getErr != nil {
			return getErr
		}
		if session.TerminalToolCall(latest.Status) && sameRecord(latest, record) {
			return nil
		}
		return session.ErrConflict
	}
	return nil
}

// settleToolCall atomically commits a terminal call and its reserved result
// message/part. Repeating the identical settlement is idempotent.
func (s *Store) settleToolCall(ctx context.Context, settlement session.ToolSettlement) error {
	if s.tx == nil {
		return s.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			store, ok := tx.(*Store)
			if !ok {
				return session.ErrConflict
			}
			return store.settleToolCall(ctx, settlement)
		})
	}
	call, err := s.GetToolCall(ctx, settlement.ID)
	if err != nil {
		return err
	}
	if !validToolResultEnvelope(call, settlement) {
		return session.ErrConflict
	}
	settled, err := settlement.Apply(call)
	if err != nil {
		return err
	}
	if err := s.finishToolCall(ctx, settled); err != nil {
		return err
	}
	if _, err := s.appendMessage(ctx, settlement.ResultMessage); err != nil {
		return err
	}
	if _, err := s.appendPart(ctx, settlement.ResultPart); err != nil {
		return err
	}
	return nil
}

func validToolResultEnvelope(call session.ToolCall, settlement session.ToolSettlement) bool {
	message := settlement.ResultMessage
	part := settlement.ResultPart
	return call.ResultMessageID != "" && call.ResultPartID != "" &&
		message.ID == call.ResultMessageID && message.SessionID == call.SessionID && message.RunID == call.RunID && message.ParentID == call.MessageID && message.Role == session.RoleTool &&
		part.ID == call.ResultPartID && part.MessageID == call.ResultMessageID && part.SessionID == call.SessionID && part.RunID == call.RunID && part.Kind == session.PartToolResult &&
		jsonequal.Equal(part.Payload, settlement.Output)
}
