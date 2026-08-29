package runtime

import (
	"encoding/json"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func testCreateToolRequest(call session.ToolCall, id session.EventID, at time.Time) session.CreateToolCallRequest {
	if call.RequestPartID == "" {
		call.RequestPartID = session.PartID("request-part-" + string(call.ID))
	}
	if len(call.Input) == 0 {
		call.Input = json.RawMessage(`{}`)
	}
	return session.CreateToolCallRequest{
		Call: call,
		RequestPart: session.Part{
			ID: call.RequestPartID, MessageID: call.MessageID, SessionID: call.SessionID, RunID: call.RunID,
			Kind: session.PartToolCall, Payload: mustJSON(toolCallPayload{ID: string(call.ID), Name: call.Name, Arguments: call.Input}), CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
		},
		Event: session.ToolTransitionEvent{ID: id, CreatedAt: at.UTC()},
	}
}

func testClaimToolRequest(call session.ToolCall, eventID session.EventID, duration time.Duration, at time.Time) session.ClaimToolCallRequest {
	return session.ClaimToolCallRequest{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken,
		StartedAt: at.UTC(), LeaseDuration: duration, Event: session.ToolTransitionEvent{ID: eventID, CreatedAt: at.UTC()},
	}
}

func testSettleToolRequest(settlement session.ToolSettlement, id session.EventID) session.SettleToolCallRequest {
	return session.SettleToolCallRequest{Settlement: settlement, Event: session.ToolTransitionEvent{ID: id, CreatedAt: settlement.CompletedAt.UTC()}}
}
