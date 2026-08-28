package runtime

import (
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func testCreateToolRequest(call session.ToolCall, id session.EventID, at time.Time) session.CreateToolCallRequest {
	return session.CreateToolCallRequest{Call: call, Event: session.ToolTransitionEvent{ID: id, CreatedAt: at.UTC()}}
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
