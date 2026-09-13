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
	parts, err := session.EncodeContentParts(session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{{
			ID: "block-" + string(call.RequestPartID), Kind: session.BlockKindFunctionToolCall,
			FunctionCall: &session.FunctionCallBlock{CallID: string(call.ID), Name: call.Name, Arguments: string(call.Input)},
		}},
	}, func() session.PartID { return call.RequestPartID }, call.MessageID, call.SessionID, call.RunID, at.UTC(), session.DefaultContentLimits())
	if err != nil {
		panic(err)
	}
	return session.CreateToolCallRequest{
		Call:        call,
		RequestPart: parts[0],
		Event:       session.ToolTransitionEvent{ID: id, CreatedAt: at.UTC()},
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
