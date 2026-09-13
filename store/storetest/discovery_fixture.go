package storetest

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func seedPrivateDiscovery(t *testing.T, st session.Store, id session.ID) {
	t.Helper()
	ctx := t.Context()
	discoveryCreate(t, st, session.Session{ID: id, WorkspaceID: "A", ParentID: "PRIVATE_PARENT", Directory: "PRIVATE_DIRECTORY", Metadata: map[string]string{"private": strings.Repeat("PRIVATE_METADATA", 10000)}})
	candidate := run(session.RunID(id)+"-run", id, "PRIVATE_OWNER")
	candidate.ClaimToken = "PRIVATE_RUN_CLAIM"
	candidate.Config = map[string]string{"private": "PRIVATE_CONFIG"}
	r := admitRun(t, ctx, st, candidate)
	ex := executionFor(st, r)
	for _, role := range []session.Role{session.RoleUser, session.RoleAssistant} {
		mid := session.MessageID(string(id) + string(role))
		appendMessage(t, ctx, ex, message(mid, id, r.ID, role))
		// Use a real public text kind, encoded through EncodeContentParts,
		// so the sentinel lands in the display_text column ObservationText
		// derives from PartUserInputText/PartAssistantGenText -- otherwise
		// discoveryPrivate's "no PRIVATE_ substring leaks" assertion never
		// exercises the discovery-vs-display_text boundary it is meant to
		// guard (ObservationText returns empty for every other part kind).
		blockKind := session.BlockKindUserInputText
		if role == session.RoleAssistant {
			blockKind = session.BlockKindAssistantGenText
		}
		content := session.Content{Role: role, Blocks: []session.ContentBlock{
			{ID: "b1", Kind: blockKind, Text: &session.TextBlock{Text: "PRIVATE_TRANSCRIPT"}},
		}}
		textID := session.PartID(mid) + "text"
		parts, err := session.EncodeContentParts(content, func() session.PartID { return textID }, mid, id, r.ID, time.Now().UTC(), session.DefaultContentLimits())
		if err != nil {
			t.Fatal(err)
		}
		appendPart(t, ctx, ex, parts[0])
	}
	mid := session.MessageID(string(id) + string(session.RoleAssistant))
	p := part(session.PartID(id)+"reasoning", mid, id, r.ID, 1)
	p.Kind = session.PartReasoning
	p.Payload = []byte(`{"text":"PRIVATE_REASONING"}`)
	appendPart(t, ctx, ex, p)
	payload, err := session.EncodeProviderStatePayload(session.ProviderStateEnvelope{CodecID: "codec", Version: 1, ProviderID: "provider", SourceModelID: "model", CompatibilityKey: "compat", Data: json.RawMessage(`{"data":"` + strings.Repeat("PRIVATE_PROVIDER", 10000) + `"}`)})
	if err != nil {
		t.Fatal(err)
	}
	p = part(session.PartID(id)+"provider", mid, id, r.ID, 2)
	p.Kind = session.PartProviderState
	p.Payload = payload
	appendPart(t, ctx, ex, p)
	mr := modelRequest(session.ModelRequestID(id)+"request", id, r.ID, 1)
	mr.System = "PRIVATE_MODEL_REQUEST"
	if _, err = ex.CreateModelRequest(ctx, mr); err != nil {
		t.Fatal(err)
	}
	discoverySeedTool(t, st, r, mid, id == "completed")
	switch id {
	case "expired":
		if _, err = ex.RenewRunLease(ctx, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
	case "completed":
		r.Status = session.RunCompleted
		r.FinishedAt = time.Now()
		if _, err = ex.SettleRun(ctx, settleRunRequest(r)); err != nil {
			t.Fatal(err)
		}
	case "live":
		if _, err = ex.StartRun(ctx, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

func discoverySeedTool(t *testing.T, st session.Store, r session.Run, mid session.MessageID, complete bool) {
	t.Helper()
	ctx := t.Context()
	ex := executionFor(st, r)
	at := time.Now().UTC()
	call := session.ToolCall{ID: session.ToolCallID(r.SessionID) + "-tool", SessionID: r.SessionID, RunID: r.ID, MessageID: mid, RequestPartID: session.PartID(mid) + "request", ResultMessageID: session.MessageID(mid) + "result", ResultPartID: session.PartID(mid) + "result", Name: "echo", Input: json.RawMessage(`{"text":"PRIVATE_TOOL_INPUT"}`), Status: session.ToolCallPending, Metadata: map[string]string{"private": "PRIVATE_TOOL_METADATA"}}
	requestParts, err := session.EncodeContentParts(session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{{
			ID: "block-" + string(call.RequestPartID), Kind: session.BlockKindFunctionToolCall,
			FunctionCall: &session.FunctionCallBlock{CallID: string(call.ID), Name: call.Name, Arguments: string(call.Input)},
		}},
	}, func() session.PartID { return call.RequestPartID }, mid, r.SessionID, r.ID, at, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = ex.CreateToolCall(ctx, session.CreateToolCallRequest{Call: call, RequestPart: requestParts[0], Event: toolEvent(session.EventID(mid)+"create", at)})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := ex.ClaimToolCall(ctx, session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: "PRIVATE_WORKER", ClaimToken: "PRIVATE_TOOL_CLAIM", StartedAt: at, LeaseDuration: time.Minute, Event: toolEvent(session.EventID(mid)+"claim", at)})
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		return
	}
	output, _ := json.Marshal(map[string]any{"tool_call_id": call.ID, "status": "completed", "content": "PRIVATE_TOOL_OUTPUT"})
	resultParts, err := session.EncodeContentParts(session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID: "block-" + string(call.ResultPartID), Kind: session.BlockKindFunctionToolResult,
			FunctionResult: &session.FunctionResultBlock{CallID: string(call.ID), Name: call.Name, Content: []session.ResultContent{{Type: session.ResultContentText, Text: string(output)}}},
		}},
	}, func() session.PartID { return call.ResultPartID }, call.ResultMessageID, r.SessionID, r.ID, at, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = ex.SettleToolCall(ctx, session.SettleToolCallRequest{Settlement: session.ToolSettlement{ID: call.ID, ClaimedBy: claimed.Call.ClaimedBy, ClaimToken: claimed.Call.ClaimToken, Status: session.ToolCallCompleted, Output: output, CompletedAt: at,
		ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: r.SessionID, RunID: r.ID, ParentID: mid, Role: session.RoleUser}, ResultPart: resultParts[0]}, Event: toolEvent(session.EventID(mid)+"settle", at)})
	if err != nil {
		t.Fatal(err)
	}
}
