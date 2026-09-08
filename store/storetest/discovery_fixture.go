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
		p := part(session.PartID(mid)+"text", mid, id, r.ID, 0)
		p.Payload = []byte(`{"text":"PRIVATE_TRANSCRIPT"}`)
		appendPart(t, ctx, ex, p)
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
	raw, _ := json.Marshal(map[string]any{"id": call.ID, "name": call.Name, "arguments": call.Input})
	_, err := ex.CreateToolCall(ctx, session.CreateToolCallRequest{Call: call, RequestPart: session.Part{ID: call.RequestPartID, MessageID: mid, SessionID: r.SessionID, RunID: r.ID, Kind: session.PartToolCall, Payload: raw}, Event: toolEvent(session.EventID(mid)+"create", at)})
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
	_, err = ex.SettleToolCall(ctx, session.SettleToolCallRequest{Settlement: session.ToolSettlement{ID: call.ID, ClaimedBy: claimed.Call.ClaimedBy, ClaimToken: claimed.Call.ClaimToken, Status: session.ToolCallCompleted, Output: output, CompletedAt: at,
		ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: r.SessionID, RunID: r.ID, ParentID: mid, Role: session.RoleTool}, ResultPart: session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: r.SessionID, RunID: r.ID, Kind: session.PartToolResult, Payload: output}}, Event: toolEvent(session.EventID(mid)+"settle", at)})
	if err != nil {
		t.Fatal(err)
	}
}
