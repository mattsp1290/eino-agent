package storetest

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func discoveryPrivate(t *testing.T, factory Factory) {
	st, reader := discoverySubject(t, factory)
	ctx := t.Context()
	for _, id := range []session.ID{"live", "expired", "completed"} {
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
	// Compare all public durable entities, including claim tokens, leases, events,
	// transcripts and request data, as well as revisions when supported.
	snapshot := func() []byte {
		t.Helper()
		var values []any
		for _, id := range []session.ID{"live", "expired", "completed"} {
			s, err := st.GetSession(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			r, err := st.GetRun(ctx, session.RunID(id)+"-run")
			if err != nil {
				t.Fatal(err)
			}
			m, err := st.ListMessages(ctx, id, session.ReplayCursor{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			e, err := st.ListEvents(ctx, id, session.EventCursor{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			tool, err := st.GetToolCall(ctx, session.ToolCallID(id)+"-tool")
			if err != nil {
				t.Fatal(err)
			}
			requests, err := st.ListModelRequests(ctx, r.ID, session.ModelRequestCursor{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, s, r, m, e, tool, requests)
			if observer, ok := st.(session.ObservationReader); ok {
				w, err := observer.ReadObservationRevision(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, w)
			}
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := snapshot()
	for range 3 {
		q := session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1}
		var ids []session.ID
		for {
			p := discoveryList(t, reader, q)
			ids = append(ids, discoveryIDs(p)...)
			raw, _ := json.Marshal(p)
			decoded, _ := base64.RawURLEncoding.DecodeString(p.NextCursor)
			if strings.Contains(string(raw), "PRIVATE_") || strings.Contains(string(decoded), "PRIVATE_") {
				t.Fatal("private data disclosed")
			}
			for _, s := range p.Sessions {
				raw, _ := json.Marshal(s)
				var keys map[string]any
				if err := json.Unmarshal(raw, &keys); err != nil {
					t.Fatal(err)
				}
				if len(keys) != 5 || keys["id"] == nil || keys["workspace_id"] == nil || keys["title"] == nil || keys["created_at"] == nil || keys["updated_at"] == nil {
					t.Fatal(keys)
				}
			}
			p.Sessions[0].Title = "local mutation"
			if p.NextCursor == "" {
				break
			}
			q.Cursor = p.NextCursor
		}
		if !reflect.DeepEqual(ids, []session.ID{"live", "expired", "completed"}) {
			t.Fatal(ids)
		}
	}
	if string(before) != string(snapshot()) {
		t.Fatal("discovery mutated durable state")
	}
	if _, err := st.ClaimRun(ctx, session.RunClaim{RunID: "live-run", OwnerID: "new", ClaimToken: "new", LeaseDuration: time.Minute}); !errors.Is(err, session.ErrSessionBusy) {
		t.Fatal("live claim changed", err)
	}
	if _, err := st.ClaimRun(ctx, session.RunClaim{RunID: "expired-run", OwnerID: "new", ClaimToken: "new", LeaseDuration: time.Minute}); err != nil {
		t.Fatal("expired recovery failed", err)
	}
	for i, tc := range []struct {
		id    session.ID
		title string
	}{
		{"oversized-title", strings.Repeat("t", session.DiscoveryMaxTitleBytes+1)},
		{session.ID(strings.Repeat("i", session.DiscoveryMaxIdentityBytes+1)), "small"},
	} {
		discoveryCreate(t, st, session.Session{ID: tc.id, WorkspaceID: []string{"size-title", "size-id"}[i], Title: tc.title})
		p, err := reader.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: []string{"size-title", "size-id"}[i]})
		discoveryErrorPage(t, p, err, session.ErrDiscoveryTooLarge)
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
