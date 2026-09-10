//go:build postgres_integration

package postgres_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

// testWideLifecycle keeps every identity in one connected lifecycle at the
// public API boundary. The values are deliberately byte-shaped (rather than
// repeated text) so this also exercises PostgreSQL's varlena/index handling.
func testWideLifecycle(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixtureWithTimeout(t, server, 60*time.Second)
	wide := func(seed uint32) string { return wideIdentity(1024, seed) }
	ids := struct {
		session, workspace, run, epoch, message, text, tool, request, requestPart, resultMessage, resultPart, pending, running, terminal, runFinished string
	}{
		session: wide(11), workspace: wide(12), run: wide(13), epoch: wide(14), message: wide(15), text: wide(16),
		tool: wide(17), request: wide(18), requestPart: wide(19), resultMessage: wide(23), resultPart: wide(24), pending: wide(20), running: wide(21), terminal: wide(22), runFinished: wide(25),
	}
	for name, value := range map[string]string{
		"session": ids.session, "workspace": ids.workspace, "run": ids.run, "epoch": ids.epoch,
		"message": ids.message, "text": ids.text, "tool": ids.tool, "request": ids.request, "request_part": ids.requestPart, "result_message": ids.resultMessage, "result_part": ids.resultPart,
		"pending": ids.pending, "running": ids.running, "terminal": ids.terminal, "run_finished": ids.runFinished,
	} {
		if len(value) != 1024 {
			t.Fatalf("%s identity length = %d, want 1024", name, len(value))
		}
	}

	created := f.now
	createdSession, err := f.store.CreateSession(f.ctx, session.Session{
		ID: session.ID(ids.session), WorkspaceID: ids.workspace, Title: "wide lifecycle", CreatedAt: created, UpdatedAt: created,
	})
	if err != nil {
		t.Fatalf("create wide session: %v", err)
	}
	if createdSession.ID != session.ID(ids.session) || createdSession.WorkspaceID != ids.workspace {
		t.Fatalf("created session identity lengths: id=%d workspace=%d", len(createdSession.ID), len(createdSession.WorkspaceID))
	}
	run, err := f.store.AdmitRun(f.ctx, session.Run{
		ID: session.RunID(ids.run), SessionID: session.ID(ids.session), OwnerID: "wide-owner", ClaimToken: "wide-claim",
		Status: session.RunPending, ProviderID: "wide-provider", ModelID: "wide-model", CreatedAt: created,
	}, time.Minute)
	if err != nil {
		t.Fatalf("admit wide run: %v", err)
	}
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	epoch := session.ContextEpoch{ID: session.EpochID(ids.epoch), SessionID: createdSession.ID, ProviderID: "wide-provider", ModelID: "wide-model", CreatedAt: created}
	if got, err := execution.StartContextEpoch(f.ctx, epoch); err != nil || got.ID != epoch.ID || got.SessionID != epoch.SessionID {
		t.Fatalf("start wide epoch: id length=%d err=%v", len(got.ID), err)
	}

	message := session.Message{ID: session.MessageID(ids.message), SessionID: createdSession.ID, RunID: run.ID, Role: session.RoleAssistant, Agent: "wide-agent", ModelID: "wide-model", CreatedAt: created, UpdatedAt: created}
	if got, err := execution.AppendMessage(f.ctx, message); err != nil || got.ID != message.ID || got.RunID != run.ID {
		t.Fatalf("append wide assistant: id length=%d err=%v", len(got.ID), err)
	}
	textPayload := json.RawMessage(`{"text":"wide lifecycle content"}`)
	textPart := session.Part{ID: session.PartID(ids.text), MessageID: message.ID, SessionID: message.SessionID, RunID: run.ID, Kind: session.PartText, Ordinal: 0, Payload: textPayload, CreatedAt: created, UpdatedAt: created}
	if got, err := execution.AppendPart(f.ctx, textPart); err != nil || got.ID != textPart.ID || !reflect.DeepEqual(got.Payload, textPayload) {
		t.Fatalf("append wide text: id length=%d payload=%d err=%v", len(got.ID), len(got.Payload), err)
	}
	if err := execution.FinalizeAssistantMessage(f.ctx, message.ID); err != nil {
		t.Fatalf("finalize wide assistant: %v", err)
	}

	model := session.ModelRequestRecord{ID: session.ModelRequestID(ids.request), SessionID: message.SessionID, RunID: run.ID, AssistantMessageID: message.ID, Attempt: 7, Step: 9, ProviderID: "wide-provider", ModelID: "wide-model", State: session.ModelRequestPrepared, Messages: json.RawMessage(`[{"role":"assistant","content":"wide lifecycle content"}]`), System: "wide system", Tools: json.RawMessage(`[{"name":"wide-tool"}]`), SafeCallConfig: json.RawMessage(`{"mode":"wide"}`), ContentSHA256: "wide-content-hash", ExtensionPlanHash: "wide-plan-hash", CreatedAt: created, UpdatedAt: created}
	if got, err := execution.CreateModelRequest(f.ctx, model); err != nil || got.ID != model.ID || got.AssistantMessageID != model.AssistantMessageID || !reflect.DeepEqual(got.Messages, model.Messages) {
		t.Fatalf("create wide model request: id length=%d err=%v", len(got.ID), err)
	}

	call := session.ToolCall{ID: session.ToolCallID(ids.tool), SessionID: run.SessionID, RunID: run.ID, MessageID: message.ID, RequestPartID: session.PartID(ids.requestPart), ResultMessageID: session.MessageID(ids.resultMessage), ResultPartID: session.PartID(ids.resultPart), Name: "wide-tool", Pattern: "exact", Input: json.RawMessage(`{"input":"wide"}`), Status: session.ToolCallPending, RetrySafe: true, Metadata: map[string]string{"owner": "wide-owner"}}
	requestPayload, err := json.Marshal(struct {
		ID        session.ToolCallID `json:"id"`
		Name      string             `json:"name"`
		Arguments json.RawMessage    `json:"arguments"`
	}{call.ID, call.Name, call.Input})
	if err != nil {
		t.Fatal(err)
	}
	requestPart := session.Part{ID: call.RequestPartID, MessageID: message.ID, SessionID: run.SessionID, RunID: run.ID, Kind: session.PartToolCall, Payload: requestPayload, CreatedAt: created, UpdatedAt: created}
	if got, err := execution.CreateToolCall(f.ctx, session.CreateToolCallRequest{Call: call, RequestPart: requestPart, Event: session.ToolTransitionEvent{ID: session.EventID(ids.pending), EpochID: epoch.ID, ProviderID: "wide-provider", ModelID: "wide-model", CreatedAt: created}}); err != nil || got.Call.ID != call.ID || got.Call.Status != session.ToolCallPending || got.Event.ID != session.EventID(ids.pending) {
		t.Fatalf("create wide tool: id length=%d status=%s event length=%d err=%v", len(got.Call.ID), got.Call.Status, len(got.Event.ID), err)
	}
	started := created.Add(time.Second)
	claimed, err := execution.ClaimToolCall(f.ctx, session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: "wide-worker", ClaimToken: "wide-tool-claim", StartedAt: started, LeaseDuration: time.Minute, Event: session.ToolTransitionEvent{ID: session.EventID(ids.running), EpochID: epoch.ID, CreatedAt: started}})
	if err != nil || claimed.Call.ID != call.ID || claimed.Call.Status != session.ToolCallRunning || claimed.Call.ClaimedBy != "wide-worker" || claimed.Event.ID != session.EventID(ids.running) {
		t.Fatalf("claim wide tool: id length=%d status=%s event length=%d err=%v", len(claimed.Call.ID), claimed.Call.Status, len(claimed.Event.ID), err)
	}
	finished := started.Add(time.Second)
	resultMessage := session.Message{ID: call.ResultMessageID, SessionID: run.SessionID, RunID: run.ID, ParentID: message.ID, Role: session.RoleTool, CreatedAt: finished, UpdatedAt: finished}
	resultPart := session.Part{ID: call.ResultPartID, MessageID: resultMessage.ID, SessionID: run.SessionID, RunID: run.ID, Kind: session.PartToolResult, Payload: json.RawMessage(`{"output":"wide result"}`), CreatedAt: finished, UpdatedAt: finished}
	settled, err := execution.SettleToolCall(f.ctx, session.SettleToolCallRequest{Settlement: session.ToolSettlement{ID: call.ID, ClaimedBy: "wide-worker", ClaimToken: "wide-tool-claim", Status: session.ToolCallCompleted, Output: json.RawMessage(`{"output":"wide result"}`), Metadata: map[string]string{"owner": "wide-owner"}, CompletedAt: finished, ResultMessage: resultMessage, ResultPart: resultPart}, Event: session.ToolTransitionEvent{ID: session.EventID(ids.terminal), EpochID: epoch.ID, ProviderID: "wide-provider", ModelID: "wide-model", CreatedAt: finished}})
	if err != nil || settled.Call.ID != call.ID || settled.Call.Status != session.ToolCallCompleted || settled.Call.ResultMessageID != resultMessage.ID || settled.Call.ResultPartID != resultPart.ID || settled.Event.ID != session.EventID(ids.terminal) {
		t.Fatalf("settle wide tool: id length=%d status=%s event length=%d err=%v", len(settled.Call.ID), settled.Call.Status, len(settled.Event.ID), err)
	}
	closedEpoch := epoch
	closedEpoch.ClosedAt = finished
	if err := execution.FinishContextEpoch(f.ctx, closedEpoch); err != nil {
		t.Fatalf("finish wide epoch: %v", err)
	}
	if err := execution.FinalizeAssistantMessage(f.ctx, message.ID); err != nil {
		t.Fatalf("finalize wide assistant: %v", err)
	}
	finishedRunAt := finished.Add(time.Nanosecond)
	settledRun, err := execution.SettleRun(f.ctx, session.SettleRunRequest{Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: finishedRunAt}, Event: session.RunSettlementEvent{ID: session.EventID(ids.runFinished), MessageID: message.ID, Usage: session.Usage{InputTokens: 1024, OutputTokens: 256}}})
	if err != nil || settledRun.Run.ID != run.ID || settledRun.Run.Status != session.RunCompleted || settledRun.Event.ID != session.EventID(ids.runFinished) {
		t.Fatalf("settle wide run: id length=%d status=%s event length=%d err=%v", len(settledRun.Run.ID), settledRun.Run.Status, len(settledRun.Event.ID), err)
	}

	f.reopen(t)
	gotSession, err := f.store.GetSession(f.ctx, createdSession.ID)
	if err != nil || gotSession.ID != createdSession.ID || gotSession.WorkspaceID != ids.workspace || gotSession.Title != "wide lifecycle" {
		t.Fatalf("reopened wide session: id length=%d workspace length=%d err=%v", len(gotSession.ID), len(gotSession.WorkspaceID), err)
	}
	gotRun, err := f.store.GetRun(f.ctx, run.ID)
	if err != nil || gotRun.ID != run.ID || gotRun.SessionID != createdSession.ID || gotRun.OwnerID != "wide-owner" || gotRun.Status != session.RunCompleted {
		t.Fatalf("reopened wide run: id length=%d owner=%q status=%s err=%v", len(gotRun.ID), gotRun.OwnerID, gotRun.Status, err)
	}
	epochs, err := f.store.(session.ContextEpochReader).ListContextEpochs(f.ctx, createdSession.ID)
	if err != nil || len(epochs) != 1 || epochs[0].ID != epoch.ID || epochs[0].SessionID != createdSession.ID || !epochs[0].ClosedAt.Equal(finished) {
		t.Fatalf("reopened wide epochs: count=%d err=%v", len(epochs), err)
	}
	batch, err := f.store.ListMessages(f.ctx, createdSession.ID, session.ReplayCursor{Limit: 10})
	if err != nil || len(batch.Messages) != 2 || len(batch.Parts) != 3 || len(batch.PartOwnerMessageIDs) != 3 {
		t.Fatalf("reopened wide replay shape: messages=%d parts=%d err=%v", len(batch.Messages), len(batch.Parts), err)
	}
	if batch.Messages[0].ID != message.ID || batch.Messages[0].RunID != run.ID || batch.Messages[0].Role != session.RoleAssistant || batch.Messages[1].ID != resultMessage.ID || batch.Messages[1].Role != session.RoleTool {
		t.Fatalf("reopened wide replay messages: first id length=%d second role=%s", len(batch.Messages[0].ID), batch.Messages[1].Role)
	}
	wantParts := map[session.PartID]session.Part{textPart.ID: textPart, requestPart.ID: requestPart, resultPart.ID: resultPart}
	for i, part := range batch.Parts {
		want, ok := wantParts[part.ID]
		if !ok || part.SessionID != run.SessionID || part.RunID != run.ID || part.MessageID != want.MessageID || batch.PartOwnerMessageIDs[i] != want.MessageID || part.Kind != want.Kind || !reflect.DeepEqual(part.Payload, want.Payload) {
			t.Fatalf("wide replay part identity, owner or payload mismatch at %d", i)
		}
		delete(wantParts, part.ID)
	}
	if len(wantParts) != 0 {
		t.Fatalf("wide replay omitted %d parts", len(wantParts))
	}

	models, err := f.store.ListModelRequests(f.ctx, run.ID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(models.Records) != 1 || models.Records[0].ID != model.ID || models.Records[0].AssistantMessageID != message.ID || !reflect.DeepEqual(models.Records[0].Messages, model.Messages) {
		t.Fatalf("wide model requests: count=%d err=%v", len(models.Records), err)
	}
	gotCall, err := f.store.GetToolCall(f.ctx, call.ID)
	if err != nil || gotCall.ID != call.ID || gotCall.SessionID != createdSession.ID || gotCall.RunID != run.ID || gotCall.MessageID != message.ID || gotCall.RequestPartID != call.RequestPartID || gotCall.ResultMessageID != resultMessage.ID || gotCall.ResultPartID != resultPart.ID || gotCall.ClaimedBy != "wide-worker" || gotCall.Status != session.ToolCallCompleted || !reflect.DeepEqual(gotCall.Output, json.RawMessage(`{"output":"wide result"}`)) {
		t.Fatalf("wide tool after reopen: id length=%d status=%s owner=%q err=%v", len(gotCall.ID), gotCall.Status, gotCall.ClaimedBy, err)
	}
	events, err := f.store.ListEvents(f.ctx, createdSession.ID, session.EventCursor{Limit: 20})
	if err != nil || len(events.Events) != 4 {
		t.Fatalf("wide events: count=%d err=%v", len(events.Events), err)
	}
	wantEvents := []session.EventID{session.EventID(ids.pending), session.EventID(ids.running), session.EventID(ids.terminal), session.EventID(ids.runFinished)}
	for i, want := range wantEvents {
		if events.Events[i].ID != want || events.Events[i].SessionID != createdSession.ID || events.Events[i].RunID != run.ID {
			t.Fatalf("wide event %d: id length=%d kind=%s", i, len(events.Events[i].ID), events.Events[i].Kind)
		}
	}
	if events.Events[0].ToolCallID != call.ID || events.Events[1].ToolCallID != call.ID || events.Events[2].ToolCallID != call.ID || events.Events[3].Kind != session.RunSettlementEventKind || events.Events[3].MessageID != message.ID {
		t.Fatalf("wide event ownership: tool ids lengths=%d/%d/%d run kind=%s", len(events.Events[0].ToolCallID), len(events.Events[1].ToolCallID), len(events.Events[2].ToolCallID), events.Events[3].Kind)
	}
	// pg_column_size reports the stored datum, including any varlena
	// compression. The random masked bytes are intentionally incompressible, so
	// a result below the payload size would indicate an unexpected transform;
	// this check remains a sanity check rather than an index tuple-size claim.
	for table, id := range map[string]string{
		"sessions": ids.session, "runs": ids.run, "context_epochs": ids.epoch, "messages": ids.message,
		"parts": ids.text, "tool_calls": ids.tool, "model_requests": ids.request, "events": ids.pending,
	} {
		var stored int
		if err := f.db.QueryRowContext(f.ctx, "SELECT pg_column_size(id) FROM public."+table+" WHERE id=$1", []byte(id)).Scan(&stored); err != nil {
			t.Fatalf("stored %s identity: %v", table, err)
		}
		if stored != 1028 {
			t.Fatalf("stored %s identity size=%d, want 1028", table, stored)
		}
	}
	var workspaceStored int
	if err := f.db.QueryRowContext(f.ctx, "SELECT pg_column_size(workspace_id) FROM public.sessions WHERE id=$1", []byte(ids.session)).Scan(&workspaceStored); err != nil || workspaceStored != 1028 {
		t.Fatalf("stored workspace size=%d err=%v", workspaceStored, err)
	}
	limits := session.ObservationLimits{MaxMessages: 10, MaxParts: 20, MaxTools: 10, MaxTextBytes: 1 << 20, MaxSnapshotBytes: 1 << 20}
	observation, err := f.store.(session.ObservationReader).ReadObservationSnapshot(f.ctx, createdSession.ID, limits)
	if err != nil || !observation.Exists || len(observation.Messages) != 1 || len(observation.Runs) != 1 || len(observation.Tools) != 1 || observation.Messages[0].ID != message.ID || observation.Messages[0].Text != "wide lifecycle content" || !observation.Messages[0].Finalized || observation.Tools[0].ID != call.ID || observation.Tools[0].Status != session.ToolCallCompleted || observation.Runs[0].ID != run.ID || observation.Runs[0].Status != session.RunCompleted {
		t.Fatalf("wide observation: exists=%t messages=%d runs=%d tools=%d err=%v", observation.Exists, len(observation.Messages), len(observation.Runs), len(observation.Tools), err)
	}
	discovery := f.store.(session.SessionDiscoveryReader)
	page, err := discovery.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: ids.workspace, Limit: 10})
	if err != nil || len(page.Sessions) != 1 || page.Sessions[0].ID != createdSession.ID || page.Sessions[0].WorkspaceID != ids.workspace {
		t.Fatalf("wide discovery exact 1024 identity: sessions=%d err=%v", len(page.Sessions), err)
	}
	tooWide := session.ID(wideIdentity(1025, 99))
	if _, err := f.store.CreateSession(f.ctx, session.Session{ID: tooWide, WorkspaceID: "too-wide", CreatedAt: finished, UpdatedAt: finished}); err != nil {
		t.Fatalf("1025-byte session write: %v", err)
	}
	page, err = discovery.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: "too-wide"})
	if !errors.Is(err, session.ErrDiscoveryTooLarge) || !reflect.DeepEqual(page, session.SessionDiscoveryPage{}) {
		t.Fatalf("1025-byte discovery: sessions=%d cursor length=%d err=%v", len(page.Sessions), len(page.NextCursor), err)
	}
	mustExec(t, f.db, `INSERT INTO public.admission_receipts(session_key,admission_key,run_key,user_message_key,assistant_message_key,fingerprint_version,fingerprint,created_at)
SELECT s.row_key,$1,r.row_key,m.row_key,m.row_key,1,$2,$3
FROM public.sessions s
JOIN public.runs r ON r.session_key=s.row_key
JOIN public.messages m ON m.run_key=r.row_key
WHERE s.id=$4 AND r.id=$5 AND m.id=$6`, []byte("wide-admission"), make([]byte, 32), pgTime, []byte(createdSession.ID), []byte(run.ID), []byte(message.ID))
	assertPGIndexFootprints(t, f.db)
}
