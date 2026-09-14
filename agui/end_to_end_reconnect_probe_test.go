package agui

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
)

// replaySweepDoneStore wraps a real *sqlite.Store and closes a channel the
// first time ListEvents is called for sessionID -- signaling that
// Reconnect's own internal replay() sweep (which calls ListEvents exactly
// once, before entering its live-tail loop) has returned. Tests use this
// to sequence "durably write MORE content" strictly AFTER the initial
// replay has already run and seen only what existed at that point, so a
// later durable write is delivered ONLY via the live tail, never doubled
// by also being picked up by the (already-completed) replay sweep.
type replaySweepDoneStore struct {
	*sqlite.Store
	sessionID session.ID
	done      chan struct{}
	fired     bool
}

func (s *replaySweepDoneStore) ListEvents(ctx context.Context, sessionID session.ID, cursor session.EventCursor) (session.EventBatch, error) {
	batch, err := s.Store.ListEvents(ctx, sessionID, cursor)
	if !s.fired && sessionID == s.sessionID && cursor.AfterEventID == "" {
		s.fired = true
		close(s.done)
	}
	return batch, err
}

// TestReconnectDeliversRealisticUserTextToolCallTurn is the end-to-end
// probe required before closing out the W7 fifth fix-pass review: a
// realistic conversation -- an already-durable user message, then a
// streamed assistant turn that calls one tool and gets a result, then
// RUN_FINISHED -- driven through the actual `Reconnect` entry point (a
// real live tail subscription plus a real durable replay sweep), against
// a real SQLite store, exactly the way transport.SSEHandler drives it.
//
// The assistant's turn (message, tool call, claim, settlement) is
// deliberately created AFTER Reconnect's own initial replay sweep has
// already run (synchronized via replaySweepDoneStore): CreateToolCall/
// ClaimToolCall/SettleToolCall durably persist BOTH the tool call's
// content AND its tool_call_updated event atomically (by design -- the
// same durability CreateToolCallRequest's own doc comment describes), so
// creating them before Reconnect starts would make its own replay sweep
// (which forwards every non-LiveOnly durable event, including
// tool_call_updated) deliver them once, independent of and in addition to
// this probe's own live-tail feed -- a double delivery that would be an
// artifact of this probe's construction, not of the bridge under test.
// Sequencing them after the sweep completes means the assistant turn
// below is genuinely LIVE-ONLY from this connection's perspective, the
// same shape a real streaming turn has for a client that reconnected
// moments before it started.
func TestReconnectDeliversRealisticUserTextToolCallTurn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeCtx := context.Background()
	rawStore, storePool, err := openTestSQLite(storeCtx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-e2e-reconnect"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := rawStore.CreateSession(storeCtx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := rawStore.AdmitRun(storeCtx, session.Run{ID: "run-e2e-reconnect", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-e2e-reconnect", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := rawStore.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})

	// A real prior turn: the user's own durable message, already committed
	// before this connection reconnects -- this, and only this, is what
	// Reconnect's own initial replay sweep will see.
	const userID session.MessageID = "user-1"
	if _, err := execution.AppendMessage(storeCtx, session.Message{
		ID: userID, SessionID: sessionID, RunID: run.ID, Role: session.RoleUser, TurnID: "turn-1", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append user message: %v", err)
	}
	userParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "blk-user", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "what's the weather in nyc?"}}},
	}, func() session.PartID { return "part-user-1" }, userID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("encode user part: %v", err)
	}
	if _, err := execution.AppendPart(storeCtx, userParts[0]); err != nil {
		t.Fatalf("append user part: %v", err)
	}

	store := &replaySweepDoneStore{Store: rawStore, sessionID: sessionID, done: make(chan struct{})}
	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
		done <- err
	}()
	<-tail.subscribed

	const timeout = 5 * time.Second
	select {
	case <-store.done:
	case err := <-done:
		t.Fatalf("Reconnect returned (err = %v) before its own initial replay sweep completed", err)
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for Reconnect's initial replay sweep to complete")
	}

	// NOW the assistant's turn happens, live: text + one tool call,
	// claimed and settled, using the exact production store APIs.
	const assistantID session.MessageID = "assistant-1"
	const callID session.ToolCallID = "call-1"
	const resultMessageID session.MessageID = "result-1"
	const resultPartID session.PartID = "result-part-1"
	assistantAt := now.Add(time.Second)
	if _, err := execution.AppendMessage(storeCtx, session.Message{
		ID: assistantID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-1", CreatedAt: assistantAt, UpdatedAt: assistantAt,
	}); err != nil {
		t.Fatalf("append assistant message: %v", err)
	}
	call := session.ToolCall{
		ID: callID, SessionID: sessionID, RunID: run.ID, MessageID: assistantID,
		RequestPartID: "request-part-1", ResultMessageID: resultMessageID, ResultPartID: resultPartID,
		Name: "get_weather", RequestedName: "get_weather", Pattern: "get_weather", Input: json.RawMessage(`{"city":"nyc"}`), Status: session.ToolCallPending,
	}
	partIDs := []session.PartID{"part-text-1", call.RequestPartID}
	next := 0
	nextPartID := func() session.PartID { id := partIDs[next]; next++; return id }
	assistantParts, err := session.EncodeContentParts(session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "blk-text", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "let me check the weather"}},
			{ID: "blk-call", Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: string(callID), Name: call.Name, Arguments: string(call.Input)}},
		},
	}, nextPartID, assistantID, sessionID, run.ID, assistantAt, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("encode assistant content parts: %v", err)
	}
	textPart, requestPart := assistantParts[0], assistantParts[1]

	var createResult session.ToolTransitionResult
	err = execution.WithinTx(storeCtx, func(ctx context.Context, store session.ExecutionStore) error {
		if _, err := store.AppendPart(ctx, textPart); err != nil {
			return err
		}
		var err error
		createResult, err = store.CreateToolCall(ctx, session.CreateToolCallRequest{
			Call: call, RequestPart: requestPart,
			Event: session.ToolTransitionEvent{ID: "evt-tool-pending", CreatedAt: assistantAt},
		})
		if err != nil {
			return err
		}
		return store.FinalizeAssistantMessage(ctx, assistantID)
	})
	if err != nil {
		t.Fatalf("persist assistant turn: %v", err)
	}

	claimedAt := assistantAt.Add(time.Millisecond)
	claimResult, err := execution.ClaimToolCall(storeCtx, session.ClaimToolCallRequest{
		ID: callID, ClaimedBy: "worker", ClaimToken: "claim-token-1", StartedAt: claimedAt, LeaseDuration: time.Minute,
		Event: session.ToolTransitionEvent{ID: "evt-tool-running", CreatedAt: claimedAt},
	})
	if err != nil {
		t.Fatalf("claim tool call: %v", err)
	}
	completedAt := claimedAt.Add(time.Millisecond)
	settlement, _, err := runtime.BuildToolSettlement(runtime.ToolSettlementInput{
		Tool:          runtime.Tool{Name: "get_weather", Retention: runtime.RetentionPolicy{MaxInlineBytes: 4096}},
		Call:          runtime.ToolCall{ID: callID, SessionID: sessionID, RunID: run.ID, MessageID: assistantID, ResultMessageID: resultMessageID, ResultPartID: resultPartID, Name: "get_weather"},
		Claimed:       claimResult.Call,
		Disposition:   runtime.ToolExecuted,
		Result:        runtime.ToolResult{Output: "68F and sunny in NYC"},
		CompletedAt:   completedAt,
		BlockID:       "blk-result",
		ContentLimits: session.DefaultContentLimits(),
	})
	if err != nil {
		t.Fatalf("build tool settlement: %v", err)
	}
	settleResult, err := execution.SettleToolCall(storeCtx, session.SettleToolCallRequest{
		Settlement: settlement, Event: session.ToolTransitionEvent{ID: "evt-tool-completed", CreatedAt: completedAt},
	})
	if err != nil {
		t.Fatalf("settle tool call: %v", err)
	}

	// The runtime publish order: text streams live before the assistant
	// message commits; the assistant commits before its own tool
	// transitions publish; the terminal transition publishes before the
	// separate result message's own commit notification.
	sendOrTimeout := func(event session.EventRecord) {
		t.Helper()
		select {
		case tail.events <- event:
		case err := <-done:
			t.Fatalf("Reconnect returned (err = %v) before the probe finished delivering live events", err)
		case <-time.After(timeout):
			t.Fatalf("timed out sending event %s to the live tail", event.ID)
		}
	}
	sendOrTimeout(session.EventRecord{
		Kind: runtime.EventMessageDelta, SessionID: sessionID, RunID: run.ID, MessageID: assistantID,
		Payload: []byte(`{"content":"let me check the weather","reasoning":""}`),
	})
	sendOrTimeout(session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: sessionID, RunID: run.ID, MessageID: assistantID,
		TurnID: "turn-1", Payload: []byte(`{"revision":1}`),
	})
	sendOrTimeout(createResult.Event)
	sendOrTimeout(claimResult.Event)
	sendOrTimeout(settleResult.Event)
	sendOrTimeout(session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: sessionID, RunID: run.ID, MessageID: resultMessageID,
		TurnID: "turn-1", Payload: []byte(`{"revision":2}`),
	})
	sendOrTimeout(session.EventRecord{Kind: runtime.EventRunFinished, ID: "evt-e2e-finished", SessionID: sessionID})
	close(tail.events)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reconnect err = %v, want nil", err)
		}
	case <-time.After(timeout):
		t.Fatalf("Reconnect did not return after the live tail closed")
	}
	if err := bridge.Err(); err != nil {
		t.Fatalf("transport error = %v, want nil", err)
	}
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("encoding error = %v, want nil", err)
	}
	if err := bridge.LiveErr(); err != nil {
		t.Fatalf("live error = %v, want nil", err)
	}
	if got := bridge.BenignCommitMisses(); got != 0 {
		t.Fatalf("BenignCommitMisses() = %d, want 0", got)
	}

	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	// CUSTOM: the durable user message's own committed-projection
	// supplement from Reconnect's initial replay sweep (user-role content
	// has no native AG-UI representation on this path -- see
	// docs/consumer-guide.md). Then the live assistant turn: text delta,
	// the assistant's own committed-projection supplement for its two
	// blocks (DeliveryModeLiveContinuation, text already streamed live so
	// no natives here), the tool-call lifecycle from the live transitions,
	// its result, and the result message's own supplement (also
	// LiveContinuation, since its result was already delivered live) --
	// then RUN_FINISHED. A real AG-UI client renders this as: the prior
	// user turn (via the custom envelope), the assistant's text streaming
	// in, then a tool call opening, receiving arguments, closing, and
	// producing a result -- exactly the shape a chat UI expects.
	want := "CUSTOM,TEXT_MESSAGE_CHUNK,CUSTOM,CUSTOM,TOOL_CALL_START,TOOL_CALL_ARGS,TOOL_CALL_END,TOOL_CALL_RESULT,CUSTOM,RUN_FINISHED"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s", got, want)
	}
	t.Logf("end-to-end Reconnect frame sequence: %v", got)
}
