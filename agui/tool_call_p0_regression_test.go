package agui

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
)

// toolCallTurnFixture is a fully durable, fully settled "assistant streams
// text, calls one tool, tool completes" turn, built through the exact
// store APIs (CreateToolCall/ClaimToolCall/SettleToolCall,
// session.ToolTransitionRecord under the hood) and runtime.BuildToolSettlement
// production uses -- not hand-typed JSON. Both regression tests below
// reuse this durable state and differ only in what they feed live to
// Bridge.Emit and in what order, since the durable content is identical
// either way; only the live streaming shape (whether text streamed before
// the assistant committed) changes which DeliveryMode the assistant's
// commit uses.
type toolCallTurnFixture struct {
	store                                session.Store
	sessionID                            session.ID
	runID                                session.RunID
	assistantID                          session.MessageID
	callID                               session.ToolCallID
	resultMessageID                      session.MessageID
	createEvent, claimEvent, settleEvent session.EventRecord
}

func buildToolCallTurnFixture(t *testing.T, sessionID session.ID, runID session.RunID) toolCallTurnFixture {
	t.Helper()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: runID, SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-" + string(runID), Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})

	const assistantID session.MessageID = "assistant-1"
	const callID session.ToolCallID = "call-1"
	const resultMessageID session.MessageID = "result-1"
	const resultPartID session.PartID = "result-part-1"

	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: assistantID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-1", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append assistant message: %v", err)
	}

	call := session.ToolCall{
		ID: callID, SessionID: sessionID, RunID: run.ID, MessageID: assistantID,
		RequestPartID: "request-part-1", ResultMessageID: resultMessageID, ResultPartID: resultPartID,
		Name: "search", RequestedName: "search", Pattern: "search", Input: json.RawMessage(`{"q":"eino"}`), Status: session.ToolCallPending,
	}
	// Both blocks are encoded through ONE EncodeContentParts call, exactly
	// as session.ContentFromAgenticMessage/persistAssistantTurn do for a
	// single assistant message with mixed text+tool-call content: ordinals
	// are assigned per call (0, 1, ...), so encoding the text and the
	// function_tool_call block through SEPARATE calls would mint two parts
	// both at ordinal 0 for the same message and fail decode validation.
	partIDs := []session.PartID{"part-text-1", call.RequestPartID}
	next := 0
	nextPartID := func() session.PartID { id := partIDs[next]; next++; return id }
	assistantParts, err := session.EncodeContentParts(session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "blk-text", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "let me check that"}},
			{ID: "blk-call", Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: string(callID), Name: call.Name, Arguments: string(call.Input)}},
		},
	}, nextPartID, assistantID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("encode assistant content parts: %v", err)
	}
	textPart, requestPart := assistantParts[0], assistantParts[1]

	// This mirrors runtime/tool_preparation.go's persistAssistantTurn: the
	// assistant's text part, its function_tool_call request part (via
	// CreateToolCall -- a generic AppendPart of that kind is rejected on
	// the fenced execution store), and FinalizeAssistantMessage all commit
	// together, atomically.
	var createResult session.ToolTransitionResult
	err = execution.WithinTx(ctx, func(ctx context.Context, store session.ExecutionStore) error {
		if _, err := store.AppendPart(ctx, textPart); err != nil {
			return err
		}
		var err error
		createResult, err = store.CreateToolCall(ctx, session.CreateToolCallRequest{
			Call: call, RequestPart: requestPart,
			Event: session.ToolTransitionEvent{ID: "evt-tool-pending", CreatedAt: now},
		})
		if err != nil {
			return err
		}
		return store.FinalizeAssistantMessage(ctx, assistantID)
	})
	if err != nil {
		t.Fatalf("persist assistant turn: %v", err)
	}

	claimedAt := now.Add(time.Millisecond)
	claimResult, err := execution.ClaimToolCall(ctx, session.ClaimToolCallRequest{
		ID: callID, ClaimedBy: "worker", ClaimToken: "claim-token-1", StartedAt: claimedAt, LeaseDuration: time.Minute,
		Event: session.ToolTransitionEvent{ID: "evt-tool-running", CreatedAt: claimedAt},
	})
	if err != nil {
		t.Fatalf("claim tool call: %v", err)
	}

	completedAt := claimedAt.Add(time.Millisecond)
	settlement, _, err := runtime.BuildToolSettlement(runtime.ToolSettlementInput{
		Tool:          runtime.Tool{Name: "search", Retention: runtime.RetentionPolicy{MaxInlineBytes: 4096}},
		Call:          runtime.ToolCall{ID: callID, SessionID: sessionID, RunID: run.ID, MessageID: assistantID, ResultMessageID: resultMessageID, ResultPartID: resultPartID, Name: "search"},
		Claimed:       claimResult.Call,
		Disposition:   runtime.ToolExecuted,
		Result:        runtime.ToolResult{Output: "3 results found"},
		CompletedAt:   completedAt,
		BlockID:       "blk-result",
		ContentLimits: session.DefaultContentLimits(),
	})
	if err != nil {
		t.Fatalf("build tool settlement: %v", err)
	}
	settleResult, err := execution.SettleToolCall(ctx, session.SettleToolCallRequest{
		Settlement: settlement, Event: session.ToolTransitionEvent{ID: "evt-tool-completed", CreatedAt: completedAt},
	})
	if err != nil {
		t.Fatalf("settle tool call: %v", err)
	}

	return toolCallTurnFixture{
		store: store, sessionID: sessionID, runID: run.ID, assistantID: assistantID, callID: callID, resultMessageID: resultMessageID,
		createEvent: createResult.Event, claimEvent: claimResult.Event, settleEvent: settleResult.Event,
	}
}

// TestBridgeDeliversFullStreamingTextThenToolCallTurn is the W7 fifth
// fix-pass review's mandated end-to-end proof for P0-1/P1-4: a REAL
// streaming turn -- assistant text streamed live, then a tool call
// requested in the SAME assistant message, claimed, executed, and settled
// -- driven through the real Bridge.Emit in the real runtime publish order
// (EventMessageDelta -> message_committed(assistant) -> tool_call_updated
// (pending) -> tool_call_updated(running) -> tool_call_updated(terminal)
// -> message_committed(result)), against a real SQLite store, using the
// SAME session.ToolTransitionRecord/runtime.BuildToolSettlement machinery
// production uses to build every event's payload (not hand-typed JSON).
//
// Before this fix pass's P0-1 fix, Bridge.emitToolCallUpdated's
// messageProjected(event.MessageID) guard fired on EVERY one of the three
// tool_call_updated events below, because event.MessageID for a
// tool_call_updated record is always the OWNING ASSISTANT message
// (session/tool_transition.go), and that message is always already
// projected by the time the first such event reaches Emit
// (runtime/tool_preparation.go publishes message_committed for the
// assistant BEFORE publishing its own tool transitions). The observed
// result on HEAD before this fix: TEXT_MESSAGE_START, TEXT_MESSAGE_CONTENT,
// CUSTOM, CUSTOM, TOOL_CALL_RESULT, CUSTOM -- no TOOL_CALL_START/ARGS/END,
// no TEXT_MESSAGE_END, and a TOOL_CALL_RESULT for a toolCallId the client
// never saw opened (a protocol-invalid sequence no validating AG-UI client
// could render).
func TestBridgeDeliversFullStreamingTextThenToolCallTurn(t *testing.T) {
	t.Parallel()

	fx := buildToolCallTurnFixture(t, "session-text-then-tool", "run-text-then-tool")
	ctx := context.Background()
	sink := newSSESink()
	bridge := NewBridge(ctx, fx.store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(fx.sessionID), string(fx.runID), nil)

	// The real runtime publish order: the delta streams live BEFORE the
	// message commits (runtime/adk_model.go), the assistant message
	// commits before any of its own tool transitions publish
	// (runtime/tool_preparation.go), and the terminal tool_call_updated
	// transition publishes strictly before the separate result message's
	// own commit notification (runtime/tool_execution.go's
	// persistToolSettlement).
	bridge.Emit(ctx, session.EventRecord{
		Kind: runtime.EventMessageDelta, SessionID: fx.sessionID, RunID: fx.runID, MessageID: fx.assistantID, Correlation: string(fx.assistantID),
		Payload: []byte(`{"content":"let me check that","reasoning":""}`),
	})
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: fx.sessionID, RunID: fx.runID, MessageID: fx.assistantID,
		TurnID: "turn-1", Payload: []byte(`{"revision":1}`),
	})
	bridge.Emit(ctx, fx.createEvent)
	bridge.Emit(ctx, fx.claimEvent)
	bridge.Emit(ctx, fx.settleEvent)
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: fx.sessionID, RunID: fx.runID, MessageID: fx.resultMessageID,
		TurnID: "turn-1", Payload: []byte(`{"revision":2}`),
	})
	bridge.Emit(ctx, session.EventRecord{Kind: runtime.EventRunFinished})

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
	// TEXT_MESSAGE_START/CONTENT: the live delta. CUSTOM,CUSTOM: the
	// assistant message's own committed-projection supplement for its two
	// content blocks (text, function_tool_call) -- DeliveryModeLiveContinuation
	// since text already streamed live, so no natives here (P1-4's
	// documented per-block/per-message granularity limit: the tool-call
	// block gets no native representation from THIS emission). TEXT_MESSAGE_END:
	// emitToolCallUpdated's own closeOpen, on the first tool_call_updated
	// event to reach Emit. TOOL_CALL_START/ARGS: from the pending
	// transition (Name+Arguments present); the running transition repeats
	// them but the connection-local nativeFrames ledger suppresses the duplicate.
	// TOOL_CALL_END,TOOL_CALL_RESULT: the terminal transition. Final CUSTOM:
	// the result message's own committed-projection supplement -- also
	// DeliveryModeLiveContinuation, because nativeFrames recognizes its one
	// function_tool_result block's call already got its
	// native TOOL_CALL_RESULT from the live path above, so no duplicate
	// native TOOL_CALL_RESULT here.
	want := "TEXT_MESSAGE_CHUNK,CUSTOM,CUSTOM,TOOL_CALL_START,TOOL_CALL_ARGS,TOOL_CALL_END,TOOL_CALL_RESULT,CUSTOM,RUN_FINISHED"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s", got, want)
	}

	// The TOOL_CALL_START/ARGS/END/RESULT frames must all name the same
	// call, and TOOL_CALL_RESULT must be for a call the client actually saw
	// opened -- the exact protocol-validity property P0-1 restores.
	startIdx, argsIdx, endIdx, resultIdx := 3, 4, 5, 6
	if id, _ := frames[startIdx]["toolCallId"].(string); id != string(fx.callID) {
		t.Fatalf("TOOL_CALL_START toolCallId = %q, want %q", id, fx.callID)
	}
	if name, _ := frames[startIdx]["toolCallName"].(string); name != "search" {
		t.Fatalf("TOOL_CALL_START toolCallName = %q, want search", name)
	}
	if id, _ := frames[argsIdx]["toolCallId"].(string); id != string(fx.callID) {
		t.Fatalf("TOOL_CALL_ARGS toolCallId = %q, want %q", id, fx.callID)
	}
	if id, _ := frames[endIdx]["toolCallId"].(string); id != string(fx.callID) {
		t.Fatalf("TOOL_CALL_END toolCallId = %q, want %q", id, fx.callID)
	}
	if id, _ := frames[resultIdx]["toolCallId"].(string); id != string(fx.callID) {
		t.Fatalf("TOOL_CALL_RESULT toolCallId = %q, want %q", id, fx.callID)
	}
	if content, _ := frames[resultIdx]["content"].(string); !strings.Contains(content, "3 results found") {
		t.Fatalf("TOOL_CALL_RESULT content = %q, want persisted output", content)
	}
}

// TestBridgeDeliversToolCallTurnWithNoPrecedingTextDelta proves the OTHER
// direction of the SAME P0-1/P1-4 fix: when the assistant message commits
// with NO text having streamed live first, its committed-projection
// emission uses DeliveryModeCommittedOnly and so DOES include a native
// TOOL_CALL_START/ARGS/END triple for the function_tool_call block
// (convert.CommittedNativeEvents, called for every block when mode !=
// DeliveryModeLiveContinuation). The live tool_call_updated events that
// follow must NOT repeat that triple -- this is what
// Bridge.nativeFrames exists to prevent -- while the terminal TOOL_CALL_RESULT, which the committed
// projection never sends for a function_tool_call block, must still reach
// the wire exactly once from the live path.
//
// This is the regression test for deleting the native-frame ledger's
// mutation: without it, this test would see TOOL_CALL_START/ARGS/END
// TWICE (once native from the commit, once from the live path).
func TestBridgeDeliversToolCallTurnWithNoPrecedingTextDelta(t *testing.T) {
	t.Parallel()

	fx := buildToolCallTurnFixture(t, "session-tool-no-text", "run-tool-no-text")
	ctx := context.Background()
	sink := newSSESink()
	bridge := NewBridge(ctx, fx.store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(fx.sessionID), string(fx.runID), nil)

	// No EventMessageDelta this time: the assistant message commits with
	// nothing streamed live for it on this connection.
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: fx.sessionID, RunID: fx.runID, MessageID: fx.assistantID,
		TurnID: "turn-1", Payload: []byte(`{"revision":1}`),
	})
	bridge.Emit(ctx, fx.createEvent)
	bridge.Emit(ctx, fx.claimEvent)
	bridge.Emit(ctx, fx.settleEvent)
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: fx.sessionID, RunID: fx.runID, MessageID: fx.resultMessageID,
		TurnID: "turn-1", Payload: []byte(`{"revision":2}`),
	})
	bridge.Emit(ctx, session.EventRecord{Kind: runtime.EventRunFinished})

	if err := bridge.Err(); err != nil {
		t.Fatalf("transport error = %v, want nil", err)
	}
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("encoding error = %v, want nil", err)
	}
	if err := bridge.LiveErr(); err != nil {
		t.Fatalf("live error = %v, want nil", err)
	}

	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	// TEXT_MESSAGE_START/CONTENT/END,CUSTOM: the assistant's text block,
	// delivered fully natively (DeliveryModeCommittedOnly). TOOL_CALL_START/
	// ARGS/END,CUSTOM: the assistant's function_tool_call block, ALSO
	// delivered fully natively by the SAME committed-projection emission --
	// this is the one native TOOL_CALL_START/ARGS/END triple in this
	// sequence; the three live tool_call_updated events that follow
	// contribute nothing to it (nativeFrames). TOOL_CALL_RESULT,
	// CUSTOM: the live terminal transition's own result (never sent by the
	// assistant's own projection) followed by the result message's
	// committed-projection supplement (DeliveryModeLiveContinuation, since
	// nativeFrames recognizes the result was already sent
	// live).
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,CUSTOM,TOOL_CALL_START,TOOL_CALL_ARGS,TOOL_CALL_END,CUSTOM,TOOL_CALL_RESULT,CUSTOM,RUN_FINISHED"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (exactly one native TOOL_CALL_START/ARGS/END triple, from the committed projection, plus exactly one TOOL_CALL_RESULT, from the live path)", got, want)
	}
	if content, _ := frames[8]["content"].(string); !strings.Contains(content, "3 results found") {
		t.Fatalf("TOOL_CALL_RESULT content = %q, want persisted output", content)
	}
}

// TestReplaySettledToolCallEmitsEachNativeFrameOnce exercises the real
// SQLite durable sweep: snapshots first project both settled messages, then
// the same durable transition records are forwarded through Bridge.Emit.
func TestReplaySettledToolCallEmitsEachNativeFrameOnce(t *testing.T) {
	t.Parallel()

	fx := buildToolCallTurnFixture(t, "session-replay-settled-tool", "run-replay-settled-tool")
	ctx := context.Background()
	sink := newSSESink()
	bridge := NewBridge(ctx, fx.store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(fx.sessionID), string(fx.runID), nil)
	if _, err := Replay(ctx, bridge, fx.store, fx.sessionID, session.EventCursor{Limit: 100}, session.ContentLimits{}, false); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if bridge.Err() != nil || bridge.EncErr() != nil || bridge.LiveErr() != nil {
		t.Fatalf("bridge errors: transport=%v encoding=%v live=%v", bridge.Err(), bridge.EncErr(), bridge.LiveErr())
	}

	frames := frameData(t, sink.Bytes())
	counts := map[string]int{}
	for _, frame := range frames {
		counts[frame["type"].(string)]++
	}
	for _, kind := range []string{"TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END", "TOOL_CALL_RESULT"} {
		if counts[kind] != 1 {
			t.Fatalf("%s count = %d, want 1; frames = %#v", kind, counts[kind], typesFromFrames(frames))
		}
	}
	assertFixtureToolLifecycle(t, frames, fx)
}

func TestReconnectTailOverlapDoesNotRepeatSettledToolLifecycle(t *testing.T) {
	t.Parallel()

	fx := buildToolCallTurnFixture(t, "session-reconnect-tool-overlap", "run-reconnect-tool-overlap")
	rawStore, ok := fx.store.(*sqlite.Store)
	if !ok {
		t.Fatalf("fixture store type = %T, want *sqlite.Store", fx.store)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &replaySweepDoneStore{Store: rawStore, sessionID: fx.sessionID, done: make(chan struct{})}
	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(fx.sessionID), string(fx.runID), nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, fx.sessionID, session.EventCursor{Limit: 100}, session.ContentLimits{}, false)
		done <- err
	}()
	<-tail.subscribed
	select {
	case <-store.done:
	case <-time.After(5 * time.Second):
		t.Fatal("replay sweep did not finish")
	}
	overlap := fx.settleEvent
	overlap.ID = "tail-overlap-terminal"
	tail.events <- overlap
	close(tail.events)
	if err := <-done; err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	frames := frameData(t, sink.Bytes())
	counts := map[string]int{}
	for _, frame := range frames {
		counts[frame["type"].(string)]++
	}
	for _, kind := range []string{"TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END", "TOOL_CALL_RESULT"} {
		if counts[kind] != 1 {
			t.Fatalf("%s count = %d, want one across replay/tail overlap", kind, counts[kind])
		}
	}
	assertFixtureToolLifecycle(t, frames, fx)
}

func assertFixtureToolLifecycle(t *testing.T, frames []map[string]any, fx toolCallTurnFixture) {
	t.Helper()
	matching := map[string][]map[string]any{}
	for _, frame := range frames {
		kind, _ := frame["type"].(string)
		if frame["toolCallId"] == string(fx.callID) {
			matching[kind] = append(matching[kind], frame)
		}
	}
	for _, kind := range []string{"TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END", "TOOL_CALL_RESULT"} {
		if got := len(matching[kind]); got != 1 {
			t.Fatalf("%s frames for fixture call = %d, want 1; frames = %#v", kind, got, frames)
		}
	}
	if name, _ := matching["TOOL_CALL_START"][0]["toolCallName"].(string); name != "search" {
		t.Fatalf("TOOL_CALL_START toolCallName = %q, want search", name)
	}
	if delta, _ := matching["TOOL_CALL_ARGS"][0]["delta"].(string); delta != `{"q":"eino"}` {
		t.Fatalf("TOOL_CALL_ARGS delta = %q, want fixture input", delta)
	}
	if content, _ := matching["TOOL_CALL_RESULT"][0]["content"].(string); !strings.Contains(content, "3 results found") {
		t.Fatalf("TOOL_CALL_RESULT content = %q, want persisted output", content)
	}
}
