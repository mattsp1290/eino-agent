package agui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	aguievents "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/events"
	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agui/convert"
	aguiemitter "github.com/mattsp1290/eino-agui/emitter"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestBridgeEmitsFullSurfaceGolden(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	// includeReasoning is true here specifically so this "full surface"
	// golden still exercises REASONING_* frames on the live delta path
	// (see TestEmitMessageDeltaGatesReasoningOnIncludeReasoning for the
	// includeReasoning=false direction, which this golden does not cover).
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, true, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.Emit(context.Background(), session.EventRecord{Kind: runtime.EventRunStarted})
	bridge.Emit(context.Background(), session.EventRecord{
		Kind:      runtime.EventMessageDelta,
		MessageID: "assistant-1",
		Payload:   []byte(`{"reasoning":"thinking","content":"hello"}`),
	})
	bridge.Emit(context.Background(), session.EventRecord{
		Kind:       runtime.EventToolCallUpdated,
		MessageID:  "tool-message-1",
		ToolCallID: "tool-1",
		Payload:    []byte(`{"name":"search","arguments":{"q":"eino"},"status":"completed","output":"result"}`),
	})
	bridge.StateSnapshot(map[string]any{"status": "working"})
	bridge.StateDelta([]aguievents.JSONPatchOperation{{Op: "replace", Path: "/status", Value: "done"}})
	bridge.MessagesSnapshot([]*einoschema.Message{
		einoschema.UserMessage("hello"),
		einoschema.AssistantMessage("world", nil),
	})
	bridge.ActivitySnapshot("assistant-1", "tool", map[string]any{"name": "search"})
	bridge.ActivityDelta("assistant-1", "tool", []aguievents.JSONPatchOperation{{Op: "add", Path: "/done", Value: true}})
	bridge.StepStarted("model")
	bridge.StepFinished("model")
	bridge.Custom("agent_note", map[string]any{"ok": true})
	bridge.ReasoningEncryptedValue(aguievents.ReasoningEncryptedValueSubtypeMessage, "assistant-1", "ciphertext")
	bridge.Emit(context.Background(), session.EventRecord{Kind: runtime.EventRunFinished})

	frames := frameData(t, sink.Bytes())
	fixture := readGolden(t, "../testdata/agui/full_surface_events.json")
	if got := typesFromFrames(frames); !reflect.DeepEqual(got, fixture.EventTypes) {
		t.Fatalf("event types = %#v, want %#v", got, fixture.EventTypes)
	}
	for _, assertion := range fixture.Assertions {
		got := frames[assertion.Index][assertion.Field]
		if !reflect.DeepEqual(got, assertion.Value) {
			t.Fatalf("frame %d field %s = %#v, want %#v", assertion.Index, assertion.Field, got, assertion.Value)
		}
	}
	if bridge.Err() != nil || bridge.EncErr() != nil {
		t.Fatalf("bridge errors: transport=%v encoding=%v", bridge.Err(), bridge.EncErr())
	}
}

func TestToolPayloadResultContentUsesDurableOutputContract(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		payload toolPayload
		want    string
	}{
		{name: "string", payload: toolPayload{Output: json.RawMessage(`"result"`)}, want: "result"},
		{name: "object", payload: toolPayload{Output: json.RawMessage(` { "n" : 1 } `)}, want: `{"n":1}`},
		{name: "array", payload: toolPayload{Output: json.RawMessage(`[1,2]`)}, want: `[1,2]`},
		{name: "number", payload: toolPayload{Output: json.RawMessage(`3`)}, want: "3"},
		{name: "null fallback", payload: toolPayload{Output: json.RawMessage(`null`), Error: "failed", Status: "failed"}, want: `{"error":"failed","status":"failed"}`},
		{name: "whitespace string fallback", payload: toolPayload{Output: json.RawMessage(`"   "`), Error: "failed", Status: "failed"}, want: `{"error":"failed","status":"failed"}`},
		{name: "absent error fallback", payload: toolPayload{Error: "failed", Status: "failed"}, want: `{"error":"failed","status":"failed"}`},
		{name: "absent status", payload: toolPayload{Status: "completed"}, want: `{"status":"completed"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.payload.ResultContent(); got != test.want {
				t.Fatalf("ResultContent() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestToolTransitionRecordOutputConversion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		output json.RawMessage
		error  string
		want   string
	}{
		{name: "string", output: json.RawMessage(`"result"`), want: "result"},
		{name: "object", output: json.RawMessage(`{"n":1}`), want: `{"n":1}`},
		{name: "null fallback", output: json.RawMessage(`null`), error: "failed", want: `{"error":"failed","status":"failed"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			record, err := session.ToolTransitionRecord(session.ToolCall{
				ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "assistant-1", Name: "search",
				Status: session.ToolCallFailed, ClaimedBy: "worker", ClaimToken: "claim", CompletedAt: now,
				Output: test.output, Error: test.error,
			}, session.ToolTransitionEvent{ID: "event-1", CreatedAt: now})
			if err != nil {
				t.Fatalf("ToolTransitionRecord() error = %v", err)
			}
			var payload toolPayload
			if err := json.Unmarshal(record.Payload, &payload); err != nil {
				t.Fatalf("unmarshal durable payload: %v", err)
			}
			if got := payload.ResultContent(); got != test.want {
				t.Fatalf("ResultContent() = %q, want %q", got, test.want)
			}
		})
	}
}

type alwaysFailWriter struct{}

func (alwaysFailWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestBridgeRetriesNativeSpanAfterFailedWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sink := newSSESink()
	bridge := NewBridge(ctx, nil, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.emit = aguiemitter.NewEmitter(ctx, bufio.NewWriter(alwaysFailWriter{}), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	event := session.EventRecord{Kind: runtime.EventMessageDelta, MessageID: "assistant-1", Payload: []byte(`{"content":"retry me"}`)}
	bridge.Emit(ctx, event)
	if bridge.textOpen[event.MessageID] || bridge.nativeAlreadyStreamed(event.MessageID) {
		t.Fatalf("failed start advanced bridge state: textOpen=%v nativeStreamed=%v", bridge.textOpen, bridge.nativeAlreadyStreamed(event.MessageID))
	}
	if bridge.nativeDelivered(nativeFrameKey{kind: aguievents.EventTypeTextMessageStart, ownerID: string(event.MessageID)}) {
		t.Fatal("failed start was registered in native ledger")
	}

	bridge.emit = aguiemitter.NewEmitter(ctx, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.Emit(ctx, event)
	frames := frameData(t, sink.Bytes())
	if got := stringsJoined(typesFromFrames(frames)); got != "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT" {
		t.Fatalf("retry frames = %s, want complete start/content span", got)
	}
	bridge.emit = aguiemitter.NewEmitter(ctx, bufio.NewWriter(alwaysFailWriter{}), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.closeOpen(bridge.emit)
	if !bridge.textOpen[event.MessageID] {
		t.Fatal("failed close discarded open text span")
	}
	bridge.emit = aguiemitter.NewEmitter(ctx, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.closeOpen(bridge.emit)
	frames = frameData(t, sink.Bytes())
	if got := stringsJoined(typesFromFrames(frames)); got != "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END" {
		t.Fatalf("retry close frames = %s, want complete span", got)
	}
}

func TestProjectedNativeFrameKeysExcludeMultipartToolResult(t *testing.T) {
	t.Parallel()

	bridge := &Bridge{}
	projection := &convert.AgenticProjection{Public: &convert.PublicAgenticMessage{ContentBlocks: []convert.PublicContentBlock{{
		Type:     einoschema.ContentBlockTypeFunctionToolResult,
		Identity: convert.AgenticIdentityV1{MessageID: "result-1", CallID: "call-1"},
		FunctionToolResult: &convert.PublicFunctionToolResult{CallID: "call-1", Name: "search", Content: []convert.PublicFunctionResultPart{
			{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: "text"},
			{Type: einoschema.FunctionToolResultContentBlockTypeImage, Media: &convert.PublicMedia{URL: "https://example.test/result.png"}},
		}},
	}}}}
	if keys := bridge.projectedNativeFrameKeys(projection); len(keys) != 0 {
		t.Fatalf("multipart result registered native keys %#v, want none", keys)
	}
}

func TestProjectedNativeFrameKeysKeepDistinctBlockIDs(t *testing.T) {
	t.Parallel()

	first, second := "first", "second"
	projection := &convert.AgenticProjection{Public: &convert.PublicAgenticMessage{ContentBlocks: []convert.PublicContentBlock{
		{Type: einoschema.ContentBlockTypeAssistantGenText, Identity: convert.AgenticIdentityV1{MessageID: "assistant-1", BlockID: "block-1"}, Text: &first},
		{Type: einoschema.ContentBlockTypeAssistantGenText, Identity: convert.AgenticIdentityV1{MessageID: "assistant-1", BlockID: "block-2"}, Text: &second},
	}}}
	keys := (&Bridge{}).projectedNativeFrameKeys(projection)
	if len(keys) != 6 {
		t.Fatalf("projected native keys = %d, want 6", len(keys))
	}
	unique := map[nativeFrameKey]bool{}
	for _, key := range keys {
		unique[key] = true
	}
	if len(unique) != len(keys) {
		t.Fatalf("projection collapsed block-identical frames: %#v", keys)
	}
}

func TestBridgeNativeLedgerKeepsDistinctDeltasAndCalls(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	for range 2 {
		bridge.Emit(context.Background(), session.EventRecord{Kind: runtime.EventMessageDelta, MessageID: "assistant-1", Payload: []byte(`{"content":"same"}`)})
	}
	for _, id := range []string{"call-1", "call-2"} {
		bridge.Emit(context.Background(), session.EventRecord{Kind: runtime.EventToolCallUpdated, MessageID: "assistant-1", ToolCallID: session.ToolCallID(id), Payload: []byte(`{"name":"search","arguments":{"q":"eino"},"status":"completed","output":"ok"}`)})
	}

	frames := frameData(t, sink.Bytes())
	counts := map[string]int{}
	for _, frame := range frames {
		counts[frame["type"].(string)]++
	}
	if counts["TEXT_MESSAGE_CONTENT"] != 2 {
		t.Fatalf("TEXT_MESSAGE_CONTENT count = %d, want 2", counts["TEXT_MESSAGE_CONTENT"])
	}
	for _, kind := range []string{"TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END", "TOOL_CALL_RESULT"} {
		if counts[kind] != 2 {
			t.Fatalf("%s count = %d, want 2", kind, counts[kind])
		}
	}
}

// TestBridgeSurfacesLiveCommittedProjectionFailures proves the W7 review's
// A6 finding: emitLiveMessageCommitted must not silently swallow a
// loadCommittedProjections failure. Before this fix, a
// session.MessageCommittedEventKind event whose reprojection failed (here:
// content encoded under the store's normal limits, but decoded under a
// bridge configured with far smaller ContentLimits) produced no output and
// no observable error anywhere -- a silently truncated stream a host had no
// way to detect.
func TestBridgeSurfacesLiveCommittedProjectionFailures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-live-err"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-live-err", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-live-err", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	const messageID session.MessageID = "assistant-live-err"
	if _, err := execution.AppendMessage(ctx, session.Message{ID: messageID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-live-err", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	// Encoded under the store's normal (large) default limits...
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "this text is longer than one byte"}}},
	}, func() session.PartID { return "part-live-err" }, messageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, parts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	sink := newSSESink()
	// ...but the bridge is configured with a MaxBlockBytes far too small to
	// decode it back, forcing loadCommittedProjections to fail inside
	// emitLiveMessageCommitted.
	tinyLimits := session.ContentLimits{MaxMessageBytes: 1024, MaxBlocks: 4, MaxBlockBytes: 1}
	bridge := NewBridge(ctx, store, tinyLimits, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		TurnID: "turn-live-err", Payload: []byte(`{"revision":1}`),
	})
	if bridge.LiveErr() == nil {
		t.Fatalf("LiveErr() = nil, want a surfaced reprojection failure instead of a silently truncated stream")
	}
	if len(sink.Bytes()) != 0 {
		t.Fatalf("expected no output for the failed reprojection, got: %s", sink.Bytes())
	}
}

// TestEmitLiveMessageCommittedRetriesAfterFailedEmission proves the W7
// fix-pass review's I3/C8 finding: emitLiveMessageCommitted's nine-line
// comment (agui/bridge.go, above the EmitCommittedProjection call) promises
// that b.markMessageProjected(id) is called only after a projection
// attempt actually SUCCEEDS, so a transient failure leaves the message
// eligible for a later retry instead of being permanently skipped.
// Mutation M2 (making the mark unconditional) leaves the rest of the suite
// green, so this test exercises the real emitLiveMessageCommitted code path
// (via bridge.Emit, not a hand-rolled call sequence) end to end.
//
// The transient failure is a genuine eino-agui emitter encoding rejection
// (allowAgenticReceipt's "commit receipt has already been emitted"), not a
// transport error: a transport error latches Bridge.Err() permanently by
// design (the connection itself is broken), so it cannot demonstrate a
// RECOVERABLE failure. To produce a receipt collision deliberately, this
// test pre-emits the exact projection+receipt loadCommittedProjections
// will independently recompute for the target message (same durable
// content, same observation revision) directly through
// bridge.EmitCommittedProjection, WITHOUT calling markMessageProjected --
// exactly the state a genuinely failed live emission would leave behind.
// The first message_committed dispatch for that message then collides on
// the identical receipt and must fail without marking the message
// projected; a second durable write (an unrelated message) advances the
// session's observation-watermark revision (session.ObservationReader),
// changing the receipt the second message_committed dispatch computes, so
// it no longer collides and must succeed.
func TestEmitLiveMessageCommittedRetriesAfterFailedEmission(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-retry-projection"
	const messageID session.MessageID = "assistant-retry-projection"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-retry-projection", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-retry-projection", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: messageID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-retry-projection", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "retry content"}}},
	}, func() session.PartID { return "part-retry-projection" }, messageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, parts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)

	// Precompute the exact projection+receipt loadCommittedProjections will
	// independently recompute below for this message at the CURRENT
	// revision, and pre-emit it directly (bypassing emitLiveMessageCommitted
	// entirely, so Bridge.projectedMessages is untouched). This poisons the
	// emitter's own receipt-dedup state exactly as a prior failed emission
	// attempt would have -- an already-emitted receipt, but a message the
	// Bridge itself has not recorded as delivered.
	projections, err := loadCommittedProjections(ctx, store, sessionID, session.ContentLimits{}, false)
	if err != nil {
		t.Fatalf("loadCommittedProjections: %v", err)
	}
	if len(projections) != 1 || projections[0].MessageID != messageID {
		t.Fatalf("projections = %#v, want exactly one for %s", projections, messageID)
	}
	if !bridge.EmitCommittedProjection(projections[0].Projection, projections[0].Receipt, aguiemitter.DeliveryModeLiveContinuation) {
		t.Fatalf("pre-emission of the poisoning receipt failed: Err=%v EncErr=%v", bridge.Err(), bridge.EncErr())
	}
	sink.Bytes() // drain/flush the poisoning emission so it doesn't pollute the assertions below.

	// First live message_committed: loadCommittedProjections recomputes the
	// IDENTICAL receipt (same revision, same content), so
	// EmitCommittedProjection must collide and fail.
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		TurnID: "turn-retry-projection", Payload: []byte(`{"revision":1}`),
	})
	if bridge.messageProjected(messageID) {
		t.Fatalf("messageProjected(%s) = true after a FAILED emission, want false (a transient failure must not permanently mark the message delivered)", messageID)
	}
	if bridge.LiveErr() != nil {
		t.Fatalf("LiveErr() = %v, want nil (EmitCommittedProjection's own failure surfaces through EncErr, not LiveErr)", bridge.LiveErr())
	}
	if bridge.EncErr() == nil {
		t.Fatalf("EncErr() = nil, want the receipt-collision encoding error from the first (poisoned) attempt")
	}

	// Advance the session's observation-watermark revision with an
	// unrelated durable write (session.ObservationReader; store/sqlite's
	// observation_revisions triggers bump it on any messages/parts
	// insert), so the second attempt's receipt genuinely differs.
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-unrelated", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-unrelated", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append unrelated message: %v", err)
	}

	// Second live message_committed for the SAME message ID: must be
	// retried (messageProjected was never set), and this time the receipt
	// no longer collides, so the projection must actually be delivered.
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		TurnID: "turn-retry-projection", Payload: []byte(`{"revision":2}`),
	})
	if !bridge.messageProjected(messageID) {
		t.Fatalf("messageProjected(%s) = false after the retried emission succeeded, want true", messageID)
	}
	raw := string(sink.Bytes())
	if !strings.Contains(raw, "retry content") {
		t.Fatalf("retried emission did not reach the wire: %s", raw)
	}
}

// TestEmitMessageDeltaGatesReasoningOnIncludeReasoning proves the W7
// fix-pass review's P1-D finding (fix-verification-reviewer I3): the live
// EventMessageDelta path (emitMessageDelta) must honor includeReasoning
// exactly like the durable committed-projection path already does
// (emitMessageSnapshot/emitLiveMessageCommitted), not stream live
// reasoning deltas to a host that has never attested
// agui.GateProviderReasoningStorage is satisfied. Before this fix,
// emitMessageDelta emitted REASONING_START/REASONING_MESSAGE_START/
// REASONING_MESSAGE_CONTENT/REASONING_MESSAGE_END/REASONING_END
// unconditionally, so SSEConfig{IncludeReasoning: false} still leaked live
// reasoning over the exact same handler that correctly withheld it from
// replay.
func TestEmitMessageDeltaGatesReasoningOnIncludeReasoning(t *testing.T) {
	t.Parallel()

	emit := func(includeReasoning bool) []byte {
		sink := newSSESink()
		bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, includeReasoning, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
		bridge.Emit(context.Background(), session.EventRecord{
			Kind:      runtime.EventMessageDelta,
			MessageID: "assistant-1",
			Payload:   []byte(`{"reasoning":"thinking","content":"hello"}`),
		})
		return sink.Bytes()
	}

	if raw := emit(false); strings.Contains(string(raw), "REASONING") || strings.Contains(string(raw), "thinking") {
		t.Fatalf("live reasoning delta leaked with includeReasoning=false: %s", raw)
	}
	if raw := emit(true); !strings.Contains(string(raw), "thinking") {
		t.Fatalf("live reasoning delta missing with includeReasoning=true: %s", raw)
	}
}

func TestBridgeImplementsRuntimeEventSink(t *testing.T) {
	t.Parallel()

	var _ runtime.EventSink = (*Bridge)(nil)
}

type sseSink struct {
	buffer bytes.Buffer
	writer *bufio.Writer
}

func newSSESink() *sseSink {
	s := &sseSink{}
	s.writer = bufio.NewWriter(&s.buffer)
	return s
}

func (s *sseSink) Writer() *bufio.Writer { return s.writer }

func (s *sseSink) Bytes() []byte {
	_ = s.writer.Flush()
	return s.buffer.Bytes()
}

func frameData(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	frames := strings.Split(string(raw), "\n\n")
	result := make([]map[string]any, 0, len(frames))
	for _, frame := range frames {
		frame = strings.TrimSpace(frame)
		if frame == "" {
			continue
		}
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var data map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
				t.Fatalf("decode frame %q: %v", frame, err)
			}
			result = append(result, data)
		}
	}
	return result
}

func typesFromFrames(frames []map[string]any) []string {
	result := make([]string, 0, len(frames))
	for _, frame := range frames {
		result = append(result, frame["type"].(string))
	}
	return result
}

func readGolden(t *testing.T, path string) aguiFixture {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var fixture aguiFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return fixture
}

type aguiFixture struct {
	EventTypes []string         `json:"event_types"`
	Assertions []frameAssertion `json:"assertions"`
}

type frameAssertion struct {
	Index int    `json:"index"`
	Field string `json:"field"`
	Value any    `json:"value"`
}
