package agui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	aguievents "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/events"
	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestBridgeEmitsFullSurfaceGolden(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
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
		Payload:    []byte(`{"name":"search","arguments":{"q":"eino"},"status":"completed","content":"result"}`),
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
