package agui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	aguievents "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/events"
	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
)

func TestBridgeEmitsFullSurfaceGolden(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	_ = bridge.Emit(context.Background(), runtime.Event{Kind: runtime.EventRunStarted})
	_ = bridge.Emit(context.Background(), runtime.Event{
		Kind:      runtime.EventMessageDelta,
		MessageID: "assistant-1",
		Payload:   []byte(`{"reasoning":"thinking","content":"hello"}`),
	})
	_ = bridge.Emit(context.Background(), runtime.Event{
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
	_ = bridge.Emit(context.Background(), runtime.Event{Kind: runtime.EventRunFinished})

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
