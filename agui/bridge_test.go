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
	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
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

func TestBridgeEmitsModelFallbackCustomEvent(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	event := runtime.NewModelFallbackEvent("gpt-primary", "gpt-fallback", "circuit_breaker")
	if err := bridge.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit error = %v", err)
	}

	frames := frameData(t, sink.Bytes())
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	frame := frames[0]
	if frame["type"] != "CUSTOM" {
		t.Fatalf("frame type = %v, want CUSTOM", frame["type"])
	}
	if frame["name"] != "model_fallback_engaged" {
		t.Fatalf("frame name = %v, want model_fallback_engaged", frame["name"])
	}
	value, ok := frame["value"].(map[string]any)
	if !ok {
		t.Fatalf("frame value = %#v, want map", frame["value"])
	}
	if value["from_model_id"] != "gpt-primary" || value["to_model_id"] != "gpt-fallback" {
		t.Fatalf("value model transition = %#v", value)
	}
	if value["reason"] != "circuit_breaker" {
		t.Fatalf("value reason = %v, want circuit_breaker", value["reason"])
	}
	// The custom value mirrors the durable payload's omitempty shape: empty
	// optional keys are absent, not present as "".
	for _, key := range []string{"from_provider_id", "to_provider_id"} {
		if _, present := value[key]; present {
			t.Fatalf("custom value should omit empty %q, got %#v", key, value)
		}
	}
	if bridge.Err() != nil {
		t.Fatalf("bridge error: %v", bridge.Err())
	}
}

func TestBridgeModelFallbackNilPayloadDoesNotPanic(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	// A host that bypasses NewModelFallbackEvent may leave Payload nil; the
	// bridge must degrade to an empty custom value, not panic.
	if err := bridge.Emit(context.Background(), runtime.Event{Kind: runtime.EventModelFallbackEngaged}); err != nil {
		t.Fatalf("Emit error = %v", err)
	}
	frames := frameData(t, sink.Bytes())
	if len(frames) != 1 || frames[0]["type"] != "CUSTOM" {
		t.Fatalf("frames = %#v, want one CUSTOM frame", frames)
	}
	if value, ok := frames[0]["value"].(map[string]any); !ok || len(value) != 0 {
		t.Fatalf("nil-payload custom value = %#v, want empty map", frames[0]["value"])
	}
}

func TestBridgeDelegatesClientToolBinding(t *testing.T) {
	t.Parallel()

	infos, err := ClientToolInfos([]aguitypes.Tool{{
		Name:        "client_lookup",
		Description: "lookup in client",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string"},
			},
		},
	}})
	if err != nil {
		t.Fatalf("ClientToolInfos error = %v", err)
	}
	if len(infos) != 1 || infos[0].Name != "client_lookup" {
		t.Fatalf("tool infos = %#v", infos)
	}
	server, client := ClassifyToolCalls([]einoschema.ToolCall{
		{ID: "server-1", Function: einoschema.FunctionCall{Name: "server_tool"}},
		{ID: "client-1", Function: einoschema.FunctionCall{Name: "client_lookup"}},
	}, map[string]bool{"client_lookup": true})
	if len(server) != 1 || server[0].ID != "server-1" || len(client) != 1 || client[0].ID != "client-1" {
		t.Fatalf("classified server=%#v client=%#v", server, client)
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
