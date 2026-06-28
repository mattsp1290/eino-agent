package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/agui"
)

func TestStreamTurnDelegatesToEinoAGUIStreamTap(t *testing.T) {
	t.Parallel()

	sink := &bytes.Buffer{}
	writer := bufio.NewWriter(sink)
	bridge := agui.NewBridge(context.Background(), writer, sse.NewSSEWriter(), "thread-1", "run-1", nil)
	msg, err := StreamTurn(context.Background(), bridge, streamModel{
		chunks: []*einoschema.Message{
			{Role: einoschema.Assistant, ReasoningContent: "think"},
			einoschema.AssistantMessage("hello", nil),
		},
	}, []*einoschema.Message{einoschema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("StreamTurn error = %v", err)
	}
	if msg.Content != "hello" || msg.ReasoningContent != "think" {
		t.Fatalf("message = %#v", msg)
	}
	_ = writer.Flush()
	got := eventTypes(t, sink.String())
	want := []string{
		"REASONING_START",
		"REASONING_MESSAGE_START",
		"REASONING_MESSAGE_CONTENT",
		"REASONING_MESSAGE_END",
		"REASONING_END",
		"TEXT_MESSAGE_START",
		"TEXT_MESSAGE_CONTENT",
		"TEXT_MESSAGE_END",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("event types = %#v, want %#v", got, want)
	}
}

type streamModel struct {
	chunks []*einoschema.Message
}

func (m streamModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return einoschema.ConcatMessages(m.chunks)
}

func (m streamModel) Stream(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	reader, writer := einoschema.Pipe[*einoschema.Message](len(m.chunks))
	go func() {
		defer writer.Close()
		for _, chunk := range m.chunks {
			if writer.Send(chunk, nil) {
				return
			}
		}
	}()
	return reader, nil
}

func (m streamModel) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	return m, nil
}

func eventTypes(t *testing.T, raw string) []string {
	t.Helper()
	frames := strings.Split(raw, "\n\n")
	result := make([]string, 0, len(frames))
	for _, frame := range frames {
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var data struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
				t.Fatalf("decode frame %q: %v", frame, err)
			}
			result = append(result, data.Type)
		}
	}
	return result
}
