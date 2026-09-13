package model

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
)

// scriptedAgenticCall records exactly one Stream() invocation.
type scriptedAgenticCall struct {
	messages []*einoschema.AgenticMessage
	options  *einomodel.Options
}

// scriptedAgenticModel is a minimal einomodel.AgenticModel that records every
// call it receives and plays back a fixed chunk script.
type scriptedAgenticModel struct {
	mu    sync.Mutex
	calls []scriptedAgenticCall

	chunks []*einoschema.AgenticMessage
	err    error // delivered after the last chunk, before EOF
}

func (m *scriptedAgenticModel) Generate(context.Context, []*einoschema.AgenticMessage, ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	return nil, errors.New("unused")
}

func (m *scriptedAgenticModel) Stream(_ context.Context, messages []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	options := einomodel.GetCommonOptions(&einomodel.Options{}, opts...)
	m.mu.Lock()
	m.calls = append(m.calls, scriptedAgenticCall{messages: messages, options: options})
	m.mu.Unlock()

	reader, writer := einoschema.Pipe[*einoschema.AgenticMessage](len(m.chunks) + 1)
	go func() {
		defer writer.Close()
		for _, chunk := range m.chunks {
			if writer.Send(chunk, nil) {
				return
			}
		}
		if m.err != nil {
			writer.Send(nil, m.err)
		}
	}()
	return reader, nil
}

func (m *scriptedAgenticModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *scriptedAgenticModel) call(i int) scriptedAgenticCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[i]
}

func fullControls() RequestControls {
	temperature := float32(0.4)
	topP := float32(0.9)
	maxTokens := 128
	return RequestControls{
		Tools:          []*einoschema.ToolInfo{{Name: "search"}},
		DeferredTools:  []*einoschema.ToolInfo{{Name: "deferred_one"}},
		ToolSearchTool: &einoschema.ToolInfo{Name: "tool_search"},
		ToolChoice: &einoschema.AgenticToolChoice{
			Type:   einoschema.ToolChoiceForced,
			Forced: &einoschema.AgenticForcedToolChoice{Tools: []*einoschema.AllowedTool{{FunctionName: "search"}}},
		},
		Temperature: &temperature,
		TopP:        &topP,
		MaxTokens:   &maxTokens,
		Stop:        []string{"STOP"},
	}
}

func TestAgenticCallOptionsExactSetAndOrder(t *testing.T) {
	controls := fullControls()
	opts := agenticCallOptions(controls)
	if len(opts) != 8 {
		t.Fatalf("option count = %d, want 8", len(opts))
	}

	// Apply options one at a time and assert exactly the expected field
	// transitions from zero at each step, proving both the set and the
	// order: Tools, DeferredTools, ToolSearchTool, AgenticToolChoice,
	// Temperature, TopP, MaxTokens, Stop.
	base := &einomodel.Options{}
	step := func(i int, check func(*einomodel.Options) bool, name string) {
		base = einomodel.GetCommonOptions(base, opts[i])
		if !check(base) {
			t.Fatalf("step %d (%s): option not applied, got %#v", i, name, base)
		}
	}
	step(0, func(o *einomodel.Options) bool {
		return len(o.Tools) == 1 && o.Tools[0].Name == "search"
	}, "Tools")
	step(1, func(o *einomodel.Options) bool {
		return len(o.DeferredTools) == 1 && o.DeferredTools[0].Name == "deferred_one"
	}, "DeferredTools")
	step(2, func(o *einomodel.Options) bool {
		return o.ToolSearchTool != nil && o.ToolSearchTool.Name == "tool_search"
	}, "ToolSearchTool")
	step(3, func(o *einomodel.Options) bool {
		return o.AgenticToolChoice != nil && o.AgenticToolChoice.Type == einoschema.ToolChoiceForced
	}, "AgenticToolChoice")
	step(4, func(o *einomodel.Options) bool { return o.Temperature != nil && *o.Temperature == 0.4 }, "Temperature")
	step(5, func(o *einomodel.Options) bool { return o.TopP != nil && *o.TopP == 0.9 }, "TopP")
	step(6, func(o *einomodel.Options) bool { return o.MaxTokens != nil && *o.MaxTokens == 128 }, "MaxTokens")
	step(7, func(o *einomodel.Options) bool { return len(o.Stop) == 1 && o.Stop[0] == "STOP" }, "Stop")
}

func TestAgenticCallOptionsEmptyToolsStillClears(t *testing.T) {
	opts := agenticCallOptions(RequestControls{})
	if len(opts) != 1 {
		t.Fatalf("option count = %d, want 1 (Tools only)", len(opts))
	}
	base := einomodel.GetCommonOptions(&einomodel.Options{Tools: []*einoschema.ToolInfo{{Name: "stale"}}}, opts[0])
	if base.Tools == nil || len(base.Tools) != 0 {
		t.Fatalf("Tools = %#v, want a cleared (empty, non-nil) slice", base.Tools)
	}
}

func TestAgenticStreamerConcurrentRequestsDoNotInterfere(t *testing.T) {
	client := &scriptedAgenticModel{}
	streamer := NewAgenticStreamer(client)

	var wg sync.WaitGroup
	names := []string{"alpha", "beta"}
	for _, name := range names {
		wg.Add(1)
		go func(toolName string) {
			defer wg.Done()
			req := Request{
				Identity: Identity{ProviderID: "fake", ModelID: "m1"},
				Controls: RequestControls{Tools: []*einoschema.ToolInfo{{Name: toolName}}},
			}
			reader, err := streamer.StreamProvider(context.Background(), req)
			if err != nil {
				t.Errorf("StreamProvider(%s) error = %v", toolName, err)
				return
			}
			reader.Close()
		}(name)
	}
	wg.Wait()

	if client.callCount() != 2 {
		t.Fatalf("calls = %d, want 2", client.callCount())
	}
	seen := map[string]bool{}
	for i := 0; i < client.callCount(); i++ {
		call := client.call(i)
		if len(call.options.Tools) != 1 {
			t.Fatalf("call %d tools = %#v", i, call.options.Tools)
		}
		seen[call.options.Tools[0].Name] = true
	}
	if !seen["alpha"] || !seen["beta"] {
		t.Fatalf("tool names not both observed: %#v", seen)
	}
}

func TestAgenticStreamerValidationFailsBeforeDispatch(t *testing.T) {
	client := &scriptedAgenticModel{}
	streamer := NewAgenticStreamer(client)
	req := Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Controls: RequestControls{Tools: []*einoschema.ToolInfo{{Name: "dup"}, {Name: "dup"}}},
	}
	_, err := streamer.StreamProvider(context.Background(), req)
	var modelErr Error
	if !errors.As(err, &modelErr) || modelErr.Code != "tool_partition_invalid" {
		t.Fatalf("error = %v, want tool_partition_invalid", err)
	}
	if client.callCount() != 0 {
		t.Fatalf("calls = %d, want 0", client.callCount())
	}
}

func TestAgenticStreamerPreservesInterleavedBlockIndices(t *testing.T) {
	client := &scriptedAgenticModel{chunks: []*einoschema.AgenticMessage{
		{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlockChunk(&einoschema.Reasoning{Text: "th"}, &einoschema.StreamingMeta{Index: 1}),
			einoschema.NewContentBlockChunk(&einoschema.AssistantGenText{Text: "an"}, &einoschema.StreamingMeta{Index: 0}),
		}},
	}}
	streamer := NewAgenticStreamer(client)
	reader, err := streamer.StreamProvider(context.Background(), Request{Identity: Identity{ProviderID: "fake", ModelID: "m1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	delta, err := reader.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Message.ContentBlocks) != 2 ||
		delta.Message.ContentBlocks[0].StreamingMeta.Index != 1 ||
		delta.Message.ContentBlocks[1].StreamingMeta.Index != 0 {
		t.Fatalf("blocks = %#v", delta.Message.ContentBlocks)
	}
}

func TestAgenticStreamerRejectsNilChunk(t *testing.T) {
	client := &scriptedAgenticModel{chunks: []*einoschema.AgenticMessage{nil}}
	streamer := NewAgenticStreamer(client)
	reader, err := streamer.StreamProvider(context.Background(), Request{Identity: Identity{ProviderID: "fake", ModelID: "m1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	_, err = reader.Recv()
	var modelErr Error
	if !errors.As(err, &modelErr) || modelErr.Code != "malformed_provider_stream" {
		t.Fatalf("error = %v, want malformed_provider_stream", err)
	}
}

func TestAgenticStreamerSurfacesTerminalErrorBeforeEOF(t *testing.T) {
	boom := errors.New("boom")
	client := &scriptedAgenticModel{
		chunks: []*einoschema.AgenticMessage{{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlockChunk(&einoschema.AssistantGenText{Text: "partial"}, &einoschema.StreamingMeta{Index: 0}),
		}}},
		err: boom,
	}
	streamer := NewAgenticStreamer(client)
	reader, err := streamer.StreamProvider(context.Background(), Request{Identity: Identity{ProviderID: "fake", ModelID: "m1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Recv(); err != nil {
		t.Fatalf("first Recv error = %v", err)
	}
	_, err = reader.Recv()
	if err == nil || errors.Is(err, io.EOF) || !errors.Is(err, boom) {
		t.Fatalf("second Recv error = %v, want boom", err)
	}
}

func TestAgenticStreamerCloseClosesUpstreamOnce(t *testing.T) {
	// Eino's schema.StreamReader.Close is documented as single-use (a second
	// call panics closing an already-closed channel), so our streamer must
	// not add its own Close call beyond the one the caller makes: closing
	// exactly once must succeed cleanly, and the underlying pipe must
	// observe the reader side as gone.
	client := &scriptedAgenticModel{chunks: []*einoschema.AgenticMessage{{Role: einoschema.AgenticRoleTypeAssistant}}}
	streamer := NewAgenticStreamer(client)
	reader, err := streamer.StreamProvider(context.Background(), Request{Identity: Identity{ProviderID: "fake", ModelID: "m1"}})
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
}
