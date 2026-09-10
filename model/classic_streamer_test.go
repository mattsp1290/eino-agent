package model

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
)

// scriptedClassicModel is a minimal einomodel.ToolCallingChatModel that
// records every call and plays back a fixed classic chunk script.
type scriptedClassicModel struct {
	messages []*einoschema.Message
	options  *einomodel.Options
	calls    int

	chunks []*einoschema.Message

	// withToolsCalls records every WithTools invocation (nil entry for a
	// call whose argument was nil, an empty-but-non-nil slice for a call
	// whose argument explicitly cleared tools).
	withToolsCalls [][]*einoschema.ToolInfo
	withToolsErr   error
}

func (m *scriptedClassicModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return nil, errors.New("unused")
}

func (m *scriptedClassicModel) Stream(_ context.Context, messages []*einoschema.Message, opts ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	m.calls++
	m.messages = messages
	m.options = einomodel.GetCommonOptions(&einomodel.Options{}, opts...)
	return einoschema.StreamReaderFromArray(append([]*einoschema.Message(nil), m.chunks...)), nil
}

func (m *scriptedClassicModel) WithTools(tools []*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	m.withToolsCalls = append(m.withToolsCalls, tools)
	if m.withToolsErr != nil {
		return nil, m.withToolsErr
	}
	return m, nil
}

func agenticSystem(text string) *einoschema.AgenticMessage {
	return einoschema.SystemAgenticMessage(text)
}

func TestClassicAdapterTranslatesRepresentableContent(t *testing.T) {
	client := &scriptedClassicModel{}
	streamer := NewClassicStreamer(client)

	url := "https://example.test/image.png"
	forced := &einoschema.AgenticToolChoice{
		Type:   einoschema.ToolChoiceForced,
		Forced: &einoschema.AgenticForcedToolChoice{Tools: []*einoschema.AllowedTool{{FunctionName: "search"}}},
	}
	req := Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		System:   "top-level system",
		Messages: []*einoschema.AgenticMessage{
			agenticSystem("inline system"),
			einoschema.UserAgenticMessage("hello"),
			{
				Role: einoschema.AgenticRoleTypeUser,
				ContentBlocks: []*einoschema.ContentBlock{
					einoschema.NewContentBlock(&einoschema.UserInputText{Text: "look at this"}),
					einoschema.NewContentBlock(&einoschema.UserInputImage{URL: url, MIMEType: "image/png"}),
				},
			},
			{
				Role: einoschema.AgenticRoleTypeAssistant,
				ContentBlocks: []*einoschema.ContentBlock{
					einoschema.NewContentBlock(&einoschema.Reasoning{Text: "thinking"}),
					einoschema.NewContentBlock(&einoschema.AssistantGenText{Text: "answer"}),
					einoschema.NewContentBlock(&einoschema.FunctionToolCall{CallID: "call-1", Name: "search", Arguments: `{"q":"x"}`}),
				},
			},
			{
				Role: einoschema.AgenticRoleTypeUser,
				ContentBlocks: []*einoschema.ContentBlock{
					einoschema.NewContentBlock(&einoschema.FunctionToolResult{
						CallID: "call-1", Name: "search",
						Content: []*einoschema.FunctionToolResultContentBlock{{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "result text"}}},
					}),
				},
			},
		},
		Controls: RequestControls{
			Tools:      []*einoschema.ToolInfo{{Name: "search"}},
			ToolChoice: forced,
		},
	}
	if _, err := streamer.StreamProvider(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 {
		t.Fatalf("calls = %d, want 1", client.calls)
	}
	msgs := client.messages
	// system(top-level) + system(inline) + user(hello) + user(multi-content) + assistant + tool
	if len(msgs) != 6 {
		t.Fatalf("messages = %d, want 6: %#v", len(msgs), msgs)
	}
	if msgs[0].Role != einoschema.System || msgs[0].Content != "top-level system" {
		t.Fatalf("msgs[0] = %#v", msgs[0])
	}
	if msgs[1].Role != einoschema.System || msgs[1].Content != "inline system" {
		t.Fatalf("msgs[1] = %#v", msgs[1])
	}
	if msgs[2].Role != einoschema.User || msgs[2].Content != "hello" {
		t.Fatalf("msgs[2] = %#v", msgs[2])
	}
	multi := msgs[3]
	if multi.Role != einoschema.User || len(multi.UserInputMultiContent) != 2 {
		t.Fatalf("msgs[3] = %#v", multi)
	}
	if multi.UserInputMultiContent[0].Type != einoschema.ChatMessagePartTypeText || multi.UserInputMultiContent[0].Text != "look at this" {
		t.Fatalf("multi part 0 = %#v", multi.UserInputMultiContent[0])
	}
	if multi.UserInputMultiContent[1].Type != einoschema.ChatMessagePartTypeImageURL || multi.UserInputMultiContent[1].Image == nil || *multi.UserInputMultiContent[1].Image.URL != url {
		t.Fatalf("multi part 1 = %#v", multi.UserInputMultiContent[1])
	}
	assistant := msgs[4]
	if assistant.Role != einoschema.Assistant || assistant.Content != "answer" || assistant.ReasoningContent != "thinking" {
		t.Fatalf("assistant = %#v", assistant)
	}
	if len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].ID != "call-1" || assistant.ToolCalls[0].Function.Name != "search" {
		t.Fatalf("assistant tool calls = %#v", assistant.ToolCalls)
	}
	toolMsg := msgs[5]
	if toolMsg.Role != einoschema.Tool || toolMsg.Content != "result text" || toolMsg.ToolCallID != "call-1" || toolMsg.ToolName != "search" {
		t.Fatalf("tool message = %#v", toolMsg)
	}
	if client.options == nil || client.options.ToolChoice == nil || *client.options.ToolChoice != einoschema.ToolChoiceForced {
		t.Fatalf("tool choice option = %#v", client.options)
	}
	if len(client.options.AllowedToolNames) != 1 || client.options.AllowedToolNames[0] != "search" {
		t.Fatalf("allowed tool names = %#v", client.options.AllowedToolNames)
	}
}

func TestClassicAdapterAllowedToolChoice(t *testing.T) {
	client := &scriptedClassicModel{}
	streamer := NewClassicStreamer(client)
	req := Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi")},
		Controls: RequestControls{
			Tools: []*einoschema.ToolInfo{{Name: "a"}, {Name: "b"}},
			ToolChoice: &einoschema.AgenticToolChoice{
				Type:    einoschema.ToolChoiceAllowed,
				Allowed: &einoschema.AgenticAllowedToolChoice{Tools: []*einoschema.AllowedTool{{FunctionName: "a"}, {FunctionName: "b"}}},
			},
		},
	}
	if _, err := streamer.StreamProvider(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if client.options == nil || client.options.ToolChoice == nil || *client.options.ToolChoice != einoschema.ToolChoiceAllowed {
		t.Fatalf("tool choice = %#v", client.options)
	}
	if len(client.options.AllowedToolNames) != 2 {
		t.Fatalf("allowed tool names = %#v", client.options.AllowedToolNames)
	}
}

func rejectionRequest(mutate func(*Request)) Request {
	req := Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi")},
	}
	mutate(&req)
	return req
}

func TestClassicAdapterRejectsUnrepresentableContentBeforeDispatch(t *testing.T) {
	tests := map[string]Request{
		"server tool call block": rejectionRequest(func(r *Request) {
			r.Messages = append(r.Messages, &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
				einoschema.NewContentBlock(&einoschema.ServerToolCall{Name: "web_search"}),
			}})
		}),
		"mcp tool result block": rejectionRequest(func(r *Request) {
			r.Messages = append(r.Messages, &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
				einoschema.NewContentBlock(&einoschema.MCPToolResult{Name: "tool"}),
			}})
		}),
		"tool search result block": rejectionRequest(func(r *Request) {
			r.Messages = append(r.Messages, &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{
				einoschema.NewContentBlock(&einoschema.ToolSearchFunctionToolResult{Name: "search"}),
			}})
		}),
		"deferred tools": rejectionRequest(func(r *Request) {
			r.Controls.DeferredTools = []*einoschema.ToolInfo{{Name: "d"}}
		}),
		"tool search tool": rejectionRequest(func(r *Request) {
			r.Controls.ToolSearchTool = &einoschema.ToolInfo{Name: "search_tool"}
		}),
		"assistant media": rejectionRequest(func(r *Request) {
			r.Messages = append(r.Messages, &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
				einoschema.NewContentBlock(&einoschema.AssistantGenImage{URL: "https://example.test/out.png"}),
			}})
		}),
		"non-text function result content": rejectionRequest(func(r *Request) {
			r.Messages = append(r.Messages, &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{
				einoschema.NewContentBlock(&einoschema.FunctionToolResult{CallID: "c", Name: "n", Content: []*einoschema.FunctionToolResultContentBlock{
					{Type: einoschema.FunctionToolResultContentBlockTypeImage, Image: &einoschema.UserInputImage{URL: "https://example.test/x.png"}},
				}}),
			}})
		}),
		"mcp tool choice selector": rejectionRequest(func(r *Request) {
			r.Controls.Tools = []*einoschema.ToolInfo{{Name: "a"}}
			r.Controls.ToolChoice = &einoschema.AgenticToolChoice{
				Type:   einoschema.ToolChoiceForced,
				Forced: &einoschema.AgenticForcedToolChoice{Tools: []*einoschema.AllowedTool{{MCPTool: &einoschema.AllowedMCPTool{ServerLabel: "s", Name: "n"}}}},
			}
		}),
		"server tool choice selector": rejectionRequest(func(r *Request) {
			r.Controls.Tools = []*einoschema.ToolInfo{{Name: "a"}}
			r.Controls.ToolChoice = &einoschema.AgenticToolChoice{
				Type:   einoschema.ToolChoiceForced,
				Forced: &einoschema.AgenticForcedToolChoice{Tools: []*einoschema.AllowedTool{{ServerTool: &einoschema.AllowedServerTool{Name: "n"}}}},
			}
		}),
	}
	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			client := &scriptedClassicModel{}
			streamer := NewClassicStreamer(client)
			_, err := streamer.StreamProvider(context.Background(), req)
			if err == nil {
				t.Fatal("expected an error")
			}
			var modelErr Error
			if !errors.As(err, &modelErr) || modelErr.Code != "capability_unsupported" || !errors.Is(err, ErrCapabilityUnsupported) {
				t.Fatalf("error = %v, want capability_unsupported/ErrCapabilityUnsupported", err)
			}
			if client.calls != 0 {
				t.Fatalf("calls = %d, want 0", client.calls)
			}
		})
	}
}

func TestClassicAdapterConvertsOutputChunksWithUsage(t *testing.T) {
	toolIndex := 0
	client := &scriptedClassicModel{chunks: []*einoschema.Message{
		{Role: einoschema.Assistant, Content: "hi", ReasoningContent: "because", ToolCalls: []einoschema.ToolCall{{ID: "c1", Index: &toolIndex, Function: einoschema.FunctionCall{Name: "search", Arguments: "{}"}}},
			ResponseMeta: &einoschema.ResponseMeta{Usage: &einoschema.TokenUsage{
				PromptTokens: 5, CompletionTokens: 2,
				CompletionTokensDetails: einoschema.CompletionTokensDetails{ReasoningTokens: 1},
				PromptTokenDetails:      einoschema.PromptTokenDetails{CachedTokens: 1},
			}}},
	}}
	streamer := NewClassicStreamer(client)
	reader, err := streamer.StreamProvider(context.Background(), Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	delta, err := reader.Recv()
	if err != nil {
		t.Fatal(err)
	}
	want := Usage{InputTokens: 5, OutputTokens: 2, ReasoningTokens: 1, CacheReadTokens: 1}
	if delta.Usage != want {
		t.Fatalf("usage = %#v, want %#v", delta.Usage, want)
	}
	blocks := delta.Message.ContentBlocks
	if len(blocks) != 3 {
		t.Fatalf("blocks = %d, want 3: %#v", len(blocks), blocks)
	}
	if blocks[0].Type != einoschema.ContentBlockTypeAssistantGenText || blocks[0].StreamingMeta.Index != 0 {
		t.Fatalf("block 0 = %#v", blocks[0])
	}
	if blocks[1].Type != einoschema.ContentBlockTypeReasoning || blocks[1].StreamingMeta.Index != 1 {
		t.Fatalf("block 1 = %#v", blocks[1])
	}
	if blocks[2].Type != einoschema.ContentBlockTypeFunctionToolCall || blocks[2].FunctionToolCall.CallID != "c1" {
		t.Fatalf("block 2 = %#v", blocks[2])
	}
}

func TestClassicProviderStateStreamerCaptureConvertsAgenticOutput(t *testing.T) {
	codec, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: "state", Contract: testProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	client := &scriptedClassicModel{}
	streamer, err := NewClassicStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	message := agenticAssistantText("done")
	message.Extra = map[string]any{"state": []json.RawMessage{json.RawMessage(`{"x":1}`)}}
	capture, err := streamer.CaptureProviderState(message)
	if err != nil {
		t.Fatal(err)
	}
	if len(capture.Items) != 1 {
		t.Fatalf("items = %#v", capture.Items)
	}
	if message.Extra != nil {
		t.Fatalf("message Extra = %#v, want nil", message.Extra)
	}
}

// TestClassicAdapterNilIndexToolCallsDoNotCollide guards against a nil-Index
// tool call from one chunk colliding on block index with a nil-Index tool
// call from a different chunk (both would otherwise land on
// toolCallBase+position == the same slot for a single-call chunk). Eino's own
// classic accumulator (schema.concatToolCalls) keeps nil-index calls
// separate; classicMessageToAgenticDelta must preserve that when converting
// to agentic blocks so schema.ConcatAgenticMessages does not merge them.
func TestClassicAdapterNilIndexToolCallsDoNotCollide(t *testing.T) {
	client := &scriptedClassicModel{chunks: []*einoschema.Message{
		{Role: einoschema.Assistant, ToolCalls: []einoschema.ToolCall{
			{ID: "call-a", Function: einoschema.FunctionCall{Name: "a", Arguments: `{"x":1}`}},
		}},
		{Role: einoschema.Assistant, ToolCalls: []einoschema.ToolCall{
			{ID: "call-b", Function: einoschema.FunctionCall{Name: "b", Arguments: `{"y":2}`}},
		}},
	}}
	streamer := NewClassicStreamer(client)
	reader, err := streamer.StreamProvider(context.Background(), Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	var chunks []*einoschema.AgenticMessage
	for {
		delta, err := reader.Recv()
		if err != nil {
			break
		}
		chunks = append(chunks, delta.Message)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	if chunks[0].ContentBlocks[0].StreamingMeta == nil || chunks[1].ContentBlocks[0].StreamingMeta == nil {
		t.Fatalf("missing StreamingMeta: %#v / %#v", chunks[0].ContentBlocks[0], chunks[1].ContentBlocks[0])
	}
	if chunks[0].ContentBlocks[0].StreamingMeta.Index == chunks[1].ContentBlocks[0].StreamingMeta.Index {
		t.Fatalf("both nil-Index tool calls got the same block index %d", chunks[0].ContentBlocks[0].StreamingMeta.Index)
	}

	merged, err := einoschema.ConcatAgenticMessages(chunks)
	if err != nil {
		t.Fatalf("ConcatAgenticMessages: %v", err)
	}
	var calls []*einoschema.FunctionToolCall
	for _, b := range merged.ContentBlocks {
		if b.Type == einoschema.ContentBlockTypeFunctionToolCall {
			calls = append(calls, b.FunctionToolCall)
		}
	}
	if len(calls) != 2 {
		t.Fatalf("merged function tool call blocks = %d, want 2: %#v", len(calls), merged.ContentBlocks)
	}
	ids := map[string]bool{}
	for _, c := range calls {
		ids[c.CallID] = true
	}
	if !ids["call-a"] || !ids["call-b"] {
		t.Fatalf("merged call ids = %#v, want call-a and call-b", ids)
	}
}

// TestClassicAdapterEmptyToolsStillClears mirrors
// TestAgenticCallOptionsEmptyToolsStillClears for the classic path: an
// explicitly empty (non-nil) Tools list must call WithTools to clear
// whatever the base client was constructed with, not silently no-op.
func TestClassicAdapterEmptyToolsStillClears(t *testing.T) {
	client := &scriptedClassicModel{}
	streamer := NewClassicStreamer(client)
	req := Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi")},
		Controls: RequestControls{Tools: []*einoschema.ToolInfo{}},
	}
	if _, err := streamer.StreamProvider(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(client.withToolsCalls) != 1 {
		t.Fatalf("WithTools calls = %d, want 1", len(client.withToolsCalls))
	}
	if client.withToolsCalls[0] == nil || len(client.withToolsCalls[0]) != 0 {
		t.Fatalf("WithTools arg = %#v, want empty non-nil slice", client.withToolsCalls[0])
	}
}

// TestClassicAdapterNilToolsDoesNotCallWithTools asserts the classic path
// leaves the base client untouched when the caller did not supply a Tools
// list at all (nil), as opposed to an explicit empty list which clears.
func TestClassicAdapterNilToolsDoesNotCallWithTools(t *testing.T) {
	client := &scriptedClassicModel{}
	streamer := NewClassicStreamer(client)
	req := Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi")},
	}
	if _, err := streamer.StreamProvider(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(client.withToolsCalls) != 0 {
		t.Fatalf("WithTools calls = %d, want 0", len(client.withToolsCalls))
	}
}

// TestClassicAdapterWithToolsErrorPropagates asserts a concrete classic
// adapter's rejection of a tool list (e.g. an empty one it cannot represent)
// surfaces as the StreamProvider error rather than silently dispatching with
// stale tools.
func TestClassicAdapterWithToolsErrorPropagates(t *testing.T) {
	wantErr := errors.New("adapter rejects empty tool list")
	client := &scriptedClassicModel{withToolsErr: wantErr}
	streamer := NewClassicStreamer(client)
	req := Request{
		Identity: Identity{ProviderID: "fake", ModelID: "m1"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi")},
		Controls: RequestControls{Tools: []*einoschema.ToolInfo{}},
	}
	_, err := streamer.StreamProvider(context.Background(), req)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if client.calls != 0 {
		t.Fatalf("calls = %d, want 0 (Stream must not run after WithTools error)", client.calls)
	}
}
