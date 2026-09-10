package model

import (
	"context"
	"fmt"
	"strings"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
)

// NewClassicStreamer adapts one immutable classic Eino chat model to the
// agentic provider boundary. It translates representable agentic content to
// the classic input shape and classic output chunks back to agentic ones.
// Its purpose is provider interoperability for classic-only adapters, not
// preservation of the old public API: it fails closed with a typed
// ErrCapabilityUnsupported error before dispatch on anything it cannot
// represent (server/MCP/tool-search blocks, deferred tools, a tool-search
// tool, MCP/server tool-choice selectors, assistant-generated media and
// non-text function-tool-result content).
func NewClassicStreamer(client einomodel.ToolCallingChatModel) Streamer {
	if client == nil {
		return nil
	}
	return &classicStreamer{client: client}
}

type classicStreamer struct {
	client einomodel.ToolCallingChatModel
}

func (s *classicStreamer) StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[StreamDelta], error) {
	if s == nil || s.client == nil {
		return nil, Error{Code: "model_client_missing", Message: "classic model client missing", Cause: ErrProviderUnavailable}
	}
	req, err := request.Clone()
	if err != nil {
		return nil, err
	}
	if len(req.ProviderState) != 0 {
		return nil, providerStateError(ErrProviderStateMismatch)
	}
	if err := ValidateControls(req.Controls); err != nil {
		return nil, err
	}
	if err := validateClassicCompatible(req); err != nil {
		return nil, err
	}
	messages, _, err := agenticRequestToClassic(req)
	if err != nil {
		return nil, err
	}
	return dispatchClassic(ctx, s.client, req, messages)
}

// NewClassicStreamerWithProviderState adapts a classic Eino model with one
// immutable classic Extra-key provider-state codec, exposed at the agentic
// provider boundary.
//
// Design note (documenting the choice design item 6 flags explicitly):
// ProviderStateCodec.CaptureAssistant/RestoreAssistant only understand a
// classic *schema.Message's Extra map. CaptureProviderState is now called
// with the agentic output message, so this adapter converts that message
// back to its classic form (assistantAgenticMessageToClassic) and reuses
// guardedCapture completely unchanged. For that conversion to ever find
// anything to capture, dispatchClassic's output conversion
// (classicMessageToAgenticDelta) carries the classic chunk's raw Extra
// through unchanged onto AgenticMessage.Extra; nothing else strips it. This
// is safe because: (a) Request.Clone/cloneAgenticMessages reject any
// message carrying Extra as *input*, so a state-aware caller must call
// CaptureProviderState (which strips claimed keys and nils Extra when
// empty, exactly like the classic codec always has) before that message can
// ever become part of a future Request; and (b) no per-request or
// per-stream state is kept on the streamer itself, so it stays safe to
// share across concurrent requests.
//
// The alternative sketched by the design ("capture happens on the classic
// chunk before conversion") would require the streamer to stash the
// pre-conversion classic message somewhere between the StreamProvider call
// and the later, separate CaptureProviderState call. Since one streamer
// instance serves concurrent requests, that would mean per-request mutable
// state on a shared object — unsafe without additional synchronization this
// package has no way to key correctly. Converting back is the simpler,
// safe choice, and it keeps guardedCapture exactly as written.
func NewClassicStreamerWithProviderState(client einomodel.ToolCallingChatModel, codec ProviderStateCodec) (ProviderStateStreamer, error) {
	if client == nil || codec == nil {
		return nil, providerStateError(ErrProviderStateInvalid)
	}
	contract, owned, err := snapshotProviderStateCodec(codec)
	if err != nil {
		return nil, err
	}
	return &classicProviderStateStreamer{client: client, codec: codec, contract: contract, owned: owned}, nil
}

type classicProviderStateStreamer struct {
	client   einomodel.ToolCallingChatModel
	codec    ProviderStateCodec
	contract ProviderStateContract
	owned    map[string]struct{}
}

func (s *classicProviderStateStreamer) ProviderStateContract() ProviderStateContract {
	return s.contract
}

func (s *classicProviderStateStreamer) CaptureProviderState(message *einoschema.AgenticMessage) (ProviderStateCapture, error) {
	if s == nil || s.codec == nil || message == nil {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	classic, err := assistantAgenticMessageToClassic(message)
	if err != nil {
		return ProviderStateCapture{}, err
	}
	classic.Extra = message.Extra
	capture, err := guardedCapture(s.codec, s.owned, s.contract, classic)
	if err != nil {
		return ProviderStateCapture{}, err
	}
	message.Extra = classic.Extra
	return capture, nil
}

func (s *classicProviderStateStreamer) StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[StreamDelta], error) {
	if s == nil || s.client == nil || s.codec == nil {
		return nil, Error{Code: "model_client_missing", Message: "classic model client missing", Cause: ErrProviderUnavailable}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req, err := request.Clone()
	if err != nil {
		return nil, err
	}
	if err := ValidateProviderStateIdentity(string(req.Identity.ProviderID), string(req.Identity.ModelID)); err != nil {
		return nil, err
	}
	if err := ValidateControls(req.Controls); err != nil {
		return nil, err
	}
	if err := validateClassicCompatible(req); err != nil {
		return nil, err
	}
	messages, origin, err := agenticRequestToClassic(req)
	if err != nil {
		return nil, err
	}
	previousIndex := -1
	for _, state := range req.ProviderState {
		if state.MessageIndex <= previousIndex || state.MessageIndex < 0 || state.MessageIndex >= len(req.Messages) ||
			state.MessageID == "" || state.SourceRunID == "" || state.SourceSessionID == "" ||
			state.SourceSessionID != req.Identity.SessionID || state.ProviderID != string(req.Identity.ProviderID) ||
			state.CodecID != s.contract.CodecID || state.CompatibilityKey != s.contract.CompatibilityKey {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if state.Version != s.contract.Version {
			return nil, providerStateError(ErrProviderStateVersion)
		}
		if err := ValidateProviderStateIdentity(state.ProviderID, state.SourceModelID); err != nil {
			return nil, err
		}
		message := req.Messages[state.MessageIndex]
		if message == nil || message.Role != einoschema.AgenticRoleTypeAssistant || len(message.Extra) != 0 {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if err := ValidateProviderStateItems(state.Items, s.contract.Limits); err != nil {
			return nil, err
		}
		classicIndex := -1
		for ci, agenticIndex := range origin {
			if agenticIndex == state.MessageIndex {
				classicIndex = ci
				break
			}
		}
		if classicIndex < 0 || messages[classicIndex] == nil || messages[classicIndex].Role != einoschema.Assistant {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if err := restoreClassicProviderState(s.codec, s.owned, messages[classicIndex], state.Items); err != nil {
			return nil, err
		}
		previousIndex = state.MessageIndex
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return dispatchClassic(ctx, s.client, req, messages)
}

// restoreClassicProviderState mirrors the deleted einoStreamer's
// restoreProviderState: it restores items onto message via codec, verifies
// the provider-neutral fields are unchanged, and requires every remaining
// Extra key be one the codec owns.
func restoreClassicProviderState(codec ProviderStateCodec, owned map[string]struct{}, message *einoschema.Message, items []ProviderStateItem) (err error) {
	defer func() {
		if recover() != nil {
			err = providerStateError(ErrProviderStateInvalid)
		}
	}()
	neutralBefore, err := providerNeutralMessageClone(message)
	if err != nil {
		return providerStateError(ErrProviderStateInvalid)
	}
	if err := codec.RestoreAssistant(message, cloneProviderStateItems(items)); err != nil {
		return providerStateErrorFrom(err)
	}
	if err := requireProviderNeutralMessageUnchanged(neutralBefore, message); err != nil {
		return err
	}
	if len(message.Extra) == 0 {
		return providerStateError(ErrProviderStateInvalid)
	}
	for key := range message.Extra {
		if _, ok := owned[key]; !ok {
			return providerStateError(ErrProviderStateInvalid)
		}
	}
	return nil
}

// dispatchClassic binds tools/options, prepends the system message and
// dispatches messages (already translated from req.Messages) to base,
// converting classic output chunks back to agentic StreamDeltas.
func dispatchClassic(ctx context.Context, base einomodel.ToolCallingChatModel, req Request, messages []*einoschema.Message) (*einoschema.StreamReader[StreamDelta], error) {
	if req.System != "" {
		messages = append([]*einoschema.Message{einoschema.SystemMessage(req.System)}, messages...)
	}
	client := base
	var err error
	if len(req.Controls.Tools) != 0 {
		client, err = client.WithTools(req.Controls.Tools)
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	upstream, err := client.Stream(ctx, messages, classicCallOptions(req.Controls)...)
	if err != nil {
		return nil, err
	}
	if upstream == nil {
		return nil, Error{Code: "nil_provider_stream", Message: "classic model returned nil stream"}
	}
	return einoschema.StreamReaderWithConvert(upstream, func(message *einoschema.Message) (StreamDelta, error) {
		if message == nil {
			return StreamDelta{}, Error{Code: "malformed_provider_stream", Message: "classic model returned a nil chunk", Cause: ErrProviderRejected}
		}
		agentic, usage := classicMessageToAgenticDelta(message)
		return StreamDelta{Message: agentic, Usage: usage}, nil
	}), nil
}

func classicCallOptions(c RequestControls) []einomodel.Option {
	var opts []einomodel.Option
	if c.Temperature != nil {
		opts = append(opts, einomodel.WithTemperature(*c.Temperature))
	}
	if c.TopP != nil {
		opts = append(opts, einomodel.WithTopP(*c.TopP))
	}
	if c.MaxTokens != nil {
		opts = append(opts, einomodel.WithMaxTokens(*c.MaxTokens))
	}
	if c.Stop != nil {
		opts = append(opts, einomodel.WithStop(c.Stop))
	}
	if tc := c.ToolChoice; tc != nil {
		switch tc.Type {
		case einoschema.ToolChoiceForced:
			opts = append(opts, einomodel.WithToolChoice(einoschema.ToolChoiceForced, allowedToolFunctionNames(forcedTools(tc))...))
		case einoschema.ToolChoiceAllowed:
			opts = append(opts, einomodel.WithToolChoice(einoschema.ToolChoiceAllowed, allowedToolFunctionNames(allowedTools(tc))...))
		case einoschema.ToolChoiceForbidden:
			opts = append(opts, einomodel.WithToolChoice(einoschema.ToolChoiceForbidden))
		}
	}
	return opts
}

func forcedTools(tc *einoschema.AgenticToolChoice) []*einoschema.AllowedTool {
	if tc.Forced == nil {
		return nil
	}
	return tc.Forced.Tools
}

func allowedTools(tc *einoschema.AgenticToolChoice) []*einoschema.AllowedTool {
	if tc.Allowed == nil {
		return nil
	}
	return tc.Allowed.Tools
}

func allowedToolFunctionNames(tools []*einoschema.AllowedTool) []string {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		if t != nil && t.FunctionName != "" {
			names = append(names, t.FunctionName)
		}
	}
	return names
}

// validateClassicCompatible rejects, before any translation or dispatch,
// request controls the classic adapter cannot represent: deferred tools, a
// tool-search tool, and MCP/server tool-choice selectors.
func validateClassicCompatible(req Request) error {
	if len(req.Controls.DeferredTools) != 0 {
		return capabilityUnsupportedError("classic adapter does not support deferred tools")
	}
	if req.Controls.ToolSearchTool != nil {
		return capabilityUnsupportedError("classic adapter does not support a tool search tool")
	}
	if tc := req.Controls.ToolChoice; tc != nil {
		for _, tools := range [][]*einoschema.AllowedTool{forcedTools(tc), allowedTools(tc)} {
			for _, t := range tools {
				if t == nil {
					continue
				}
				if t.MCPTool != nil || t.ServerTool != nil {
					return capabilityUnsupportedError("classic adapter does not support MCP/server tool choice selectors")
				}
			}
		}
	}
	return nil
}

func capabilityUnsupportedError(format string, args ...any) error {
	return Error{
		Code:    "capability_unsupported",
		Message: fmt.Sprintf(format, args...),
		Cause:   ErrCapabilityUnsupported,
	}
}

// agenticRequestToClassic translates every message in req.Messages to its
// classic representation, in order. origin[i] names the source agentic
// message index that produced messages[i] (a user message can expand into
// several classic messages; an assistant message always maps to exactly
// one).
func agenticRequestToClassic(req Request) (messages []*einoschema.Message, origin []int, err error) {
	for index, msg := range req.Messages {
		if msg == nil {
			continue
		}
		converted, cerr := agenticMessageToClassic(msg)
		if cerr != nil {
			return nil, nil, fmt.Errorf("message %d: %w", index, cerr)
		}
		for _, m := range converted {
			messages = append(messages, m)
			origin = append(origin, index)
		}
	}
	return messages, origin, nil
}

func agenticMessageToClassic(msg *einoschema.AgenticMessage) ([]*einoschema.Message, error) {
	if msg == nil {
		return nil, nil
	}
	switch msg.Role {
	case einoschema.AgenticRoleTypeSystem:
		text, err := joinSystemTextBlocks(msg.ContentBlocks)
		if err != nil {
			return nil, err
		}
		return []*einoschema.Message{einoschema.SystemMessage(text)}, nil
	case einoschema.AgenticRoleTypeUser:
		return userAgenticMessageToClassic(msg.ContentBlocks)
	case einoschema.AgenticRoleTypeAssistant:
		message, err := assistantAgenticMessageToClassic(msg)
		if err != nil {
			return nil, err
		}
		return []*einoschema.Message{message}, nil
	default:
		return nil, capabilityUnsupportedError("unsupported agentic role %q", msg.Role)
	}
}

func joinSystemTextBlocks(blocks []*einoschema.ContentBlock) (string, error) {
	var sb strings.Builder
	for _, block := range blocks {
		if block == nil {
			continue
		}
		if block.Type != einoschema.ContentBlockTypeUserInputText || block.UserInputText == nil {
			return "", capabilityUnsupportedError("unsupported system content block type %q", block.Type)
		}
		sb.WriteString(block.UserInputText.Text)
	}
	return sb.String(), nil
}

// userAgenticMessageToClassic walks blocks in order, accumulating
// consecutive text/media parts into one classic user message (text-only
// parts collapse to a plain UserMessage; any media forces
// UserInputMultiContent) and emitting one classic tool message per
// function-tool-result block encountered, preserving overall order.
func userAgenticMessageToClassic(blocks []*einoschema.ContentBlock) ([]*einoschema.Message, error) {
	var out []*einoschema.Message
	var parts []einoschema.MessageInputPart
	hasMedia := false

	flush := func() {
		if len(parts) == 0 {
			return
		}
		if !hasMedia {
			var sb strings.Builder
			for _, p := range parts {
				sb.WriteString(p.Text)
			}
			out = append(out, einoschema.UserMessage(sb.String()))
		} else {
			cloned := append([]einoschema.MessageInputPart(nil), parts...)
			out = append(out, &einoschema.Message{Role: einoschema.User, UserInputMultiContent: cloned})
		}
		parts = nil
		hasMedia = false
	}

	for _, block := range blocks {
		if block == nil {
			continue
		}
		switch block.Type {
		case einoschema.ContentBlockTypeUserInputText:
			if block.UserInputText == nil {
				return nil, capabilityUnsupportedError("empty user text block")
			}
			parts = append(parts, einoschema.MessageInputPart{Type: einoschema.ChatMessagePartTypeText, Text: block.UserInputText.Text})
		case einoschema.ContentBlockTypeUserInputImage:
			if block.UserInputImage == nil {
				return nil, capabilityUnsupportedError("empty user image block")
			}
			hasMedia = true
			m := block.UserInputImage
			parts = append(parts, einoschema.MessageInputPart{
				Type: einoschema.ChatMessagePartTypeImageURL,
				Image: &einoschema.MessageInputImage{
					MessagePartCommon: einoschema.MessagePartCommon{URL: strPtrIfSet(m.URL), Base64Data: strPtrIfSet(m.Base64Data), MIMEType: m.MIMEType},
					Detail:            einoschema.ImageURLDetail(m.Detail),
				},
			})
		case einoschema.ContentBlockTypeUserInputAudio:
			if block.UserInputAudio == nil {
				return nil, capabilityUnsupportedError("empty user audio block")
			}
			hasMedia = true
			m := block.UserInputAudio
			parts = append(parts, einoschema.MessageInputPart{
				Type:  einoschema.ChatMessagePartTypeAudioURL,
				Audio: &einoschema.MessageInputAudio{MessagePartCommon: einoschema.MessagePartCommon{URL: strPtrIfSet(m.URL), Base64Data: strPtrIfSet(m.Base64Data), MIMEType: m.MIMEType}},
			})
		case einoschema.ContentBlockTypeUserInputVideo:
			if block.UserInputVideo == nil {
				return nil, capabilityUnsupportedError("empty user video block")
			}
			hasMedia = true
			m := block.UserInputVideo
			parts = append(parts, einoschema.MessageInputPart{
				Type:  einoschema.ChatMessagePartTypeVideoURL,
				Video: &einoschema.MessageInputVideo{MessagePartCommon: einoschema.MessagePartCommon{URL: strPtrIfSet(m.URL), Base64Data: strPtrIfSet(m.Base64Data), MIMEType: m.MIMEType}},
			})
		case einoschema.ContentBlockTypeUserInputFile:
			if block.UserInputFile == nil {
				return nil, capabilityUnsupportedError("empty user file block")
			}
			hasMedia = true
			m := block.UserInputFile
			parts = append(parts, einoschema.MessageInputPart{
				Type: einoschema.ChatMessagePartTypeFileURL,
				File: &einoschema.MessageInputFile{
					MessagePartCommon: einoschema.MessagePartCommon{URL: strPtrIfSet(m.URL), Base64Data: strPtrIfSet(m.Base64Data), MIMEType: m.MIMEType},
					Name:              m.Name,
				},
			})
		case einoschema.ContentBlockTypeFunctionToolResult:
			flush()
			result := block.FunctionToolResult
			if result == nil {
				return nil, capabilityUnsupportedError("empty function tool result block")
			}
			text, err := joinFunctionResultText(result.Content)
			if err != nil {
				return nil, err
			}
			out = append(out, einoschema.ToolMessage(text, result.CallID, einoschema.WithToolName(result.Name)))
		default:
			return nil, capabilityUnsupportedError("unsupported user content block type %q", block.Type)
		}
	}
	flush()
	return out, nil
}

func joinFunctionResultText(items []*einoschema.FunctionToolResultContentBlock) (string, error) {
	var sb strings.Builder
	for _, item := range items {
		if item == nil {
			continue
		}
		if item.Type != einoschema.FunctionToolResultContentBlockTypeText || item.Text == nil {
			return "", capabilityUnsupportedError("unsupported function tool result content type %q", item.Type)
		}
		sb.WriteString(item.Text.Text)
	}
	return sb.String(), nil
}

func assistantAgenticMessageToClassic(msg *einoschema.AgenticMessage) (*einoschema.Message, error) {
	if msg == nil {
		return nil, capabilityUnsupportedError("nil assistant message")
	}
	out := &einoschema.Message{Role: einoschema.Assistant}
	var text strings.Builder
	var reasoning strings.Builder
	var calls []einoschema.ToolCall
	for _, block := range msg.ContentBlocks {
		if block == nil {
			continue
		}
		switch block.Type {
		case einoschema.ContentBlockTypeAssistantGenText:
			if block.AssistantGenText == nil {
				return nil, capabilityUnsupportedError("empty assistant text block")
			}
			text.WriteString(block.AssistantGenText.Text)
		case einoschema.ContentBlockTypeReasoning:
			if block.Reasoning == nil {
				return nil, capabilityUnsupportedError("empty reasoning block")
			}
			reasoning.WriteString(block.Reasoning.Text)
		case einoschema.ContentBlockTypeFunctionToolCall:
			if block.FunctionToolCall == nil {
				return nil, capabilityUnsupportedError("empty function tool call block")
			}
			calls = append(calls, einoschema.ToolCall{
				ID:       block.FunctionToolCall.CallID,
				Type:     "function",
				Function: einoschema.FunctionCall{Name: block.FunctionToolCall.Name, Arguments: block.FunctionToolCall.Arguments},
			})
		case einoschema.ContentBlockTypeAssistantGenImage, einoschema.ContentBlockTypeAssistantGenAudio, einoschema.ContentBlockTypeAssistantGenVideo:
			return nil, capabilityUnsupportedError("assistant-generated media is unsupported by the classic adapter")
		default:
			return nil, capabilityUnsupportedError("unsupported assistant content block type %q", block.Type)
		}
	}
	out.Content = text.String()
	out.ReasoningContent = reasoning.String()
	out.ToolCalls = calls
	return out, nil
}

// classicMessageToAgenticDelta converts one classic streamed message into an
// agentic chunk. StreamingMeta.Index is a stable per-kind slot: 0 for text,
// 1 for reasoning, and 2+position (or 2+call.Index when the classic model
// sets it) for tool calls, so accumulation across chunks groups correctly.
func classicMessageToAgenticDelta(message *einoschema.Message) (*einoschema.AgenticMessage, Usage) {
	const (
		textIndex      = 0
		reasoningIndex = 1
		toolCallBase   = 2
	)
	var blocks []*einoschema.ContentBlock
	if message.Content != "" {
		blocks = append(blocks, einoschema.NewContentBlockChunk(&einoschema.AssistantGenText{Text: message.Content}, &einoschema.StreamingMeta{Index: textIndex}))
	}
	if message.ReasoningContent != "" {
		blocks = append(blocks, einoschema.NewContentBlockChunk(&einoschema.Reasoning{Text: message.ReasoningContent}, &einoschema.StreamingMeta{Index: reasoningIndex}))
	}
	for position, call := range message.ToolCalls {
		idx := toolCallBase + position
		if call.Index != nil {
			idx = toolCallBase + *call.Index
		}
		blocks = append(blocks, einoschema.NewContentBlockChunk(&einoschema.FunctionToolCall{
			CallID: call.ID, Name: call.Function.Name, Arguments: call.Function.Arguments,
		}, &einoschema.StreamingMeta{Index: idx}))
	}
	agentic := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: blocks}
	if len(message.Extra) != 0 {
		agentic.Extra = message.Extra
	}
	var usage Usage
	if message.ResponseMeta != nil && message.ResponseMeta.Usage != nil {
		tu := message.ResponseMeta.Usage
		usage = Usage{
			InputTokens:     int64(tu.PromptTokens),
			OutputTokens:    int64(tu.CompletionTokens),
			ReasoningTokens: int64(tu.CompletionTokensDetails.ReasoningTokens),
			CacheReadTokens: int64(tu.PromptTokenDetails.CachedTokens),
		}
		agentic.ResponseMeta = &einoschema.AgenticResponseMeta{TokenUsage: &einoschema.TokenUsage{
			PromptTokens:            tu.PromptTokens,
			CompletionTokens:        tu.CompletionTokens,
			TotalTokens:             tu.TotalTokens,
			CompletionTokensDetails: einoschema.CompletionTokensDetails{ReasoningTokens: tu.CompletionTokensDetails.ReasoningTokens},
			PromptTokenDetails:      einoschema.PromptTokenDetails{CachedTokens: tu.PromptTokenDetails.CachedTokens},
		}}
	}
	return agentic, usage
}

func strPtrIfSet(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
