package model

import (
	"context"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
)

// NewAgenticStreamer adapts one immutable Eino agentic model to the provider
// boundary. Controls are translated to call-time options exactly once per
// request; nothing is mutated on client.
func NewAgenticStreamer(client einomodel.AgenticModel) Streamer {
	if client == nil {
		return nil
	}
	return &agenticStreamer{client: client}
}

type agenticStreamer struct {
	client einomodel.AgenticModel
}

func (s *agenticStreamer) StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[StreamDelta], error) {
	if s == nil || s.client == nil {
		return nil, Error{Code: "model_client_missing", Message: "Eino agentic model client missing", Cause: ErrProviderUnavailable}
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
	return streamAgentic(ctx, s.client, req, false)
}

// streamAgentic dispatches one already-cloned, already-validated request.
func streamAgentic(ctx context.Context, client einomodel.AgenticModel, req Request, retainResponseMetaState bool) (*einoschema.StreamReader[StreamDelta], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	messages := req.Messages
	if req.System != "" {
		messages = append([]*einoschema.AgenticMessage{einoschema.SystemAgenticMessage(req.System)}, messages...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	upstream, err := client.Stream(ctx, messages, agenticCallOptions(req.Controls)...)
	if err != nil {
		return nil, err
	}
	if upstream == nil {
		return nil, Error{Code: "nil_provider_stream", Message: "Eino agentic model returned nil stream"}
	}
	return einoschema.StreamReaderWithConvert(upstream, func(message *einoschema.AgenticMessage) (StreamDelta, error) {
		if message == nil {
			return StreamDelta{}, Error{Code: "malformed_provider_stream", Message: "Eino agentic model returned a nil chunk", Cause: ErrProviderRejected}
		}
		normalized, err := normalizeResponseMetaExtension(message, retainResponseMetaState)
		if err != nil {
			return StreamDelta{}, providerStateError(ErrProviderStateInvalid)
		}
		return StreamDelta{Message: normalized, Usage: UsageFromAgenticMessage(normalized)}, nil
	}), nil
}

// normalizeResponseMetaExtension clones only the message/meta shells. A
// provider extension is never mutated. Plain streaming removes the declared
// state before any public serialization; stateful streaming substitutes an
// agent-owned raw marker for later Capture.
func normalizeResponseMetaExtension(message *einoschema.AgenticMessage, retain bool) (*einoschema.AgenticMessage, error) {
	if message == nil || message.ResponseMeta == nil || message.ResponseMeta.Extension == nil {
		return message, nil
	}
	state, ok := message.ResponseMeta.Extension.(AgenticResponseMetaExtensionState)
	if !ok {
		return nil, ErrProviderStateInvalid
	}
	raw, err := state.EinoAgentResponseMetaState()
	if err != nil {
		return nil, err
	}
	if _, err := parseAgenticResponseMetaState(raw); err != nil {
		return nil, err
	}
	clone := *message
	meta := *message.ResponseMeta
	if retain {
		meta.Extension = responseMetaStateMarker(raw)
	} else {
		meta.Extension = nil
	}
	clone.ResponseMeta = &meta
	return &clone, nil
}

// agenticCallOptions translates RequestControls into einomodel.Option values
// in a fixed order: Tools (always, so an empty/nil list clears), DeferredTools
// (only when set), ToolSearchTool (when set), AgenticToolChoice (when set),
// then the scalar controls in Temperature, TopP, MaxTokens, Stop order.
func agenticCallOptions(c RequestControls) []einomodel.Option {
	opts := make([]einomodel.Option, 0, 8)
	opts = append(opts, einomodel.WithTools(c.Tools))
	if c.DeferredTools != nil {
		opts = append(opts, einomodel.WithDeferredTools(c.DeferredTools))
	}
	if c.ToolSearchTool != nil {
		opts = append(opts, einomodel.WithToolSearchTool(c.ToolSearchTool))
	}
	if c.ToolChoice != nil {
		opts = append(opts, einomodel.WithAgenticToolChoice(c.ToolChoice))
	}
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
	return opts
}
