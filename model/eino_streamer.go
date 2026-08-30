package model

import (
	"context"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
)

// NewEinoStreamer adapts one immutable Eino chat model to the provider boundary.
// Tools are bound exactly once for each request.
func NewEinoStreamer(client einomodel.ToolCallingChatModel) Streamer {
	if client == nil {
		return nil
	}
	return &einoStreamer{client: client}
}

type einoStreamer struct {
	client einomodel.ToolCallingChatModel
}

func (s *einoStreamer) StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[StreamDelta], error) {
	if s == nil || s.client == nil {
		return nil, Error{Code: "model_client_missing", Message: "Eino model client missing", Cause: ErrProviderUnavailable}
	}
	req, err := request.Clone()
	if err != nil {
		return nil, err
	}
	client := s.client
	if len(req.Tools) != 0 {
		client, err = client.WithTools(req.Tools)
		if err != nil {
			return nil, err
		}
	}
	messages := req.Messages
	if req.System != "" {
		messages = append([]*einoschema.Message{einoschema.SystemMessage(req.System)}, messages...)
	}
	upstream, err := client.Stream(ctx, messages)
	if err != nil {
		return nil, err
	}
	if upstream == nil {
		err = Error{Code: "nil_provider_stream", Message: "Eino model returned nil stream"}
		return nil, err
	}
	return einoschema.StreamReaderWithConvert(upstream, func(message *einoschema.Message) (StreamDelta, error) {
		return StreamDelta{Message: message, Usage: UsageFromMessage(message)}, nil
	}), nil
}

// UsageFromMessage maps the usage metadata Eino exposes on a streamed message
// into the provider-neutral cumulative usage shape.
func UsageFromMessage(message *einoschema.Message) Usage {
	if message == nil || message.ResponseMeta == nil || message.ResponseMeta.Usage == nil {
		return Usage{}
	}
	usage := message.ResponseMeta.Usage
	return Usage{
		InputTokens:     int64(usage.PromptTokens),
		OutputTokens:    int64(usage.CompletionTokens),
		ReasoningTokens: int64(usage.CompletionTokensDetails.ReasoningTokens),
		CacheReadTokens: int64(usage.PromptTokenDetails.CachedTokens),
	}
}
