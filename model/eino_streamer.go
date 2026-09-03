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
	if len(req.ProviderState) != 0 {
		return nil, providerStateError(ErrProviderStateMismatch)
	}
	return streamEino(ctx, s.client, req)
}

type einoProviderStateStreamer struct {
	client    einomodel.ToolCallingChatModel
	codec     ProviderStateCodec
	contract  ProviderStateContract
	owned     map[string]struct{}
	ownedKeys []string
}

// NewEinoStreamerWithProviderState adapts an Eino model with one immutable
// provider-state contract. State is restored only on the final private clone.
func NewEinoStreamerWithProviderState(client einomodel.ToolCallingChatModel, codec ProviderStateCodec) (ProviderStateStreamer, error) {
	if client == nil || codec == nil {
		return nil, providerStateError(ErrProviderStateInvalid)
	}
	contract, owned, ownedKeys, err := snapshotProviderStateCodec(codec)
	if err != nil {
		return nil, err
	}
	return &einoProviderStateStreamer{client: client, codec: codec, contract: cloneProviderStateContract(contract), owned: owned, ownedKeys: ownedKeys}, nil
}

func (s *einoProviderStateStreamer) ProviderStateContract() ProviderStateContract {
	return cloneProviderStateContract(s.contract)
}

func (s *einoProviderStateStreamer) ProviderStateOwnedExtraKeys() []string {
	return append([]string(nil), s.ownedKeys...)
}

func (s *einoProviderStateStreamer) CaptureProviderState(message *einoschema.Message) (ProviderStateCapture, error) {
	if s == nil || s.codec == nil {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	return guardedCapture(s.codec, s.owned, s.contract, message)
}

func (s *einoProviderStateStreamer) StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[StreamDelta], error) {
	if s == nil || s.client == nil || s.codec == nil {
		return nil, Error{Code: "model_client_missing", Message: "Eino model client missing", Cause: ErrProviderUnavailable}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req, err := request.Clone()
	if err != nil {
		return nil, err
	}
	if len(req.ProviderState) != 0 {
		if err := ValidateProviderStateIdentity(string(req.Identity.ProviderID), string(req.Identity.ModelID)); err != nil {
			return nil, err
		}
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
		if message == nil || message.Role != einoschema.Assistant || len(message.Extra) != 0 {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if err := ValidateProviderStateItems(state.Items, s.contract.Limits); err != nil {
			return nil, err
		}
		if err := restoreProviderState(s.codec, s.owned, message, state.Items); err != nil {
			return nil, err
		}
		previousIndex = state.MessageIndex
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return streamEino(ctx, s.client, req)
}

func restoreProviderState(codec ProviderStateCodec, owned map[string]struct{}, message *einoschema.Message, items []ProviderStateItem) (err error) {
	defer func() {
		if recover() != nil {
			err = providerStateError(ErrProviderStateInvalid)
		}
	}()
	if err := codec.RestoreAssistant(message, cloneProviderStateItems(items)); err != nil {
		return providerStateErrorFrom(err)
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

func streamEino(ctx context.Context, base einomodel.ToolCallingChatModel, req Request) (*einoschema.StreamReader[StreamDelta], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client := base
	var err error
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
	if err := ctx.Err(); err != nil {
		return nil, err
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
