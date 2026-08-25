package model

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

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

func (s *einoStreamer) StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	if s == nil || s.client == nil {
		return nil, Error{Code: "model_client_missing", Message: "Eino model client missing", Cause: ErrProviderUnavailable}
	}
	req, err := request.Clone()
	if err != nil {
		return nil, err
	}
	if err := notifyStart(ctx, req.Observer, req); err != nil {
		return nil, err
	}
	client := s.client
	if len(req.Tools) != 0 {
		client, err = client.WithTools(req.Tools)
		if err != nil {
			notifyProviderError(ctx, req.Observer, err)
			return nil, err
		}
	}
	messages := req.Messages
	if req.System != "" {
		messages = append([]*einoschema.Message{einoschema.SystemMessage(req.System)}, messages...)
	}
	upstream, err := client.Stream(ctx, messages)
	if err != nil {
		notifyProviderError(ctx, req.Observer, err)
		return nil, err
	}
	if upstream == nil {
		err = Error{Code: "nil_provider_stream", Message: "Eino model returned nil stream"}
		notifyProviderError(ctx, req.Observer, err)
		return nil, err
	}
	var index atomic.Int64
	return einoschema.StreamReaderWithConvert(upstream, func(message *einoschema.Message) (*einoschema.Message, error) {
		if err := notifyDelta(ctx, req.Observer, StreamDelta{Message: message, Index: index.Add(1) - 1}); err != nil {
			return nil, err
		}
		return message, nil
	}, einoschema.WithErrWrapper(func(err error) error {
		notifyProviderError(ctx, req.Observer, err)
		return err
	})), nil
}

func notifyStart(ctx context.Context, observer StreamObserver, request Request) (err error) {
	if observer == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("provider observer start panic: %v", recovered)
		}
	}()
	observer.OnProviderStart(ctx, request)
	return nil
}

func notifyDelta(ctx context.Context, observer StreamObserver, delta StreamDelta) (err error) {
	if observer == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("provider observer delta panic: %v", recovered)
		}
	}()
	observer.OnProviderDelta(ctx, delta)
	return nil
}

func notifyProviderError(ctx context.Context, observer StreamObserver, err error) {
	if observer == nil {
		return
	}
	providerErr := Error{Code: "provider_stream_error", Message: err.Error(), Cause: err}
	var normalized Error
	if errors.As(err, &normalized) {
		providerErr = normalized
	}
	func() {
		defer func() { _ = recover() }()
		observer.OnProviderError(ctx, providerErr)
	}()
}
