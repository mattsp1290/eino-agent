package fake

import (
	"context"
	"errors"
	"sync/atomic"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

// Step is one scripted fake provider stream chunk.
type Step struct {
	Content string
	Usage   model.Usage
	Err     error
}

// Provider is an in-memory model.Adapter implementation.
type Provider struct {
	ID          model.ProviderID
	Name        string
	Descriptors []model.Descriptor
	Steps       []Step
	Builds      atomic.Int64
	Options     map[string]string
}

// Provider returns provider metadata.
func (p *Provider) Provider() model.Provider {
	if p == nil {
		return model.Provider{}
	}
	return model.Provider{
		ID:      p.ID,
		Name:    p.Name,
		Source:  "fake",
		Options: cloneMap(p.Options),
	}
}

// Models returns configured model metadata.
func (p *Provider) Models(context.Context) ([]model.Descriptor, error) {
	if p == nil {
		return nil, errors.New("fake provider is nil")
	}
	models := make([]model.Descriptor, len(p.Descriptors))
	for i, descriptor := range p.Descriptors {
		models[i] = cloneDescriptor(descriptor)
	}
	return models, nil
}

// Build returns an immutable fake Eino chat model.
func (p *Provider) Build(_ context.Context, selection model.Selection, runtime model.Runtime) (einomodel.ToolCallingChatModel, error) {
	if p == nil {
		return nil, errors.New("fake provider is nil")
	}
	p.Builds.Add(1)
	return &chatModel{
		providerID: p.ID,
		modelID:    selection.ModelID,
		steps:      cloneSteps(p.Steps),
		runtime:    cloneRuntime(runtime),
	}, nil
}

// StreamProvider returns a fake stream and emits normalized observer callbacks.
func (p *Provider) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	if p == nil {
		return nil, errors.New("fake provider is nil")
	}
	req := request.Clone()
	if req.Observer != nil {
		req.Observer.OnProviderStart(ctx, req)
	}
	providerID := p.ID
	modelID := req.Identity.ModelID
	steps := cloneSteps(p.Steps)
	reader, writer := einoschema.Pipe[*einoschema.Message](len(steps))
	go func() {
		defer writer.Close()
		var usage model.Usage
		for index, step := range steps {
			if err := ctx.Err(); err != nil {
				notifyError(ctx, req.Observer, err)
				writer.Send(nil, err)
				return
			}
			if step.Err != nil {
				err := normalizeError(step.Err)
				notifyError(ctx, req.Observer, err)
				writer.Send(nil, err)
				return
			}
			usage = addUsage(usage, step.Usage)
			msg := messageForStep(providerID, modelID, step)
			delta := model.StreamDelta{
				Message: msg,
				Usage:   step.Usage,
				Index:   int64(index),
				Done:    index == len(steps)-1,
			}
			if req.Observer != nil {
				req.Observer.OnProviderDelta(ctx, delta)
			}
			if writer.Send(msg, nil) {
				return
			}
		}
		if req.Observer != nil {
			req.Observer.OnProviderEnd(ctx, model.Response{
				Message: einoschema.AssistantMessage("", nil),
				Usage:   usage,
			})
		}
	}()
	return reader, nil
}

type chatModel struct {
	providerID model.ProviderID
	modelID    model.ID
	steps      []Step
	runtime    model.Runtime
	tools      []*einoschema.ToolInfo
}

func (m *chatModel) Generate(ctx context.Context, input []*einoschema.Message, opts ...einomodel.Option) (*einoschema.Message, error) {
	_ = input
	_ = opts
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var content string
	var usage model.Usage
	for _, step := range m.steps {
		if step.Err != nil {
			return nil, normalizeError(step.Err)
		}
		content += step.Content
		usage = addUsage(usage, step.Usage)
	}
	msg := einoschema.AssistantMessage(content, nil)
	msg.Extra = map[string]any{
		"provider_id": string(m.providerID),
		"model_id":    string(m.modelID),
		"usage":       usage,
	}
	return msg, nil
}

func (m *chatModel) Stream(ctx context.Context, input []*einoschema.Message, opts ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	_ = input
	_ = opts
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[*einoschema.Message](len(m.steps))
	go func() {
		defer writer.Close()
		for _, step := range m.steps {
			if err := ctx.Err(); err != nil {
				writer.Send(nil, err)
				return
			}
			if step.Err != nil {
				writer.Send(nil, normalizeError(step.Err))
				return
			}
			msg := messageForStep(m.providerID, m.modelID, step)
			if writer.Send(msg, nil) {
				return
			}
		}
	}()
	return reader, nil
}

func (m *chatModel) WithTools(tools []*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	next := *m
	next.tools = cloneTools(tools)
	return &next, nil
}

func notifyError(ctx context.Context, observer model.StreamObserver, err error) {
	if observer == nil {
		return
	}
	var providerErr model.Error
	if errors.As(err, &providerErr) {
		observer.OnProviderError(ctx, providerErr)
		return
	}
	observer.OnProviderError(ctx, model.Error{
		Code:    "fake_provider_error",
		Message: err.Error(),
		Cause:   err,
	})
}

func messageForStep(providerID model.ProviderID, modelID model.ID, step Step) *einoschema.Message {
	msg := einoschema.AssistantMessage(step.Content, nil)
	msg.Extra = map[string]any{
		"provider_id": string(providerID),
		"model_id":    string(modelID),
		"usage":       step.Usage,
	}
	return msg
}

func normalizeError(err error) error {
	var providerErr model.Error
	if errors.As(err, &providerErr) {
		return providerErr
	}
	switch {
	case errors.Is(err, model.ErrProviderRateLimited):
		return model.Error{
			Code:      "provider_rate_limited",
			Message:   err.Error(),
			Retryable: true,
			Cause:     err,
		}
	case errors.Is(err, model.ErrProviderUnavailable):
		return model.Error{
			Code:      "provider_unavailable",
			Message:   err.Error(),
			Retryable: true,
			Cause:     err,
		}
	case errors.Is(err, model.ErrProviderRejected):
		return model.Error{
			Code:    "provider_rejected",
			Message: err.Error(),
			Cause:   err,
		}
	}
	return model.Error{
		Code:    "fake_provider_error",
		Message: err.Error(),
		Cause:   err,
	}
}

func addUsage(left model.Usage, right model.Usage) model.Usage {
	left.InputTokens += right.InputTokens
	left.OutputTokens += right.OutputTokens
	left.ReasoningTokens += right.ReasoningTokens
	left.CacheReadTokens += right.CacheReadTokens
	left.CacheWriteTokens += right.CacheWriteTokens
	left.Cost += right.Cost
	return left
}

func cloneSteps(src []Step) []Step {
	if src == nil {
		return nil
	}
	dst := make([]Step, len(src))
	copy(dst, src)
	return dst
}

func cloneTools(src []*einoschema.ToolInfo) []*einoschema.ToolInfo {
	if src == nil {
		return nil
	}
	dst := make([]*einoschema.ToolInfo, len(src))
	copy(dst, src)
	return dst
}

func cloneRuntime(src model.Runtime) model.Runtime {
	next := src
	next.Env = cloneMap(src.Env)
	next.Auth = cloneMap(src.Auth)
	next.Options = cloneMap(src.Options)
	return next
}

func cloneDescriptor(src model.Descriptor) model.Descriptor {
	next := src
	next.Capabilities = cloneBoolMap(src.Capabilities)
	next.Options = cloneMap(src.Options)
	return next
}

func cloneMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if src == nil {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
