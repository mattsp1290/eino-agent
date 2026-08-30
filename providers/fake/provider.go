package fake

import (
	"context"
	"errors"
	"sync/atomic"

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

// Build returns an immutable fake provider streamer.
func (p *Provider) Build(_ context.Context, selection model.Selection, runtime model.Runtime) (model.Streamer, error) {
	if p == nil {
		return nil, errors.New("fake provider is nil")
	}
	p.Builds.Add(1)
	return &providerStreamer{
		providerID: p.ID,
		modelID:    selection.ModelID,
		steps:      cloneSteps(p.Steps),
		runtime:    cloneRuntime(runtime),
	}, nil
}

type providerStreamer struct {
	providerID model.ProviderID
	modelID    model.ID
	steps      []Step
	runtime    model.Runtime
}

// StreamProvider returns a fake stream of normalized cumulative deltas.
func (s *providerStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	if s == nil {
		return nil, errors.New("fake provider streamer is nil")
	}
	if _, err := request.Clone(); err != nil {
		return nil, err
	}
	providerID := s.providerID
	modelID := s.modelID
	steps := cloneSteps(s.steps)
	reader, writer := einoschema.Pipe[model.StreamDelta](len(steps))
	go func() {
		defer writer.Close()
		var usage model.Usage
		for _, step := range steps {
			if err := ctx.Err(); err != nil {
				writer.Send(model.StreamDelta{Usage: usage}, err)
				return
			}
			if step.Err != nil {
				err := normalizeError(step.Err)
				writer.Send(model.StreamDelta{Usage: usage}, err)
				return
			}
			usage = addUsage(usage, step.Usage)
			msg := messageForStep(providerID, modelID, step)
			delta := model.StreamDelta{
				Message: msg,
				Usage:   usage,
			}
			if writer.Send(delta, nil) {
				return
			}
		}
	}()
	return reader, nil
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
