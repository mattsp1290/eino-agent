package fake

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

// Step is one scripted fake provider stream chunk. When Blocks is non-empty
// it is emitted as-is, with each block's StreamingMeta.Index set to its
// position in Blocks. Otherwise, when Content is non-empty, it becomes a
// single assistant_gen_text block at index 0.
type Step struct {
	Content string
	Blocks  []*einoschema.ContentBlock
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

// Build returns an immutable fake agentic provider streamer.
func (p *Provider) Build(_ context.Context, selection model.Selection, runtime model.Runtime) (model.Streamer, error) {
	if p == nil {
		return nil, errors.New("fake provider is nil")
	}
	p.Builds.Add(1)
	agentic := NewAgenticModel(p.ID, selection.ModelID, p.Steps)
	return &providerStreamer{inner: model.NewAgenticStreamer(agentic), runtime: cloneRuntime(runtime)}, nil
}

// providerStreamer wraps the model.NewAgenticStreamer result so Build keeps
// its own defensive Runtime clone (independent of any clone the resolver
// already performed), matching the immutable-adapter contract other
// adapters follow.
type providerStreamer struct {
	inner   model.Streamer
	runtime model.Runtime
}

func (s *providerStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	return s.inner.StreamProvider(ctx, request)
}

// NewAgenticModel returns an einomodel.AgenticModel that plays back steps as
// scripted stream chunks, tagging each emitted message with providerID and
// modelID. It performs no network I/O and can be used directly by tests that
// need an einomodel.AgenticModel rather than a model.Streamer.
func NewAgenticModel(providerID model.ProviderID, modelID model.ID, steps []Step) einomodel.AgenticModel {
	return &agenticModel{providerID: providerID, modelID: modelID, steps: cloneSteps(steps)}
}

type agenticModel struct {
	providerID model.ProviderID
	modelID    model.ID
	steps      []Step
}

func (m *agenticModel) Generate(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	reader, err := m.Stream(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var messages []*einoschema.AgenticMessage
	for {
		msg, err := reader.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		messages = append(messages, msg)
	}
	if len(messages) == 0 {
		return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant}, nil
	}
	return einoschema.ConcatAgenticMessages(messages)
}

func (m *agenticModel) Stream(ctx context.Context, _ []*einoschema.AgenticMessage, _ ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	providerID := m.providerID
	modelID := m.modelID
	steps := cloneSteps(m.steps)
	reader, writer := einoschema.Pipe[*einoschema.AgenticMessage](len(steps))
	go func() {
		defer writer.Close()
		var usage model.Usage
		for _, step := range steps {
			if err := ctx.Err(); err != nil {
				writer.Send(nil, err)
				return
			}
			if step.Err != nil {
				writer.Send(nil, normalizeError(step.Err))
				return
			}
			usage = addUsage(usage, step.Usage)
			msg := agenticMessageForStep(providerID, modelID, step, usage)
			if writer.Send(msg, nil) {
				return
			}
		}
	}()
	return reader, nil
}

// agenticMessageForStep builds the message for one scripted step. usage is
// the cumulative attempt-to-date usage (steps carry per-step deltas; the
// emitted message carries the running total, matching StreamDelta.Usage's
// documented cumulative contract).
func agenticMessageForStep(providerID model.ProviderID, modelID model.ID, step Step, usage model.Usage) *einoschema.AgenticMessage {
	blocks := step.Blocks
	if len(blocks) == 0 && step.Content != "" {
		blocks = []*einoschema.ContentBlock{einoschema.NewContentBlockChunk(&einoschema.AssistantGenText{Text: step.Content}, &einoschema.StreamingMeta{Index: 0})}
	}
	msg := &einoschema.AgenticMessage{
		Role:          einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: blocks,
		ResponseMeta: &einoschema.AgenticResponseMeta{TokenUsage: &einoschema.TokenUsage{
			PromptTokens:            int(usage.InputTokens),
			CompletionTokens:        int(usage.OutputTokens),
			TotalTokens:             int(usage.InputTokens + usage.OutputTokens),
			CompletionTokensDetails: einoschema.CompletionTokensDetails{ReasoningTokens: int(usage.ReasoningTokens)},
			PromptTokenDetails:      einoschema.PromptTokenDetails{CachedTokens: int(usage.CacheReadTokens)},
		}},
	}
	// Provider and model identity are request identity, not message Extra:
	// the agentic content contract rejects Extra on messages.
	_ = providerID
	_ = modelID
	return msg
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

func cloneSteps(src []Step) []Step {
	if src == nil {
		return nil
	}
	dst := make([]Step, len(src))
	for i, step := range src {
		dst[i] = step
		dst[i].Blocks = cloneContentBlocks(step.Blocks)
	}
	return dst
}

// cloneContentBlocks deep-copies each block (and its StreamingMeta) so that
// fixtures handed to Provider are never mutated by a consumer. The runtime
// clears StreamingMeta on the blocks it dispatches (clearStreamingMeta), and
// Eino's single-chunk ConcatAgenticMessages returns the input unchanged, so
// without this copy a replayed Provider or two concurrent streams over one
// Provider would read and write the same *ContentBlock from different call
// sites.
func cloneContentBlocks(src []*einoschema.ContentBlock) []*einoschema.ContentBlock {
	if src == nil {
		return nil
	}
	dst := make([]*einoschema.ContentBlock, len(src))
	for i, b := range src {
		if b == nil {
			continue
		}
		clone := *b
		if b.StreamingMeta != nil {
			meta := *b.StreamingMeta
			clone.StreamingMeta = &meta
		}
		dst[i] = &clone
	}
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
