package fake

import (
	"context"
	"errors"
	"io"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

// textOf concatenates every assistant_gen_text block's text, in order.
func textOf(msg *einoschema.AgenticMessage) string {
	if msg == nil {
		return ""
	}
	var out string
	for _, block := range msg.ContentBlocks {
		if block != nil && block.Type == einoschema.ContentBlockTypeAssistantGenText && block.AssistantGenText != nil {
			out += block.AssistantGenText.Text
		}
	}
	return out
}

func TestStreamProviderEmitsCumulativeUsageAndChunks(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		ID: "fake",
		Steps: []Step{
			{Content: "hello ", Usage: model.Usage{InputTokens: 3}},
			{Content: "world", Usage: model.Usage{OutputTokens: 2}},
		},
	}
	streamer, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, model.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := streamer.StreamProvider(context.Background(), model.Request{
		Identity: model.Identity{ProviderID: "fake", ModelID: "m1"},
	})
	if err != nil {
		t.Fatalf("StreamProvider error = %v", err)
	}
	defer reader.Close()

	var content string
	var usages []model.Usage
	for {
		delta, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv error = %v", err)
		}
		content += textOf(delta.Message)
		usages = append(usages, delta.Usage)
	}
	if content != "hello world" {
		t.Fatalf("content = %q, want hello world", content)
	}
	if len(usages) != 2 || usages[0].InputTokens != 3 || usages[0].OutputTokens != 0 {
		t.Fatalf("first usage = %#v", usages)
	}
	if usages[1].InputTokens != 3 || usages[1].OutputTokens != 2 {
		t.Fatalf("final usage = %#v", usages[1])
	}
}

func TestStreamProviderNormalizesErrors(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		ID:    "fake",
		Steps: []Step{{Err: errors.New("boom")}},
	}
	streamer, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, model.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := streamer.StreamProvider(context.Background(), model.Request{})
	if err != nil {
		t.Fatalf("StreamProvider error = %v", err)
	}
	defer reader.Close()

	_, err = reader.Recv()
	var providerErr model.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("Recv error = %T %[1]v, want model.Error", err)
	}
	if providerErr.Code != "fake_provider_error" {
		t.Fatalf("provider err = %#v", providerErr)
	}
}

func TestStreamProviderSnapshotsProviderState(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		ID:    "fake",
		Steps: []Step{{Content: "original"}},
	}
	streamer, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, model.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	provider.ID = "mutated"
	provider.Steps = []Step{{Content: "mutated"}}
	reader, err := streamer.StreamProvider(context.Background(), model.Request{
		Identity: model.Identity{ModelID: "m1"},
	})
	if err != nil {
		t.Fatalf("StreamProvider error = %v", err)
	}
	defer reader.Close()
	delta, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv error = %v", err)
	}
	if textOf(delta.Message) != "original" {
		t.Fatalf("content = %q, want original", textOf(delta.Message))
	}
	if len(delta.Message.Extra) != 0 {
		t.Fatalf("agentic chunks must not carry Extra, got %v", delta.Message.Extra)
	}
}

func TestProviderSentinelErrorsPreserveRetryability(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		err       error
		code      string
		retryable bool
	}{
		{name: "rate limited", err: model.ErrProviderRateLimited, code: "provider_rate_limited", retryable: true},
		{name: "unavailable", err: model.ErrProviderUnavailable, code: "provider_unavailable", retryable: true},
		{name: "rejected", err: model.ErrProviderRejected, code: "provider_rejected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			provider := &Provider{ID: "fake", Steps: []Step{{Err: test.err}}}
			built, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, model.Runtime{})
			if err != nil {
				t.Fatalf("Build error = %v", err)
			}
			reader, streamErr := built.StreamProvider(context.Background(), model.Request{})
			if streamErr != nil {
				t.Fatalf("StreamProvider error = %v", streamErr)
			}
			defer reader.Close()
			_, err = reader.Recv()
			var providerErr model.Error
			if !errors.As(err, &providerErr) {
				t.Fatalf("Generate error = %T %[1]v, want model.Error", err)
			}
			if providerErr.Code != test.code || providerErr.Retryable != test.retryable {
				t.Fatalf("provider error = %#v", providerErr)
			}
		})
	}
}

func TestStepBlocksAreEmittedWithStreamingIndex(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		ID: "fake",
		Steps: []Step{{Blocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlockChunk(&einoschema.Reasoning{Text: "thinking"}, &einoschema.StreamingMeta{Index: 1}),
			einoschema.NewContentBlockChunk(&einoschema.AssistantGenText{Text: "answer"}, &einoschema.StreamingMeta{Index: 0}),
		}}},
	}
	streamer, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, model.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := streamer.StreamProvider(context.Background(), model.Request{})
	if err != nil {
		t.Fatalf("StreamProvider error = %v", err)
	}
	defer reader.Close()
	delta, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv error = %v", err)
	}
	if len(delta.Message.ContentBlocks) != 2 {
		t.Fatalf("blocks = %d, want 2", len(delta.Message.ContentBlocks))
	}
	if delta.Message.ContentBlocks[0].StreamingMeta == nil || delta.Message.ContentBlocks[0].StreamingMeta.Index != 1 {
		t.Fatalf("block 0 streaming meta = %#v", delta.Message.ContentBlocks[0].StreamingMeta)
	}
	if delta.Message.ContentBlocks[1].StreamingMeta == nil || delta.Message.ContentBlocks[1].StreamingMeta.Index != 0 {
		t.Fatalf("block 1 streaming meta = %#v", delta.Message.ContentBlocks[1].StreamingMeta)
	}
}

func TestNewAgenticModelDirectUse(t *testing.T) {
	t.Parallel()

	agentic := NewAgenticModel("fake", "m1", []Step{{Content: "hi"}})
	reader, err := agentic.Stream(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	msg, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv error = %v", err)
	}
	if textOf(msg) != "hi" {
		t.Fatalf("content = %q, want hi", textOf(msg))
	}
}

func TestBuildClonesRuntime(t *testing.T) {
	t.Parallel()

	provider := &Provider{ID: "fake", Steps: []Step{{Content: "ok"}}}
	runtime := model.Runtime{Options: map[string]string{"session": "a"}}
	built, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, runtime)
	if err != nil {
		t.Fatalf("Build error = %v", err)
	}
	base := built.(*providerStreamer)
	runtime.Options["session"] = "changed"
	if base.runtime.Options["session"] != "a" {
		t.Fatalf("runtime options mutated to %q", base.runtime.Options["session"])
	}
}

// TestProviderStepBlocksAreClonedIndependently guards against a fixture's
// Step.Blocks being shared by reference across streams: the runtime clears
// StreamingMeta on the blocks it dispatches (runtime.clearStreamingMeta), and
// Eino's single-chunk ConcatAgenticMessages returns the chunk unmodified, so
// a shared *ContentBlock would (a) make a replayed Provider emit different
// blocks on its second use and (b) race under concurrent streams. cloneSteps
// must deep-copy each block and its StreamingMeta so every stream — and the
// original fixture — owns independent *ContentBlock values.
func TestProviderStepBlocksAreClonedIndependently(t *testing.T) {
	t.Parallel()

	originalBlock := einoschema.NewContentBlockChunk(&einoschema.AssistantGenText{Text: "answer"}, &einoschema.StreamingMeta{Index: 0})
	provider := &Provider{
		ID:    "fake",
		Steps: []Step{{Blocks: []*einoschema.ContentBlock{originalBlock}}},
	}

	streamOnce := func() *einoschema.ContentBlock {
		streamer, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, model.Runtime{})
		if err != nil {
			t.Fatal(err)
		}
		reader, err := streamer.StreamProvider(context.Background(), model.Request{})
		if err != nil {
			t.Fatalf("StreamProvider error = %v", err)
		}
		defer reader.Close()
		delta, err := reader.Recv()
		if err != nil {
			t.Fatalf("Recv error = %v", err)
		}
		if len(delta.Message.ContentBlocks) != 1 {
			t.Fatalf("blocks = %d, want 1", len(delta.Message.ContentBlocks))
		}
		return delta.Message.ContentBlocks[0]
	}

	first := streamOnce()
	second := streamOnce()

	if first == originalBlock || second == originalBlock {
		t.Fatal("emitted block shares a pointer with the fixture's Step.Blocks entry")
	}
	if first == second {
		t.Fatal("two streams over one Provider emitted the same *ContentBlock pointer")
	}
	if first.StreamingMeta == originalBlock.StreamingMeta || second.StreamingMeta == originalBlock.StreamingMeta {
		t.Fatal("emitted StreamingMeta shares a pointer with the fixture's")
	}

	// Simulate runtime.clearStreamingMeta mutating the first emitted message
	// in place; neither the fixture nor the second stream's block may see it.
	first.StreamingMeta = nil

	if originalBlock.StreamingMeta == nil {
		t.Fatal("clearing the first stream's block mutated the fixture's Step.Blocks entry")
	}
	if second.StreamingMeta == nil {
		t.Fatal("clearing the first stream's block mutated the second stream's block")
	}

	third := streamOnce()
	if third.StreamingMeta == nil {
		t.Fatal("replaying the Provider after a prior stream's mutation emitted a block with nil StreamingMeta")
	}
}
