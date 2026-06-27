package fake

import (
	"context"
	"errors"
	"io"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

func TestStreamProviderEmitsCallbacksUsageAndChunks(t *testing.T) {
	t.Parallel()

	observer := &recordingObserver{}
	provider := &Provider{
		ID: "fake",
		Steps: []Step{
			{Content: "hello ", Usage: model.Usage{InputTokens: 3}},
			{Content: "world", Usage: model.Usage{OutputTokens: 2}},
		},
	}
	reader, err := provider.StreamProvider(context.Background(), model.Request{
		Identity: model.Identity{ProviderID: "fake", ModelID: "m1"},
		Observer: observer,
	})
	if err != nil {
		t.Fatalf("StreamProvider error = %v", err)
	}
	defer reader.Close()

	var content string
	for {
		msg, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv error = %v", err)
		}
		content += msg.Content
	}
	if content != "hello world" {
		t.Fatalf("content = %q, want hello world", content)
	}
	if observer.started != 1 || observer.ended != 1 || len(observer.deltas) != 2 {
		t.Fatalf("observer started=%d ended=%d deltas=%d", observer.started, observer.ended, len(observer.deltas))
	}
	if observer.response.Usage.InputTokens != 3 || observer.response.Usage.OutputTokens != 2 {
		t.Fatalf("usage = %#v", observer.response.Usage)
	}
}

func TestStreamProviderNormalizesErrors(t *testing.T) {
	t.Parallel()

	observer := &recordingObserver{}
	provider := &Provider{
		ID:    "fake",
		Steps: []Step{{Err: errors.New("boom")}},
	}
	reader, err := provider.StreamProvider(context.Background(), model.Request{Observer: observer})
	if err != nil {
		t.Fatalf("StreamProvider error = %v", err)
	}
	defer reader.Close()

	_, err = reader.Recv()
	var providerErr model.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("Recv error = %T %[1]v, want model.Error", err)
	}
	if observer.err.Code != "fake_provider_error" {
		t.Fatalf("observer err = %#v", observer.err)
	}
}

func TestStreamProviderSnapshotsProviderState(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		ID:    "fake",
		Steps: []Step{{Content: "original"}},
	}
	reader, err := provider.StreamProvider(context.Background(), model.Request{
		Identity: model.Identity{ModelID: "m1"},
	})
	if err != nil {
		t.Fatalf("StreamProvider error = %v", err)
	}
	defer reader.Close()
	provider.ID = "mutated"
	provider.Steps = []Step{{Content: "mutated"}}

	msg, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv error = %v", err)
	}
	if msg.Content != "original" {
		t.Fatalf("content = %q, want original", msg.Content)
	}
	if msg.Extra["provider_id"] != "fake" {
		t.Fatalf("provider id = %q, want fake", msg.Extra["provider_id"])
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
			_, err = built.Generate(context.Background(), nil)
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

func TestBuildClonesRuntimeAndWithToolsDoesNotMutateBase(t *testing.T) {
	t.Parallel()

	provider := &Provider{ID: "fake", Steps: []Step{{Content: "ok"}}}
	runtime := model.Runtime{Options: map[string]string{"session": "a"}}
	built, err := provider.Build(context.Background(), model.Selection{ProviderID: "fake", ModelID: "m1"}, runtime)
	if err != nil {
		t.Fatalf("Build error = %v", err)
	}
	base := built.(*chatModel)
	runtime.Options["session"] = "changed"
	if base.runtime.Options["session"] != "a" {
		t.Fatalf("runtime options mutated to %q", base.runtime.Options["session"])
	}
	withTools, err := base.WithTools([]*einoschema.ToolInfo{{Name: "tool-a"}})
	if err != nil {
		t.Fatalf("WithTools error = %v", err)
	}
	withToolsModel := withTools.(*chatModel)
	if len(base.tools) != 0 {
		t.Fatalf("base tools mutated: %d", len(base.tools))
	}
	if len(withToolsModel.tools) != 1 || withToolsModel.tools[0].Name != "tool-a" {
		t.Fatalf("withTools tools = %#v", withToolsModel.tools)
	}
}

type recordingObserver struct {
	started  int
	ended    int
	deltas   []model.StreamDelta
	err      model.Error
	response model.Response
}

func (o *recordingObserver) OnProviderStart(context.Context, model.Request) {
	o.started++
}

func (o *recordingObserver) OnProviderDelta(_ context.Context, delta model.StreamDelta) {
	o.deltas = append(o.deltas, delta)
}

func (o *recordingObserver) OnProviderError(_ context.Context, err model.Error) {
	o.err = err
}

func (o *recordingObserver) OnProviderEnd(_ context.Context, response model.Response) {
	o.ended++
	o.response = response
}
