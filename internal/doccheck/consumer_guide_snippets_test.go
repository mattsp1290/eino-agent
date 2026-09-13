// Package doccheck compile-checks the Go fences in docs/consumer-guide.md
// against the real package APIs, so a renamed or removed symbol fails
// go vet/golangci-lint instead of silently rotting in prose (round-seven
// fix-pass-6 item 6/MG-I3: two of the guide's twelve fences did not
// compile). Each function here is a direct transcription of one fence;
// TestConsumerGuideSnippetsCompile exists only to reference them so the
// `unused` linter does not flag transcriptions nothing else calls -- their
// argument types, not their runtime behavior, are what this package checks.
// When a consumer-guide.md snippet changes, update its transcription here
// in the same commit.
package doccheck

import (
	"context"
	"testing"
	"time"

	einomodel "github.com/cloudwego/eino/components/model"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/wasmext"
)

// providerStateSnippet transcribes docs/consumer-guide.md's "Durable
// Provider-Private State" fence.
func providerStateSnippet(einoModel einomodel.ToolCallingChatModel) error {
	codec, err := model.NewEinoJSONExtraStateCodec(model.EinoJSONExtraStateConfig{
		ExtraKey: "openaicodex:reasoning_items",
		Contract: model.ProviderStateContract{
			CodecID:          "github.com/mattsp1290/eino-providers/openaicodex/reasoning-items",
			Version:          1,
			CompatibilityKey: "openaicodex-responses-reasoning-v1",
			Limits: model.ProviderStateLimits{
				MaxItems:              32,
				MaxItemBytes:          10 * 1024 * 1024,
				MaxMessageBytes:       16 * 1024 * 1024,
				MaxEnvelopeBytes:      13_985_112,
				MaxStoredMessageBytes: 22_632_024,
			},
		},
	})
	if err != nil {
		return err
	}
	streamer, err := model.NewClassicStreamerWithProviderState(einoModel, codec)
	if err != nil {
		return err
	}
	_ = streamer
	return nil
}

// toolLifecycleSnippet transcribes docs/consumer-guide.md's "Tool Lifecycle"
// fence.
func toolLifecycleSnippet(ctx context.Context, store session.Store, resolver model.Resolver, ids runtime.IDGenerator, expectedDigest, configDigest string) error {
	loader := wasmext.NewLoader()
	plans, err := composition.NewRegistry(nil)
	if err != nil {
		return err
	}
	component := extension.Component{
		InstanceID: "review-tool-v1",
		Artifact: extension.Artifact{
			Name: "review-tool", Version: "1", Hash: expectedDigest,
			ConfigHash: configDigest, SourceKind: extension.SourceWasm,
		},
	}
	mount, err := plans.Mount(ctx, component, composition.InstallerFunc(
		func(ctx context.Context, registrar *composition.Registrar) error {
			return loader.RegisterTool(ctx, registrar, composition.ToolRegistration{
				ID:    "review-tool",
				Scope: extension.GlobalScope(),
			}, wasmext.ModuleConfig{
				Name:           "review_tool",
				Path:           "extensions/review-tool.wasm",
				AllowedRoot:    "extensions",
				ExpectedSHA256: expectedDigest,
			})
		},
	))
	if err != nil {
		return err
	}
	defer func() {
		mount.Deactivate()
		closeWithin := func(closeFn func(context.Context) error) error {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return closeFn(shutdownCtx)
		}
		_ = closeWithin(mount.Close)
		_ = closeWithin(loader.Close)
	}()

	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store),
		runtime.WithModelResolver(resolver),
		runtime.WithIDGenerator(ids),
		runtime.WithRunPlanProvider(plans),
	)
	if err != nil {
		return err
	}
	_ = orchestrator
	return nil
}

// TestConsumerGuideSnippetsCompile references providerStateSnippet and
// toolLifecycleSnippet so `unused` does not flag them: this package exists
// purely so go vet/golangci-lint type-check the guide's example code, not to
// exercise it at runtime.
func TestConsumerGuideSnippetsCompile(t *testing.T) {
	_ = providerStateSnippet
	_ = toolLifecycleSnippet
}
