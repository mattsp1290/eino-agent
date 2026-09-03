package consumer

import (
	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/providers/fake"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/store/sqlite"
	"github.com/mattsp1290/eino-agent/stream"
)

var (
	_ = composition.NewRegistry
	_ = model.Provider{}
	_ = fake.Provider{}
	_ = runtime.Request{
		Message: runtime.UserMessage{Content: "current user submission"},
	}
	_ *sqlite.Store
	_ = stream.NewTail
	_ model.ProviderStateCodec
	_ model.ProviderStateStreamer
	_ = model.ProviderStateContract{
		CodecID: "example.test/reasoning-items", Version: 1, CompatibilityKey: "reasoning-v1",
		Limits: model.ProviderStateLimits{MaxItems: 1, MaxItemBytes: 1024, MaxMessageBytes: 1024, MaxEnvelopeBytes: 4096, MaxStoredMessageBytes: 4096},
	}
	_ = model.NewEinoJSONExtraStateCodec
	_ = model.NewEinoStreamerWithProviderState
)
