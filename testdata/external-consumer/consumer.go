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
	_ = runtime.Request{}
	_ *sqlite.Store
	_ = stream.NewTail
)
