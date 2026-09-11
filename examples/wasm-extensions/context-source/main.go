// Command context-source is the Phase B context-source@0.2.0 fixture.
package main

import (
	"go.bytecodealliance.org/cm"

	_ "github.com/mattsp1290/eino-agent/examples/wasm-extensions/internal/guestabi"
	contextapi "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.2.0/context-source-api"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.2.0/types"
)

func init() {
	contextapi.Exports.LoadContext = func(contextapi.TurnMetadata) cm.Result[contextapi.StructuredErrorShape, cm.List[contextapi.Message], contextapi.StructuredError] {
		blocks := []wittypes.ContentBlock{wittypes.ContentBlockText("wasm context")}
		messages := []contextapi.Message{{Role: wittypes.TextRoleUser, Blocks: cm.ToList(blocks)}}
		return cm.OK[cm.Result[contextapi.StructuredErrorShape, cm.List[contextapi.Message], contextapi.StructuredError]](cm.ToList(messages))
	}
}

func main() {}
