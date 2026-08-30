// Command context-source is the Phase B context-source@0.1.0 fixture.
package main

import (
	"go.bytecodealliance.org/cm"

	_ "github.com/mattsp1290/eino-agent/examples/wasm-extensions/internal/guestabi"
	contextapi "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/context-source-api"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

func init() {
	contextapi.Exports.LoadContext = func(contextapi.TurnMetadata) cm.Result[contextapi.StructuredErrorShape, cm.List[contextapi.TextMessage], contextapi.StructuredError] {
		messages := []contextapi.TextMessage{{Role: wittypes.TextRoleUser, Text: "wasm context"}}
		return cm.OK[cm.Result[contextapi.StructuredErrorShape, cm.List[contextapi.TextMessage], contextapi.StructuredError]](cm.ToList(messages))
	}
}

func main() {}
