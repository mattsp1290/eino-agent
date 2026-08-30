// Command tool-middleware is the Phase B tool-middleware@0.1.0 fixture.
package main

import (
	"strings"

	_ "github.com/mattsp1290/eino-agent/examples/wasm-extensions/internal/guestabi"
	middlewareapi "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/tool-middleware-api"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

func init() {
	middlewareapi.Exports.BeforeToolCall = func(_, _, input string, _ middlewareapi.TurnMetadata) middlewareapi.Replacement {
		if strings.Contains(input, "replace") {
			return wittypes.ReplacementJSON(`{"from":"wasm"}`)
		}
		return wittypes.ReplacementUnchanged()
	}
	middlewareapi.Exports.AfterToolCall = func(_, _, _, output string, _ middlewareapi.TurnMetadata) middlewareapi.Replacement {
		if strings.Contains(output, "replace") {
			return wittypes.ReplacementJSON(`{"result":"wasm"}`)
		}
		return wittypes.ReplacementUnchanged()
	}
}

func main() {}
