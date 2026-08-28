// Command hook is the Phase B hook@0.1.0 fixture.
package main

import (
	"go.bytecodealliance.org/cm"

	_ "github.com/mattsp1290/eino-agent/examples/wasm-extensions/internal/guestabi"
	hookapi "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/hook-api"
)

func ok(hookapi.TurnMetadata) cm.Result[hookapi.StructuredError, struct{}, hookapi.StructuredError] {
	return cm.OK[cm.Result[hookapi.StructuredError, struct{}, hookapi.StructuredError]](struct{}{})
}

func init() {
	hookapi.Exports.BeforeRun = ok
	hookapi.Exports.AfterRun = ok
}

func main() {}
