// Command event-sink is the Phase B event-sink@0.1.0 fixture.
package main

import (
	"go.bytecodealliance.org/cm"

	_ "github.com/mattsp1290/eino-agent/examples/wasm-extensions/internal/guestabi"
	eventapi "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/event-sink-api"
)

func init() {
	eventapi.Exports.Emit = func(eventapi.BoundedEvent) cm.Result[eventapi.StructuredError, struct{}, eventapi.StructuredError] {
		return cm.OK[cm.Result[eventapi.StructuredError, struct{}, eventapi.StructuredError]](struct{}{})
	}
}

func main() {}
