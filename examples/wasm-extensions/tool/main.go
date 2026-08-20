// Command tool is a minimal guest implementation of the
// eino-agent:extensions/tool@0.1.0 world.
package main

import (
	"strings"

	"go.bytecodealliance.org/cm"

	_ "github.com/mattsp1290/eino-agent/examples/wasm-extensions/internal/guestabi"
	toolapi "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/tool-api"
	hostlog "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/host/v0.1.0/log"
)

func init() {
	toolapi.Exports.Metadata = metadata
	toolapi.Exports.Execute = execute
}

func metadata() cm.Result[toolapi.ToolMetadataShape, toolapi.ToolMetadata, toolapi.StructuredError] {
	permissions := []string{"example.echo"}
	return cm.OK[cm.Result[toolapi.ToolMetadataShape, toolapi.ToolMetadata, toolapi.StructuredError]](toolapi.ToolMetadata{
		Name:                 "wasm_echo",
		Description:          "Echoes normalized JSON input from a Wasm component.",
		ParametersJSONSchema: `{"type":"object"}`,
		RetrySafe:            true,
		RequiredPermissions:  cm.ToList(permissions),
	})
}

func execute(_ string, inputJSON string, _ toolapi.TurnMetadata) cm.Result[toolapi.StructuredErrorShape, string, toolapi.StructuredError] {
	hostlog.Log(hostlog.LevelInfo, "executing example echo tool")
	switch {
	case strings.Contains(inputJSON, `"mode":"trap"`):
		panic("fixture trap")
	case strings.Contains(inputJSON, `"mode":"hang"`):
		for {
		}
	case strings.Contains(inputJSON, `"mode":"malformed"`):
		return cm.OK[cm.Result[toolapi.StructuredErrorShape, string, toolapi.StructuredError]](`{`)
	case strings.Contains(inputJSON, `"mode":"oversized"`):
		return cm.OK[cm.Result[toolapi.StructuredErrorShape, string, toolapi.StructuredError]](`{"value":"` + strings.Repeat("x", 300<<10) + `"}`)
	}
	return cm.OK[cm.Result[toolapi.StructuredErrorShape, string, toolapi.StructuredError]](`{"echo":` + inputJSON + `}`)
}

func main() {}
