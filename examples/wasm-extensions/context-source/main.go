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
		// The media-reference block below (round-two W6 review item 13 /
		// RA I8) exercises the real component path for media-reference
		// conversion: a bounded https URI with a required, recognized
		// image/png mime-type. It is a SECOND block in the SAME message as
		// the original text block, not a new message or a change to the
		// text block's own content, so it does not affect any test that
		// only inspects message count or the text block's own text.
		blocks := []wittypes.ContentBlock{
			wittypes.ContentBlockText("wasm context"),
			wittypes.ContentBlockMediaReference(wittypes.MediaReference{
				URI: "https://example.com/wasm-context-fixture.png", MIMEType: "image/png",
			}),
		}
		messages := []contextapi.Message{{Role: wittypes.TextRoleUser, Blocks: cm.ToList(blocks)}}
		return cm.OK[cm.Result[contextapi.StructuredErrorShape, cm.List[contextapi.Message], contextapi.StructuredError]](cm.ToList(messages))
	}
}

func main() {}
