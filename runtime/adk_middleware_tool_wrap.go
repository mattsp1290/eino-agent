package runtime

import (
	"context"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"
)

// applyHandlerToolResultWrappers threads result through every mounted
// agent-handler middleware's own WrapInvokableToolCall (plain-string
// results) or WrapEnhancedInvokableToolCall (multi-part results), in ADK's
// own "first-registered-is-outermost" order, so a handler that transforms a
// tool's result -- reduction's MaxLengthForTrunc truncation, including its
// offload write, is the motivating case -- takes effect on the EXACT
// content this runtime durably settles, before settlement and event
// emission (the plan's own requirement), not as an after-settlement
// rewrite subject to settlementSeal's authorization dance. This is
// deliberately NOT a security-relevant seam the way settlementSeal is:
// every handler here is host-configured at plan-compile time (not
// model/attacker-controlled), and it runs strictly BEFORE the result ever
// becomes "the settled truth" -- there is nothing to tamper with yet.
//
// Only the two endpoint shapes this runtime's own tools ever actually
// return are supported: a plain string (ToolResult.Output, no Parts) or an
// enhanced multi-part result (ToolResult.Parts non-empty). A handler that
// does not override the relevant Wrap* method inherits
// TypedBaseChatModelAgentMiddleware's passthrough default, so calling this
// for every handler is a no-op for the six of this package's own eight
// recipes that never transform tool results at all.
func (e *adkEngine) applyHandlerToolResultWrappers(ctx context.Context, name, callID string, argumentsJSON []byte, result ToolResult) (ToolResult, error) {
	if len(e.handlerMiddlewares) == 0 {
		return result, nil
	}
	tCtx := &adk.ToolContext{Name: name, CallID: callID}
	if len(result.Parts) != 0 {
		return e.applyEnhancedToolResultWrappers(ctx, tCtx, argumentsJSON, result)
	}
	return e.applyPlainToolResultWrappers(ctx, tCtx, argumentsJSON, result)
}

func (e *adkEngine) applyPlainToolResultWrappers(ctx context.Context, tCtx *adk.ToolContext, argumentsJSON []byte, result ToolResult) (ToolResult, error) {
	base := result.Output
	var endpoint adk.InvokableToolCallEndpoint = func(context.Context, string, ...tool.Option) (string, error) {
		return base, nil
	}
	// First-registered handler must end up OUTERMOST (its own
	// post-processing, if any, runs last, over whatever every later-
	// registered handler already produced) -- build from the end of the
	// slice backward so handlerMiddlewares[0] wraps everything already
	// built by handlerMiddlewares[1:].
	for i := len(e.handlerMiddlewares) - 1; i >= 0; i-- {
		wrapped, err := e.handlerMiddlewares[i].WrapInvokableToolCall(ctx, endpoint, tCtx)
		if err != nil {
			return ToolResult{}, err
		}
		if wrapped != nil {
			endpoint = wrapped
		}
	}
	transformed, err := endpoint(ctx, string(argumentsJSON))
	if err != nil {
		return ToolResult{}, err
	}
	out := result
	out.Output = transformed
	if transformed != base {
		// A tool built through tools.Definition's standard (non-Rich)
		// Execute path always sets BOTH Output (the string form) and
		// Structured (cloneRaw of the identical JSON) -- see
		// tools/definition.go. Once a handler wrapper has transformed
		// Output into an opaque placeholder, Structured no longer
		// represents the same content and must not be left as a durable
		// side channel carrying the full, untransformed original back into
		// the settled row and the model-visible result.
		out.Structured = nil
	}
	return out, nil
}

func (e *adkEngine) applyEnhancedToolResultWrappers(ctx context.Context, tCtx *adk.ToolContext, argumentsJSON []byte, result ToolResult) (ToolResult, error) {
	base, err := runtimeResultPartsToEinoToolResult(result.Parts)
	if err != nil {
		return ToolResult{}, err
	}
	var endpoint adk.EnhancedInvokableToolCallEndpoint = func(context.Context, *einoschema.ToolArgument, ...tool.Option) (*einoschema.ToolResult, error) {
		return base, nil
	}
	for i := len(e.handlerMiddlewares) - 1; i >= 0; i-- {
		wrapped, err := e.handlerMiddlewares[i].WrapEnhancedInvokableToolCall(ctx, endpoint, tCtx)
		if err != nil {
			return ToolResult{}, err
		}
		if wrapped != nil {
			endpoint = wrapped
		}
	}
	transformed, err := endpoint(ctx, &einoschema.ToolArgument{Text: string(argumentsJSON)})
	if err != nil {
		return ToolResult{}, err
	}
	parts, err := einoToolResultToRuntimeParts(transformed)
	if err != nil {
		return ToolResult{}, err
	}
	out := result
	out.Parts = parts
	return out, nil
}

// runtimeResultPartsToEinoToolResult is the reverse of
// convertEnhancedToolResult/convertToolOutputPart
// (adk_middleware_frozen_tools.go): it maps this runtime's own
// ToolResultPart back onto the upstream schema.ToolResult shape a handler's
// WrapEnhancedInvokableToolCall expects to receive and return.
func runtimeResultPartsToEinoToolResult(parts []ToolResultPart) (*einoschema.ToolResult, error) {
	out := make([]einoschema.ToolOutputPart, 0, len(parts))
	for _, part := range parts {
		switch part.Type {
		case ToolResultPartText:
			out = append(out, einoschema.ToolOutputPart{Type: einoschema.ToolPartTypeText, Text: part.Text})
		case ToolResultPartImage:
			out = append(out, einoschema.ToolOutputPart{Type: einoschema.ToolPartTypeImage, Image: mediaToToolOutputImage(part.Media)})
		case ToolResultPartAudio:
			out = append(out, einoschema.ToolOutputPart{Type: einoschema.ToolPartTypeAudio, Audio: mediaToToolOutputAudio(part.Media)})
		case ToolResultPartVideo:
			out = append(out, einoschema.ToolOutputPart{Type: einoschema.ToolPartTypeVideo, Video: mediaToToolOutputVideo(part.Media)})
		case ToolResultPartFile:
			out = append(out, einoschema.ToolOutputPart{Type: einoschema.ToolPartTypeFile, File: mediaToToolOutputFile(part.Media)})
		default:
			return nil, errADKUnsupportedBlock
		}
	}
	return &einoschema.ToolResult{Parts: out}, nil
}

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func mediaToToolOutputImage(m *ToolResultMedia) *einoschema.ToolOutputImage {
	if m == nil {
		return nil
	}
	return &einoschema.ToolOutputImage{MessagePartCommon: einoschema.MessagePartCommon{URL: stringPtrOrNil(m.URL), Base64Data: stringPtrOrNil(m.Base64Data), MIMEType: m.MIMEType}}
}

func mediaToToolOutputAudio(m *ToolResultMedia) *einoschema.ToolOutputAudio {
	if m == nil {
		return nil
	}
	return &einoschema.ToolOutputAudio{MessagePartCommon: einoschema.MessagePartCommon{URL: stringPtrOrNil(m.URL), Base64Data: stringPtrOrNil(m.Base64Data), MIMEType: m.MIMEType}}
}

func mediaToToolOutputVideo(m *ToolResultMedia) *einoschema.ToolOutputVideo {
	if m == nil {
		return nil
	}
	return &einoschema.ToolOutputVideo{MessagePartCommon: einoschema.MessagePartCommon{URL: stringPtrOrNil(m.URL), Base64Data: stringPtrOrNil(m.Base64Data), MIMEType: m.MIMEType}}
}

func mediaToToolOutputFile(m *ToolResultMedia) *einoschema.ToolOutputFile {
	if m == nil {
		return nil
	}
	return &einoschema.ToolOutputFile{MessagePartCommon: einoschema.MessagePartCommon{URL: stringPtrOrNil(m.URL), Base64Data: stringPtrOrNil(m.Base64Data), MIMEType: m.MIMEType}}
}

// einoToolResultToRuntimeParts is the forward direction (upstream schema ->
// this runtime's own ToolResultPart), reusing convertToolOutputPart
// (adk_middleware_frozen_tools.go) so both handler-tool dispatch and
// handler-Wrap*ToolCall result mapping agree on exactly the same shape.
func einoToolResultToRuntimeParts(result *einoschema.ToolResult) ([]ToolResultPart, error) {
	if result == nil {
		return nil, nil
	}
	parts := make([]ToolResultPart, 0, len(result.Parts))
	for _, part := range result.Parts {
		converted, ok := convertToolOutputPart(part)
		if !ok {
			return nil, errADKUnsupportedBlock
		}
		parts = append(parts, converted)
	}
	return parts, nil
}
