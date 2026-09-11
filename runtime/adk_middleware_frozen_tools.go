package runtime

import (
	"context"
	"fmt"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"
)

// handlerToolExecutor dispatches a sealed handler tool call to its live
// tool.BaseTool instance (adkEngine.handlerTools, collected by
// adkEngine.buildAgentHandlers), routed here through the full durable
// claim/permission/execute/settle pipeline via adkTool -- exactly like any
// other frozen, composition-registered tool. It supports both
// tool.InvokableTool and tool.EnhancedInvokableTool (UseMultiModalRead's
// multimodal read_file), mapping an enhanced result's Parts into
// runtime.ToolResultPart so rich content (media result parts) flows through
// unflattened, per W2/W4.
type handlerToolExecutor struct {
	engine *adkEngine
	name   string
}

var _ ToolExecutor = handlerToolExecutor{}

func (h handlerToolExecutor) Execute(ctx context.Context, call ToolCall) (ToolResult, error) {
	live, ok := h.engine.handlerTools[h.name]
	if !ok {
		return ToolResult{}, fmt.Errorf("%w: handler tool %q has no live instance this turn", errADKUnsupportedBlock, h.name)
	}
	if enhanced, ok := live.(tool.EnhancedInvokableTool); ok {
		result, err := enhanced.InvokableRun(ctx, &einoschema.ToolArgument{Text: string(call.Input)})
		if err != nil {
			return ToolResult{}, err
		}
		return convertEnhancedToolResult(result), nil
	}
	if invokable, ok := live.(tool.InvokableTool); ok {
		output, err := invokable.InvokableRun(ctx, string(call.Input))
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{Output: output}, nil
	}
	return ToolResult{}, fmt.Errorf("%w: handler tool %q supports neither invokable nor enhanced-invokable execution", errADKUnsupportedBlock, h.name)
}

// convertEnhancedToolResult maps an upstream *schema.ToolResult (an
// EnhancedInvokableTool's multimodal result, e.g. filesystem's
// UseMultiModalRead read_file) into runtime.ToolResult.Parts, never
// flattening media content into text -- see W2/W4's rich-content contract.
func convertEnhancedToolResult(result *einoschema.ToolResult) ToolResult {
	if result == nil {
		return ToolResult{}
	}
	parts := make([]ToolResultPart, 0, len(result.Parts))
	for _, part := range result.Parts {
		if converted, ok := convertToolOutputPart(part); ok {
			parts = append(parts, converted)
		}
	}
	return ToolResult{Parts: parts}
}

func convertToolOutputPart(part einoschema.ToolOutputPart) (ToolResultPart, bool) {
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	switch part.Type {
	case einoschema.ToolPartTypeText:
		return ToolResultPart{Type: ToolResultPartText, Text: part.Text}, true
	case einoschema.ToolPartTypeImage:
		if part.Image == nil {
			return ToolResultPart{}, false
		}
		return mediaResultPart(ToolResultPartImage, deref(part.Image.URL), deref(part.Image.Base64Data), part.Image.MIMEType)
	case einoschema.ToolPartTypeAudio:
		if part.Audio == nil {
			return ToolResultPart{}, false
		}
		return mediaResultPart(ToolResultPartAudio, deref(part.Audio.URL), deref(part.Audio.Base64Data), part.Audio.MIMEType)
	case einoschema.ToolPartTypeVideo:
		if part.Video == nil {
			return ToolResultPart{}, false
		}
		return mediaResultPart(ToolResultPartVideo, deref(part.Video.URL), deref(part.Video.Base64Data), part.Video.MIMEType)
	case einoschema.ToolPartTypeFile:
		if part.File == nil {
			return ToolResultPart{}, false
		}
		return mediaResultPart(ToolResultPartFile, deref(part.File.URL), deref(part.File.Base64Data), part.File.MIMEType)
	default:
		return ToolResultPart{}, false
	}
}

func mediaResultPart(kind ToolResultPartType, url, base64Data, mimeType string) (ToolResultPart, bool) {
	if url == "" && base64Data == "" {
		return ToolResultPart{}, false
	}
	return ToolResultPart{Type: kind, Media: &ToolResultMedia{URL: url, Base64Data: base64Data, MIMEType: mimeType}}, true
}
