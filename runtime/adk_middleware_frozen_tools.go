package runtime

import (
	"context"
	"encoding/json"
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

// toolSearchHandlerToolExecutor wraps handlerToolExecutor for the
// dynamictool/toolsearch recipe's own sealed search tool (HandlerKindToolSearch):
// upstream's toolSearchTool.InvokableRun settles as an ordinary function
// result and never calls this runtime's own markDiscovered the way the
// native tool-search path (adkToolSearch/executeToolSearchCall,
// runtime/tool_search.go) does, so a deferred tool the model just found
// through it was denied at its next call with "is deferred and no tool
// search is configured" even though discovery genuinely happened -- see the
// W6 round-1 review's I4 finding (the exact chain:
// adk_middleware_frozen_tools.go's plain handlerToolExecutor ->
// tool_preparation.go's undiscovered-deferred-tool gate ->
// tool_execution.go settling it "expected_failure").
//
// This wrapper closes that gap the bounded way: after a successful call, it
// parses the matched tool names out of the result and marks them
// discovered in this run's execution (runExecution.markDiscovered), the
// SAME in-memory set TurnSnapshot.ProviderRequest and the undiscovered-
// deferred-tool gate both consult. It is NOT full parity with the native
// tool-search path: discovery here is recorded only in-memory for the live
// run, never durably as a tool_search_result content block, so
// discoveredToolsFromHistory cannot recover it after a resume/restart, and
// the model is never actually re-advertised the tool the SAME cycle it
// searched (only the next one) -- both are the same "next cycle, not
// mid-cycle" limitation the native path already has. See
// docs/architecture/eino-feature-support.md's W6 section for the preferred,
// not-yet-built fix (mapping this recipe onto a native ToolSearchRegistration
// instead).
type toolSearchHandlerToolExecutor struct {
	handlerToolExecutor
}

var _ ToolExecutor = toolSearchHandlerToolExecutor{}

func (h toolSearchHandlerToolExecutor) Execute(ctx context.Context, call ToolCall) (ToolResult, error) {
	result, err := h.handlerToolExecutor.Execute(ctx, call)
	if err != nil {
		return result, err
	}
	h.engine.execution.markDiscovered(toolSearchMatchedNames(result)...)
	return result, nil
}

// toolSearchMatchedNames extracts matched deferred-tool names from a
// toolsearch handler tool's result. The default (UseModelToolSearch: false)
// shape is upstream's own {"matches": [...]} JSON text output; the
// UseModelToolSearch: true shape is an enhanced result whose text parts
// carry schema.ToolInfo JSON, best-effort parsed for a "name" field.
func toolSearchMatchedNames(result ToolResult) []string {
	if result.Output != "" {
		var parsed struct {
			Matches []string `json:"matches"`
		}
		if err := json.Unmarshal([]byte(result.Output), &parsed); err == nil && len(parsed.Matches) != 0 {
			return parsed.Matches
		}
	}
	var names []string
	for _, part := range result.Parts {
		if part.Type != ToolResultPartText || part.Text == "" {
			continue
		}
		var info struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal([]byte(part.Text), &info); err == nil && info.Name != "" {
			names = append(names, info.Name)
		}
	}
	return names
}
