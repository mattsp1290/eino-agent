package einotools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-tools/catalog"

	"github.com/mattsp1290/eino-agent/internal/workspace"
	"github.com/mattsp1290/eino-agent/runtime"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func executeDefinition(definition catalog.Definition) agenttools.Executor {
	return func(ctx context.Context, execution agenttools.Execution) (json.RawMessage, error) {
		return withLeaf(ctx, definition, execution.Context.WorkspaceRoot, func(leaf tool.InvokableTool) (json.RawMessage, error) {
			return invoke(ctx, leaf, execution.Input)
		})
	}
}

// ExecuteEnhancedLeaf adapts an already-typed tool.EnhancedInvokableTool leaf
// into a tools.Definition ExecuteRich executor (see tools.Definition.
// ExecuteRich and Materialize's preference for it over Execute).
//
// This is NOT wired automatically from catalog.Definition: Eino v0.9.19's
// tool.InvokableTool and tool.EnhancedInvokableTool both declare a method
// named InvokableRun with different parameter types, so a single concrete
// type can never implement both — the Go compiler rejects the type
// assertion outright ("conflicting types for InvokableRun method"). Since
// catalog.Definition.New is statically typed to return tool.InvokableTool,
// no leaf constructed through the standard eino-tools catalog can ever also
// satisfy EnhancedInvokableTool (matching the verified seam note that
// eino-tools exposes classic invokable leaves only, today). ExecuteEnhancedLeaf
// exists so a host-authored (or, if eino-tools later grows a distinct
// enhanced-leaf constructor) tool.EnhancedInvokableTool value can still be
// wired through this package's schema.ToolResult -> RichResult conversion.
func ExecuteEnhancedLeaf(leaf tool.EnhancedInvokableTool) agenttools.RichExecutor {
	return func(ctx context.Context, execution agenttools.Execution) (agenttools.RichResult, error) {
		toolResult, err := leaf.InvokableRun(ctx, &einoschema.ToolArgument{Text: string(execution.Input)})
		if err != nil {
			return agenttools.RichResult{}, err
		}
		parts, err := convertEnhancedToolResult(toolResult)
		if err != nil {
			return agenttools.RichResult{}, err
		}
		return agenttools.RichResult{Parts: parts}, nil
	}
}

// ExecuteEnhancedStreamableLeaf adapts an already-typed
// tool.EnhancedStreamableTool leaf (eino@v0.9.19 components/tool/interface.go's
// StreamableRun(ctx, *schema.ToolArgument, ...Option) (*schema.StreamReader[*schema.ToolResult], error))
// into a tools.Definition ExecuteRich executor: it drains the leaf's stream
// of schema.ToolResult chunks and settles the final, concatenated parts
// exactly once via schema.ConcatToolResults, mirroring how the model's own
// streamed message is concatenated before anything downstream sees it (see
// runtime/model_stream.go's receiveModelStream) -- an intermediate/partial
// chunk must never become the tool's durable settlement.
//
// Not wired automatically for the same reason ExecuteEnhancedLeaf isn't (see
// its doc comment): no leaf constructed through the standard eino-tools
// catalog can satisfy this interface today.
func ExecuteEnhancedStreamableLeaf(leaf tool.EnhancedStreamableTool) agenttools.RichExecutor {
	return func(ctx context.Context, execution agenttools.Execution) (agenttools.RichResult, error) {
		reader, err := leaf.StreamableRun(ctx, &einoschema.ToolArgument{Text: string(execution.Input)})
		if err != nil {
			return agenttools.RichResult{}, err
		}
		defer reader.Close()
		var chunks []*einoschema.ToolResult
		for {
			if err := ctx.Err(); err != nil {
				return agenttools.RichResult{}, err
			}
			chunk, err := reader.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return agenttools.RichResult{}, err
			}
			chunks = append(chunks, chunk)
		}
		toolResult, err := einoschema.ConcatToolResults(chunks)
		if err != nil {
			return agenttools.RichResult{}, err
		}
		parts, err := convertEnhancedToolResult(toolResult)
		if err != nil {
			return agenttools.RichResult{}, err
		}
		return agenttools.RichResult{Parts: parts}, nil
	}
}

// withLeaf constructs definition's leaf (honoring its workspace binding and
// concurrency/locking policy exactly as before enhanced-tool support
// existed) and invokes fn with it, returning fn's result.
func withLeaf[T any](ctx context.Context, definition catalog.Definition, workspaceRoot string, fn func(tool.InvokableTool) (T, error)) (T, error) {
	var zero T
	instance := catalog.Instance{}
	lockKey := "registration:" + definition.ID
	if definition.Binding == catalog.BindingWorkspace {
		root, err := workspace.CanonicalRoot(workspaceRoot)
		if err != nil {
			return zero, err
		}
		if root != workspaceRoot {
			return zero, fmt.Errorf("%w: admitted workspace root is not canonical", workspace.ErrInvalidRoot)
		}
		instance.WorkspaceRoot = root
		lockKey = "workspace:" + root
	}
	call := func() (T, error) {
		leaf, err := definition.New(ctx, instance)
		if err != nil {
			return zero, err
		}
		return fn(leaf)
	}
	if definition.Concurrent {
		return call()
	}
	var result T
	err := standardLocks.Do(ctx, lockKey, func() error {
		var callErr error
		result, callErr = call()
		return callErr
	})
	return result, err
}

func invoke(ctx context.Context, leaf tool.InvokableTool, input json.RawMessage) (json.RawMessage, error) {
	output, err := leaf.InvokableRun(ctx, string(input))
	if err != nil {
		return nil, err
	}
	raw := json.RawMessage(output)
	if !json.Valid(raw) {
		return nil, fmt.Errorf("eino-tools returned invalid JSON")
	}
	return cloneRaw(raw), nil
}

// convertEnhancedToolResult converts an Eino schema.ToolResult (returned by
// an EnhancedInvokableTool leaf) into the runtime's durable ToolResultPart
// representation.
func convertEnhancedToolResult(result *einoschema.ToolResult) ([]runtime.ToolResultPart, error) {
	if result == nil {
		return nil, nil
	}
	parts := make([]runtime.ToolResultPart, 0, len(result.Parts))
	for _, part := range result.Parts {
		converted, err := convertEnhancedPart(part)
		if err != nil {
			return nil, err
		}
		parts = append(parts, converted)
	}
	return parts, nil
}

func convertEnhancedPart(part einoschema.ToolOutputPart) (runtime.ToolResultPart, error) {
	switch part.Type {
	case einoschema.ToolPartTypeText:
		return runtime.ToolResultPart{Type: runtime.ToolResultPartText, Text: part.Text}, nil
	case einoschema.ToolPartTypeImage:
		if part.Image == nil {
			return runtime.ToolResultPart{}, fmt.Errorf("eino-tools enhanced result: image content is nil")
		}
		return runtime.ToolResultPart{Type: runtime.ToolResultPartImage, Media: commonToToolResultMedia(part.Image.MessagePartCommon)}, nil
	case einoschema.ToolPartTypeAudio:
		if part.Audio == nil {
			return runtime.ToolResultPart{}, fmt.Errorf("eino-tools enhanced result: audio content is nil")
		}
		return runtime.ToolResultPart{Type: runtime.ToolResultPartAudio, Media: commonToToolResultMedia(part.Audio.MessagePartCommon)}, nil
	case einoschema.ToolPartTypeVideo:
		if part.Video == nil {
			return runtime.ToolResultPart{}, fmt.Errorf("eino-tools enhanced result: video content is nil")
		}
		return runtime.ToolResultPart{Type: runtime.ToolResultPartVideo, Media: commonToToolResultMedia(part.Video.MessagePartCommon)}, nil
	case einoschema.ToolPartTypeFile:
		if part.File == nil {
			return runtime.ToolResultPart{}, fmt.Errorf("eino-tools enhanced result: file content is nil")
		}
		return runtime.ToolResultPart{Type: runtime.ToolResultPartFile, Media: commonToToolResultMedia(part.File.MessagePartCommon)}, nil
	case einoschema.ToolPartTypeToolSearchResult:
		if part.ToolSearchResult == nil {
			return runtime.ToolResultPart{}, fmt.Errorf("eino-tools enhanced result: tool search result is nil")
		}
		raw, err := json.Marshal(part.ToolSearchResult)
		if err != nil {
			return runtime.ToolResultPart{}, err
		}
		return runtime.ToolResultPart{Type: runtime.ToolResultPartToolSearch, ToolSearch: raw}, nil
	default:
		return runtime.ToolResultPart{}, fmt.Errorf("eino-tools enhanced result: unknown part type %q", part.Type)
	}
}

func commonToToolResultMedia(common einoschema.MessagePartCommon) *runtime.ToolResultMedia {
	media := &runtime.ToolResultMedia{MIMEType: common.MIMEType}
	if common.URL != nil {
		media.URL = *common.URL
	}
	if common.Base64Data != nil {
		media.Base64Data = *common.Base64Data
	}
	return media
}
