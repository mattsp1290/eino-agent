package einotools

import (
	"context"
	"testing"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

// fakeEnhancedStreamableTool is a minimal tool.EnhancedStreamableTool that
// streams a fixed sequence of *schema.ToolResult chunks.
type fakeEnhancedStreamableTool struct {
	chunks []*einoschema.ToolResult
}

func (f fakeEnhancedStreamableTool) Info(context.Context) (*einoschema.ToolInfo, error) {
	return &einoschema.ToolInfo{Name: "fake_streaming_tool"}, nil
}

func (f fakeEnhancedStreamableTool) StreamableRun(context.Context, *einoschema.ToolArgument, ...tool.Option) (*einoschema.StreamReader[*einoschema.ToolResult], error) {
	reader, writer := einoschema.Pipe[*einoschema.ToolResult](len(f.chunks))
	go func() {
		defer writer.Close()
		for _, chunk := range f.chunks {
			if writer.Send(chunk, nil) {
				return
			}
		}
	}()
	return reader, nil
}

// TestExecuteEnhancedStreamableLeafDrainsStreamAndSettlesOnce guards
// tool-identity-reviewer I4 (streaming enhanced tool coverage): the
// streamed chunks must be drained and settled as ONE final RichResult
// (contiguous text parts concatenated across chunks, per
// schema.ConcatToolResults, and the non-text image part kept as-is),
// not surfaced as a partial/intermediate result.
func TestExecuteEnhancedStreamableLeafDrainsStreamAndSettlesOnce(t *testing.T) {
	leaf := fakeEnhancedStreamableTool{chunks: []*einoschema.ToolResult{
		{Parts: []einoschema.ToolOutputPart{{Type: einoschema.ToolPartTypeText, Text: "Hello, "}}},
		{Parts: []einoschema.ToolOutputPart{
			{Type: einoschema.ToolPartTypeText, Text: "world!"},
			{Type: einoschema.ToolPartTypeImage, Image: &einoschema.ToolOutputImage{
				MessagePartCommon: einoschema.MessagePartCommon{URL: strPtr("https://example.com/a.png"), MIMEType: "image/png"},
			}},
		}},
	}}

	executor := ExecuteEnhancedStreamableLeaf(leaf)
	result, err := executor(context.Background(), agenttools.Execution{Input: []byte(`{}`)})
	if err != nil {
		t.Fatalf("ExecuteEnhancedStreamableLeaf executor: %v", err)
	}
	if len(result.Parts) != 2 {
		t.Fatalf("Parts = %#v, want 2 (one merged text part, one image part)", result.Parts)
	}
	if result.Parts[0].Type != runtime.ToolResultPartText || result.Parts[0].Text != "Hello, world!" {
		t.Fatalf("Parts[0] = %#v, want merged text \"Hello, world!\"", result.Parts[0])
	}
	if result.Parts[1].Type != runtime.ToolResultPartImage || result.Parts[1].Media == nil || result.Parts[1].Media.URL != "https://example.com/a.png" {
		t.Fatalf("Parts[1] = %#v, want the image part carried through unmerged", result.Parts[1])
	}
}

func strPtr(s string) *string { return &s }
