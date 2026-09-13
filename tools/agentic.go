package tools

import (
	"context"
	"errors"

	einotool "github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
)

// ErrDispatchRequired reports that WrapEnhanced was called without a
// Dispatch callback.
var ErrDispatchRequired = errors.New("tools: WrapEnhanced requires a non-nil Dispatch")

// Dispatch executes one durable tool call by canonical name, given the
// model's raw JSON arguments, and returns the enhanced multimodal result.
// A full runtime-owned implementation (claim-fenced, replay-safe, settled
// through runtime.BuildToolSettlement exactly like the classic tool
// pipeline) is provided by the orchestrator in a later work package; until
// then, a caller wiring an Eino compose.Graph/AgenticToolsNode (see
// examples/agentic-graph) injects its own Dispatch.
type Dispatch func(ctx context.Context, name, args string) (*einoschema.ToolResult, error)

// WrapEnhanced adapts one materialized runtime.Tool into an Eino
// tool.EnhancedInvokableTool for use in a compose.Graph or
// compose.AgenticToolsNode. InvokableRun always goes through dispatch rather
// than def's own Executor: dispatch is the seam a durable-execution host
// plugs into (claim, execute, settle, persist) so a graph-driven tool call
// gets the same durability guarantees as the classic orchestrator pipeline.
func WrapEnhanced(def runtime.Tool, dispatch Dispatch) einotool.EnhancedInvokableTool {
	return &enhancedToolAdapter{def: def, dispatch: dispatch}
}

type enhancedToolAdapter struct {
	def      runtime.Tool
	dispatch Dispatch
}

func (a *enhancedToolAdapter) Info(context.Context) (*einoschema.ToolInfo, error) {
	return a.def.Info, nil
}

func (a *enhancedToolAdapter) InvokableRun(ctx context.Context, argument *einoschema.ToolArgument, _ ...einotool.Option) (*einoschema.ToolResult, error) {
	if a.dispatch == nil {
		return nil, ErrDispatchRequired
	}
	text := ""
	if argument != nil {
		text = argument.Text
	}
	return a.dispatch(ctx, a.def.Name, text)
}

var _ einotool.EnhancedInvokableTool = (*enhancedToolAdapter)(nil)
