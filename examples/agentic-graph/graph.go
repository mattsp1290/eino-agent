// Package agenticgraph sketches a real Eino compose.Graph built from
// eino-agent runtime primitives: an AddAgenticChatTemplateNode, an
// AddAgenticModelNode (wrapped with a small audit-counting adapter that
// records the audited request through runtime.AuditAgenticRequest, exported
// for exactly this purpose), and an AddAgenticToolsNode built from
// tools.WrapEnhanced adapters plus compose.ToolAliasConfig. It exists to
// prove alias resolution, enhanced-tool dispatch, and audit/settlement
// counting work through the real Eino graph and tools node, not only
// through eino-agent's own StreamingOrchestrator pipeline.
//
// Runtime does not yet supply a durable, claim-fenced Dispatch callback for
// tools.WrapEnhanced (that lands with the graph-execution work package); this
// example injects its own Dispatch, exactly as tools.WrapEnhanced's doc
// comment anticipates.
package agenticgraph

import (
	"context"
	"fmt"
	"sync"

	einomodel "github.com/cloudwego/eino/components/model"
	einoprompt "github.com/cloudwego/eino/components/prompt"
	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	agentmodel "github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/tools"
)

// CallCounters counts, across every invocation of the built graph, how many
// times the model request was audited and how many times the fake Dispatch
// closure was invoked. A correct single-tool-call turn produces exactly one
// of each. This is NOT a durable settlement count: dispatchFor is an
// in-process stand-in for the claim-fenced runtime.BuildToolSettlement +
// store settle a real host performs (see its doc comment and BuildGraph's
// package doc), so Dispatch counts one call through this example's fake
// closure, not one durable ledger row or one store-settled tool call.
type CallCounters struct {
	mu       sync.Mutex
	Audited  int
	Dispatch int
}

func (c *CallCounters) addAudited() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Audited++
}

func (c *CallCounters) addDispatch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Dispatch++
}

// auditingAgenticModel wraps an Eino model.AgenticModel so every Generate
// call is first recorded through runtime.AuditAgenticRequest, exactly like
// StreamingOrchestrator's own request ledger (see runtime/ledger.go), before
// delegating to inner.
type auditingAgenticModel struct {
	inner    einomodel.AgenticModel
	agentID  string
	counters *CallCounters
}

func (a *auditingAgenticModel) Generate(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	request := agentmodel.Request{Identity: agentmodel.Identity{AgentID: a.agentID}, Messages: input}
	if _, _, err := runtime.AuditAgenticRequest(request, nil, 0); err != nil {
		return nil, fmt.Errorf("audit agentic request: %w", err)
	}
	a.counters.addAudited()
	return a.inner.Generate(ctx, input, opts...)
}

func (a *auditingAgenticModel) Stream(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	request := agentmodel.Request{Identity: agentmodel.Identity{AgentID: a.agentID}, Messages: input}
	if _, _, err := runtime.AuditAgenticRequest(request, nil, 0); err != nil {
		return nil, fmt.Errorf("audit agentic request: %w", err)
	}
	a.counters.addAudited()
	return a.inner.Stream(ctx, input, opts...)
}

var _ einomodel.AgenticModel = (*auditingAgenticModel)(nil)

// fixedToolCallModel is a minimal fake AgenticModel: it always returns one
// assistant message calling ToolName (as ToolAliasName is not fed back to
// Generate — Eino resolves the alias inside the tools node) with a fixed
// JSON argument payload.
type fixedToolCallModel struct {
	toolCallName string
	arguments    string
}

func (m *fixedToolCallModel) Generate(context.Context, []*einoschema.AgenticMessage, ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	return &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{{
			Type: einoschema.ContentBlockTypeFunctionToolCall,
			FunctionToolCall: &einoschema.FunctionToolCall{
				CallID: "call-1", Name: m.toolCallName, Arguments: m.arguments,
			},
		}},
	}, nil
}

func (m *fixedToolCallModel) Stream(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	msg, err := m.Generate(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[*einoschema.AgenticMessage](1)
	writer.Send(msg, nil)
	writer.Close()
	return reader, nil
}

var _ einomodel.AgenticModel = (*fixedToolCallModel)(nil)

// dispatchFor builds a tools.Dispatch that returns a fixed text result for
// one canonical tool name, incrementing counters exactly once per call. It
// stands in for the durable, claim-fenced Dispatch a real runtime host
// injects (see tools.WrapEnhanced).
func dispatchFor(canonicalName, resultText string, counters *CallCounters) tools.Dispatch {
	return func(_ context.Context, name, _ string) (*einoschema.ToolResult, error) {
		counters.addDispatch()
		if name != canonicalName {
			return nil, fmt.Errorf("dispatch: got tool name %q, want canonical %q", name, canonicalName)
		}
		return &einoschema.ToolResult{Parts: []einoschema.ToolOutputPart{{Type: einoschema.ToolPartTypeText, Text: resultText}}}, nil
	}
}

// BuildEchoTool returns one runtime.Tool named "echo" with no parameters,
// suitable for tools.WrapEnhanced.
func BuildEchoTool() runtime.Tool {
	return runtime.Tool{Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Desc: "echoes its input back as text"}}
}

// BuildGraph assembles the example compose.Graph: a chat template seeded
// with a single static user message, an audited fake agentic model that
// always calls the echo tool by its alias ("echo_alias"), and a real
// compose.AgenticToolsNode built from tools.WrapEnhanced with a
// ToolAliasConfig resolving that alias to the canonical "echo" tool.
// counters is populated as the compiled graph runs.
func BuildGraph(ctx context.Context, counters *CallCounters) (compose.Runnable[map[string]any, []*einoschema.AgenticMessage], error) {
	template := einoprompt.FromAgenticMessages(einoschema.FString, &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeUser,
		ContentBlocks: []*einoschema.ContentBlock{{
			Type:          einoschema.ContentBlockTypeUserInputText,
			UserInputText: &einoschema.UserInputText{Text: "echo hello"},
		}},
	})

	echoDef := BuildEchoTool()
	wrapped := tools.WrapEnhanced(echoDef, dispatchFor(echoDef.Name, "hello", counters))
	toolsNode, err := compose.NewAgenticToolsNode(ctx, &compose.ToolsNodeConfig{
		Tools:       []einotool.BaseTool{wrapped},
		ToolAliases: map[string]compose.ToolAliasConfig{echoDef.Name: {NameAliases: []string{"echo_alias"}}},
	})
	if err != nil {
		return nil, fmt.Errorf("build agentic tools node: %w", err)
	}

	model := &auditingAgenticModel{
		inner:    &fixedToolCallModel{toolCallName: "echo_alias", arguments: `{"text":"hello"}`},
		agentID:  "agentic-graph-example",
		counters: counters,
	}

	g := compose.NewGraph[map[string]any, []*einoschema.AgenticMessage]()
	if err := g.AddAgenticChatTemplateNode("template", template); err != nil {
		return nil, fmt.Errorf("add chat template node: %w", err)
	}
	if err := g.AddAgenticModelNode("model", model); err != nil {
		return nil, fmt.Errorf("add model node: %w", err)
	}
	if err := g.AddAgenticToolsNode("tools", toolsNode); err != nil {
		return nil, fmt.Errorf("add tools node: %w", err)
	}
	for _, edge := range [][2]string{
		{compose.START, "template"},
		{"template", "model"},
		{"model", "tools"},
		{"tools", compose.END},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			return nil, fmt.Errorf("add edge %s->%s: %w", edge[0], edge[1], err)
		}
	}
	runnable, err := g.Compile(ctx)
	if err != nil {
		return nil, fmt.Errorf("compile graph: %w", err)
	}
	return runnable, nil
}

func chatTemplate() einoprompt.AgenticChatTemplate {
	return einoprompt.FromAgenticMessages(einoschema.FString, &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeUser,
		ContentBlocks: []*einoschema.ContentBlock{{
			Type:          einoschema.ContentBlockTypeUserInputText,
			UserInputText: &einoschema.UserInputText{Text: "echo hello"},
		}},
	})
}

func aliasedEchoToolsNode(ctx context.Context, alias string, counters *CallCounters) (*compose.AgenticToolsNode, error) {
	echoDef := BuildEchoTool()
	wrapped := tools.WrapEnhanced(echoDef, dispatchFor(echoDef.Name, "hello", counters))
	return compose.NewAgenticToolsNode(ctx, &compose.ToolsNodeConfig{
		Tools:       []einotool.BaseTool{wrapped},
		ToolAliases: map[string]compose.ToolAliasConfig{echoDef.Name: {NameAliases: []string{alias}}},
	})
}

// BuildBranchExample assembles a compose.Chain that routes its model output
// through a ChainBranch to one of two AgenticToolsNode destinations,
// proving branch registration of an AddAgenticToolsNode-shaped node
// compiles. It does not invoke the compiled chain.
func BuildBranchExample(ctx context.Context, counters *CallCounters) (compose.Runnable[map[string]any, []*einoschema.AgenticMessage], error) {
	toolsLeft, err := aliasedEchoToolsNode(ctx, "echo_alias_left", counters)
	if err != nil {
		return nil, fmt.Errorf("build left tools node: %w", err)
	}
	toolsRight, err := aliasedEchoToolsNode(ctx, "echo_alias_right", counters)
	if err != nil {
		return nil, fmt.Errorf("build right tools node: %w", err)
	}

	chain := compose.NewChain[map[string]any, []*einoschema.AgenticMessage]()
	chain.AppendAgenticChatTemplate(chatTemplate())
	chain.AppendAgenticModel(&fixedToolCallModel{toolCallName: "echo_alias_left", arguments: `{}`})

	branch := compose.NewChainBranch[*einoschema.AgenticMessage](func(context.Context, *einoschema.AgenticMessage) (string, error) {
		return "left", nil
	})
	branch.AddAgenticToolsNode("left", toolsLeft)
	branch.AddAgenticToolsNode("right", toolsRight)
	chain.AppendBranch(branch)

	// AppendBranch/AppendAgenticChatTemplate/AppendAgenticModel record their
	// errors on the chain rather than returning them directly; Compile
	// surfaces the first one, if any.
	runnable, err := chain.Compile(ctx)
	if err != nil {
		return nil, fmt.Errorf("compile branch chain: %w", err)
	}
	return runnable, nil
}

// BuildParallelExample assembles a compose.Chain that fans its model output
// out to two independent AgenticToolsNode destinations via chain-parallel
// AddAgenticToolsNode, proving parallel registration compiles and both
// members execute. Both are registered under the same alias the fixed model
// always calls, so a successful Invoke exercises both members (a
// mismatched-alias member would instead error the whole fan-out, since
// neither ToolsNodeConfig sets UnknownToolsHandler). It does not invoke the
// compiled chain itself.
func BuildParallelExample(ctx context.Context, counters *CallCounters) (compose.Runnable[map[string]any, map[string]any], error) {
	toolsA, err := aliasedEchoToolsNode(ctx, "echo_alias_a", counters)
	if err != nil {
		return nil, fmt.Errorf("build tools node a: %w", err)
	}
	toolsB, err := aliasedEchoToolsNode(ctx, "echo_alias_a", counters)
	if err != nil {
		return nil, fmt.Errorf("build tools node b: %w", err)
	}

	chain := compose.NewChain[map[string]any, map[string]any]()
	chain.AppendAgenticChatTemplate(chatTemplate())
	chain.AppendAgenticModel(&fixedToolCallModel{toolCallName: "echo_alias_a", arguments: `{}`})

	parallel := compose.NewParallel()
	parallel.AddAgenticToolsNode("result_a", toolsA)
	parallel.AddAgenticToolsNode("result_b", toolsB)
	chain.AppendParallel(parallel)

	return chain.Compile(ctx)
}
