package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/eino/adk"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func proofToolsNodeConfig(tools []tool.BaseTool) compose.ToolsNodeConfig {
	return compose.ToolsNodeConfig{Tools: tools}
}

// W1 proof: child agents, middleware-added tools and retry/failover model
// calls cannot evade the mandatory adapters. ADK callbacks alone are not
// authority for side effects; the frozen registry and the adapters are.

// rawTool is an upstream tool with no durable wrapper.
type rawTool struct {
	name  string
	calls atomic.Int32
}

func (r *rawTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: r.name, Desc: "raw", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"value": {Type: schema.String}})}, nil
}

func (r *rawTool) InvokableRun(context.Context, string, ...tool.Option) (string, error) {
	r.calls.Add(1)
	return "raw executed", nil
}

// toolInjector is a middleware that adds a tool to the agent at run time.
type toolInjector struct {
	adk.TypedBaseChatModelAgentMiddleware[*schema.AgenticMessage]
	tool tool.BaseTool
}

func (m *toolInjector) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	runCtx.Tools = append(runCtx.Tools, m.tool)
	return ctx, runCtx, nil
}

// durableGuard is the runtime's last-registered handler: every tool visible
// to ADK must be a durable wrapper over a frozen registry entry, otherwise it
// is refused before the run starts.
type durableGuard struct {
	adk.TypedBaseChatModelAgentMiddleware[*schema.AgenticMessage]
	proof *adkProof
}

func (g *durableGuard) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	wrapped := make([]tool.BaseTool, 0, len(runCtx.Tools))
	for _, candidate := range runCtx.Tools {
		if _, ok := candidate.(*adkDurableTool); ok {
			wrapped = append(wrapped, candidate)
			continue
		}
		info, err := candidate.Info(ctx)
		if err != nil {
			return ctx, nil, err
		}
		var frozen *Tool
		for index := range g.proof.tools {
			if g.proof.tools[index].Name == info.Name {
				frozen = &g.proof.tools[index]
			}
		}
		if frozen == nil {
			return ctx, nil, errors.New("tool " + info.Name + " is not in the frozen registry")
		}
		g.proof.trace.add("guard.wrapped %s", info.Name)
		wrapped = append(wrapped, &adkDurableTool{proof: g.proof, tool: *frozen})
	}
	runCtx.Tools = wrapped
	return ctx, runCtx, nil
}

func TestADKWrappingMiddlewareAddedToolsCannotEvadeSettlement(t *testing.T) {
	t.Parallel()
	t.Run("absent from frozen registry", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		raw := &rawTool{name: "raw"}
		scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticCall("", "raw", `{"value":"x"}`))}}
		agent := proof.newAgent(t, proof.ledgerModel(scripted), &toolInjector{tool: raw})
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: agent})
		drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
		// The model adapter refuses to commit a call to a tool outside the
		// frozen registry, so the raw tool never executes.
		if len(drained.errs) != 1 || !strings.Contains(drained.errs[0].Error(), `tool "raw" unavailable`) {
			t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
		}
		if raw.calls.Load() != 0 || len(proof.toolCalls()) != 0 {
			t.Fatalf("raw tool evaded the registry: calls=%d rows=%d", raw.calls.Load(), len(proof.toolCalls()))
		}
	})
	t.Run("guard wraps registry tools added by middleware", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		// The frozen registry knows "late", but the agent was built without it;
		// a middleware injects a raw upstream implementation at run time.
		late := proofTool("late", proof.countingExecutor("late", nil), nil)
		proof.tools, proof.snapshot.Tools = []Tool{late}, []Tool{late}
		raw := &rawTool{name: "late"}
		scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
			agenticAssistant(agenticCall("", "late", `{"value":"x"}`)),
			agenticAssistant(agenticText("done")),
		}}
		ledger := proof.ledgerModel(scripted)
		agent, err := adk.NewTypedChatModelAgent[*schema.AgenticMessage](proof.ctx, &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{
			Name: "proof", Model: ledger,
			Handlers: []adk.TypedChatModelAgentMiddleware[*schema.AgenticMessage]{&toolInjector{tool: raw}, &durableGuard{proof: proof}},
		})
		if err != nil {
			t.Fatal(err)
		}
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: agent})
		drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
		if len(drained.errs) != 0 {
			t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
		}
		calls := proof.toolCalls()
		if raw.calls.Load() != 0 || proof.executions("late") != 1 || len(calls) != 1 || calls[0].Status != session.ToolCallCompleted {
			t.Fatalf("raw=%d durable=%d rows=%+v", raw.calls.Load(), proof.executions("late"), calls)
		}
		assertTraceOrder(t, trace, "guard.wrapped late", "tool.claimed", "executor.late", "tool.settled")
	})
	t.Run("guard refuses unknown middleware tools", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		raw := &rawTool{name: "unknown"}
		scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("unreachable"))}}
		agent := proof.newAgent(t, proof.ledgerModel(scripted), &toolInjector{tool: raw}, &durableGuard{proof: proof})
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: agent})
		drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
		if len(drained.errs) != 1 || !strings.Contains(drained.errs[0].Error(), "not in the frozen registry") || scripted.calls.Load() != 0 || len(proof.modelRequests()) != 0 {
			t.Fatalf("errors=%v calls=%d ledger=%d", drained.errs, scripted.calls.Load(), len(proof.modelRequests()))
		}
	})
}

func TestADKWrappingChildAgentCallsPassLedgerAndSettlement(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	// The child agent uses its own ledger adapter over the same run fence and
	// a durable leaf tool.
	leaf := proofTool("leaf", proof.countingExecutor("leaf", nil), nil)
	childModel := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticCall("", "leaf", `{"value":"c"}`)),
		agenticAssistant(agenticText("child result")),
	}}
	childLedger := proof.ledgerModel(childModel)
	child, err := adk.NewTypedChatModelAgent[*schema.AgenticMessage](proof.ctx, &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{
		Name: "child", Description: "child agent", Model: childLedger,
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: proofToolsNodeConfig([]tool.BaseTool{&adkDurableTool{proof: proof, tool: leaf}})},
	})
	if err != nil {
		t.Fatal(err)
	}
	agentTool := adk.NewTypedAgentTool[*schema.AgenticMessage](proof.ctx, child)
	invokable, ok := agentTool.(tool.InvokableTool)
	if !ok {
		t.Fatalf("agent tool %T is not invokable", agentTool)
	}
	// The child is exposed to the parent as a frozen-registry tool whose
	// executor runs the ADK agent tool: the durable wrapper claims and
	// settles around the whole child execution.
	var childCtx context.Context
	childTool := proofTool("child", proof.countingExecutor("child", func(call ToolCall) (ToolResult, error) {
		output, err := invokable.InvokableRun(childCtx, string(call.Input))
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{Output: output}, nil
	}), nil)
	childTool.Info = &schema.ToolInfo{Name: "child", Desc: "child agent", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"request": {Type: schema.String}})}
	proof.tools, proof.snapshot.Tools = []Tool{childTool, leaf}, []Tool{childTool, leaf}
	parentModel := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticCall("", "child", `{"request":"delegate"}`)),
		agenticAssistant(agenticText("parent done")),
	}}
	parent, err := adk.NewTypedChatModelAgent[*schema.AgenticMessage](proof.ctx, &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{
		Name: "parent", Model: proof.ledgerModel(parentModel),
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: proofToolsNodeConfig([]tool.BaseTool{&adkDurableTool{proof: proof, tool: childTool}})},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: parent})
	childCtx = proof.ctx
	drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
	if len(drained.errs) != 0 {
		t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
	}
	records := proof.modelRequests()
	if len(records) != 4 || parentModel.calls.Load() != 2 || childModel.calls.Load() != 2 {
		t.Fatalf("ledger rows = %d parent=%d child=%d\n%s", len(records), parentModel.calls.Load(), childModel.calls.Load(), strings.Join(trace.list(), "\n"))
	}
	seen := map[session.MessageID]bool{}
	for _, record := range records {
		if record.State != session.ModelRequestCompleted || seen[record.AssistantMessageID] {
			t.Fatalf("ledger row %+v", record)
		}
		seen[record.AssistantMessageID] = true
	}
	calls := statusByName(proof.toolCalls())
	if calls["child"].Status != session.ToolCallCompleted || calls["leaf"].Status != session.ToolCallCompleted || proof.executions("child") != 1 || proof.executions("leaf") != 1 {
		t.Fatalf("tool rows = %+v", calls)
	}
	// The child's dispatches and leaf settlement happen inside the parent's
	// claim/settle window for the child tool.
	assertTraceOrder(t, trace, "tool.claimed "+string(calls["child"].ID), "executor.child", "adapter.generate.begin call=1", "tool.claimed "+string(calls["leaf"].ID), "executor.leaf", "tool.settled "+string(calls["leaf"].ID), "tool.settled "+string(calls["child"].ID), "provider.call 2")
	if text := assistantText(drained.outputs[len(drained.outputs)-1]); text != "parent done" {
		t.Fatalf("final text = %q", text)
	}
}

func TestADKWrappingRetryAndFailoverPassTheLedger(t *testing.T) {
	t.Parallel()
	retryable := model.Error{Code: "provider_rate_limited", Message: "slow down", Retryable: true, Cause: model.ErrProviderRateLimited}
	t.Run("retry", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		scripted := &adkScriptedModel{trace: trace, errs: []error{retryable}, responses: []*schema.AgenticMessage{nil, agenticAssistant(agenticText("second attempt"))}}
		agent, err := adk.NewTypedChatModelAgent[*schema.AgenticMessage](proof.ctx, &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{
			Name: "proof", Model: proof.ledgerModel(scripted),
			ModelRetryConfig: &adk.TypedModelRetryConfig[*schema.AgenticMessage]{MaxRetries: 1, IsRetryAble: func(context.Context, error) bool { return true }},
		})
		if err != nil {
			t.Fatal(err)
		}
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: agent})
		drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
		if len(drained.errs) != 0 {
			t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
		}
		records := proof.modelRequests()
		if len(records) != 2 || records[0].State != session.ModelRequestFailed || records[1].State != session.ModelRequestCompleted || records[0].Step == records[1].Step {
			t.Fatalf("ledger = %+v", records)
		}
		if text := assistantText(drained.outputs[len(drained.outputs)-1]); text != "second attempt" {
			t.Fatalf("final text = %q", text)
		}
	})
	t.Run("failover", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		primary := &adkScriptedModel{trace: trace, errs: []error{retryable}}
		secondary := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("from failover"))}}
		secondaryLedger := proof.ledgerModel(secondary)
		agent, err := adk.NewTypedChatModelAgent[*schema.AgenticMessage](proof.ctx, &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{
			Name: "proof", Model: proof.ledgerModel(primary),
			ModelFailoverConfig: &adk.ModelFailoverConfig[*schema.AgenticMessage]{
				MaxRetries:     1,
				ShouldFailover: func(context.Context, *schema.AgenticMessage, error) bool { return true },
				GetFailoverModel: func(ctx context.Context, failoverCtx *adk.FailoverContext[*schema.AgenticMessage]) (einomodel.BaseModel[*schema.AgenticMessage], []*schema.AgenticMessage, error) {
					trace.add("failover.select attempt=%d", failoverCtx.FailoverAttempt)
					return secondaryLedger, nil, nil
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: agent})
		drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
		if len(drained.errs) != 0 {
			t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
		}
		records := proof.modelRequests()
		if len(records) != 2 || records[0].State != session.ModelRequestFailed || records[1].State != session.ModelRequestCompleted || primary.calls.Load() != 1 || secondary.calls.Load() != 1 {
			t.Fatalf("ledger = %+v primary=%d secondary=%d", records, primary.calls.Load(), secondary.calls.Load())
		}
		assertTraceOrder(t, trace, "ledger.failed", "failover.select attempt=1", "ledger.dispatch_started", "ledger.completed")
	})
}
