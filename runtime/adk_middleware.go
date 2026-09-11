package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cloudwego/eino/adk"
	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/skill"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// HandlerBuildContext is the bounded construction context a HandlerFactory
// (a composition.Registrar.Handler registration) receives when this
// package's engine snapshots it for one turn's agent build. Every value here
// is scoped to the current run: Model is the same mandatory ledger-audited
// adapter AgentBuildContext.Model carries (so any model call a handler's
// middleware makes -- e.g. summarization's own summary-generation call -- is
// still durably ledgered through the same audit boundary), ToolWrapper turns
// an arbitrary tool.BaseTool the middleware constructs (e.g. filesystem's
// ls/read_file/write_file tools) into one durableGuard accepts, and
// FilesystemBackend/SkillBackend are read-only views rooted at the admitted
// canonical workspace (nil when no workspace root is configured for this
// run, so a recipe that requires one fails construction closed rather than
// silently operating unscoped).
type HandlerBuildContext struct {
	SessionID     session.ID
	RunID         session.RunID
	WorkspaceRoot string
	Model         einomodel.AgenticModel
	ToolWrapper   func(tool.BaseTool) tool.BaseTool
	// FilesystemBackend and SkillBackend are read-only views rooted at the
	// admitted canonical workspace.
	FilesystemBackend adkfilesystem.Backend
	SkillBackend      skill.Backend
	// PlanTaskBackend and ReductionBackend are writable, workspace-private
	// scratch backends (not part of the read-only content view above): each
	// is rooted at its own subdirectory under the admitted workspace,
	// scoped to that one recipe's own state, never the workspace's real
	// content.
	PlanTaskBackend  *writableWorkspaceBackend
	ReductionBackend *writableWorkspaceBackend
	// DeferredTools are this turn's frozen, already-durably-wrapped deferred
	// tools (session composition tools registered with Deferred: true),
	// ready to hand to dynamictool/toolsearch.Config.DynamicTools.
	DeferredTools []tool.BaseTool
	// Store, RunID, IDs and Now let a handler factory perform its own
	// bounded durable reads/writes (e.g. summarization's Finalize mapping a
	// completed summary into a session.ContextEpoch via
	// compaction.AppendBoundary). Store is the same durable store this run's
	// engine uses; IDs mints canonical identities the same way the engine
	// does.
	Store session.Store
	// Execution is the current run's fenced ExecutionStore -- the same
	// mutation capability this run's own engine writes through -- for a
	// handler factory that needs to durably write (e.g. summarization's
	// Finalize appending a compaction boundary).
	Execution session.ExecutionStore
	IDs       IDGenerator
	Now       func() time.Time
}

// HandlerFactory builds one typed ADK agent middleware instance for one
// turn's agent from a bounded HandlerBuildContext. Registered via
// composition.Registrar.Handler(HandlerRegistration{Factory: ...}); the
// closure is never serialized or part of the sealed plan fingerprint (only
// its HandlerDescriptor{Kind,Version,Config} identity is -- see
// session.AgentHandlerPlanIdentity). AgentFactory.BuildAgent's caller
// (adkEngine.buildAgent) invokes every plan-ordered factory fresh for each
// admitted turn and installs the results into AgentBuildContext.Handlers,
// ahead of the runtime's mandatory tail handlers (durableGuard,
// settlementSeal).
type HandlerFactory func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error)

// errSettledToolResultDiverged reports that a function_tool_result content
// block about to be sent to the model no longer matches the durably settled
// ToolCall output for that call ID -- i.e. some host handler's
// BeforeModelRewriteState mutated a settled, model-visible tool result after
// this runtime's own durable settlement already committed it. See
// settlementSeal's doc comment.
var errSettledToolResultDiverged = errors.New("host handler diverged a settled tool result before it reached the model")

// settlementSeal is one of this runtime's mandatory tail handlers (the other
// being durableGuard): it is appended to AgentBuildContext.Handlers/
// TypedChatModelAgentConfig.Handlers after every host-registered handler, so
// its BeforeModelRewriteState hook -- hooks run in registration order, first
// registered first called -- observes state.Messages only after every host
// handler's own BeforeModelRewriteState has already run, i.e. exactly what
// this turn is about to send to the model.
//
// For every function_tool_result content block that still carries a call ID
// matching a durably settled session.ToolCall row, the block's model-visible
// content must byte-for-byte match that row's settled Output: a host handler
// may still legitimately rewrite content for a call this runtime never
// settled at all (patchtoolcalls' whole purpose -- patching a dangling call
// with no durable ToolCall row), but it can never rewrite what this runtime
// already durably committed as the model-visible result of a call it
// actually executed. A CallID with no matching settled row is left alone.
type settlementSeal struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
	store session.Store
}

func newSettlementSeal(store session.Store) *settlementSeal {
	return &settlementSeal{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}, store: store}
}

func (s *settlementSeal) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	if state == nil {
		return ctx, state, nil
	}
	for _, msg := range state.Messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			callID := block.FunctionToolResult.CallID
			if callID == "" {
				continue
			}
			record, err := s.store.GetToolCall(ctx, session.ToolCallID(callID))
			if err != nil {
				// No durable row for this call ID (a dangling call a
				// middleware like patchtoolcalls legitimately patched, or a
				// call this run never settled at all): nothing to seal
				// against.
				continue
			}
			if !session.TerminalToolCall(record.Status) {
				continue
			}
			// Every durable adkTool.InvokableRun result is a plain string
			// (this runtime never dispatches EnhancedInvokableTool through
			// the mandatory tool adapter -- see runtime/adk_execution.go's
			// adkTool), which ADK always wraps as exactly one text content
			// block (adk.textToFunctionToolResultBlocks); a settled call's
			// model-visible content is therefore always that single block
			// holding the durable Output bytes verbatim.
			content := block.FunctionToolResult.Content
			if len(content) != 1 || content[0] == nil || content[0].Type != einoschema.FunctionToolResultContentBlockTypeText ||
				content[0].Text == nil || content[0].Text.Text != string(record.Output) {
				return ctx, nil, fmt.Errorf("%w: call %s", errSettledToolResultDiverged, callID)
			}
		}
	}
	return ctx, state, nil
}

// toolWrappingMiddleware decorates a host-built adk.TypedChatModelAgentMiddleware
// so that any tool it injects into ChatModelAgentContext.Tools via its own
// BeforeAgent (e.g. the filesystem middleware's ls/read_file/write_file
// tools, plantask's task tools, skill's "skill" tool) is wrapped through the
// mandatory durable adapter (HandlerBuildContext.ToolWrapper) before
// durableGuard ever sees it -- durableGuard rejects any tool that does not
// implement the unexported durableTool marker, so an upstream middleware's
// own tools would otherwise fail every turn that reaches BeforeAgent. Every
// recipe in this package wraps its upstream-built middleware with this
// before returning it from its HandlerFactory.
type toolWrappingMiddleware struct {
	adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]
	wrap func(tool.BaseTool) tool.BaseTool
}

func wrapHandlerTools(inner adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], wrap func(tool.BaseTool) tool.BaseTool) adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage] {
	if inner == nil || wrap == nil {
		return inner
	}
	return &toolWrappingMiddleware{TypedChatModelAgentMiddleware: inner, wrap: wrap}
}

func (m *toolWrappingMiddleware) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	ctx, runCtx, err := m.TypedChatModelAgentMiddleware.BeforeAgent(ctx, runCtx)
	if err != nil || runCtx == nil {
		return ctx, runCtx, err
	}
	for i, t := range runCtx.Tools {
		if t == nil {
			continue
		}
		if _, ok := t.(durableTool); ok {
			continue
		}
		runCtx.Tools[i] = m.wrap(t)
	}
	return ctx, runCtx, nil
}

// newAdkGenericDurableTool wraps an arbitrary tool.BaseTool a host handler's
// middleware constructs into the narrowest concrete type implementing
// exactly the run/stream capabilities the underlying tool actually has (so
// ADK's own tool.BaseTool capability type-assertions in its tools node stay
// correct -- see adkDurable* below) plus the durableTool marker durableGuard
// requires, and registers its advertised name into durable (a run's
// adkEngine-owned allow-set, shared with the turn's durableGuard) so the
// guard accepts it.
//
// This runtime does not route these calls through the session ToolCall
// ledger/claim/settle pipeline the way runtime.Tool-backed adapters
// (adkTool) do: that pipeline's identity model is built around the frozen
// composition-registered tool registry (PlanTool/ToolCall rows), which does
// not naturally extend to tools an upstream middleware constructs
// dynamically at agent-build time from its own Config. durableGuard's
// allow-set check is still enforced (no tool can silently bypass the
// runtime's mandatory adapters), but calls dispatched this way are not
// independently durably audited the way ordinary session tools are -- see
// the W6 docs section for this documented, bounded scope decision.
func newAdkGenericDurableTool(ctx context.Context, inner tool.BaseTool, durable map[string]bool) tool.BaseTool {
	info, infoErr := inner.Info(ctx)
	name := ""
	if info != nil {
		name = info.Name
	}
	if infoErr != nil || name == "" {
		return adkFailedDurableTool{inner: inner, err: infoErr}
	}
	if durable != nil {
		durable[name] = true
	}
	base := adkDurableBase{inner: inner, name: name}
	_, enhancedInvokable := inner.(tool.EnhancedInvokableTool)
	_, enhancedStreamable := inner.(tool.EnhancedStreamableTool)
	if enhancedInvokable && enhancedStreamable {
		return adkDurableEnhancedInvokableStreamable{base}
	}
	if enhancedInvokable {
		return adkDurableEnhancedInvokable{base}
	}
	if enhancedStreamable {
		return adkDurableEnhancedStreamable{base}
	}
	_, invokable := inner.(tool.InvokableTool)
	_, streamable := inner.(tool.StreamableTool)
	if invokable && streamable {
		return adkDurableInvokableStreamable{base}
	}
	if streamable {
		return adkDurableStreamable{base}
	}
	// Default to invokable even when the underlying tool declares neither
	// capability: this keeps every wrapped tool a durableTool (so
	// durableGuard never rejects it purely for lacking a marker), and
	// InvokableRun below fails safely and closed at call time instead.
	return adkDurableInvokable{base}
}

type adkDurableBase struct {
	inner tool.BaseTool
	name  string
}

func (t adkDurableBase) Info(ctx context.Context) (*einoschema.ToolInfo, error) {
	return t.inner.Info(ctx)
}
func (t adkDurableBase) durableToolName() string { return t.name }

var _ durableTool = adkDurableBase{}

// adkFailedDurableTool is returned when the underlying tool's own Info call
// failed or reported no name; it still satisfies durableTool (with an empty
// name, which durableGuard's allow-set will never contain) so wrapping never
// panics, and fails closed on every execution attempt.
type adkFailedDurableTool struct {
	inner tool.BaseTool
	err   error
}

var _ durableTool = adkFailedDurableTool{}

func (t adkFailedDurableTool) Info(ctx context.Context) (*einoschema.ToolInfo, error) {
	return t.inner.Info(ctx)
}
func (t adkFailedDurableTool) durableToolName() string { return "" }
func (t adkFailedDurableTool) InvokableRun(context.Context, string, ...tool.Option) (string, error) {
	return "", fmt.Errorf("%w: tool metadata unavailable: %v", errADKUnsupportedBlock, t.err)
}

type adkDurableInvokable struct{ adkDurableBase }

var _ tool.InvokableTool = adkDurableInvokable{}

func (t adkDurableInvokable) InvokableRun(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
	invokable, ok := t.inner.(tool.InvokableTool)
	if !ok {
		return "", fmt.Errorf("%w: tool %q is not invokable", errADKUnsupportedBlock, t.name)
	}
	return invokable.InvokableRun(ctx, argumentsInJSON, opts...)
}

type adkDurableStreamable struct{ adkDurableBase }

var _ tool.StreamableTool = adkDurableStreamable{}

func (t adkDurableStreamable) StreamableRun(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*einoschema.StreamReader[string], error) {
	streamable, ok := t.inner.(tool.StreamableTool)
	if !ok {
		return nil, fmt.Errorf("%w: tool %q is not streamable", errADKUnsupportedBlock, t.name)
	}
	return streamable.StreamableRun(ctx, argumentsInJSON, opts...)
}

type adkDurableInvokableStreamable struct{ adkDurableBase }

var (
	_ tool.InvokableTool  = adkDurableInvokableStreamable{}
	_ tool.StreamableTool = adkDurableInvokableStreamable{}
)

func (t adkDurableInvokableStreamable) InvokableRun(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
	invokable, ok := t.inner.(tool.InvokableTool)
	if !ok {
		return "", fmt.Errorf("%w: tool %q is not invokable", errADKUnsupportedBlock, t.name)
	}
	return invokable.InvokableRun(ctx, argumentsInJSON, opts...)
}

func (t adkDurableInvokableStreamable) StreamableRun(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*einoschema.StreamReader[string], error) {
	streamable, ok := t.inner.(tool.StreamableTool)
	if !ok {
		return nil, fmt.Errorf("%w: tool %q is not streamable", errADKUnsupportedBlock, t.name)
	}
	return streamable.StreamableRun(ctx, argumentsInJSON, opts...)
}

type adkDurableEnhancedInvokable struct{ adkDurableBase }

var _ tool.EnhancedInvokableTool = adkDurableEnhancedInvokable{}

func (t adkDurableEnhancedInvokable) InvokableRun(ctx context.Context, toolArgument *einoschema.ToolArgument, opts ...tool.Option) (*einoschema.ToolResult, error) {
	enhanced, ok := t.inner.(tool.EnhancedInvokableTool)
	if !ok {
		return nil, fmt.Errorf("%w: tool %q is not enhanced-invokable", errADKUnsupportedBlock, t.name)
	}
	return enhanced.InvokableRun(ctx, toolArgument, opts...)
}

type adkDurableEnhancedStreamable struct{ adkDurableBase }

var _ tool.EnhancedStreamableTool = adkDurableEnhancedStreamable{}

func (t adkDurableEnhancedStreamable) StreamableRun(ctx context.Context, toolArgument *einoschema.ToolArgument, opts ...tool.Option) (*einoschema.StreamReader[*einoschema.ToolResult], error) {
	enhanced, ok := t.inner.(tool.EnhancedStreamableTool)
	if !ok {
		return nil, fmt.Errorf("%w: tool %q is not enhanced-streamable", errADKUnsupportedBlock, t.name)
	}
	return enhanced.StreamableRun(ctx, toolArgument, opts...)
}

type adkDurableEnhancedInvokableStreamable struct{ adkDurableBase }

var (
	_ tool.EnhancedInvokableTool  = adkDurableEnhancedInvokableStreamable{}
	_ tool.EnhancedStreamableTool = adkDurableEnhancedInvokableStreamable{}
)

func (t adkDurableEnhancedInvokableStreamable) InvokableRun(ctx context.Context, toolArgument *einoschema.ToolArgument, opts ...tool.Option) (*einoschema.ToolResult, error) {
	enhanced, ok := t.inner.(tool.EnhancedInvokableTool)
	if !ok {
		return nil, fmt.Errorf("%w: tool %q is not enhanced-invokable", errADKUnsupportedBlock, t.name)
	}
	return enhanced.InvokableRun(ctx, toolArgument, opts...)
}

func (t adkDurableEnhancedInvokableStreamable) StreamableRun(ctx context.Context, toolArgument *einoschema.ToolArgument, opts ...tool.Option) (*einoschema.StreamReader[*einoschema.ToolResult], error) {
	enhanced, ok := t.inner.(tool.EnhancedStreamableTool)
	if !ok {
		return nil, fmt.Errorf("%w: tool %q is not enhanced-streamable", errADKUnsupportedBlock, t.name)
	}
	return enhanced.StreamableRun(ctx, toolArgument, opts...)
}
