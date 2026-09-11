package runtime

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// onlyToolCallID returns the single durable tool-call id minted into an
// admissionStore during a test run. prepareToolCalls always mints a fresh,
// store-unique ID now (see ProviderCallID on session.ToolCall/runtime.ToolCall),
// so tests can no longer assume a scripted provider CallID literal (e.g.
// "call-1") became the durable ID -- they must discover the minted id from
// the store instead. Fails the test if zero or more than one tool call exists.
func onlyToolCallID(t *testing.T, store *admissionStore) session.ToolCallID {
	t.Helper()
	var id session.ToolCallID
	var count int
	for candidate := range store.toolCalls {
		id = candidate
		count++
	}
	if count != 1 {
		t.Fatalf("onlyToolCallID: store has %d tool calls, want exactly 1", count)
	}
	return id
}

// toolCallIDByName returns the durable id of the single tool call in the
// admissionStore whose Name matches, for tests where more than one tool
// call is created and onlyToolCallID's single-entry assumption doesn't hold.
func toolCallIDByName(t *testing.T, store *admissionStore, name string) session.ToolCallID {
	t.Helper()
	var id session.ToolCallID
	var count int
	for candidate, call := range store.toolCalls {
		if call.Name == name {
			id = candidate
			count++
		}
	}
	if count != 1 {
		t.Fatalf("toolCallIDByName(%q): found %d matching tool calls, want exactly 1", name, count)
	}
	return id
}

// testScratchRootOnce lazily creates ONE process-scoped temp directory used
// as every test orchestrator's default scratch root (WithScratchRoot),
// unless a test explicitly overrides it via extra. Without this, every
// runtime test exercising plantask/reduction would otherwise fall back to
// NewStreamingOrchestrator's real, machine-global default
// (os.UserCacheDir()/eino-agent/scratch), writing real files outside the
// test sandbox and risking cross-test-run collisions on a repeated literal
// session ID (sessionScratchDirName hashes the session id, but does not
// scope it to a single test run) -- see round-two W6 review I5.
var testScratchRootOnce = sync.OnceValue(func() string {
	dir, err := os.MkdirTemp("", "eino-agent-test-scratch-")
	if err != nil {
		panic(err)
	}
	return dir
})

func newTestOrchestrator(store *admissionStore, streamer model.Streamer, extra ...Option) *StreamingOrchestrator {
	options := []Option{
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("owner-1"),
		WithQueueSize(2),
		WithScratchRoot(testScratchRootOnce()),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	}
	return mustConfiguredOrchestrator(append(options, extra...)...)
}

func mustConfiguredOrchestrator(extra ...Option) *StreamingOrchestrator {
	options := []Option{
		WithStore(newAdmissionStore()),
		WithModelResolver(resolvedModel{}),
		WithIDGenerator(&sequenceIDs{}),
		WithScratchRoot(testScratchRootOnce()),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	}
	orchestrator, err := NewStreamingOrchestrator(append(options, extra...)...)
	if err != nil {
		panic(err)
	}
	return orchestrator
}

func newTestRunExecution(host *StreamingOrchestrator, plan *RunPlan) *runExecution {
	return newRunExecution(host, plan, session.Run{
		ID: "test-run", SessionID: "test-session", ClaimToken: "test-claim", Status: session.RunRunning,
	})
}

func orchestratorConfig() config.Snapshot {
	selection := model.Selection{ProviderID: "fake", ModelID: "test"}
	return config.Snapshot{
		Agent: config.Agent{
			Name:    "agent",
			Model:   selection,
			Options: map[string]string{"temperature": "0"},
		},
		Model: selection,
		Metadata: map[string]string{
			"workspace_id":   "workspace-1",
			"workspace_root": os.TempDir(),
		},
	}
}

type resolvedModel struct {
	streamer model.Streamer
}

func (r resolvedModel) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{
		Provider: model.Provider{ID: "fake"},
		Model:    model.Descriptor{ID: "test", ProviderID: "fake"},
		Streamer: r.streamer,
	}, nil
}

type scriptedStreamer func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error)

func (s scriptedStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	messages, err := s(ctx, request)
	if err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[model.StreamDelta](len(messages))
	go func() {
		defer writer.Close()
		for _, msg := range messages {
			if writer.Send(model.StreamDelta{Message: msg, Usage: model.UsageFromAgenticMessage(msg)}, nil) {
				return
			}
		}
	}()
	return reader, nil
}

// --- Agentic test message helpers -----------------------------------------
//
// These build/inspect *schema.AgenticMessage values for scriptedStreamer
// scripts and assertions, mirroring the classic schema.AssistantMessage /
// schema.UserMessage / .Content convenience the suite used before the
// agentic cutover.

// agenticTextChunk returns one streamed assistant_gen_text chunk at the
// given StreamingMeta index, so concatenating several chunks at the same
// index merges them into one final block (mirrors a provider streaming one
// block across multiple deltas).
func agenticTextChunk(index int, text string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlockChunk(&einoschema.AssistantGenText{Text: text}, &einoschema.StreamingMeta{Index: index}),
		},
	}
}

// agenticToolCallChunk returns one streamed function_tool_call chunk at the
// given StreamingMeta index.
func agenticToolCallChunk(index int, callID, name, arguments string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlockChunk(&einoschema.FunctionToolCall{CallID: callID, Name: name, Arguments: arguments}, &einoschema.StreamingMeta{Index: index}),
		},
	}
}

// agenticAssistantText returns one complete (non-chunked) assistant message
// carrying a single assistant_gen_text block.
func agenticAssistantText(text string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role:          einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: text}}},
	}
}

// agenticAssistantReasoning returns one complete assistant message carrying
// a single reasoning block.
func agenticAssistantReasoning(text string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role:          einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: text}}},
	}
}

// agenticAssistantToolCalls returns one complete assistant message carrying
// one function_tool_call block per call.
func agenticAssistantToolCalls(calls ...*einoschema.FunctionToolCall) *einoschema.AgenticMessage {
	blocks := make([]*einoschema.ContentBlock, len(calls))
	for index, call := range calls {
		blocks[index] = &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: call}
	}
	return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: blocks}
}

// agenticToolCall is a small constructor for one function_tool_call value.
func agenticToolCall(callID, name, arguments string) *einoschema.FunctionToolCall {
	return &einoschema.FunctionToolCall{CallID: callID, Name: name, Arguments: arguments}
}

// agenticUserText returns one user message carrying a single
// user_input_text block.
func agenticUserText(text string) *einoschema.AgenticMessage {
	return einoschema.UserAgenticMessage(text)
}

// agenticSystemText returns one system message carrying a single
// user_input_text block.
func agenticSystemText(text string) *einoschema.AgenticMessage {
	return einoschema.SystemAgenticMessage(text)
}

// agenticMessageText concatenates every user_input_text/assistant_gen_text
// block's text on message, in order.
func agenticMessageText(message *einoschema.AgenticMessage) string {
	if message == nil {
		return ""
	}
	var sb []byte
	for _, block := range message.ContentBlocks {
		if block == nil {
			continue
		}
		switch block.Type {
		case einoschema.ContentBlockTypeUserInputText:
			if block.UserInputText != nil {
				sb = append(sb, block.UserInputText.Text...)
			}
		case einoschema.ContentBlockTypeAssistantGenText:
			if block.AssistantGenText != nil {
				sb = append(sb, block.AssistantGenText.Text...)
			}
		}
	}
	return string(sb)
}

// agenticReasoningText concatenates every reasoning block's text on message.
func agenticReasoningText(message *einoschema.AgenticMessage) string {
	if message == nil {
		return ""
	}
	var sb []byte
	for _, block := range message.ContentBlocks {
		if block != nil && block.Type == einoschema.ContentBlockTypeReasoning && block.Reasoning != nil {
			sb = append(sb, block.Reasoning.Text...)
		}
	}
	return string(sb)
}

// agenticToolCallsOf returns every function_tool_call block on message, in
// order.
func agenticToolCallsOf(message *einoschema.AgenticMessage) []*einoschema.FunctionToolCall {
	if message == nil {
		return nil
	}
	var calls []*einoschema.FunctionToolCall
	for _, block := range message.ContentBlocks {
		if block != nil && block.Type == einoschema.ContentBlockTypeFunctionToolCall && block.FunctionToolCall != nil {
			calls = append(calls, block.FunctionToolCall)
		}
	}
	return calls
}

// agenticFunctionResultText concatenates the text content of every
// function_tool_result block on message.
func agenticFunctionResultText(message *einoschema.AgenticMessage) string {
	if message == nil {
		return ""
	}
	var sb []byte
	for _, block := range message.ContentBlocks {
		if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
			continue
		}
		for _, item := range block.FunctionToolResult.Content {
			if item != nil && item.Type == einoschema.FunctionToolResultContentBlockTypeText && item.Text != nil {
				sb = append(sb, item.Text.Text...)
			}
		}
	}
	return string(sb)
}

// isFunctionToolResultMessage reports whether message carries at least one
// function_tool_result block (the agentic replacement for the classic
// schema.Tool role check).
func isFunctionToolResultMessage(message *einoschema.AgenticMessage) bool {
	if message == nil {
		return false
	}
	for _, block := range message.ContentBlocks {
		if block != nil && block.Type == einoschema.ContentBlockTypeFunctionToolResult {
			return true
		}
	}
	return false
}

type deltaStreamerFunc func(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error)

func (f deltaStreamerFunc) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	return f(ctx, request)
}

type sequenceIDs struct {
	mu sync.Mutex
	n  int
}

func (s *sequenceIDs) next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return prefix + "-" + strconv.Itoa(s.n)
}

func (s *sequenceIDs) NewRunID() session.RunID         { return session.RunID(s.next("run")) }
func (s *sequenceIDs) NewMessageID() session.MessageID { return session.MessageID(s.next("message")) }
func (s *sequenceIDs) NewPartID() session.PartID       { return session.PartID(s.next("part")) }
func (s *sequenceIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next("tool-call"))
}
func (s *sequenceIDs) NewEventID() session.EventID { return session.EventID(s.next("event")) }
func (s *sequenceIDs) NewEpochID() session.EpochID { return session.EpochID(s.next("epoch")) }
func (s *sequenceIDs) NewTurnID() session.TurnID   { return session.TurnID(s.next("turn")) }
func (s *sequenceIDs) NewInboxID() session.InboxID { return session.InboxID(s.next("inbox")) }
func (s *sequenceIDs) NewInvocationID() string     { return s.next("invocation") }

type blockingSink struct {
	mu     sync.Mutex
	events []session.EventRecord
	delay  time.Duration
}

func (s *blockingSink) Emit(_ context.Context, event session.EventRecord) {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *blockingSink) count(kind string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, event := range s.events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

type blockingSinkFunc func(context.Context, session.EventRecord)

func (f blockingSinkFunc) Emit(ctx context.Context, event session.EventRecord) {
	f(ctx, event)
}

type staticToolRegistry struct {
	tools []Tool
}

func (r staticToolRegistry) ResolveTools(context.Context, ToolScopeContext) ([]Tool, error) {
	return r.tools, nil
}

type orchestratorToolExecutorFunc func(context.Context, ToolCall) (ToolResult, error)

func (f orchestratorToolExecutorFunc) Execute(ctx context.Context, call ToolCall) (ToolResult, error) {
	return f(ctx, call)
}

func permissionPatternField(field string) PermissionPatternResolver {
	return PermissionPatternResolverFunc(func(_ context.Context, input json.RawMessage) (string, error) {
		var object map[string]string
		if err := json.Unmarshal(input, &object); err != nil {
			return "", err
		}
		return object[field], nil
	})
}
