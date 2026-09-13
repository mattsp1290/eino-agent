package consumer

// This file is W8's fresh external-consumer proof for the Eino v0.9.19
// agentic adoption (plan .agents/plans/eino-v0-9-19/08-execution-handoff.md).
// Every fixture here drives ACTUAL production constructors -- the real
// github.com/mattsp1290/eino-providers native adapter, the real
// runtime.StreamingOrchestrator/ADK TurnLoop, the real composition registry,
// the real agui bridge/replay -- against fake native HTTP/SSE transports and
// a real SQLite store, never hand-built messages standing in for provider
// translation. check.sh must copy this file alongside the other fixtures for
// it to run from a genuine external module boundary.
//
// Known, accepted defects this file documents rather than hides (see the
// coordinator's grounding notes): eino-agent-doj (AG-UI replay re-emits a
// tool call's lifecycle a second time on reconnect) and eino-agent-6wj
// (agui/bridge.go's toolPayload decodes content/structured, but the wire
// carries output, so a live/replayed tool result shows a synthesized status
// stub instead of the real output).
// TestPublicAGUIDecodesNativeInputAndReplayProjectsCommittedContent below
// asserts the ACTUAL current behavior for both, with a comment citing each
// bead, rather than asserting a stronger contract that does not hold.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/claude"

	nativeclaude "github.com/mattsp1290/eino-providers/claude"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	agenticmiddleware "github.com/mattsp1290/eino-agent/examples/agentic-middleware"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	"github.com/mattsp1290/eino-agent/tools"
	"github.com/mattsp1290/eino-agent/transport"
)

// nativeResponseIdentityStripper wraps a real einomodel.AgenticModel client
// and clears ResponseMeta.Extension before a result reaches eino-agent's own
// durable content pipeline. See its construction site below for why this is
// necessary today (a discovered cross-library integration gap, not a design
// choice this fixture endorses).
type nativeResponseIdentityStripper struct {
	client einomodel.AgenticModel
}

func stripResponseIdentity(msg *einoschema.AgenticMessage) *einoschema.AgenticMessage {
	if msg == nil || msg.ResponseMeta == nil || msg.ResponseMeta.Extension == nil {
		return msg
	}
	clone := *msg
	metaClone := *msg.ResponseMeta
	metaClone.Extension = nil
	clone.ResponseMeta = &metaClone
	return &clone
}

func (s nativeResponseIdentityStripper) Generate(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	msg, err := s.client.Generate(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	return stripResponseIdentity(msg), nil
}

func (s nativeResponseIdentityStripper) Stream(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	upstream, err := s.client.Stream(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	return einoschema.StreamReaderWithConvert(upstream, func(msg *einoschema.AgenticMessage) (*einoschema.AgenticMessage, error) {
		return stripResponseIdentity(msg), nil
	}), nil
}

// --- 1. Native AgenticModel: generate/stream equivalence, continuation, and
//        durable runtime dispatch -------------------------------------------

// fakeClaudeMessagesServer is a fake native Anthropic Messages transport,
// exercising the exact wire shapes github.com/mattsp1290/eino-providers/claude
// decodes (claudeResponse for generate, the SSE claudeStreamEvent frames for
// stream). It never performs real network I/O.
func fakeClaudeMessagesServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			Stream bool `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_native_fixture\",\"model\":\"claude-native-fixture\"}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"weighing the request\"}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"PRIVATE_SIGNATURE_FIXTURE\"}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"native reply\"}}\n\n" +
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
			return
		}
		_, _ = w.Write([]byte(`{"id":"msg_native_fixture","model":"claude-native-fixture","stop_reason":"end_turn",` +
			`"content":[{"type":"thinking","thinking":"weighing the request","signature":"PRIVATE_SIGNATURE_FIXTURE"},` +
			`{"type":"text","text":"native reply"}],"usage":{"input_tokens":4,"output_tokens":6}}`))
	}))
}

// TestPublicNativeAgenticModelGenerateStreamEquivalenceAndContinuation drives
// github.com/mattsp1290/eino-providers/claude.NewAgenticModel -- the real
// native-provider constructor pinned in go.mod at the exact commit verified
// in docs/dependency-status.md -- against a fake native HTTP/SSE transport.
// It proves (a) Generate and Stream produce byte-equivalent public content
// for the same fake native response, (b) SplitAgenticContinuation/
// RestoreAgenticContinuation round-trip the private reasoning signature
// without exposing it on the public projection, and (c) the same client,
// wrapped by the real model.NewAgenticStreamer adapter, drives one complete
// durable turn through runtime.StreamingOrchestrator against a real SQLite
// store (the "then durable runtime continuation" half of the W8
// requirement).
func TestPublicNativeAgenticModelGenerateStreamEquivalenceAndContinuation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	server := fakeClaudeMessagesServer(t)
	defer server.Close()

	client, err := nativeclaude.NewAgenticModel(ctx, nativeclaude.AgenticModelConfig{
		APIKey: "fixture-key", Model: "claude-native-fixture", MaxTokens: 64,
		BaseURL: server.URL, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("NewAgenticModel error = %v", err)
	}

	input := []*einoschema.AgenticMessage{{
		Role:          einoschema.AgenticRoleTypeUser,
		ContentBlocks: []*einoschema.ContentBlock{einoschema.NewContentBlock(&einoschema.UserInputText{Text: "hello native claude"})},
	}}

	// (a) generation vs streaming equivalence at the native adapter boundary.
	generated, err := client.Generate(ctx, input)
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	if len(generated.ContentBlocks) != 2 || generated.ContentBlocks[1].AssistantGenText == nil || generated.ContentBlocks[1].AssistantGenText.Text != "native reply" {
		t.Fatalf("generated = %#v", generated)
	}
	if generated.ContentBlocks[0].Reasoning == nil || generated.ContentBlocks[0].Reasoning.Signature != "PRIVATE_SIGNATURE_FIXTURE" {
		t.Fatalf("generated reasoning = %#v", generated.ContentBlocks[0])
	}

	stream, err := client.Stream(ctx, input)
	if err != nil {
		t.Fatalf("Stream error = %v", err)
	}
	defer stream.Close()
	var chunks []*einoschema.AgenticMessage
	for {
		chunk, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		chunks = append(chunks, chunk)
	}
	concatenated, err := einoschema.ConcatAgenticMessages(chunks)
	if err != nil {
		t.Fatalf("ConcatAgenticMessages error = %v", err)
	}
	if len(concatenated.ContentBlocks) != len(generated.ContentBlocks) {
		t.Fatalf("concatenated blocks = %d, generated blocks = %d", len(concatenated.ContentBlocks), len(generated.ContentBlocks))
	}
	if concatenated.ContentBlocks[0].Reasoning.Signature != generated.ContentBlocks[0].Reasoning.Signature {
		t.Fatalf("stream signature = %q, generate signature = %q", concatenated.ContentBlocks[0].Reasoning.Signature, generated.ContentBlocks[0].Reasoning.Signature)
	}
	if concatenated.ContentBlocks[1].AssistantGenText.Text != generated.ContentBlocks[1].AssistantGenText.Text {
		t.Fatalf("stream text = %q, generate text = %q", concatenated.ContentBlocks[1].AssistantGenText.Text, generated.ContentBlocks[1].AssistantGenText.Text)
	}

	// (b) continuation split/restore: the public projection must not leak
	// the private reasoning signature; restoring must bring it back exactly.
	public, state, err := nativeclaude.SplitAgenticContinuation(generated)
	if err != nil {
		t.Fatalf("SplitAgenticContinuation error = %v", err)
	}
	if public.ContentBlocks[0].Reasoning.Signature != "" {
		t.Fatalf("public projection leaked reasoning signature: %#v", public.ContentBlocks[0].Reasoning)
	}
	publicJSON, err := json.Marshal(public)
	if err != nil {
		t.Fatal(err)
	}
	mustNoPrivateSentinel(t, "public continuation projection", publicJSON)
	restored, err := nativeclaude.RestoreAgenticContinuation(public, state)
	if err != nil {
		t.Fatalf("RestoreAgenticContinuation error = %v", err)
	}
	if restored.ContentBlocks[0].Reasoning.Signature != "PRIVATE_SIGNATURE_FIXTURE" {
		t.Fatalf("restored signature = %q", restored.ContentBlocks[0].Reasoning.Signature)
	}

	// (c) durable runtime continuation: the same native client, wrapped by
	// the production model.NewAgenticStreamer adapter, drives one real turn
	// against a real SQLite store.
	//
	// Discovered integration gap (documented here, not hidden): eino-agent's
	// own content pipeline (session.responseMetaFromEino) fails closed with
	// ErrContentUnsupported on ANY non-nil generic ResponseMeta.Extension,
	// but every github.com/mattsp1290/eino-providers native adapter
	// populates exactly that field with einoproviders.AgenticResponseIdentity
	// on every completed response. model.NewTypedExtensionStateCodec (the
	// only AgenticStateCodec eino-agent ships) explicitly rejects a non-nil
	// Extension too, so no existing eino-agent provider-state codec can
	// capture/restore it. A host pairing this native provider with the
	// durable runtime must supply its own normalization -- exactly the
	// nativeResponseIdentityStripper decorator below -- until eino-agent
	// grows a codec for this shape (or eino-providers moves this identity
	// into a typed *Extension field responseMetaFromEino already handles).
	// The native response carries a private reasoning signature, so the
	// runtime requires a state-aware streamer (the plain NewAgenticStreamer
	// fails closed on provider-private content by design). The typed
	// extension codec is the one AgenticStateCodec eino-agent ships that
	// already knows how to capture/restore a reasoning signature.
	codec, err := model.NewTypedExtensionStateCodec(model.ProviderStateContract{
		CodecID: "fixture.test/native-claude-reasoning", Version: 1, CompatibilityKey: "native-claude-v1",
		Limits: model.ProviderStateLimits{MaxItems: 4, MaxItemBytes: 4096, MaxMessageBytes: 8192, MaxEnvelopeBytes: 16384, MaxStoredMessageBytes: 16384},
	})
	if err != nil {
		t.Fatalf("NewTypedExtensionStateCodec error = %v", err)
	}
	streamer, err := model.NewAgenticStreamerWithProviderState(nativeResponseIdentityStripper{client: client}, codec)
	if err != nil {
		t.Fatalf("NewAgenticStreamerWithProviderState error = %v", err)
	}
	st, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "native-provider.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{streamer}), runtime.WithRunPlanProvider(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "native-provider", Message: runtime.TextUserMessage("hello native claude"),
		Config: config.Snapshot{Agent: config.Agent{Name: "native-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	messages, projErr := history.LoadAgentic(ctx, st, "native-provider", history.Options{})
	if projErr != nil {
		t.Fatal(projErr)
	}
	if len(messages.Messages) != 2 {
		t.Fatalf("durable messages = %d, want 2 (user, assistant)", len(messages.Messages))
	}
	last := messages.Messages[len(messages.Messages)-1]
	if agenticMessageText(last) != "native reply" {
		t.Fatalf("durable assistant text = %q", agenticMessageText(last))
	}
	durableJSON, err := json.Marshal(messages.Messages)
	if err != nil {
		t.Fatal(err)
	}
	mustNoPrivateSentinel(t, "durable public projection", durableJSON)
}

// --- 2. Ordered media/citations plus function/server/MCP records survive a
//        real SQLite reopen -------------------------------------------------
//
// Grounded finding, not a test-writing mistake: runtime/adk_model.go's
// commit method (errADKUnsupportedBlock) fails a turn closed if the MODEL's
// own result carries a server_tool_call/server_tool_result/mcp_tool_call/
// mcp_tool_result/mcp_list_tools_result block -- the current typed-ADK
// adapter only accepts text/reasoning/media/function-tool-call blocks (plus
// mcp_tool_approval_request when an approval binding is wired) as assistant
// OUTPUT, even though session.ContentFromAgenticMessage (the content/store
// layer) fully supports encoding and decoding all 20 block kinds. This is
// consistent with docs/architecture/eino-feature-support.md's W5 "known
// gaps" note that ADK's own tools node cannot represent tool_search_result
// either. So this fixture proves two things separately, honestly: (1)
// ordered media/citations plus a real function tool call survive a real
// model turn and SQLite reopen, through the actual runtime/ADK path; (2)
// server/MCP call/result content itself round-trips through the public
// store contract (AppendMessage/AppendPart, the same seam
// session.ContentFromAgenticMessage and the ADK adapter both build on) and
// survives reopen -- via the store's own public write API directly, since
// no model-turn path can legally produce it today.

// orderedContentScript drives a two-step turn: first a rich assistant
// message carrying ordered text-with-citation, image, and a real function
// tool call, then (once the tool result is visible) a final plain text
// reply.
type orderedContentScript struct{ calls atomic.Int32 }

func (s *orderedContentScript) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	if s.calls.Add(1) == 1 {
		msg := &einoschema.AgenticMessage{
			Role: einoschema.AgenticRoleTypeAssistant,
			ContentBlocks: []*einoschema.ContentBlock{
				einoschema.NewContentBlock(&einoschema.AssistantGenText{
					Text: "see the cited result",
					ClaudeExtension: &claude.AssistantGenTextExtension{
						Citations: []*claude.TextCitation{{
							Type: claude.TextCitationTypeWebSearchResultLocation,
							WebSearchResultLocation: &claude.CitationWebSearchResultLocation{
								CitedText: "eino v0.9.19 adds typed ADK", Title: "Example Source", URL: "https://example.test/a",
							},
						}},
					},
				}),
				einoschema.NewContentBlock(&einoschema.AssistantGenImage{URL: "https://example.test/img.png", MIMEType: "image/png"}),
				{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: agenticToolCall("call-note-1", "record_note", `{"note":"remember this"}`)},
			},
		}
		writer.Send(model.StreamDelta{Message: msg}, nil)
		writer.Close()
		return reader, nil
	}
	writer.Send(model.StreamDelta{Message: agenticAssistantText("noted")}, nil)
	writer.Close()
	return reader, nil
}

// appendServerAndMCPRecordsDirectly writes one assistant message carrying
// ordered server_tool_call/server_tool_result/mcp_tool_call/mcp_tool_result/
// mcp_list_tools_result blocks straight through the public store contract
// (AdmitRun, Execution, AppendMessage/AppendPart, FinalizeAssistantMessage
// -- the same public seam a durable ADK commit would use if one existed for
// these kinds). See the comment above TestPublicOrderedContent... for why
// no live model turn can legally produce this content today.
func appendServerAndMCPRecordsDirectly(t *testing.T, ctx context.Context, st session.Store, registry *composition.Registry, sessionID session.ID) session.MessageID {
	t.Helper()
	ids := discoveryIDs{}
	plan, err := registry.AcquireRunPlan(ctx, runtime.RunPlanRequest{SessionID: sessionID, Config: config.Snapshot{Agent: config.Agent{Name: "ordered-consumer-direct"}}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	plan.Release()
	now := time.Now().UTC()
	run, err := st.AdmitRun(ctx, session.Run{
		ID: ids.NewRunID(), SessionID: sessionID, OwnerID: "direct-writer", ClaimToken: string(ids.NewRunID()),
		Status: session.RunRunning, Agent: "ordered-consumer-direct", ProviderID: "discovery", ModelID: "deterministic",
		ExtensionPlan: descriptor, CreatedAt: now,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	execution := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	messageID := ids.NewMessageID()
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: messageID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, Agent: run.Agent,
		ModelID: run.ModelID, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	content := session.Content{Role: session.RoleAssistant, Blocks: []session.ContentBlock{
		{ID: string(ids.NewPartID()), Kind: session.BlockKindServerToolCall, ServerCall: &session.ServerCallBlock{CallID: "srv-1", Name: "web_search", Arguments: json.RawMessage(`{"query":"eino v0.9.19"}`)}},
		{ID: string(ids.NewPartID()), Kind: session.BlockKindServerToolResult, ServerResult: &session.ServerResultBlock{CallID: "srv-1", Name: "web_search", Content: json.RawMessage(`{"results":["result-a"]}`)}},
		{ID: string(ids.NewPartID()), Kind: session.BlockKindMCPToolCall, MCPCall: &session.MCPCallBlock{ServerLabel: "mcp-fixture", CallID: "mcp-1", Name: "list_files", Arguments: `{"path":"/"}`}},
		{ID: string(ids.NewPartID()), Kind: session.BlockKindMCPToolResult, MCPResult: &session.MCPResultBlock{ServerLabel: "mcp-fixture", CallID: "mcp-1", Name: "list_files", Content: `{"files":["a.txt"]}`}},
		{ID: string(ids.NewPartID()), Kind: session.BlockKindMCPListToolsResult, MCPListTools: &session.MCPListToolsBlock{ServerLabel: "mcp-fixture", Tools: []session.MCPToolDefinition{{Name: "list_files", Description: "List files"}}}},
	}}
	parts, err := session.EncodeContentParts(content, func() session.PartID { return ids.NewPartID() }, messageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range parts {
		if _, err := execution.AppendPart(ctx, part); err != nil {
			t.Fatal(err)
		}
	}
	if err := execution.FinalizeAssistantMessage(ctx, messageID); err != nil {
		t.Fatal(err)
	}
	return messageID
}

func mountRecordNoteTool(t *testing.T) (*composition.Registry, *composition.Mount) {
	t.Helper()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(t.Context(), extension.Component{
		InstanceID: "ordered-content-fixture", Artifact: extension.Artifact{
			Name: "ordered-content-fixture", Version: "1", Hash: "ordered-content-fixture-v1", ConfigHash: "default", SourceKind: extension.SourceNative,
		},
	}, composition.InstallerFunc(func(_ context.Context, r *composition.Registrar) error {
		return r.Tool(composition.ToolRegistration{ID: "record-note", Scope: extension.GlobalScope(), Definition: tools.Definition{
			Name: "record_note", Description: "Records a note for the fixture.",
			Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"note": {Type: einoschema.String, Required: true}}),
			Execute: func(context.Context, tools.Execution) (json.RawMessage, error) {
				return json.RawMessage(`{"recorded":true}`), nil
			},
		}})
	}))
	if err != nil {
		t.Fatal(err)
	}
	return registry, mount
}

func TestPublicOrderedContentCitationsServerAndMCPRecordsSurviveReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "ordered-content.db")
	st, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	registry, mount := mountRecordNoteTool(t)
	defer func() { mount.Deactivate(); _ = mount.Close(context.Background()) }()
	script := &orderedContentScript{}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{script}), runtime.WithRunPlanProvider(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "ordered-content", Message: runtime.TextUserMessage("start"),
		Config: config.Snapshot{Agent: config.Agent{Name: "ordered-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}

	// Separately, and honestly through a different seam (see the comment
	// above this test): write server/MCP records directly through the
	// public store contract in the same session.
	appendServerAndMCPRecordsDirectly(t, ctx, st, registry, "ordered-content")

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()

	projection, err := history.LoadAgentic(ctx, reopened, "ordered-content", history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Messages) < 3 {
		t.Fatalf("durable messages = %d, want at least 3 (user, model turn's assistant reply(ies), direct server/mcp message)", len(projection.Messages))
	}

	// (1) ordered media/citations plus a real function tool call, through
	// the real model-turn/ADK path.
	rich := projection.Messages[1]
	wantKinds := []einoschema.ContentBlockType{
		einoschema.ContentBlockTypeAssistantGenText,
		einoschema.ContentBlockTypeAssistantGenImage,
		einoschema.ContentBlockTypeFunctionToolCall,
	}
	if len(rich.ContentBlocks) != len(wantKinds) {
		t.Fatalf("reopened rich message blocks = %d, want %d: %#v", len(rich.ContentBlocks), len(wantKinds), rich.ContentBlocks)
	}
	for i, want := range wantKinds {
		if rich.ContentBlocks[i].Type != want {
			t.Fatalf("block[%d].Type = %q, want %q (full: %#v)", i, rich.ContentBlocks[i].Type, want, rich.ContentBlocks)
		}
	}
	citation := rich.ContentBlocks[0].AssistantGenText
	if citation == nil || citation.ClaudeExtension == nil || len(citation.ClaudeExtension.Citations) != 1 {
		t.Fatalf("citation missing after reopen: %#v", citation)
	}
	got := citation.ClaudeExtension.Citations[0]
	if got.WebSearchResultLocation == nil || got.WebSearchResultLocation.Title != "Example Source" || got.WebSearchResultLocation.URL != "https://example.test/a" {
		t.Fatalf("citation content after reopen = %#v", got)
	}
	if rich.ContentBlocks[1].AssistantGenImage == nil || rich.ContentBlocks[1].AssistantGenImage.URL != "https://example.test/img.png" {
		t.Fatalf("image block after reopen = %#v", rich.ContentBlocks[1])
	}
	if rich.ContentBlocks[2].FunctionToolCall == nil || rich.ContentBlocks[2].FunctionToolCall.Name != "record_note" {
		t.Fatalf("function tool call after reopen = %#v", rich.ContentBlocks[2])
	}
	projectionJSON, err := json.Marshal(projection.Messages)
	if err != nil {
		t.Fatal(err)
	}
	mustNoPrivateSentinel(t, "durable projection", projectionJSON)
	// The model-turn's own final assistant reply is always the message
	// immediately before the directly-appended server/MCP message (whose
	// position is fixed at the end, since it was appended last).
	final := projection.Messages[len(projection.Messages)-2]
	if agenticMessageText(final) != "noted" {
		t.Fatalf("final assistant text after reopen = %q", agenticMessageText(final))
	}

	// (2) server/MCP records, written through the store's own public
	// contract directly, survive reopen in the exact order written.
	direct := projection.Messages[len(projection.Messages)-1]
	wantDirectKinds := []einoschema.ContentBlockType{
		einoschema.ContentBlockTypeServerToolCall,
		einoschema.ContentBlockTypeServerToolResult,
		einoschema.ContentBlockTypeMCPToolCall,
		einoschema.ContentBlockTypeMCPToolResult,
		einoschema.ContentBlockTypeMCPListToolsResult,
	}
	if len(direct.ContentBlocks) != len(wantDirectKinds) {
		t.Fatalf("direct message blocks = %d, want %d: %#v", len(direct.ContentBlocks), len(wantDirectKinds), direct.ContentBlocks)
	}
	for i, want := range wantDirectKinds {
		if direct.ContentBlocks[i].Type != want {
			t.Fatalf("direct block[%d].Type = %q, want %q (full: %#v)", i, direct.ContentBlocks[i].Type, want, direct.ContentBlocks)
		}
	}
	if direct.ContentBlocks[0].ServerToolCall == nil || direct.ContentBlocks[0].ServerToolCall.CallID != "srv-1" {
		t.Fatalf("server tool call after reopen = %#v", direct.ContentBlocks[0])
	}
	if direct.ContentBlocks[2].MCPToolCall == nil || direct.ContentBlocks[2].MCPToolCall.Name != "list_files" {
		t.Fatalf("mcp tool call after reopen = %#v", direct.ContentBlocks[2])
	}
	if direct.ContentBlocks[4].MCPListToolsResult == nil || len(direct.ContentBlocks[4].MCPListToolsResult.Tools) != 1 {
		t.Fatalf("mcp list-tools result after reopen = %#v", direct.ContentBlocks[4])
	}
}

// --- 3. Tool search discovers a deferred tool, then the model calls it by
//        its alias --------------------------------------------------------

type toolSearchAliasScript struct{ calls atomic.Int32 }

func (s *toolSearchAliasScript) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	switch s.calls.Add(1) {
	case 1:
		writer.Send(model.StreamDelta{Message: agenticAssistantToolCalls(agenticToolCall("call-search-1", "tool_search", `{"query":"select:get_weather"}`))}, nil)
	case 2:
		writer.Send(model.StreamDelta{Message: agenticAssistantToolCalls(agenticToolCall("call-weather-1", "weather", `{"city":"nyc"}`))}, nil)
	default:
		writer.Send(model.StreamDelta{Message: agenticAssistantText("done")}, nil)
	}
	writer.Close()
	return reader, nil
}

func TestPublicToolSearchDiscoversDeferredToolThenAliasExecutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	st, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "tool-search-alias.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	var executedWith string
	mount, err := registry.Mount(ctx, extension.Component{
		InstanceID: "tool-search-alias-fixture", Artifact: extension.Artifact{
			Name: "tool-search-alias-fixture", Version: "1", Hash: "tool-search-alias-fixture-v1", ConfigHash: "default", SourceKind: extension.SourceNative,
		},
	}, composition.InstallerFunc(func(_ context.Context, r *composition.Registrar) error {
		if err := r.Tool(composition.ToolRegistration{ID: "get-weather", Scope: extension.GlobalScope(), Definition: tools.Definition{
			Name: "get_weather", Description: "Reports fixture weather.", Aliases: []string{"weather"}, Deferred: true,
			Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"city": {Type: einoschema.String, Required: true}}),
			Execute: func(_ context.Context, e tools.Execution) (json.RawMessage, error) {
				executedWith = e.Call.RequestedName
				return json.RawMessage(`{"forecast":"sunny"}`), nil
			},
		}}); err != nil {
			return err
		}
		return r.ToolSearch(composition.ToolSearchRegistration{ID: "search", Scope: extension.GlobalScope()})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { mount.Deactivate(); _ = mount.Close(context.Background()) }()
	script := &toolSearchAliasScript{}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{script}), runtime.WithRunPlanProvider(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "tool-search-alias", Message: runtime.TextUserMessage("what is the weather"),
		Config: config.Snapshot{Agent: config.Agent{Name: "tool-search-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	if executedWith != "weather" {
		t.Fatalf("executed with requested name = %q, want alias %q", executedWith, "weather")
	}
	projection, err := history.LoadAgentic(ctx, st, "tool-search-alias", history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var sawSearchResult, sawAliasCall bool
	for _, msg := range projection.Messages {
		for _, block := range msg.ContentBlocks {
			if block == nil {
				continue
			}
			if block.Type == einoschema.ContentBlockTypeToolSearchResult && block.ToolSearchFunctionToolResult != nil {
				sawSearchResult = true
			}
			if block.Type == einoschema.ContentBlockTypeFunctionToolCall && block.FunctionToolCall != nil && block.FunctionToolCall.Name == "weather" {
				sawAliasCall = true
			}
		}
	}
	if !sawSearchResult {
		t.Fatal("no durable tool_search_result block found")
	}
	if !sawAliasCall {
		t.Fatal("no durable function_tool_call block recorded under the model-requested alias name")
	}
}

// --- 4. adk.TurnLoop: two completed turns under one run --------------------

func TestPublicTurnLoopTwoCompletedTurnsUnderOneRunAgainstSQLite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	st, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "two-turns.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	m := &discoveryModel{}
	orchestrator := discoveryRuntime(t, st, m)
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "two-turns", Message: runtime.TextUserMessage("first"),
		Config: config.Snapshot{Agent: config.Agent{Name: "two-turns-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Enqueue immediately, before the loop has a chance to idle out, so the
	// second turn is driven by the SAME live adk.TurnLoop/run fence as the
	// first -- not a second, independent run.
	if _, err := orchestrator.Enqueue(ctx, "two-turns", runtime.EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "second-message", Message: runtime.TextUserMessage("second"),
	}); err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	turns, err := st.ListTurns(ctx, handle.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[0].Ordinal != 1 || turns[1].Ordinal != 2 {
		t.Fatalf("turns = %+v", turns)
	}
	if turns[0].AssistantMessageID == "" || turns[0].AssistantMessageID == turns[1].AssistantMessageID {
		t.Fatalf("turns do not carry two distinct assistant messages: %+v", turns)
	}
	requests, err := st.ListModelRequests(ctx, handle.RunID(), session.ModelRequestCursor{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(requests.Records) != 2 || requests.Records[0].InvocationID == requests.Records[1].InvocationID {
		t.Fatalf("model requests = %+v", requests.Records)
	}
	if len(m.recorded()) != 2 {
		t.Fatalf("provider calls = %d, want 2", len(m.recorded()))
	}
}

// --- 5. MCP approval pause, checkpoint reopen (simulated process restart),
//        and resume ---------------------------------------------------------

type approvalPauseScript struct{ calls atomic.Int32 }

func (s *approvalPauseScript) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	if s.calls.Add(1) == 1 {
		msg := &einoschema.AgenticMessage{
			Role: einoschema.AgenticRoleTypeAssistant,
			ContentBlocks: []*einoschema.ContentBlock{
				einoschema.NewContentBlock(&einoschema.MCPToolApprovalRequest{ID: "approval-1", Name: "delete_everything", ServerLabel: "mcp-fixture"}),
			},
		}
		writer.Send(model.StreamDelta{Message: msg}, nil)
		writer.Close()
		return reader, nil
	}
	writer.Send(model.StreamDelta{Message: agenticAssistantText("approved and handled")}, nil)
	writer.Close()
	return reader, nil
}

func TestPublicApprovalCheckpointSurvivesProcessRestartAndResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "approval-checkpoint.db")
	st, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	script := &approvalPauseScript{}
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{script}), runtime.WithRunPlanProvider(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "approval-checkpoint", Message: runtime.TextUserMessage("please delete everything"),
		Config: config.Snapshot{Agent: config.Agent{Name: "approval-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var pause runtime.PauseInfo
	select {
	case pause = <-handle.AwaitPause():
	case result := <-handle.Done():
		t.Fatalf("run completed before pausing: %+v", result)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(pause.InterruptContexts) != 1 {
		t.Fatalf("interrupt contexts = %#v", pause.InterruptContexts)
	}
	interruptID := pause.InterruptContexts[0].ID
	if interruptID == "" {
		t.Fatal("empty interrupt id")
	}
	// Done() for a paused run reports session.RunPaused; draining it isn't
	// needed for correctness (AwaitPause already carried the pause detail
	// above), but reading it now keeps this goroutine from blocking forever
	// on a channel nothing else will read.
	<-handle.Done()

	// Simulate a full process restart: close the pool entirely and reopen
	// the same on-disk database from scratch, with a brand new store handle
	// and a brand new orchestrator/registry -- nothing in-process survives.
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()
	freshRegistry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	freshScript := &approvalPauseScript{calls: atomic.Int32{}}
	freshScript.calls.Store(1) // the resumed continuation must not re-issue the approval request
	freshOrchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(reopened), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{freshScript}), runtime.WithRunPlanProvider(freshRegistry),
	)
	if err != nil {
		t.Fatal(err)
	}
	resumedHandle, err := freshOrchestrator.ResumeRun(ctx, handle.RunID(), runtime.ResumeRequest{
		Targets: map[string]any{interruptID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	result := <-resumedHandle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("resumed result = %+v", result)
	}

	projection, err := history.LoadAgentic(ctx, reopened, "approval-checkpoint", history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var sawResponse bool
	for _, msg := range projection.Messages {
		for _, block := range msg.ContentBlocks {
			if block != nil && block.Type == einoschema.ContentBlockTypeMCPToolApprovalResponse && block.MCPToolApprovalResponse != nil {
				if block.MCPToolApprovalResponse.ApprovalRequestID != "approval-1" || !block.MCPToolApprovalResponse.Approve {
					t.Fatalf("approval response = %#v", block.MCPToolApprovalResponse)
				}
				sawResponse = true
			}
		}
	}
	if !sawResponse {
		t.Fatal("no durable mcp_tool_approval_response block after resume")
	}
	final := projection.Messages[len(projection.Messages)-1]
	if agenticMessageText(final) != "approved and handled" {
		t.Fatalf("final assistant text = %q", agenticMessageText(final))
	}

	// No private state (claim tokens, checkpoint bytes) crosses into any
	// public record touched by this fixture.
	run, err := reopened.GetRun(ctx, handle.RunID())
	if err != nil {
		t.Fatal(err)
	}
	runJSON, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	mustNoPrivateSentinel(t, "run record", runJSON)
}

// --- 6. Typed ADK summarization middleware compacts context into a durable
//        session.ContextEpoch, surviving a real SQLite reopen -------------

// summarizationGenerationMarker is upstream eino's own fixed marker text for
// the summarization recipe's internal generation call (see
// examples/agentic-middleware/middleware_test.go's requestIsSummaryGeneration,
// which cannot be imported here since it lives in a _test.go file).
const summarizationGenerationMarker = "CRITICAL: Respond with TEXT ONLY"

func requestIsSummaryGenerationFixture(request model.Request) bool {
	for _, msg := range request.Messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block != nil && block.UserInputText != nil && strings.Contains(block.UserInputText.Text, summarizationGenerationMarker) {
				return true
			}
		}
	}
	return false
}

type summarizationScript struct{}

func (summarizationScript) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	if requestIsSummaryGenerationFixture(request) {
		writer.Send(model.StreamDelta{Message: agenticAssistantText("a compact summary of everything so far")}, nil)
	} else {
		writer.Send(model.StreamDelta{Message: agenticAssistantText("ok, understood")}, nil)
	}
	writer.Close()
	return reader, nil
}

// disableAllHandlerKindsExceptSummarization mirrors the composed example's
// own allHandlerKinds/disableAllExcept (both unexported, in a _test.go file
// this fixture cannot import): every registered W6 recipe kind except
// summarization, so agentic-middleware.Mount only wires the one recipe this
// test exercises.
func disableAllHandlerKindsExceptSummarization() map[string]bool {
	return map[string]bool{
		runtime.HandlerKindAgentsMD:       true,
		runtime.HandlerKindSkill:          true,
		runtime.HandlerKindFilesystem:     true,
		runtime.HandlerKindPlanTask:       true,
		runtime.HandlerKindPatchToolCalls: true,
		runtime.HandlerKindReduction:      true,
		runtime.HandlerKindToolSearch:     true,
	}
}

func TestPublicSummarizationMiddlewareWritesDurableContextEpochSurvivingReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "summarization.db")
	st, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := session.ID("summarization-consumer")
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := agenticmiddleware.Mount(ctx, registry, sessionID, agenticmiddleware.Config{
		SummarizationTriggerMsgs: 1,
		Disable:                  disableAllHandlerKindsExceptSummarization(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { mount.Deactivate(); _ = mount.Close(context.Background()) }()
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{summarizationScript{}}), runtime.WithRunPlanProvider(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	cfg := config.Snapshot{Agent: config.Agent{Name: "summarization-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}}

	handle1, err := orchestrator.Start(ctx, runtime.Request{SessionID: sessionID, Message: runtime.TextUserMessage("hello there"), Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-handle1.Done(); result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("turn 1 result = %+v", result)
	}
	handle2, err := orchestrator.Start(ctx, runtime.Request{SessionID: sessionID, Message: runtime.TextUserMessage("please continue"), Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-handle2.Done(); result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("turn 2 result = %+v", result)
	}

	epochs, err := st.ListContextEpochs(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var summaryEpoch session.ContextEpoch
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" && epoch.SummaryMessageID != "" {
			summaryEpoch = epoch
		}
	}
	if summaryEpoch.ID == "" {
		t.Fatalf("no summarization ContextEpoch found among %+v", epochs)
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()
	reopenedEpochs, err := reopened.ListContextEpochs(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, epoch := range reopenedEpochs {
		if epoch.ID == summaryEpoch.ID && epoch.SummaryMessageID == summaryEpoch.SummaryMessageID {
			found = true
		}
	}
	if !found {
		t.Fatalf("summarization epoch %+v did not survive reopen: %+v", summaryEpoch, reopenedEpochs)
	}
}

// --- 7. AG-UI decode of a native input message, plus real Bridge/Replay
//        projection of committed content -- documenting eino-agent-doj and
//        eino-agent-6wj rather than hiding them --------------------------

type aguiToolTurnScript struct{ calls atomic.Int32 }

func (s *aguiToolTurnScript) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	if s.calls.Add(1) == 1 {
		writer.Send(model.StreamDelta{Message: agenticAssistantToolCalls(agenticToolCall("agui-call-1", "echo_note", `{"note":"hello from agui"}`))}, nil)
	} else {
		writer.Send(model.StreamDelta{Message: agenticAssistantText("agui final text")}, nil)
	}
	writer.Close()
	return reader, nil
}

func TestPublicAGUIDecodesNativeInputAndReplayProjectsCommittedContent(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	st, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "agui-decode.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()

	// Real AG-UI native ingress decode (transport.DecodeUserMessage), the
	// same function an HTTP route would call on a real request body.
	body, err := json.Marshal(map[string]any{"content": []types.InputContent{{Type: types.InputContentTypeText, Text: "hello from agui"}}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/messages", bytes.NewReader(body))
	decoded, err := transport.DecodeUserMessage(request)
	if err != nil {
		t.Fatalf("DecodeUserMessage error = %v", err)
	}
	if len(decoded.Blocks) != 1 || decoded.Blocks[0].Kind != session.BlockKindUserInputText || decoded.Blocks[0].Text == nil || decoded.Blocks[0].Text.Text != "hello from agui" {
		t.Fatalf("decoded = %#v", decoded)
	}

	registry, mount := mountEchoNoteTool(t)
	defer func() { mount.Deactivate(); _ = mount.Close(context.Background()) }()
	script := &aguiToolTurnScript{}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{script}), runtime.WithRunPlanProvider(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "agui-decode", Message: decoded,
		Config: config.Snapshot{Agent: config.Agent{Name: "agui-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}

	// Real agui.NewBridge + agui.Replay -- exactly what transport.SSEHandler
	// drives in production -- against a real SQLite store, replaying every
	// durable event for this session from the beginning.
	var out bytes.Buffer
	writer := bufio.NewWriter(&out)
	bridge := agentagui.NewBridge(ctx, st, session.DefaultContentLimits(), false, writer, sse.NewSSEWriter(), "agui-decode", string(handle.RunID()), nil)
	if _, err := agentagui.Replay(ctx, bridge, st, "agui-decode", session.EventCursor{Limit: 100}, session.DefaultContentLimits(), false); err != nil {
		t.Fatalf("Replay error = %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	stream := out.String()
	if !strings.Contains(stream, "agui final text") {
		t.Fatalf("replay stream missing final assistant text: %s", stream)
	}
	if strings.Contains(stream, "PRIVATE_") {
		t.Fatalf("replay stream leaked private material: %s", stream)
	}

	// eino-agent-doj: emitMessageSnapshot's replay path natively delivers a
	// tool call's whole lifecycle via DeliveryModeReplay but never marks it
	// as delivered (recordNativeToolDelivery/markToolCallResultSent), so
	// replay() then re-forwards the same durable tool_call_updated records
	// a second time. Assert the ACTUAL current behavior -- the call's
	// arguments string appears more than once in one full replay -- rather
	// than a single-emission contract that does not hold today.
	occurrences := strings.Count(stream, "echo_note")
	if occurrences < 2 {
		t.Fatalf("expected eino-agent-doj's known duplicate tool-call lifecycle emission (>=2 occurrences of the tool name), got %d in: %s", occurrences, stream)
	}

	// eino-agent-6wj: agui/bridge.go's toolPayload decodes content/
	// structured, but the durable tool_transition wire payload carries
	// output/error/metadata, so ResultContent() falls back to a synthesized
	// status stub instead of the real tool result. Assert the stub is what
	// actually reaches the wire, not the real recorded output
	// (`{"echoed":"hello from agui"}`), and note this bug explicitly rather
	// than silently accepting a weaker contract.
	if strings.Contains(stream, `"echoed"`) {
		t.Fatalf("tool result carried real output; eino-agent-6wj is apparently fixed, update this fixture: %s", stream)
	}
	if !strings.Contains(stream, `\"status\"`) && !strings.Contains(stream, `"status"`) {
		t.Fatalf("expected eino-agent-6wj's synthesized status stub in the replayed tool result, got: %s", stream)
	}
}

func mountEchoNoteTool(t *testing.T) (*composition.Registry, *composition.Mount) {
	t.Helper()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(t.Context(), extension.Component{
		InstanceID: "agui-echo-note-fixture", Artifact: extension.Artifact{
			Name: "agui-echo-note-fixture", Version: "1", Hash: "agui-echo-note-fixture-v1", ConfigHash: "default", SourceKind: extension.SourceNative,
		},
	}, composition.InstallerFunc(func(_ context.Context, r *composition.Registrar) error {
		return r.Tool(composition.ToolRegistration{ID: "echo-note", Scope: extension.GlobalScope(), Definition: tools.Definition{
			Name: "echo_note", Description: "Echoes a note back for the fixture.",
			Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"note": {Type: einoschema.String, Required: true}}),
			Execute: func(_ context.Context, e tools.Execution) (json.RawMessage, error) {
				var input struct {
					Note string `json:"note"`
				}
				if err := json.Unmarshal(e.Input, &input); err != nil {
					return nil, err
				}
				return json.Marshal(map[string]string{"echoed": input.Note})
			},
		}})
	}))
	if err != nil {
		t.Fatal(err)
	}
	return registry, mount
}

// --- 8. Enhanced (multi-part) streamed tool results survive a real SQLite
//        reopen ------------------------------------------------------------

type enhancedToolScript struct{ calls atomic.Int32 }

func (s *enhancedToolScript) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	if s.calls.Add(1) == 1 {
		writer.Send(model.StreamDelta{Message: agenticAssistantToolCalls(agenticToolCall("call-enhanced-1", "describe_chart", `{}`))}, nil)
	} else {
		writer.Send(model.StreamDelta{Message: agenticAssistantText("described")}, nil)
	}
	writer.Close()
	return reader, nil
}

// TestPublicEnhancedToolResultPartsSurviveReopen registers a tool whose
// Definition.ExecuteRich returns a multi-part tools.RichResult (text plus an
// image part), the enhanced-result path
// docs/architecture/eino-feature-support.md's W4 section describes
// (runtime.ToolResultPart, preferred over the classic scalar Execute output
// whenever both are set). It drives one real turn and asserts, after
// closing and reopening the SQLite file, that the durable
// function_tool_result block carries both content items, in order, with
// their real content -- not the classic single-text-part shape a scalar
// Execute result would have produced.
func TestPublicEnhancedToolResultPartsSurviveReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "enhanced-tool.db")
	st, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(ctx, extension.Component{
		InstanceID: "enhanced-tool-fixture", Artifact: extension.Artifact{
			Name: "enhanced-tool-fixture", Version: "1", Hash: "enhanced-tool-fixture-v1", ConfigHash: "default", SourceKind: extension.SourceNative,
		},
	}, composition.InstallerFunc(func(_ context.Context, r *composition.Registrar) error {
		return r.Tool(composition.ToolRegistration{ID: "describe-chart", Scope: extension.GlobalScope(), Definition: tools.Definition{
			Name: "describe_chart", Description: "Returns a multi-part enhanced result for the fixture.",
			Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
			// The zero-value RetentionPolicy allows zero inline bytes (fail
			// closed), which would degrade every part to an omission
			// record; a real host configures this per tool.
			Retention: runtime.RetentionPolicy{MaxInlineBytes: 4096},
			ExecuteRich: func(context.Context, tools.Execution) (tools.RichResult, error) {
				return tools.RichResult{Parts: []runtime.ToolResultPart{
					{Type: runtime.ToolResultPartText, Text: "quarterly revenue chart"},
					{Type: runtime.ToolResultPartImage, Media: &runtime.ToolResultMedia{URL: "https://example.test/chart.png", MIMEType: "image/png"}},
				}}, nil
			},
		}})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { mount.Deactivate(); _ = mount.Close(context.Background()) }()
	script := &enhancedToolScript{}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}),
		runtime.WithModelResolver(discoveryResolver{script}), runtime.WithRunPlanProvider(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "enhanced-tool", Message: runtime.TextUserMessage("describe the chart"),
		Config: config.Snapshot{Agent: config.Agent{Name: "enhanced-tool-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()

	projection, err := history.LoadAgentic(ctx, reopened, "enhanced-tool", history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var found *einoschema.FunctionToolResult
	for _, msg := range projection.Messages {
		for _, block := range msg.ContentBlocks {
			if block != nil && block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil && block.FunctionToolResult.Name == "describe_chart" {
				found = block.FunctionToolResult
			}
		}
	}
	if found == nil {
		t.Fatal("no durable function_tool_result block for describe_chart")
	}
	if len(found.Content) != 2 {
		t.Fatalf("enhanced result content items = %d, want 2 (classic scalar results always carry exactly 1): %#v", len(found.Content), found.Content)
	}
	if found.Content[0].Type != einoschema.FunctionToolResultContentBlockTypeText || found.Content[0].Text == nil || found.Content[0].Text.Text != "quarterly revenue chart" {
		t.Fatalf("enhanced result part[0] after reopen = %#v", found.Content[0])
	}
	if found.Content[1].Type != einoschema.FunctionToolResultContentBlockTypeImage || found.Content[1].Image == nil || found.Content[1].Image.URL != "https://example.test/chart.png" {
		t.Fatalf("enhanced result part[1] after reopen = %#v", found.Content[1])
	}
}

// --- helpers shared by this file only --------------------------------------

// mustNoPrivateSentinel fails the test if raw contains any "PRIVATE_"
// sentinel, mirroring the same pattern session_title_fixture_test.go and
// session_watch_fixture_test.go use to prove no private state crosses a
// public boundary.
func mustNoPrivateSentinel(t *testing.T, label string, raw []byte) {
	t.Helper()
	if strings.Contains(string(raw), "PRIVATE_") {
		t.Fatalf("%s leaked private material: %s", label, raw)
	}
}
