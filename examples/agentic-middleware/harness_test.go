package agenticmiddleware

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// taskCreatedIDPattern extracts the numeric task id out of plantask's own
// human-readable TaskCreate output ("Task #<N> created successfully: ..."),
// mirroring the same pattern runtime's internal e2e tests use.
var taskCreatedIDPattern = regexp.MustCompile(`Task #(\d+) created`)

// testNativeComponent/testScope build a minimal extension.Component/Scope
// for a session-scoped native tool/handler mount in these tests, separate
// from Mount's own component identity.
func testNativeComponent(name string, sessionID session.ID) extension.Component {
	return extension.Component{InstanceID: "example.agentic-middleware/" + name + "/" + string(sessionID), Artifact: extension.Artifact{
		Name: name, Version: "1", Hash: name + "-artifact", ConfigHash: "default", SourceKind: extension.SourceNative,
	}}
}

func testScope(sessionID session.ID) extension.Scope {
	return extension.SessionScope(string(sessionID))
}

// --- IDs -------------------------------------------------------------------

type testIDs struct{ next atomic.Int64 }

func (i *testIDs) id(prefix string) string           { return fmt.Sprintf("%s-%d", prefix, i.next.Add(1)) }
func (i *testIDs) NewRunID() session.RunID           { return session.RunID(i.id("run")) }
func (i *testIDs) NewMessageID() session.MessageID   { return session.MessageID(i.id("message")) }
func (i *testIDs) NewPartID() session.PartID         { return session.PartID(i.id("part")) }
func (i *testIDs) NewToolCallID() session.ToolCallID { return session.ToolCallID(i.id("tool-call")) }
func (i *testIDs) NewEventID() session.EventID       { return session.EventID(i.id("event")) }
func (i *testIDs) NewEpochID() session.EpochID       { return session.EpochID(i.id("epoch")) }
func (i *testIDs) NewTurnID() session.TurnID         { return session.TurnID(i.id("turn")) }
func (i *testIDs) NewInboxID() session.InboxID       { return session.InboxID(i.id("inbox")) }
func (i *testIDs) NewInvocationID() string           { return i.id("invocation") }

// --- Model resolution / scripted streaming ---------------------------------

type testResolver struct{ streamer model.Streamer }

func (r testResolver) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{Provider: model.Provider{ID: "fake"}, Model: model.Descriptor{ID: "test", ProviderID: "fake"}, Streamer: r.streamer}, nil
}

// scriptedStreamer drives a turn's Generate/Stream calls from a plain Go
// func, mirroring the same proven pattern the runtime package's own
// internal e2e tests use (runtime/orchestrator_test_support_test.go), but
// built here from only public types (model.Streamer/model.Request/
// model.StreamDelta) so it works from outside the runtime package.
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

// --- Agentic message helpers (mirroring runtime's own internal test
// helpers, built from only public *schema.AgenticMessage constructors) ----

func agenticToolCallChunk(index int, callID, name, arguments string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlockChunk(&einoschema.FunctionToolCall{CallID: callID, Name: name, Arguments: arguments}, &einoschema.StreamingMeta{Index: index}),
		},
	}
}

func agenticAssistantText(text string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role:          einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: text}}},
	}
}

// toolResultTexts concatenates every function_tool_result text part across
// every message in messages, and reports whether any image part was seen.
func toolResultParts(messages []*einoschema.AgenticMessage) (text string, sawImage bool) {
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			for _, part := range block.FunctionToolResult.Content {
				if part == nil {
					continue
				}
				if part.Type == einoschema.FunctionToolResultContentBlockTypeText && part.Text != nil {
					text += part.Text.Text
				}
				if part.Type == einoschema.FunctionToolResultContentBlockTypeImage {
					sawImage = true
				}
			}
		}
	}
	return text, sawImage
}

// toolSearchResultDiscoveredNames extracts every discovered tool name out
// of any tool_search_result content block across messages -- the durable
// settlement shape both the native tool-search path and (since item 1 of
// the round-two W6 review) the toolsearch recipe's own sealed search tool
// now use, in place of an ordinary function_tool_result.
func toolSearchResultDiscoveredNames(messages []*einoschema.AgenticMessage) []string {
	var names []string
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeToolSearchResult || block.ToolSearchFunctionToolResult == nil {
				continue
			}
			result := block.ToolSearchFunctionToolResult.Result
			if result == nil {
				continue
			}
			for _, info := range result.Tools {
				if info != nil && info.Name != "" {
					names = append(names, info.Name)
				}
			}
		}
	}
	return names
}

// --- Orchestrator construction ---------------------------------------------

// testScratchRootOnce lazily creates ONE process-scoped temp directory used
// as every test orchestrator's scratch root (runtime.WithScratchRoot),
// mirroring the runtime package's own identically-named test helper
// (runtime/orchestrator_test_support_test.go). Without this, every test in
// this package that mounts reduction/plantask falls back to
// NewStreamingOrchestrator's real, machine-global default
// (os.UserCacheDir()/eino-agent/scratch), writing real, never-cleaned
// directories into the actual user's cache directory every test run
// instead of a temp directory the OS eventually reclaims (round-three W6
// authority-regression review S6).
var testScratchRootOnce = sync.OnceValue(func() string {
	dir, err := os.MkdirTemp("", "eino-agent-example-scratch-")
	if err != nil {
		panic(err)
	}
	return dir
})

func newTestOrchestrator(t *testing.T, store session.Store, registry *composition.Registry, streamer model.Streamer) *runtime.StreamingOrchestrator {
	t.Helper()
	return newTestOrchestratorWithIDs(t, store, registry, streamer, &testIDs{})
}

// newTestOrchestratorWithIDs is newTestOrchestrator with an explicit
// runtime.IDGenerator, for tests that construct a second, independent
// *runtime.StreamingOrchestrator instance (simulating a process restart)
// against a store a first instance already wrote to: a second bare
// &testIDs{} would restart its counter from 1 and collide with IDs the
// first instance already durably used (this harness's testIDs is a
// process-local sequential counter, not the UUID/ULID generation a real
// deployment would use, which never collides like this across restarts).
// Passing the SAME *testIDs across both instances keeps the counter
// monotonic across the simulated restart without reintroducing any of the
// production-relevant in-memory state (discovered-tool sets, provider
// state, etc.) the restart is meant to drop.
func newTestOrchestratorWithIDs(t *testing.T, store session.Store, registry *composition.Registry, streamer model.Streamer, ids *testIDs) *runtime.StreamingOrchestrator {
	t.Helper()
	orch, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store),
		runtime.WithRunPlanProvider(registry),
		runtime.WithIDGenerator(ids),
		runtime.WithOwnerID("agentic-middleware-example"),
		runtime.WithModelResolver(testResolver{streamer: streamer}),
		runtime.WithScratchRoot(testScratchRootOnce()),
	)
	if err != nil {
		t.Fatal(err)
	}
	return orch
}

func testConfig(workspaceRoot string) config.Snapshot {
	selection := model.Selection{ProviderID: "fake", ModelID: "test"}
	metadata := map[string]string{}
	if workspaceRoot != "" {
		metadata["workspace_root"] = workspaceRoot
		metadata["workspace_id"] = "example-workspace"
	}
	return config.Snapshot{
		Agent:    config.Agent{Name: "agent", Model: selection, Options: map[string]string{}},
		Model:    selection,
		Metadata: metadata,
	}
}

// allHandlerKinds is every Kind this example's Mount can install; used with
// disableAllExcept to mount only the recipe(s) a given test needs, since an
// unrelated recipe that requires a backing store/workspace this test never
// configured (e.g. toolsearch's deferred-tool requirement) would otherwise
// fail the whole turn's agent construction closed.
var allHandlerKinds = []string{
	runtime.HandlerKindAgentsMD, runtime.HandlerKindSkill, runtime.HandlerKindFilesystem,
	runtime.HandlerKindPlanTask, runtime.HandlerKindPatchToolCalls, runtime.HandlerKindReduction,
	runtime.HandlerKindSummarization, runtime.HandlerKindToolSearch,
}

func disableAllExcept(keep ...string) map[string]bool {
	wanted := make(map[string]bool, len(keep))
	for _, kind := range keep {
		wanted[kind] = true
	}
	disable := make(map[string]bool)
	for _, kind := range allHandlerKinds {
		if !wanted[kind] {
			disable[kind] = true
		}
	}
	return disable
}

func awaitDone(t *testing.T, handle runtime.Handle, timeout time.Duration) runtime.Result {
	t.Helper()
	select {
	case result := <-handle.Done():
		return result
	case <-time.After(timeout):
		t.Fatal("run did not complete in time")
		return runtime.Result{}
	}
}
