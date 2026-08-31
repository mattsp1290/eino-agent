package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestFrozenToolLoopHistoryRemainsOrderedAfterSQLiteReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "store.db")
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
	}()
	frozenAt := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "echoed"}, nil
		}),
	}}}
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
			for _, message := range request.Messages {
				if message.Role == einoschema.Tool {
					return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
				}
			}
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
				ID: "call-frozen", Type: "function",
				Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"text":"hi"}`},
			}})}, nil
		})}),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
		WithIDGenerator(&reverseAdmissionIDs{}),
		WithClock(func() time.Time { return frozenAt }),
		WithOwnerID("sqlite-tool-order-test"),
	)
	const sessionID session.ID = "frozen-tool-session"
	handle, err := orchestrator.Start(ctx, Request{SessionID: sessionID, Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-handle.Done(); result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()

	var rawMessages []session.Message
	var rawParts []session.Part
	cursor := session.ReplayCursor{Limit: 1}
	for {
		page, err := reopened.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			t.Fatal(err)
		}
		rawMessages = append(rawMessages, page.Messages...)
		rawParts = append(rawParts, page.Parts...)
		if page.Next == (session.ReplayCursor{}) {
			break
		}
		cursor = page.Next
	}
	wantIDs := []session.MessageID{"z-user-1", "a-assistant-1", "z-user-2", "a-assistant-2"}
	wantRoles := []session.Role{session.RoleUser, session.RoleAssistant, session.RoleTool, session.RoleAssistant}
	if len(rawMessages) != len(wantIDs) {
		t.Fatalf("raw messages = %#v", rawMessages)
	}
	for index := range wantIDs {
		if rawMessages[index].ID != wantIDs[index] || rawMessages[index].Role != wantRoles[index] {
			t.Fatalf("raw message order = %#v, want ids %#v roles %#v", rawMessages, wantIDs, wantRoles)
		}
		if index > 0 && !rawMessages[index].CreatedAt.After(rawMessages[index-1].CreatedAt) {
			t.Fatalf("message times are not strictly increasing: %#v", rawMessages)
		}
	}
	if rawMessages[1].ParentID != rawMessages[0].ID || rawMessages[2].ParentID != rawMessages[1].ID || rawMessages[3].ParentID != rawMessages[1].ID {
		t.Fatalf("message parentage changed: %#v", rawMessages)
	}
	toolCall, err := reopened.GetToolCall(ctx, "call-frozen")
	if err != nil {
		t.Fatal(err)
	}
	if !toolCall.CompletedAt.Equal(frozenAt) {
		t.Fatalf("observed tool completion = %s, want frozen clock %s", toolCall.CompletedAt, frozenAt)
	}
	var resultPart session.Part
	for _, part := range rawParts {
		if part.Kind == session.PartToolResult {
			resultPart = part
		}
	}
	if resultPart.ID == "" || resultPart.MessageID != rawMessages[2].ID || !resultPart.CreatedAt.Equal(rawMessages[2].CreatedAt) || !resultPart.UpdatedAt.Equal(rawMessages[2].UpdatedAt) {
		t.Fatalf("tool result part = %+v, tool message = %+v", resultPart, rawMessages[2])
	}

	providerHistory, err := LoadHistory(ctx, reopened, sessionID, history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(providerHistory) != 4 || providerHistory[0].Role != einoschema.User || providerHistory[0].Content != "hello" ||
		providerHistory[1].Role != einoschema.Assistant || len(providerHistory[1].ToolCalls) != 1 || providerHistory[1].ToolCalls[0].ID != "call-frozen" ||
		providerHistory[2].Role != einoschema.Tool || providerHistory[2].Content != "echoed" ||
		providerHistory[3].Role != einoschema.Assistant || providerHistory[3].Content != "done" {
		t.Fatalf("provider history = %#v", providerHistory)
	}
}
