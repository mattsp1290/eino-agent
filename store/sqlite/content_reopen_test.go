package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/openai"

	"github.com/mattsp1290/eino-agent/session"
)

// TestContentPartsSurviveSQLiteCloseAndReopen appends durable rich content
// parts (session.EncodeContentParts, covering reasoning, annotated assistant
// text, a function call, and a trailing response_meta part), closes the
// underlying *sql.DB, reopens the same file without running migrations (see
// reopenSQLiteFixture / TestProviderStatePayloadSurvivesSQLiteCloseAndReopen
// for the established pattern), and decodes the replayed parts back into an
// identical session.Content.
func TestContentPartsSurviveSQLiteCloseAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "content-reopen.db")
	store, err := openSQLiteFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "content-reopen-session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID: "content-reopen-run", SessionID: "content-reopen-session", ProviderID: "provider", ModelID: "model",
		OwnerID: "owner", ClaimToken: "claim", Status: session.RunPending, CreatedAt: now,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := session.Message{
		ID: "content-reopen-message", SessionID: run.SessionID, RunID: run.ID,
		Role: session.RoleAssistant, ModelID: run.ModelID, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := execution.AppendMessage(ctx, message); err != nil {
		t.Fatal(err)
	}

	msg := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: "reopen reasoning"}},
			{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{
				Text: "reopen answer",
				OpenAIExtension: &openai.AssistantGenTextExtension{
					Annotations: []*openai.TextAnnotation{
						{Type: openai.TextAnnotationTypeURLCitation, URLCitation: &openai.TextAnnotationURLCitation{
							Title: "src", URL: "https://example.com/reopen", StartIndex: 0, EndIndex: 4,
						}},
					},
				},
			}},
			{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{
				CallID: "reopen-call-1", Name: "get_weather", Arguments: `{"city":"nyc"}`,
			}},
		},
		ResponseMeta: &einoschema.AgenticResponseMeta{
			TokenUsage: &einoschema.TokenUsage{PromptTokens: 7, CompletionTokens: 3, TotalTokens: 10},
		},
	}
	blockIDs := 0
	content, _, err := session.ContentFromAgenticMessage(msg, func() string {
		blockIDs++
		return "reopen-block-" + string(rune('0'+blockIDs))
	})
	if err != nil {
		t.Fatalf("ContentFromAgenticMessage: %v", err)
	}
	partIDs := 0
	parts, err := session.EncodeContentParts(content, func() session.PartID {
		partIDs++
		return session.PartID("reopen-part-" + string(rune('0'+partIDs)))
	}, message.ID, message.SessionID, message.RunID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	for _, p := range parts {
		if _, err := execution.AppendPart(ctx, p); err != nil {
			t.Fatalf("append part %s: %v", p.ID, err)
		}
	}

	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = reopenSQLiteFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.db.Close() }()

	batch, err := store.ListMessages(ctx, message.SessionID, session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list messages after reopen: %v", err)
	}
	if len(batch.Messages) != 1 || len(batch.Parts) != len(parts) {
		t.Fatalf("replay after reopen = %d messages, %d parts; want 1, %d", len(batch.Messages), len(batch.Parts), len(parts))
	}

	decoded, err := session.DecodeContentParts(session.RoleAssistant, batch.Parts, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts after reopen: %v", err)
	}
	wantJSON, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	gotJSON, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("decoded content after reopen = %s, want %s", gotJSON, wantJSON)
	}
	if decoded.Meta == nil || decoded.Meta.Usage == nil || decoded.Meta.Usage.InputTokens != 7 || decoded.Meta.Usage.OutputTokens != 3 {
		t.Fatalf("decoded response meta after reopen = %#v, want usage 7/3", decoded.Meta)
	}

	wantMsg, err := session.ContentToAgenticMessage(content)
	if err != nil {
		t.Fatalf("ContentToAgenticMessage(original): %v", err)
	}
	gotMsg, err := session.ContentToAgenticMessage(decoded)
	if err != nil {
		t.Fatalf("ContentToAgenticMessage(decoded): %v", err)
	}
	wantMsgJSON, err := json.Marshal(wantMsg)
	if err != nil {
		t.Fatal(err)
	}
	gotMsgJSON, err := json.Marshal(gotMsg)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotMsgJSON) != string(wantMsgJSON) {
		t.Fatalf("rebuilt agentic message after reopen = %s, want %s", gotMsgJSON, wantMsgJSON)
	}
}
