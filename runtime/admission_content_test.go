package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// richTestBlocks builds a small but varied user submission spanning two
// user_input_text blocks and one user_input_image block, all with empty IDs
// so Start's admission-time assignment is exercised.
func richTestBlocks() []session.ContentBlock {
	return []session.ContentBlock{
		{Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "first "}},
		{Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "second"}},
		{Kind: session.BlockKindUserInputImage, Media: &session.MediaBlock{URL: "https://example.com/pic.png", MIMEType: "image/png"}},
	}
}

// TestAdmissionPersistsRichContentAtomicallyWithRunAndUserMessage exercises
// admission against a real SQLite-backed store: every content block must be
// durable, in ordinal order, with the correct kind, atomically alongside the
// admitted run and user message, and decode back to the exact input.
func TestAdmissionPersistsRichContentAtomicallyWithRunAndUserMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()

	orchestrator := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		})}),
		WithClock(func() time.Time { return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("rich-admission-sqlite"),
	)
	const sessionID session.ID = "rich-admission-session"
	handle, err := orchestrator.Start(ctx, Request{
		SessionID: sessionID,
		Message:   UserMessage{Blocks: richTestBlocks()},
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}

	// Admission runs synchronously inside Start: the run and every content
	// part it admitted must already be durable, atomically, before Start
	// returns a handle.
	run, err := store.GetRun(ctx, handle.RunID())
	if err != nil {
		t.Fatalf("GetRun error = %v", err)
	}
	batch, err := store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	var userMessageID session.MessageID
	for _, message := range batch.Messages {
		if message.Role == session.RoleUser {
			userMessageID = message.ID
			break
		}
	}
	if userMessageID == "" {
		t.Fatal("admitted user message not found")
	}
	var userParts []session.Part
	for _, part := range batch.Parts {
		if part.MessageID == userMessageID {
			userParts = append(userParts, part)
		}
	}
	sort.Slice(userParts, func(i, j int) bool { return userParts[i].Ordinal < userParts[j].Ordinal })
	if len(userParts) != 3 {
		t.Fatalf("user parts = %#v, want 3", userParts)
	}
	wantKinds := []session.PartKind{session.PartUserInputText, session.PartUserInputText, session.PartUserInputImage}
	for i, part := range userParts {
		if part.Ordinal != int64(i) || part.Kind != wantKinds[i] || part.RunID != run.ID || part.SessionID != sessionID {
			t.Fatalf("user part[%d] = %#v, want ordinal %d kind %s", i, part, i, wantKinds[i])
		}
	}
	decoded, err := session.DecodeContentParts(session.RoleUser, userParts, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts error = %v", err)
	}
	if len(decoded.Blocks) != 3 ||
		decoded.Blocks[0].Text == nil || decoded.Blocks[0].Text.Text != "first " ||
		decoded.Blocks[1].Text == nil || decoded.Blocks[1].Text.Text != "second" ||
		decoded.Blocks[2].Media == nil || decoded.Blocks[2].Media.URL != "https://example.com/pic.png" || decoded.Blocks[2].Media.MIMEType != "image/png" {
		t.Fatalf("decoded content = %#v", decoded)
	}

	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
}

// TestAdmissionAcceptsMediaOnlySubmission covers a submission with no text
// block at all: it must be admitted, the media block must be durable, and
// the classic provider snapshot carries an empty user text (acceptable until
// W5 switches the classic path to the agentic projection).
func TestAdmissionAcceptsMediaOnlySubmission(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	var providerContent string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		providerContent = request.Messages[len(request.Messages)-1].Content
		return []*einoschema.Message{einoschema.AssistantMessage("seen", nil)}, nil
	}))
	blocks := []session.ContentBlock{{Kind: session.BlockKindUserInputImage, Media: &session.MediaBlock{URL: "https://example.com/only.png", MIMEType: "image/png"}}}
	result := startAndWaitRequest(t, orch, Request{SessionID: "media-only-session", Message: UserMessage{Blocks: blocks}, Config: orchestratorConfig()})
	if result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	if providerContent != "" {
		t.Fatalf("classic provider content = %q, want empty for a media-only submission", providerContent)
	}
	var mediaPart session.Part
	found := false
	for _, part := range store.parts {
		if part.Kind == session.PartUserInputImage {
			mediaPart, found = part, true
		}
	}
	if !found {
		t.Fatal("media part was not persisted")
	}
	decoded, err := session.DecodeContentParts(session.RoleUser, []session.Part{mediaPart}, session.DefaultContentLimits())
	if err != nil || len(decoded.Blocks) != 1 || decoded.Blocks[0].Media == nil || decoded.Blocks[0].Media.URL != "https://example.com/only.png" {
		t.Fatalf("decoded media = %#v, error = %v", decoded, err)
	}
}

// TestAdmissionRejectsAssistantKindBlockBeforeAnyRunRow submits an
// assistant-only block kind (function_tool_call) as user content: it must be
// rejected by validate before any store side effect, including run
// admission.
func TestAdmissionRejectsAssistantKindBlockBeforeAnyRunRow(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, nil }))
	blocks := []session.ContentBlock{{Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: "call-1", Name: "lookup"}}}
	_, err := orch.Start(context.Background(), Request{SessionID: "assistant-kind-session", Message: UserMessage{Blocks: blocks}, Config: orchestratorConfig()})
	if !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("Start error = %v, want ErrInvalidOrchestrator", err)
	}
	if _, err := store.ActiveRun(context.Background(), "assistant-kind-session"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("ActiveRun error = %v, want ErrNotFound", err)
	}
}

// TestAdmissionRejectsEmptySubmissionBeforeAnyRunRow submits zero blocks: it
// must be rejected before any store side effect.
func TestAdmissionRejectsEmptySubmissionBeforeAnyRunRow(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, nil }))
	_, err := orch.Start(context.Background(), Request{SessionID: "empty-session", Message: UserMessage{}, Config: orchestratorConfig()})
	if !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("Start error = %v, want ErrInvalidOrchestrator", err)
	}
	if _, err := store.ActiveRun(context.Background(), "empty-session"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("ActiveRun error = %v, want ErrNotFound", err)
	}
}

// TestAdmissionRejectsOverLimitSubmissionBeforeAnyRunRow configures a
// one-block content limit and submits two blocks: it must be rejected before
// any store side effect.
func TestAdmissionRejectsOverLimitSubmissionBeforeAnyRunRow(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, nil }),
		WithContentLimits(session.ContentLimits{MaxMessageBytes: 1 << 20, MaxBlocks: 1, MaxBlockBytes: 1 << 20}),
	)
	blocks := []session.ContentBlock{
		{Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "one"}},
		{Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "two"}},
	}
	_, err := orch.Start(context.Background(), Request{SessionID: "over-limit-session", Message: UserMessage{Blocks: blocks}, Config: orchestratorConfig()})
	if !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("Start error = %v, want ErrInvalidOrchestrator", err)
	}
	if !errors.Is(err, session.ErrContentTooLarge) {
		t.Fatalf("Start error = %v, want wrapped session.ErrContentTooLarge", err)
	}
	if _, err := store.ActiveRun(context.Background(), "over-limit-session"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("ActiveRun error = %v, want ErrNotFound", err)
	}
}

// TestAdmissionMutatingCallerBlocksAfterStartDoesNotChangeStoredParts proves
// that once Start returns, the caller is free to mutate (or reuse) the
// ContentBlock values it passed in without affecting what was persisted:
// admission encodes every block to durable bytes synchronously before Start
// returns.
func TestAdmissionMutatingCallerBlocksAfterStartDoesNotChangeStoredParts(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	}))
	textBlock := &session.TextBlock{Text: "original"}
	blocks := []session.ContentBlock{{Kind: session.BlockKindUserInputText, Text: textBlock}}
	handle, err := orch.Start(context.Background(), Request{SessionID: "mutation-session", Message: UserMessage{Blocks: blocks}, Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}

	// Mutate the caller's own block content and ID after Start has returned.
	textBlock.Text = "mutated"
	blocks[0].ID = "caller-mutated-id"

	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	var userPart session.Part
	found := false
	for _, part := range store.parts {
		if part.Kind == session.PartUserInputText {
			userPart, found = part, true
		}
	}
	if !found {
		t.Fatal("user part was not persisted")
	}
	decoded, err := session.DecodeContentParts(session.RoleUser, []session.Part{userPart}, session.DefaultContentLimits())
	if err != nil || len(decoded.Blocks) != 1 || decoded.Blocks[0].Text == nil || decoded.Blocks[0].Text.Text != "original" {
		t.Fatalf("stored content was mutated by the caller: decoded=%#v error=%v", decoded, err)
	}
	if decoded.Blocks[0].ID == "caller-mutated-id" {
		t.Fatalf("stored block ID was mutated by the caller: %#v", decoded.Blocks[0])
	}
}
