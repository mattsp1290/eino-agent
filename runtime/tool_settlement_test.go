package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestToolOutputTruncatesOversizedContentInternally(t *testing.T) {
	raw, output, _, _ := encodeToolOutput("call-1", ToolResult{Output: "abcdef", Structured: json.RawMessage(`{"raw":"abcdef"}`)}, RetentionPolicy{MaxInlineBytes: 3, StoreExternal: true}, ToolExecuted, nil)
	if output.Content != "abc" || !output.Truncated || !output.External {
		t.Fatalf("output = %+v", output)
	}
	if strings.Contains(string(raw), "abcdef") || strings.Contains(string(raw), "structured") {
		t.Fatalf("payload leaked oversized output: %s", raw)
	}
}

func TestToolOutputRedactsRawAndStructuredPayloadInternally(t *testing.T) {
	raw, output, _, _ := encodeToolOutput("call-1", ToolResult{
		Output: "secret-output", Structured: json.RawMessage(`{"secret":"output"}`), Metadata: map[string]string{"token": "secret-output"},
		Attachments: []Attachment{{ID: "attachment-1", MIMEType: "text/plain", Name: "secret-output", URL: "file:///private/secret-output", Metadata: map[string]string{"token": "secret-output"}}},
	}, RetentionPolicy{MaxInlineBytes: 20, StoreExternal: true, Redact: true}, ToolExecuted, nil)
	if output.Content != "" || !output.Redacted || !output.External {
		t.Fatalf("output = %+v", output)
	}
	if strings.Contains(string(raw), "secret-output") || strings.Contains(string(raw), "secret") {
		t.Fatalf("redacted payload leaked raw content: %s", raw)
	}
}

func TestToolOutputBoundsStructuredPayloadInternally(t *testing.T) {
	raw, output, _, _ := encodeToolOutput("call-1", ToolResult{Output: "ok", Structured: json.RawMessage(`{"secret":"oversized-structured-payload"}`)}, RetentionPolicy{MaxInlineBytes: 10, StoreExternal: true}, ToolExecuted, nil)
	if !output.Truncated || !output.External || output.Structured != nil {
		t.Fatalf("output = %+v", output)
	}
	if strings.Contains(string(raw), "oversized-structured-payload") {
		t.Fatalf("structured payload leaked oversized content: %s", raw)
	}
}

func TestToolOutputSuppressesToolControlledFieldsWhenTruncatedInternally(t *testing.T) {
	raw, _, _, _ := encodeToolOutput("call-1", ToolResult{
		Output: "abcdef", Metadata: map[string]string{"permission_status": "denied", "token": "abcdef"},
		Attachments: []Attachment{{ID: "attachment-1", MIMEType: "text/plain", Name: "abcdef", URL: "file:///private/abcdef", Metadata: map[string]string{"token": "abcdef"}}},
	}, RetentionPolicy{MaxInlineBytes: 3, StoreExternal: true}, ToolExecuted, nil)
	for _, leaked := range []string{"abcdef", "file:///private", "attachment-1", "permission_status"} {
		if strings.Contains(string(raw), leaked) {
			t.Fatalf("truncated payload leaked %q: %s", leaked, raw)
		}
	}
}

func TestBuildToolSettlementClassifiesProtectedOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		disposition ToolDisposition
		err         error
		wantCall    session.ToolCallStatus
		wantOutput  string
	}{
		{name: "expected failure", disposition: ToolDenied, wantCall: session.ToolCallFailed, wantOutput: "expected_failure"},
		{name: "operational failure", disposition: ToolFailed, err: errors.New("database password appeared in a lower layer"), wantCall: session.ToolCallFailed, wantOutput: "operational_failure"},
		{name: "interrupted", disposition: ToolInterrupted, err: context.Canceled, wantCall: session.ToolCallInterrupted, wantOutput: "interrupted"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := settlementTestInput(Tool{}, settlementTestCall(), ToolResult{Output: "denied by policy"}, test.err)
			input.Disposition = test.disposition
			settlement, output, err := BuildToolSettlement(input)
			if err != nil {
				t.Fatal(err)
			}
			if settlement.Status != test.wantCall || settlement.Metadata[ToolMetadataOutputStatus] != test.wantOutput || output.Status != test.wantOutput {
				t.Fatalf("settlement=%+v output=%+v", settlement, output)
			}
			if settlement.ResultMessage.ID != "result-message-1" || settlement.ResultMessage.ParentID != "message-1" || settlement.ResultPart.ID != "result-part-1" {
				t.Fatalf("result envelope = %+v", settlement)
			}
			if test.err != nil && strings.Contains(string(settlement.ResultPart.Payload), "database password") {
				t.Fatalf("model-facing part leaked operational error: %s", settlement.ResultPart.Payload)
			}
		})
	}
}

func TestBuildToolSettlementSeparatesObservedCompletionFromDurableMessageOrder(t *testing.T) {
	input := settlementTestInput(Tool{}, settlementTestCall(), ToolResult{Output: "ok"}, nil)
	messageAt := input.CompletedAt.Add(time.Nanosecond)
	settlement, _, err := buildToolSettlement(input, messageAt)
	if err != nil {
		t.Fatal(err)
	}
	if !settlement.CompletedAt.Equal(input.CompletedAt) {
		t.Fatalf("completed at = %s, want observed %s", settlement.CompletedAt, input.CompletedAt)
	}
	if !settlement.ResultMessage.CreatedAt.Equal(messageAt) || !settlement.ResultMessage.UpdatedAt.Equal(messageAt) ||
		!settlement.ResultPart.CreatedAt.Equal(messageAt) || !settlement.ResultPart.UpdatedAt.Equal(messageAt) {
		t.Fatalf("durable result envelope = %+v, want message time %s", settlement, messageAt)
	}
}

func TestBuildToolSettlementRequiresIdentityReservedIDsAndCompletionTime(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ToolSettlementInput)
	}{
		{name: "claimed by", mutate: func(input *ToolSettlementInput) { input.Claimed.ClaimedBy = "" }},
		{name: "claim token", mutate: func(input *ToolSettlementInput) { input.Claimed.ClaimToken = "" }},
		{name: "message id", mutate: func(input *ToolSettlementInput) { input.Call.ResultMessageID = "" }},
		{name: "part id", mutate: func(input *ToolSettlementInput) { input.Call.ResultPartID = "" }},
		{name: "completion time", mutate: func(input *ToolSettlementInput) { input.CompletedAt = time.Time{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := settlementTestInput(Tool{}, settlementTestCall(), ToolResult{}, nil)
			test.mutate(&input)
			if _, _, err := BuildToolSettlement(input); err == nil {
				t.Fatal("BuildToolSettlement accepted invalid input")
			}
		})
	}
}

func TestBuildToolSettlementRejectsInvalidDisposition(t *testing.T) {
	input := settlementTestInput(Tool{}, settlementTestCall(), ToolResult{}, nil)
	input.Disposition = ToolDisposition("invalid")
	settlement, output, err := BuildToolSettlement(input)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.Status != session.ToolCallFailed || output.Status != "operational_failure" || settlement.Error != "invalid tool disposition" {
		t.Fatalf("settlement=%+v output=%+v", settlement, output)
	}
}

// TestBuildToolSettlementClampsRetentionPolicyToBlockBudget guards against
// runtime-persistence-reviewer I1: a tool's own RetentionPolicy.MaxInlineBytes
// can be larger than -- or (via a negative sentinel) unbounded relative to --
// session.ContentLimits.MaxBlockBytes, the hard per-block byte cap
// buildTerminalToolEnvelope's session.EncodeContentParts call enforces. Before
// the fix, that combination made the whole run fail
// ("encode function tool result content: session content exceeds configured
// limits") instead of truncating, and the tool call was then re-terminalized
// as interrupted so the model never saw any result at all.
func TestBuildToolSettlementClampsRetentionPolicyToBlockBudget(t *testing.T) {
	oversized := strings.Repeat("a", 2<<20) // 2 MiB > the 1 MiB default MaxBlockBytes
	tests := map[string]RetentionPolicy{
		"policy larger than block budget":      {MaxInlineBytes: 8 << 20, StoreExternal: true},
		"policy unbounded (negative sentinel)": {MaxInlineBytes: -1, StoreExternal: true},
	}
	for name, retention := range tests {
		t.Run(name, func(t *testing.T) {
			input := settlementTestInput(Tool{Retention: retention}, settlementTestCall(), ToolResult{Output: oversized}, nil)
			settlement, output, err := BuildToolSettlement(input)
			if err != nil {
				t.Fatalf("BuildToolSettlement returned an error instead of truncating: %v", err)
			}
			if !output.Truncated || !output.External {
				t.Fatalf("output = %+v, want Truncated and External", output)
			}
			if int64(len(output.Content)) >= int64(len(oversized)) {
				t.Fatalf("content was not truncated: len = %d", len(output.Content))
			}
			if len(settlement.ResultPart.Payload) == 0 {
				t.Fatal("result part payload is empty")
			}
			if int64(len(settlement.ResultPart.Payload)) > int64(input.ContentLimits.MaxBlockBytes) {
				t.Fatalf("result part payload (%d bytes) still exceeds MaxBlockBytes (%d)", len(settlement.ResultPart.Payload), input.ContentLimits.MaxBlockBytes)
			}
		})
	}
}

func settlementTestInput(tool Tool, call ToolCall, result ToolResult, err error) ToolSettlementInput {
	disposition := ToolExecuted
	if err != nil {
		disposition = ToolFailed
		if errors.Is(err, context.Canceled) {
			disposition = ToolInterrupted
		}
	}
	return ToolSettlementInput{
		Tool: tool, Call: call,
		Claimed:     session.ToolCall{ID: call.ID, SessionID: call.SessionID, RunID: call.RunID, MessageID: call.MessageID, ResultMessageID: call.ResultMessageID, ResultPartID: call.ResultPartID, Name: call.Name, ClaimedBy: "worker", ClaimToken: "token"},
		Disposition: disposition, Result: result, Err: err,
		CompletedAt:   time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
		BlockID:       "block-1",
		ContentLimits: session.DefaultContentLimits(),
	}
}

func settlementTestCall() ToolCall {
	return ToolCall{ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1", ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "read_file"}
}

// TestToolCallSettlementRoundTripsUnderRaisedContentLimits is the runtime
// integration regression pin for runtime-persistence-reviewer I1 and I2
// together: a tool that returns oversized output, executed under an
// orchestrator configured with WithContentLimits raised above
// session.DefaultContentLimits, must settle successfully end to end
// (through buildToolSettlement's RetentionPolicy clamp and
// store/internal/sqlstore's ValidToolResultEnvelope decoding with
// session.MaxContentLimits) rather than fail the run or surface an opaque
// session.ErrConflict.
func TestToolCallSettlementRoundTripsUnderRaisedContentLimits(t *testing.T) {
	store, storePool, err := openTestSQLite(context.Background(), filepath.Join(t.TempDir(), "raised-limits.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storePool.Close() }()

	raised := session.ContentLimits{MaxMessageBytes: 32 << 20, MaxBlocks: 1024, MaxBlockBytes: 4 << 20}
	oversized := strings.Repeat("x", 2<<20) // 2 MiB, over the 1 MiB default MaxBlockBytes but under the 4 MiB raised one
	providerCalls := 0
	streamer := scriptedStreamer(func(_ context.Context, _ model.Request) ([]*einoschema.AgenticMessage, error) {
		providerCalls++
		if providerCalls == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-oversized", "big_tool", `{}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()), WithContentLimits(raised),
	)
	if err != nil {
		t.Fatal(err)
	}
	// The scripted provider CallID ("call-oversized") is preserved separately
	// as ProviderCallID; the durable session.ToolCall.ID is always a fresh
	// mint now, so capture the id the executor actually observed.
	var executedCall ToolCall
	configureTestTools(orchestrator, staticToolRegistry{tools: []Tool{{
		Name:      "big_tool",
		Retention: RetentionPolicy{MaxInlineBytes: 8 << 20, StoreExternal: true}, // wider than even the raised block budget
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executedCall = call
			return ToolResult{Output: oversized}, nil
		}),
	}}})

	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "raised-limits-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("result = %#v, want RunCompleted with no error", result)
	}
	call, err := store.GetToolCall(context.Background(), executedCall.ID)
	if err != nil || call.Status != session.ToolCallCompleted {
		t.Fatalf("tool call = %#v, err = %v, want ToolCallCompleted", call, err)
	}
}
