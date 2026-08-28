package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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
		Claimed:     session.ToolCall{ID: call.ID, SessionID: call.SessionID, RunID: call.RunID, MessageID: call.MessageID, ResultMessageID: call.ResultMessageID, ResultPartID: call.ResultPartID, ClaimedBy: "worker", ClaimToken: "token"},
		Disposition: disposition, Result: result, Err: err,
		CompletedAt: time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
	}
}

func settlementTestCall() ToolCall {
	return ToolCall{ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1", ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "read_file"}
}
