package tools

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestEncodeModelOutputTruncatesOversizedContent(t *testing.T) {
	call := runtime.ToolCall{ID: "call-1"}
	raw, output, err := EncodeModelOutput(call, runtime.ToolResult{
		Output:     "abcdef",
		Structured: json.RawMessage(`{"raw":"abcdef"}`),
	}, runtime.RetentionPolicy{MaxInlineBytes: 3, StoreExternal: true})
	if err != nil {
		t.Fatalf("encode model output: %v", err)
	}
	if output.Content != "abc" || !output.Truncated || !output.External {
		t.Fatalf("output = %+v", output)
	}
	if strings.Contains(string(raw), "abcdef") {
		t.Fatalf("payload leaked oversized raw content: %s", raw)
	}
	if strings.Contains(string(raw), "structured") {
		t.Fatalf("truncated payload included unbounded structured output: %s", raw)
	}
}

func TestEncodeModelOutputRedactsRawAndStructuredPayload(t *testing.T) {
	raw, output, err := EncodeModelOutput(runtime.ToolCall{ID: "call-1"}, runtime.ToolResult{
		Output:     "secret-output",
		Structured: json.RawMessage(`{"secret":"output"}`),
		Metadata:   map[string]string{"token": "secret-output"},
		Attachments: []runtime.Attachment{{
			ID:       "attachment-1",
			MIMEType: "text/plain",
			Name:     "secret-output",
			URL:      "file:///private/secret-output",
			Metadata: map[string]string{"token": "secret-output"},
		}},
	}, runtime.RetentionPolicy{MaxInlineBytes: 20, StoreExternal: true, Redact: true})
	if err != nil {
		t.Fatalf("encode redacted model output: %v", err)
	}
	if output.Content != "" || !output.Redacted || !output.External {
		t.Fatalf("output = %+v", output)
	}
	if strings.Contains(string(raw), "secret-output") || strings.Contains(string(raw), "secret") {
		t.Fatalf("redacted payload leaked raw content: %s", raw)
	}
}

func TestEncodeModelOutputBoundsStructuredPayload(t *testing.T) {
	raw, output, err := EncodeModelOutput(runtime.ToolCall{ID: "call-1"}, runtime.ToolResult{
		Output:     "ok",
		Structured: json.RawMessage(`{"secret":"oversized-structured-payload"}`),
	}, runtime.RetentionPolicy{MaxInlineBytes: 10, StoreExternal: true})
	if err != nil {
		t.Fatalf("encode model output: %v", err)
	}
	if !output.Truncated || !output.External || output.Structured != nil {
		t.Fatalf("output = %+v", output)
	}
	if strings.Contains(string(raw), "oversized-structured-payload") {
		t.Fatalf("structured payload leaked oversized raw content: %s", raw)
	}
}

func TestEncodeModelOutputSuppressesToolControlledFieldsWhenTruncated(t *testing.T) {
	raw, _, err := EncodeModelOutput(runtime.ToolCall{ID: "call-1"}, runtime.ToolResult{
		Output: "abcdef",
		Metadata: map[string]string{
			MetadataPermissionStatus: "denied",
			"token":                  "abcdef",
		},
		Attachments: []runtime.Attachment{{
			ID:       "attachment-1",
			MIMEType: "text/plain",
			Name:     "abcdef",
			URL:      "file:///private/abcdef",
			Metadata: map[string]string{"token": "abcdef"},
		}},
	}, runtime.RetentionPolicy{MaxInlineBytes: 3, StoreExternal: true})
	if err != nil {
		t.Fatalf("encode model output: %v", err)
	}
	payload := string(raw)
	for _, leaked := range []string{"abcdef", "file:///private", "attachment-1", MetadataPermissionStatus} {
		if strings.Contains(payload, leaked) {
			t.Fatalf("truncated payload leaked %q: %s", leaked, payload)
		}
	}
}

func TestBuildToolSettlementClassifiesExpectedFailure(t *testing.T) {
	settlement, part, err := BuildToolSettlement(runtime.Tool{}, toolCall(), runtime.ToolResult{
		Output: "denied by policy",
		Metadata: map[string]string{
			MetadataPermissionStatus: "denied",
		},
	}, nil, toolClaim())
	if err != nil {
		t.Fatalf("build settlement: %v", err)
	}
	if settlement.Status != session.ToolCallFailed || settlement.Metadata[MetadataOutputStatus] != outputStatusExpectedFailure {
		t.Fatalf("settlement = %+v", settlement)
	}
	if settlement.CompletedAt.IsZero() {
		t.Fatalf("settlement CompletedAt was not populated")
	}
	if settlement.ResultMessage.ID != "result-message-1" || settlement.ResultMessage.ParentID != "message-1" || settlement.ResultMessage.Role != session.RoleTool {
		t.Fatalf("result message = %+v", settlement.ResultMessage)
	}
	if settlement.ResultPart.ID != "result-part-1" || settlement.ResultPart.MessageID != settlement.ResultMessage.ID || settlement.ResultPart.Kind != session.PartToolResult {
		t.Fatalf("result part = %+v", settlement.ResultPart)
	}
	if !settlement.ResultMessage.CreatedAt.Equal(settlement.CompletedAt) || !settlement.ResultMessage.UpdatedAt.Equal(settlement.CompletedAt) ||
		!settlement.ResultPart.CreatedAt.Equal(settlement.CompletedAt) || !settlement.ResultPart.UpdatedAt.Equal(settlement.CompletedAt) {
		t.Fatalf("result timestamps differ from settlement: %+v", settlement)
	}
	if !reflect.DeepEqual(part, settlement.ResultPart) || !strings.Contains(string(part.Payload), outputStatusExpectedFailure) {
		t.Fatalf("part = %+v", part)
	}
}

func TestBuildToolSettlementClassifiesOperationalFailureWithoutLeakingErrorToModel(t *testing.T) {
	errBoom := errors.New("database password appeared in a lower layer")
	settlement, part, err := BuildToolSettlement(runtime.Tool{
		Retention: runtime.RetentionPolicy{MaxInlineBytes: 200},
	}, toolCall(), runtime.ToolResult{}, errBoom, toolClaim())
	if err != nil {
		t.Fatalf("build settlement: %v", err)
	}
	if settlement.Status != session.ToolCallFailed || settlement.Error != errBoom.Error() {
		t.Fatalf("settlement = %+v", settlement)
	}
	if strings.Contains(string(part.Payload), "database password") {
		t.Fatalf("model-facing part leaked operational error: %s", part.Payload)
	}
}

func TestBuildToolSettlementClassifiesInterruption(t *testing.T) {
	settlement, part, err := BuildToolSettlement(runtime.Tool{}, toolCall(), runtime.ToolResult{}, context.Canceled, toolClaim())
	if err != nil {
		t.Fatalf("build settlement: %v", err)
	}
	if settlement.Status != session.ToolCallInterrupted || settlement.Metadata[MetadataOutputStatus] != outputStatusInterrupted {
		t.Fatalf("settlement = %+v", settlement)
	}
	if !strings.Contains(string(part.Payload), outputStatusInterrupted) {
		t.Fatalf("part payload = %s", part.Payload)
	}
}

func TestBuildToolSettlementRequiresClaimIdentity(t *testing.T) {
	if _, _, err := BuildToolSettlement(runtime.Tool{}, toolCall(), runtime.ToolResult{}, nil); err == nil {
		t.Fatal("BuildToolSettlement accepted missing claim identity")
	}
	if _, _, err := BuildToolSettlement(runtime.Tool{}, toolCall(), runtime.ToolResult{}, nil, session.ToolClaimIdentity{}); err == nil {
		t.Fatal("BuildToolSettlement accepted empty claim identity")
	}
}

func TestBuildToolSettlementRequiresReservedResultIDs(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*runtime.ToolCall)
	}{
		{name: "message", mutate: func(call *runtime.ToolCall) { call.ResultMessageID = "" }},
		{name: "part", mutate: func(call *runtime.ToolCall) { call.ResultPartID = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			call := toolCall()
			test.mutate(&call)
			if _, _, err := BuildToolSettlement(runtime.Tool{}, call, runtime.ToolResult{}, nil, toolClaim()); err == nil {
				t.Fatal("BuildToolSettlement accepted missing reserved result ID")
			}
		})
	}
}

func toolClaim() session.ToolClaimIdentity {
	return session.ToolClaimIdentity{ClaimedBy: "worker", ClaimToken: "token"}
}

func toolCall() runtime.ToolCall {
	return runtime.ToolCall{
		ID:              "call-1",
		SessionID:       "session-1",
		RunID:           "run-1",
		MessageID:       "message-1",
		ResultMessageID: "result-message-1",
		ResultPartID:    "result-part-1",
		Name:            "read_file",
	}
}
