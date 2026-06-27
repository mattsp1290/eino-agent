package tools

import (
	"context"
	"encoding/json"
	"errors"
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

func TestBuildToolSettlementClassifiesExpectedFailure(t *testing.T) {
	settlement, part, err := BuildToolSettlement(runtime.Tool{}, toolCall(), runtime.ToolResult{
		Output: "denied by policy",
		Metadata: map[string]string{
			MetadataPermissionStatus: "denied",
		},
	}, nil)
	if err != nil {
		t.Fatalf("build settlement: %v", err)
	}
	if settlement.Status != session.ToolCallFailed || settlement.Metadata[MetadataOutputStatus] != outputStatusExpectedFailure {
		t.Fatalf("settlement = %+v", settlement)
	}
	if part.Kind != session.PartToolResult || !strings.Contains(string(part.Payload), outputStatusExpectedFailure) {
		t.Fatalf("part = %+v", part)
	}
}

func TestBuildToolSettlementClassifiesOperationalFailureWithoutLeakingErrorToModel(t *testing.T) {
	errBoom := errors.New("database password appeared in a lower layer")
	settlement, part, err := BuildToolSettlement(runtime.Tool{
		Retention: runtime.RetentionPolicy{MaxInlineBytes: 200},
	}, toolCall(), runtime.ToolResult{}, errBoom)
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
	settlement, part, err := BuildToolSettlement(runtime.Tool{}, toolCall(), runtime.ToolResult{}, context.Canceled)
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

func toolCall() runtime.ToolCall {
	return runtime.ToolCall{
		ID:        "call-1",
		SessionID: "session-1",
		RunID:     "run-1",
		MessageID: "message-1",
		Name:      "read_file",
	}
}
