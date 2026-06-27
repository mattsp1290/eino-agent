package tools

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

const (
	// MetadataOutputStatus records the stable settlement class in durable
	// metadata and model-facing payloads.
	MetadataOutputStatus = "output_status"
	// MetadataPermissionStatus is emitted by runtime permission hooks.
	MetadataPermissionStatus = "permission_status"
)

const (
	outputStatusCompleted          = "completed"
	outputStatusExpectedFailure    = "expected_failure"
	outputStatusOperationalFailure = "operational_failure"
	outputStatusInterrupted        = "interrupted"
)

// ModelOutput is the bounded tool result payload stored in PartToolResult and
// sent back to the model. Content contains only the inline-safe prefix selected
// by the tool retention policy.
type ModelOutput struct {
	ToolCallID   string               `json:"tool_call_id"`
	Status       string               `json:"status"`
	Content      string               `json:"content,omitempty"`
	Structured   json.RawMessage      `json:"structured,omitempty"`
	Attachments  []runtime.Attachment `json:"attachments,omitempty"`
	Metadata     map[string]string    `json:"metadata,omitempty"`
	Truncated    bool                 `json:"truncated,omitempty"`
	OriginalSize int64                `json:"original_size,omitempty"`
	InlineSize   int64                `json:"inline_size,omitempty"`
	External     bool                 `json:"external,omitempty"`
	Redacted     bool                 `json:"redacted,omitempty"`
}

// EncodeModelOutput bounds a runtime tool result according to policy and
// returns the JSON payload used for replayable model-facing history.
func EncodeModelOutput(call runtime.ToolCall, result runtime.ToolResult, policy runtime.RetentionPolicy) (json.RawMessage, ModelOutput, error) {
	output := ModelOutput{
		ToolCallID: string(call.ID),
		Status:     outputStatusFromResult(result, nil),
	}
	applyBoundedContent(&output, result, policy)
	applyBoundedStructured(&output, result.Structured, policy)
	if !output.Redacted && !output.Truncated {
		output.Metadata = safeModelMetadata(result.Metadata)
		output.Attachments = safeModelAttachments(result.Attachments)
	}
	raw, err := json.Marshal(output)
	if err != nil {
		return nil, ModelOutput{}, err
	}
	return raw, output, nil
}

// BuildToolSettlement converts a tool execution outcome into the durable tool
// call settlement plus the replayable tool-result part payload.
func BuildToolSettlement(tool runtime.Tool, call runtime.ToolCall, result runtime.ToolResult, execErr error) (session.ToolSettlement, session.Part, error) {
	status := outputStatusFromResult(result, execErr)
	modelResult := result
	if execErr != nil {
		modelResult = runtime.ToolResult{
			Output: "tool execution failed",
			Metadata: map[string]string{
				MetadataOutputStatus: status,
			},
		}
	}
	_, output, err := EncodeModelOutput(call, modelResult, tool.Retention)
	if err != nil {
		return session.ToolSettlement{}, session.Part{}, err
	}
	output.Status = status
	raw, err := json.Marshal(output)
	if err != nil {
		return session.ToolSettlement{}, session.Part{}, err
	}
	settlement := session.ToolSettlement{
		ID:          call.ID,
		Status:      toolCallStatus(status),
		Output:      raw,
		Metadata:    settlementMetadata(tool, status, output),
		CompletedAt: time.Now().UTC(),
	}
	if execErr != nil {
		settlement.Error = execErr.Error()
	}
	part := session.Part{
		MessageID: call.MessageID,
		SessionID: call.SessionID,
		RunID:     call.RunID,
		Kind:      session.PartToolResult,
		Payload:   raw,
	}
	return settlement, part, nil
}

func outputStatusFromResult(result runtime.ToolResult, err error) string {
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return outputStatusInterrupted
		}
		return outputStatusOperationalFailure
	}
	switch result.Metadata[MetadataPermissionStatus] {
	case "interrupted":
		return outputStatusInterrupted
	case "denied", "approval_required":
		return outputStatusExpectedFailure
	default:
		return outputStatusCompleted
	}
}

func toolCallStatus(outputStatus string) session.ToolCallStatus {
	switch outputStatus {
	case outputStatusInterrupted:
		return session.ToolCallInterrupted
	case outputStatusExpectedFailure, outputStatusOperationalFailure:
		return session.ToolCallFailed
	default:
		return session.ToolCallCompleted
	}
}

func applyBoundedContent(output *ModelOutput, result runtime.ToolResult, policy runtime.RetentionPolicy) {
	output.OriginalSize = int64(len(result.Output))
	if policy.Redact {
		output.Redacted = true
		output.InlineSize = 0
		output.External = policy.StoreExternal && result.Output != ""
		return
	}
	content := result.Output
	if policy.MaxInlineBytes >= 0 && int64(len(content)) > policy.MaxInlineBytes {
		content = validUTF8Prefix(content, int(policy.MaxInlineBytes))
		output.Truncated = true
		output.External = policy.StoreExternal
	}
	output.Content = content
	output.InlineSize = int64(len(content))
}

func applyBoundedStructured(output *ModelOutput, structured json.RawMessage, policy runtime.RetentionPolicy) {
	if len(structured) == 0 || output.Redacted {
		return
	}
	output.OriginalSize += int64(len(structured))
	if policy.MaxInlineBytes >= 0 {
		remaining := policy.MaxInlineBytes - output.InlineSize
		if remaining < int64(len(structured)) {
			output.Truncated = true
			output.External = policy.StoreExternal
			return
		}
	}
	output.Structured = cloneRawMessage(structured)
	output.InlineSize += int64(len(structured))
}

func validUTF8Prefix(content string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if limit > len(content) {
		limit = len(content)
	}
	for limit > 0 && !utf8.ValidString(content[:limit]) {
		limit--
	}
	return content[:limit]
}

func safeModelMetadata(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := map[string]string{}
	for _, key := range []string{MetadataPermissionStatus, MetadataOutputStatus} {
		if value, ok := src[key]; ok {
			dst[key] = value
		}
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}

func safeModelAttachments(src []runtime.Attachment) []runtime.Attachment {
	if len(src) == 0 {
		return nil
	}
	dst := make([]runtime.Attachment, 0, len(src))
	for _, attachment := range src {
		if attachment.ID == "" {
			continue
		}
		dst = append(dst, runtime.Attachment{
			ID:       attachment.ID,
			MIMEType: attachment.MIMEType,
		})
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}

func settlementMetadata(tool runtime.Tool, status string, output ModelOutput) map[string]string {
	metadata := cloneStringMap(tool.Metadata)
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata[MetadataOutputStatus] = status
	if output.Truncated {
		metadata["output_truncated"] = "true"
	}
	if output.External {
		metadata["output_external"] = "true"
	}
	if output.Redacted {
		metadata["output_redacted"] = "true"
	}
	metadata["output_original_size"] = strconv.FormatInt(output.OriginalSize, 10)
	metadata["output_inline_size"] = strconv.FormatInt(output.InlineSize, 10)
	return metadata
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	clone := make(json.RawMessage, len(raw))
	copy(clone, raw)
	return clone
}
