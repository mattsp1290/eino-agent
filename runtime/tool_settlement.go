package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

const (
	ToolMetadataOutputStatus = "output_status"
	toolMetadataTruncated    = "output_truncated"
	toolMetadataExternal     = "output_external"
	toolMetadataRedacted     = "output_redacted"
	toolMetadataOriginalSize = "output_original_size"
	toolMetadataInlineSize   = "output_inline_size"
)

// ToolOutput is the bounded model-visible payload persisted for a tool call.
// It deliberately excludes tool-controlled metadata and attachment locations.
type ToolOutput struct {
	ToolCallID   string          `json:"tool_call_id"`
	Status       string          `json:"status"`
	Content      string          `json:"content,omitempty"`
	Structured   json.RawMessage `json:"structured,omitempty"`
	Truncated    bool            `json:"truncated,omitempty"`
	OriginalSize int64           `json:"original_size,omitempty"`
	InlineSize   int64           `json:"inline_size,omitempty"`
	External     bool            `json:"external,omitempty"`
	Redacted     bool            `json:"redacted,omitempty"`
}

// ToolSettlementInput contains every authoritative value required to build a
// fenced durable tool settlement without consulting a clock or store.
type ToolSettlementInput struct {
	Tool        Tool
	Call        ToolCall
	Claimed     session.ToolCall
	Disposition ToolDisposition
	Result      ToolResult
	Err         error
	ModelID     string
	CompletedAt time.Time
}

// BuildToolSettlement builds the canonical terminal call, result message, and
// result part envelope used by runtime execution.
func BuildToolSettlement(input ToolSettlementInput) (session.ToolSettlement, ToolOutput, error) {
	if err := validateSettlementInput(input); err != nil {
		return session.ToolSettlement{}, ToolOutput{}, err
	}
	raw, output, status, errText := encodeToolOutput(input.Call.ID, input.Result, input.Tool.Retention, input.Disposition, input.Err)
	metadata := toolSettlementMetadata(input.Claimed.Metadata, output)
	settlement, err := buildTerminalToolEnvelope(terminalToolEnvelopeInput{
		Claimed:     input.Claimed,
		Status:      status,
		Output:      raw,
		Error:       errText,
		Metadata:    metadata,
		ModelID:     input.ModelID,
		CompletedAt: input.CompletedAt,
	})
	return settlement, output, err
}

type terminalToolEnvelopeInput struct {
	Claimed     session.ToolCall
	Status      session.ToolCallStatus
	Output      json.RawMessage
	Error       string
	Metadata    map[string]string
	ModelID     string
	CompletedAt time.Time
}

func buildTerminalToolEnvelope(input terminalToolEnvelopeInput) (session.ToolSettlement, error) {
	call := input.Claimed
	if call.ID == "" || call.ClaimedBy == "" || call.ClaimToken == "" || call.ResultMessageID == "" || call.ResultPartID == "" {
		return session.ToolSettlement{}, errors.New("tool settlement requires claim identity and reserved result IDs")
	}
	if input.CompletedAt.IsZero() || !session.TerminalToolCall(input.Status) {
		return session.ToolSettlement{}, errors.New("tool settlement requires terminal status and completion time")
	}
	settlement := session.ToolSettlement{
		ID:          call.ID,
		ClaimedBy:   call.ClaimedBy,
		ClaimToken:  call.ClaimToken,
		Status:      input.Status,
		Output:      cloneJSON(input.Output),
		Error:       input.Error,
		Metadata:    cloneStringMap(input.Metadata),
		CompletedAt: input.CompletedAt.UTC(),
		ResultMessage: session.Message{
			ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID,
			Role: session.RoleTool, ModelID: input.ModelID, CreatedAt: input.CompletedAt.UTC(), UpdatedAt: input.CompletedAt.UTC(),
		},
		ResultPart: session.Part{
			ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID,
			Kind: session.PartToolResult, Payload: cloneJSON(input.Output), CreatedAt: input.CompletedAt.UTC(), UpdatedAt: input.CompletedAt.UTC(),
		},
	}
	return settlement, nil
}

func validateSettlementInput(input ToolSettlementInput) error {
	if input.Call.ID == "" || input.Claimed.ID != input.Call.ID || input.Claimed.SessionID != input.Call.SessionID ||
		input.Claimed.RunID != input.Call.RunID || input.Claimed.MessageID != input.Call.MessageID ||
		input.Claimed.ResultMessageID != input.Call.ResultMessageID || input.Claimed.ResultPartID != input.Call.ResultPartID {
		return errors.New("tool settlement call identity mismatch")
	}
	if input.Claimed.ClaimedBy == "" || input.Claimed.ClaimToken == "" {
		return errors.New("tool settlement claim identity required")
	}
	if input.CompletedAt.IsZero() {
		return errors.New("tool settlement completion time required")
	}
	return nil
}

func encodeToolOutput(callID session.ToolCallID, result ToolResult, policy RetentionPolicy, disposition ToolDisposition, err error) (json.RawMessage, ToolOutput, session.ToolCallStatus, string) {
	output := ToolOutput{ToolCallID: string(callID), Status: "completed"}
	status := session.ToolCallCompleted
	errText := ""
	switch disposition {
	case ToolDenied, ToolApprovalRequired:
		output.Status = "expected_failure"
		status = session.ToolCallFailed
		applyToolOutputBounds(&output, result, policy)
	case ToolInterrupted:
		output.Status = "interrupted"
		status = session.ToolCallInterrupted
		if err == nil {
			applyToolOutputBounds(&output, result, policy)
		} else {
			output.Content = "tool execution failed"
			errText = err.Error()
		}
	case ToolFailed:
		output.Status = "operational_failure"
		status = session.ToolCallFailed
		output.Content = "tool execution failed"
		if err != nil {
			errText = err.Error()
		}
	case ToolExecuted:
		if err != nil {
			output.Status = "operational_failure"
			status = session.ToolCallFailed
			output.Content = "tool execution failed"
			errText = err.Error()
		} else {
			applyToolOutputBounds(&output, result, policy)
		}
	default:
		output.Status = "operational_failure"
		status = session.ToolCallFailed
		output.Content = "tool execution failed"
		errText = "invalid tool disposition"
	}
	raw, marshalErr := json.Marshal(output)
	if marshalErr != nil {
		return json.RawMessage(`{"tool_call_id":"` + string(callID) + `","status":"operational_failure","content":"tool execution failed"}`), output, session.ToolCallFailed, fmt.Sprintf("encode tool output: %v", marshalErr)
	}
	return raw, output, status, errText
}

func applyToolOutputBounds(output *ToolOutput, result ToolResult, policy RetentionPolicy) {
	output.OriginalSize = int64(len(result.Output))
	if policy.Redact {
		output.Redacted = true
		output.External = policy.StoreExternal && (result.Output != "" || len(result.Structured) > 0)
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
	if len(result.Structured) == 0 {
		return
	}
	output.OriginalSize += int64(len(result.Structured))
	if policy.MaxInlineBytes >= 0 {
		remaining := policy.MaxInlineBytes - output.InlineSize
		if remaining < int64(len(result.Structured)) {
			output.Truncated = true
			output.External = policy.StoreExternal
			return
		}
	}
	output.Structured = cloneJSON(result.Structured)
	output.InlineSize += int64(len(result.Structured))
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

func toolSettlementMetadata(base map[string]string, output ToolOutput) map[string]string {
	metadata := cloneStringMap(base)
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[ToolMetadataOutputStatus] = output.Status
	if output.Truncated {
		metadata[toolMetadataTruncated] = "true"
	}
	if output.External {
		metadata[toolMetadataExternal] = "true"
	}
	if output.Redacted {
		metadata[toolMetadataRedacted] = "true"
	}
	metadata[toolMetadataOriginalSize] = strconv.FormatInt(output.OriginalSize, 10)
	metadata[toolMetadataInlineSize] = strconv.FormatInt(output.InlineSize, 10)
	return metadata
}
