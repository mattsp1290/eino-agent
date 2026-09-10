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
	// BlockID is the durable content-block identity minted for the single
	// function_tool_result block this settlement persists. Required.
	BlockID string
	// ContentLimits bounds the encoded result content. Required.
	ContentLimits session.ContentLimits
}

// BuildToolSettlement builds the canonical terminal call, result message, and
// result part envelope used by runtime execution.
func BuildToolSettlement(input ToolSettlementInput) (session.ToolSettlement, ToolOutput, error) {
	return buildToolSettlement(input, input.CompletedAt)
}

func buildToolSettlement(input ToolSettlementInput, messageAt time.Time) (session.ToolSettlement, ToolOutput, error) {
	if err := validateSettlementInput(input); err != nil {
		return session.ToolSettlement{}, ToolOutput{}, err
	}
	policy := effectiveToolRetentionPolicy(input.Tool.Retention, input.ContentLimits)
	raw, output, status, errText := encodeToolOutput(input.Call.ID, input.Result, policy, input.Disposition, input.Err)
	metadata := toolSettlementMetadata(input.Claimed.Metadata, output)
	settlement, err := buildTerminalToolEnvelope(terminalToolEnvelopeInput{
		Claimed:       input.Claimed,
		Status:        status,
		Output:        raw,
		Error:         errText,
		Metadata:      metadata,
		ModelID:       input.ModelID,
		CompletedAt:   input.CompletedAt,
		MessageAt:     messageAt,
		BlockID:       input.BlockID,
		ContentLimits: input.ContentLimits,
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
	MessageAt   time.Time
	// BlockID is the durable content-block identity minted for the single
	// function_tool_result block this settlement persists. Required.
	BlockID string
	// ContentLimits bounds the encoded result content. Required.
	ContentLimits session.ContentLimits
}

// buildTerminalToolEnvelope persists a tool result as a single
// function_tool_result content block on a user-role result message: the
// durable content contract only allows BlockKindFunctionToolResult under
// RoleUser (see session/content.go's roleAllowedKinds), and
// session/history/agentic_projection.go's projector maps that role/kind
// pairing straight back to a user-role agentic message carrying one
// function_tool_result block, matching the model-visible shape a tool
// result had before this durable representation existed. The block's single
// text content is exactly string(input.Output) (the ToolOutput JSON), so the
// model-visible payload is unchanged.
func buildTerminalToolEnvelope(input terminalToolEnvelopeInput) (session.ToolSettlement, error) {
	call := input.Claimed
	if call.ID == "" || call.ClaimedBy == "" || call.ClaimToken == "" || call.ResultMessageID == "" || call.ResultPartID == "" {
		return session.ToolSettlement{}, errors.New("tool settlement requires claim identity and reserved result IDs")
	}
	if input.CompletedAt.IsZero() || !session.TerminalToolCall(input.Status) {
		return session.ToolSettlement{}, errors.New("tool settlement requires terminal status and completion time")
	}
	if input.MessageAt.IsZero() {
		return session.ToolSettlement{}, errors.New("tool settlement requires durable message time")
	}
	if input.BlockID == "" {
		return session.ToolSettlement{}, errors.New("tool settlement requires a content block id")
	}
	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID:   input.BlockID,
			Kind: session.BlockKindFunctionToolResult,
			FunctionResult: &session.FunctionResultBlock{
				CallID:  string(call.ID),
				Name:    call.Name,
				Content: []session.ResultContent{{Type: session.ResultContentText, Text: string(input.Output)}},
			},
		}},
	}
	idUsed := false
	parts, err := session.EncodeContentParts(content, func() session.PartID {
		if idUsed {
			return ""
		}
		idUsed = true
		return call.ResultPartID
	}, call.ResultMessageID, call.SessionID, call.RunID, input.MessageAt, input.ContentLimits)
	if err != nil {
		return session.ToolSettlement{}, fmt.Errorf("encode function tool result content: %w", err)
	}
	if len(parts) != 1 {
		return session.ToolSettlement{}, errors.New("encode function tool result content: unexpected part count")
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
			Role: session.RoleUser, ModelID: input.ModelID, CreatedAt: input.MessageAt.UTC(), UpdatedAt: input.MessageAt.UTC(),
		},
		ResultPart: parts[0],
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

// toolResultEnvelopeOverhead reserves headroom in the content-block byte
// budget for the function_tool_result envelope surrounding the tool's raw
// output bytes (the durable content-block/part JSON, the ToolOutput field
// names and status, an escaped call ID, JSON string-escaping overhead on the
// content itself, etc.), so a MaxInlineBytes clamped to exactly
// ContentLimits.MaxBlockBytes doesn't itself overflow the block once
// wrapped.
const toolResultEnvelopeOverhead = 4 << 10 // 4 KiB

// effectiveToolRetentionPolicy clamps policy.MaxInlineBytes so the encoded
// tool result can never exceed contentLimits.MaxBlockBytes. A tool result is
// always persisted as exactly one function_tool_result content block
// (buildTerminalToolEnvelope), so an output the tool's own RetentionPolicy
// would let through uncapped (MaxInlineBytes < 0) -- or capped above the
// block budget -- must still be clamped here, or
// session.EncodeContentParts hard-fails the whole run instead of truncating
// it. An oversized tool output should become a truncated/external
// ToolOutput via the existing Truncated/External signalling, not a
// run-killer: buildTerminalToolEnvelope then re-terminalizes the call as
// interrupted and the model never sees any result at all.
func effectiveToolRetentionPolicy(policy RetentionPolicy, contentLimits session.ContentLimits) RetentionPolicy {
	budget := int64(contentLimits.MaxBlockBytes) - toolResultEnvelopeOverhead
	if budget <= 0 {
		return policy
	}
	if policy.MaxInlineBytes < 0 || policy.MaxInlineBytes > budget {
		policy.MaxInlineBytes = budget
	}
	return policy
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
