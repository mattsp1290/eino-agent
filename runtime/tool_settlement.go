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
	// Parts, when non-empty, is the bounded enhanced-tool result. It is
	// authoritative over Content/Structured (which stay empty) and is bounded
	// by RetentionPolicy the same way, part by part: an oversized or redacted
	// part becomes an omission record ({Type, Omitted:true, OriginalSize})
	// rather than a truncated/corrupt payload.
	Parts []ToolOutputPart `json:"parts,omitempty"`
}

// ToolOutputMedia is the bounded, durable projection of a ToolResultMedia.
type ToolOutputMedia struct {
	URL        string `json:"url,omitempty"`
	Base64Data string `json:"base64_data,omitempty"`
	MIMEType   string `json:"mime_type,omitempty"`
	Name       string `json:"name,omitempty"`
}

// ToolOutputPart is one bounded, durable unit of an enhanced tool result.
type ToolOutputPart struct {
	Type       string           `json:"type"`
	Text       string           `json:"text,omitempty"`
	Media      *ToolOutputMedia `json:"media,omitempty"`
	ToolSearch json.RawMessage  `json:"tool_search,omitempty"`
	// Omitted marks a part whose payload was dropped by RetentionPolicy
	// (oversized or redacted) instead of being persisted, corrupted or
	// silently truncated mid-encoding. OriginalSize records its true size.
	Omitted      bool  `json:"omitted,omitempty"`
	OriginalSize int64 `json:"original_size,omitempty"`
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
	settlement, persisted, err := buildTerminalToolEnvelope(terminalToolEnvelopeInput{
		Claimed:       input.Claimed,
		Status:        status,
		Output:        raw,
		OutputRecord:  output,
		Error:         errText,
		Metadata:      metadata,
		ModelID:       input.ModelID,
		CompletedAt:   input.CompletedAt,
		MessageAt:     messageAt,
		BlockID:       input.BlockID,
		ContentLimits: input.ContentLimits,
	})
	if err != nil {
		return session.ToolSettlement{}, ToolOutput{}, err
	}
	return settlement, persisted, nil
}

type terminalToolEnvelopeInput struct {
	Claimed session.ToolCall
	Status  session.ToolCallStatus
	Output  json.RawMessage
	// OutputRecord is the decoded ToolOutput raw marshals; it drives the
	// model-visible content built via toolOutputToResultContent. Zero value
	// (no Parts) reproduces the classic single-text-part shape.
	OutputRecord ToolOutput
	Error        string
	Metadata     map[string]string
	ModelID      string
	CompletedAt  time.Time
	MessageAt    time.Time
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
// result had before this durable representation existed. The block's content
// is built by toolOutputToResultContent: for a scalar (non-Parts) result it
// is a single text content item that is exactly string(input.Output) (the
// ToolOutput JSON), so the model-visible payload is unchanged from before
// enhanced results existed; for an enhanced (Parts) result it is one content
// item per bounded part.
// buildTerminalToolEnvelope returns the settlement and the ToolOutput record
// it actually persisted, which differs from input.OutputRecord only when the
// degrade path below replaced parts with omission records. Callers must use
// the returned record for the model-visible result so persisted, model-visible
// and replayed content stay identical.
func buildTerminalToolEnvelope(input terminalToolEnvelopeInput) (session.ToolSettlement, ToolOutput, error) {
	call := input.Claimed
	if call.ID == "" || call.ClaimedBy == "" || call.ClaimToken == "" || call.ResultMessageID == "" || call.ResultPartID == "" {
		return session.ToolSettlement{}, ToolOutput{}, errors.New("tool settlement requires claim identity and reserved result IDs")
	}
	if input.CompletedAt.IsZero() || !session.TerminalToolCall(input.Status) {
		return session.ToolSettlement{}, ToolOutput{}, errors.New("tool settlement requires terminal status and completion time")
	}
	if input.MessageAt.IsZero() {
		return session.ToolSettlement{}, ToolOutput{}, errors.New("tool settlement requires durable message time")
	}
	if input.BlockID == "" {
		return session.ToolSettlement{}, ToolOutput{}, errors.New("tool settlement requires a content block id")
	}
	// The persisted block's Name must be the model-facing name (RequestedName)
	// so a live turn and a later replay always show the model the exact name
	// it used to call the tool, whether or not that name was an alias (see
	// resolveToolCall/session.ToolCall.RequestedName). RequestedName is
	// empty only for settlements built before call aliasing existed or
	// constructed without it, in which case Name (already canonical) is the
	// correct fallback.
	resultName := call.RequestedName
	if resultName == "" {
		resultName = call.Name
	}
	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID:   input.BlockID,
			Kind: session.BlockKindFunctionToolResult,
			FunctionResult: &session.FunctionResultBlock{
				CallID:  string(call.ID),
				Name:    resultName,
				Content: toolOutputToResultContent(input.Output, input.OutputRecord),
			},
		}},
	}
	mintResultPartID := func() func() session.PartID {
		idUsed := false
		return func() session.PartID {
			if idUsed {
				return ""
			}
			idUsed = true
			return call.ResultPartID
		}
	}
	parts, err := session.EncodeContentParts(content, mintResultPartID(), call.ResultMessageID, call.SessionID, call.RunID, input.MessageAt, input.ContentLimits)
	if err != nil && len(input.OutputRecord.Parts) != 0 {
		// Belt-and-braces: validateToolResultPart already rejects malformed
		// enhanced parts at the executor boundary, but if an encoder rule
		// change ever rejects a part that passed that validation, degrade
		// every non-omitted part to an omission record rather than failing
		// the whole settlement -- a tool result that executed must never
		// turn into a run-killer (see effectiveToolRetentionPolicy's own
		// comment on this invariant). Re-marshal the degraded record so the
		// durable Output JSON and the model-visible content agree, which is
		// what the store settle fence (validFunctionToolResultEnvelope)
		// checks.
		degraded := degradeToolOutputPartsToOmissions(input.OutputRecord)
		degradedRaw, marshalErr := json.Marshal(degraded)
		if marshalErr != nil {
			return session.ToolSettlement{}, ToolOutput{}, fmt.Errorf("encode function tool result content: %w", err)
		}
		input.Output = degradedRaw
		input.OutputRecord = degraded
		input.Metadata = toolSettlementMetadata(call.Metadata, degraded)
		content.Blocks[0].FunctionResult.Content = toolOutputToResultContent(input.Output, input.OutputRecord)
		parts, err = session.EncodeContentParts(content, mintResultPartID(), call.ResultMessageID, call.SessionID, call.RunID, input.MessageAt, input.ContentLimits)
	}
	if err != nil {
		return session.ToolSettlement{}, ToolOutput{}, fmt.Errorf("encode function tool result content: %w", err)
	}
	if len(parts) != 1 {
		return session.ToolSettlement{}, ToolOutput{}, errors.New("encode function tool result content: unexpected part count")
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
	return settlement, input.OutputRecord, nil
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
	applyBounds := func() {
		if len(result.Parts) != 0 {
			applyToolOutputPartsBounds(&output, result, policy)
		} else {
			applyToolOutputBounds(&output, result, policy)
		}
	}
	switch disposition {
	case ToolDenied, ToolApprovalRequired:
		output.Status = "expected_failure"
		status = session.ToolCallFailed
		applyBounds()
	case ToolInterrupted:
		output.Status = "interrupted"
		status = session.ToolCallInterrupted
		if err == nil {
			applyBounds()
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
			applyBounds()
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

// applyToolOutputPartsBounds bounds an enhanced tool result part by part.
// Redacted or oversized parts become omission records ({Type, Omitted:true,
// OriginalSize}) instead of truncated or corrupt payloads: unlike text
// output, a part-level payload (base64 media, a tool-search JSON blob) has no
// safe byte-prefix truncation, so it is either kept whole or omitted whole.
func applyToolOutputPartsBounds(output *ToolOutput, result ToolResult, policy RetentionPolicy) {
	unbounded := policy.MaxInlineBytes < 0
	remaining := policy.MaxInlineBytes
	parts := make([]ToolOutputPart, 0, len(result.Parts))
	var originalTotal, inlineTotal int64
	for _, part := range result.Parts {
		size := int64(toolResultPartByteSize(part))
		originalTotal += size
		switch {
		case policy.Redact:
			parts = append(parts, ToolOutputPart{Type: string(part.Type), Omitted: true, OriginalSize: size})
		case !unbounded && size > remaining:
			parts = append(parts, ToolOutputPart{Type: string(part.Type), Omitted: true, OriginalSize: size})
			output.Truncated = true
			output.External = policy.StoreExternal
		default:
			parts = append(parts, toolResultPartToOutputPart(part, size))
			inlineTotal += size
			if !unbounded {
				remaining -= size
			}
		}
	}
	output.Parts = parts
	output.OriginalSize = originalTotal
	output.InlineSize = inlineTotal
	if policy.Redact {
		output.Redacted = true
		output.External = policy.StoreExternal && originalTotal > 0
	}
}

func toolResultPartByteSize(part ToolResultPart) int {
	size := len(part.Text)
	if part.Media != nil {
		size += len(part.Media.Base64Data) + len(part.Media.URL) + len(part.Media.MIMEType) + len(part.Media.Name)
	}
	size += len(part.ToolSearch)
	return size
}

func toolResultPartToOutputPart(part ToolResultPart, size int64) ToolOutputPart {
	out := ToolOutputPart{Type: string(part.Type), Text: part.Text, OriginalSize: size}
	if part.Media != nil {
		out.Media = &ToolOutputMedia{URL: part.Media.URL, Base64Data: part.Media.Base64Data, MIMEType: part.Media.MIMEType, Name: part.Media.Name}
	}
	if len(part.ToolSearch) != 0 {
		out.ToolSearch = cloneJSON(part.ToolSearch)
	}
	return out
}

// toolOutputToResultContent is the single function settlement uses to build
// the model-visible session.ResultContent list for a persisted
// function_tool_result block from a ToolOutput. Replayed history projects
// that persisted block straight back (session.ContentToAgenticMessage), so
// this is the one place the durable record and the model-visible shape are
// derived together.
//
// When output.Parts is empty (the ordinary scalar-tool case) the result is
// exactly today's shape: one text content item whose text is the complete
// marshaled ToolOutput JSON (rawOutput), unchanged. When output.Parts is
// non-empty it is authoritative: each part becomes its own content item and
// rawOutput/output.Content/output.Structured are not surfaced.
func toolOutputToResultContent(rawOutput json.RawMessage, output ToolOutput) []session.ResultContent {
	if len(output.Parts) == 0 {
		return []session.ResultContent{{Type: session.ResultContentText, Text: string(rawOutput)}}
	}
	content := make([]session.ResultContent, 0, len(output.Parts))
	for _, part := range output.Parts {
		content = append(content, toolOutputPartToResultContent(part))
	}
	return content
}

// degradeToolOutputPartsToOmissions converts every non-omitted part of
// output to an omission record ({Type, Omitted:true, OriginalSize}), the
// belt-and-braces fallback buildTerminalToolEnvelope uses when a part that
// passed validateToolResultPart still fails to encode.
func degradeToolOutputPartsToOmissions(output ToolOutput) ToolOutput {
	degraded := output
	degraded.Parts = make([]ToolOutputPart, len(output.Parts))
	for index, part := range output.Parts {
		if part.Omitted {
			degraded.Parts[index] = part
			continue
		}
		degraded.Parts[index] = ToolOutputPart{Type: part.Type, Omitted: true, OriginalSize: part.OriginalSize}
	}
	return degraded
}

func toolOutputPartToResultContent(part ToolOutputPart) session.ResultContent {
	if part.Omitted {
		raw, _ := json.Marshal(struct {
			Type         string `json:"type"`
			Omitted      bool   `json:"omitted"`
			OriginalSize int64  `json:"original_size"`
		}{Type: part.Type, Omitted: true, OriginalSize: part.OriginalSize})
		return session.ResultContent{Type: session.ResultContentText, Text: string(raw)}
	}
	switch ToolResultPartType(part.Type) {
	case ToolResultPartImage:
		return session.ResultContent{Type: session.ResultContentImage, Media: toolOutputMediaToBlock(part.Media)}
	case ToolResultPartAudio:
		return session.ResultContent{Type: session.ResultContentAudio, Media: toolOutputMediaToBlock(part.Media)}
	case ToolResultPartVideo:
		return session.ResultContent{Type: session.ResultContentVideo, Media: toolOutputMediaToBlock(part.Media)}
	case ToolResultPartFile:
		return session.ResultContent{Type: session.ResultContentFile, Media: toolOutputMediaToBlock(part.Media)}
	case ToolResultPartToolSearch:
		// session.ResultContentType has no tool_search variant (Eino's
		// FunctionToolResultContentBlockType defines no such wire shape
		// either): degrade to a text content item carrying the raw
		// schema.ToolSearchResult JSON rather than failing closed.
		return session.ResultContent{Type: session.ResultContentText, Text: string(part.ToolSearch)}
	default:
		return session.ResultContent{Type: session.ResultContentText, Text: part.Text}
	}
}

func toolOutputMediaToBlock(media *ToolOutputMedia) *session.MediaBlock {
	if media == nil {
		return &session.MediaBlock{}
	}
	return &session.MediaBlock{URL: media.URL, Base64Data: media.Base64Data, MIMEType: media.MIMEType, Name: media.Name}
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
