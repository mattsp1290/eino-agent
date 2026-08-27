package wasmext

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.bytecodealliance.org/cm"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

func middlewareTurn(call runtime.ToolCall, tool runtime.Tool) wittypes.TurnMetadata {
	return wittypes.TurnMetadata{RunID: string(call.RunID), SessionID: string(call.SessionID), ToolNames: cm.ToList([]string{tool.Name})}
}

func turnMetadataSize(turn wittypes.TurnMetadata) int {
	size := len(turn.RunID) + len(turn.SessionID) + len(turn.EpochID) + len(turn.AgentName) + len(turn.AgentMode) + len(turn.ProviderID) + len(turn.ModelID)
	for _, name := range turn.ToolNames.Slice() {
		size += len(name)
	}
	return size
}

func boundedEventSize(event wittypes.BoundedEvent) int {
	return len(event.Kind) + len(event.SessionID) + len(event.RunID) + len(event.MessageID) + len(event.ToolCallID) + len(event.EpochID) + len(event.PayloadSummary)
}

func boundedEventSummary(event runtime.Event, limit int64) string {
	summary := fmt.Sprintf("payload_bytes=%d redaction=%s live_only=%t", len(event.Payload), event.Redaction, event.LiveOnly)
	if int64(len(summary)) > limit {
		return summary[:limit]
	}
	return summary
}

func toolResultJSON(result runtime.ToolResult) (json.RawMessage, error) {
	if len(result.Structured) != 0 && json.Valid(result.Structured) {
		return cloneRawMessage(result.Structured), nil
	}
	return json.Marshal(result.Output)
}

func applyInputReplacement(module *module, operation string, original json.RawMessage, replacement wittypes.Replacement) (json.RawMessage, error) {
	if replacement.Unchanged() {
		return cloneRawMessage(original), nil
	}
	if raw := replacement.JSON(); raw != nil {
		if err := validateBoundedJSON([]byte(*raw), module.limits.MaxOutputBytes); err != nil {
			return nil, extensionError(payloadErrorKind(err), module.identity, operation, err)
		}
		return json.RawMessage(*raw), nil
	}
	if guestErr := replacement.Error(); guestErr != nil {
		return nil, structuredGuestError(*guestErr)
	}
	return nil, extensionError(ErrorContract, module.identity, operation, nil)
}

func applyResultReplacement(module *module, original runtime.ToolResult, replacement wittypes.Replacement) (runtime.ToolResult, error) {
	if replacement.Unchanged() {
		return cloneToolResult(original), nil
	}
	if rawText := replacement.JSON(); rawText != nil {
		raw := json.RawMessage(*rawText)
		if err := validateBoundedJSON(raw, module.limits.MaxOutputBytes); err != nil {
			return runtime.ToolResult{}, extensionError(payloadErrorKind(err), module.identity, "tool-middleware.after-tool-call", err)
		}
		next := cloneToolResult(original)
		var text string
		if json.Unmarshal(raw, &text) == nil {
			next.Output = text
			next.Structured = nil
		} else {
			next.Output = string(raw)
			next.Structured = cloneRawMessage(raw)
		}
		return next, nil
	}
	if guestErr := replacement.Error(); guestErr != nil {
		return runtime.ToolResult{}, structuredGuestError(*guestErr)
	}
	return runtime.ToolResult{}, extensionError(ErrorContract, module.identity, "tool-middleware.after-tool-call", nil)
}

func cloneToolResult(result runtime.ToolResult) runtime.ToolResult {
	next := result
	next.Structured = cloneRawMessage(result.Structured)
	next.Attachments = append([]runtime.Attachment(nil), result.Attachments...)
	next.Metadata = make(map[string]string, len(result.Metadata))
	for key, value := range result.Metadata {
		next.Metadata[key] = value
	}
	return next
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func structuredGuestError(input wittypes.StructuredError) error {
	code := strings.TrimSpace(input.Code)
	if code == "" || len(code) > 128 {
		code = "wasm_extension_rejected"
	}
	message := input.Message
	if len(message) > 1024 {
		message = message[:1024]
	}
	return model.Error{Code: code, Message: message, Retryable: input.Retryable}
}

func turnMetadataFromBounded(metadata runtime.BoundedTurnMetadata) wittypes.TurnMetadata {
	return wittypes.TurnMetadata{
		RunID: string(metadata.RunID), SessionID: string(metadata.SessionID), EpochID: string(metadata.EpochID),
		AgentName: metadata.AgentName, AgentMode: metadata.AgentMode,
		ProviderID: metadata.ProviderID, ModelID: metadata.ModelID,
		ToolNames: cm.ToList(append([]string(nil), metadata.ToolNames...)), MessageCount: metadata.MessageCount,
		RoleCounts: wittypes.RoleCounts{
			System: metadata.RoleCounts.System, User: metadata.RoleCounts.User,
			Assistant: metadata.RoleCounts.Assistant, Tool: metadata.RoleCounts.Tool,
		},
		HasSystemPrompt: metadata.HasSystemPrompt,
	}
}

func payloadErrorKind(err error) ErrorKind {
	if errors.Is(err, errModuleTooLarge) {
		return ErrorSize
	}
	return ErrorPayload
}

var (
	_ permissions.Policy = (*loadedPermissionsPolicy)(nil)
)
