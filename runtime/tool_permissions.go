package runtime

import (
	"context"
	"errors"
	"strings"

	"github.com/mattsp1290/eino-agent/permissions"
)

type toolPermissionState uint8

const (
	toolPermissionAllowed toolPermissionState = iota
	toolPermissionDenied
	toolPermissionApprovalRequired
	toolPermissionInterrupted
)

type toolPermissionExecution struct {
	Result ToolResult
	State  toolPermissionState
}

// executeToolWithPermissions checks policy and approval hooks while keeping
// the authoritative decision out of callback-controlled result metadata.
func executeToolWithPermissions(ctx context.Context, tool Tool, call ToolCall, policy permissions.Policy) (toolPermissionExecution, error) {
	if policy == nil {
		result, err := tool.Executor.Execute(ctx, call)
		return toolPermissionExecution{Result: result, State: toolPermissionAllowed}, err
	}
	for _, permission := range toolPermissions(tool, call) {
		request := permissionRequest(tool, call, permission)
		decision, err := policy.Decide(ctx, request)
		if err != nil {
			if errors.Is(err, permissions.ErrInterrupted) {
				return permissionExecution(toolPermissionInterrupted, "tool call interrupted during permission check"), nil
			}
			return toolPermissionExecution{State: toolPermissionAllowed}, err
		}
		switch decision.Action {
		case permissions.ActionAllow:
			continue
		case permissions.ActionDeny:
			return permissionExecution(toolPermissionDenied, messageOrDefault(decision.Message, "tool call denied by policy")), nil
		case permissions.ActionAsk:
			if call.Approval == nil {
				return permissionExecution(toolPermissionApprovalRequired, messageOrDefault(decision.Message, "tool call requires approval")), nil
			}
			err := call.Approval.Ask(ctx, ApprovalRequest{
				SessionID:  call.SessionID,
				RunID:      call.RunID,
				ToolCallID: call.ID,
				Permission: permission,
				Patterns:   []string{request.Pattern},
				Metadata:   cloneStringMap(tool.Metadata),
			})
			if err == nil {
				continue
			}
			if errors.Is(err, permissions.ErrInterrupted) {
				return permissionExecution(toolPermissionInterrupted, "tool call interrupted while waiting for approval"), nil
			}
			if errors.Is(err, permissions.ErrDenied) {
				return permissionExecution(toolPermissionDenied, err.Error()), nil
			}
			if errors.Is(err, permissions.ErrApprovalRequired) {
				return permissionExecution(toolPermissionApprovalRequired, err.Error()), nil
			}
			return toolPermissionExecution{State: toolPermissionAllowed}, err
		default:
			return toolPermissionExecution{State: toolPermissionAllowed}, permissions.ErrOperational
		}
	}
	result, err := tool.Executor.Execute(ctx, call)
	return toolPermissionExecution{Result: result, State: toolPermissionAllowed}, err
}

func permissionExecution(state toolPermissionState, message string) toolPermissionExecution {
	return toolPermissionExecution{Result: modelVisiblePermissionResult(permissionStatus(state), message), State: state}
}

func permissionStatus(state toolPermissionState) string {
	switch state {
	case toolPermissionDenied:
		return "denied"
	case toolPermissionApprovalRequired:
		return "approval_required"
	case toolPermissionInterrupted:
		return "interrupted"
	default:
		return ""
	}
}

func protectPermissionResult(result ToolResult, state toolPermissionState) ToolResult {
	result = cloneRuntimeToolResult(result)
	for key := range result.Metadata {
		if strings.HasPrefix(key, "permission_") {
			delete(result.Metadata, key)
		}
	}
	if status := permissionStatus(state); status != "" {
		if result.Metadata == nil {
			result.Metadata = make(map[string]string)
		}
		result.Metadata["permission_status"] = status
	}
	return result
}

func toolPermissions(tool Tool, call ToolCall) []string {
	if len(call.Scope.Permissions) > 0 {
		return cloneSlice(call.Scope.Permissions)
	}
	if len(tool.Scope.Permissions) > 0 {
		return cloneSlice(tool.Scope.Permissions)
	}
	return []string{tool.Name}
}

func permissionRequest(tool Tool, call ToolCall, permission string) permissions.Request {
	pattern := call.Pattern
	if pattern == "" {
		pattern = call.Name
	}
	if pattern == "" {
		pattern = tool.Name
	}
	return permissions.Request{
		SessionID:  string(call.SessionID),
		RunID:      string(call.RunID),
		ToolCallID: string(call.ID),
		ToolName:   tool.Name,
		Permission: permission,
		Pattern:    pattern,
		Metadata:   cloneStringMap(tool.Metadata),
	}
}

func modelVisiblePermissionResult(code string, message string) ToolResult {
	payload := map[string]string{
		"status":  code,
		"message": message,
	}
	raw := mustJSON(payload)
	return ToolResult{
		Output:     message,
		Structured: raw,
		Metadata: map[string]string{
			"permission_status": code,
		},
	}
}

func messageOrDefault(message string, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}
