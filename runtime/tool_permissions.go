package runtime

import (
	"context"
	"errors"

	"github.com/mattsp1290/eino-agent/permissions"
)

// ExecuteToolWithPermissions checks policy and approval hooks before executing
// a materialized tool. Denial, missing approval, and interruption are returned
// as model-visible tool output; operational policy failures are returned as
// errors for runtime settlement.
func ExecuteToolWithPermissions(ctx context.Context, tool Tool, call ToolCall, policy permissions.Policy) (ToolResult, error) {
	if policy == nil {
		return tool.Executor.Execute(ctx, call)
	}
	for _, permission := range toolPermissions(tool, call) {
		request := permissionRequest(tool, call, permission)
		decision, err := policy.Decide(ctx, request)
		if err != nil {
			if errors.Is(err, permissions.ErrInterrupted) {
				return modelVisiblePermissionResult("interrupted", "tool call interrupted during permission check"), nil
			}
			return ToolResult{}, err
		}
		switch decision.Action {
		case permissions.ActionAllow:
			continue
		case permissions.ActionDeny:
			return modelVisiblePermissionResult("denied", messageOrDefault(decision.Message, "tool call denied by policy")), nil
		case permissions.ActionAsk:
			if call.Approval == nil {
				return modelVisiblePermissionResult("approval_required", messageOrDefault(decision.Message, "tool call requires approval")), nil
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
				return modelVisiblePermissionResult("interrupted", "tool call interrupted while waiting for approval"), nil
			}
			if errors.Is(err, permissions.ErrDenied) {
				return modelVisiblePermissionResult("denied", err.Error()), nil
			}
			if errors.Is(err, permissions.ErrApprovalRequired) {
				return modelVisiblePermissionResult("approval_required", err.Error()), nil
			}
			return ToolResult{}, err
		default:
			return ToolResult{}, permissions.ErrOperational
		}
	}
	return tool.Executor.Execute(ctx, call)
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
