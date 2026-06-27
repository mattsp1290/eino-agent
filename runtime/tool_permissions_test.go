package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
)

func TestExecuteToolWithPermissionsAllow(t *testing.T) {
	t.Parallel()

	tool := executableTool()
	result, err := ExecuteToolWithPermissions(context.Background(), tool, toolCall(), permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{Action: permissions.ActionAllow}, nil
	}))
	if err != nil {
		t.Fatalf("ExecuteToolWithPermissions error = %v", err)
	}
	if result.Output != "executed" {
		t.Fatalf("output = %q, want executed", result.Output)
	}
}

func TestExecuteToolWithPermissionsDenyIsModelVisible(t *testing.T) {
	t.Parallel()

	result, err := ExecuteToolWithPermissions(context.Background(), executableTool(), toolCall(), permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{Action: permissions.ActionDeny, Message: "blocked"}, nil
	}))
	if err != nil {
		t.Fatalf("ExecuteToolWithPermissions error = %v", err)
	}
	assertPermissionResult(t, result, "denied", "blocked")
}

func TestExecuteToolWithPermissionsApprovalRequiredWithoutRequester(t *testing.T) {
	t.Parallel()

	result, err := ExecuteToolWithPermissions(context.Background(), executableTool(), toolCall(), permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{Action: permissions.ActionAsk, Message: "needs approval"}, nil
	}))
	if err != nil {
		t.Fatalf("ExecuteToolWithPermissions error = %v", err)
	}
	assertPermissionResult(t, result, "approval_required", "needs approval")
}

func TestExecuteToolWithPermissionsApprovalInterruptionIsModelVisible(t *testing.T) {
	t.Parallel()

	call := toolCall()
	call.Approval = approvalFunc(func(context.Context, ApprovalRequest) error {
		return permissions.ErrInterrupted
	})
	result, err := ExecuteToolWithPermissions(context.Background(), executableTool(), call, permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{Action: permissions.ActionAsk}, nil
	}))
	if err != nil {
		t.Fatalf("ExecuteToolWithPermissions error = %v", err)
	}
	assertPermissionResult(t, result, "interrupted", "tool call interrupted while waiting for approval")
}

func TestExecuteToolWithPermissionsApprovalDenialIsModelVisibleDenied(t *testing.T) {
	t.Parallel()

	call := toolCall()
	call.Approval = approvalFunc(func(context.Context, ApprovalRequest) error {
		return permissions.ErrDenied
	})
	result, err := ExecuteToolWithPermissions(context.Background(), executableTool(), call, permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{Action: permissions.ActionAsk}, nil
	}))
	if err != nil {
		t.Fatalf("ExecuteToolWithPermissions error = %v", err)
	}
	assertPermissionResult(t, result, "denied", permissions.ErrDenied.Error())
}

func TestExecuteToolWithPermissionsContextCancellationIsOperational(t *testing.T) {
	t.Parallel()

	_, err := ExecuteToolWithPermissions(context.Background(), executableTool(), toolCall(), permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{}, context.Canceled
	}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ExecuteToolWithPermissions error = %v, want context.Canceled", err)
	}
}

func TestExecuteToolWithPermissionsUsesOperationPattern(t *testing.T) {
	t.Parallel()

	call := toolCall()
	call.Pattern = "rm -rf tmp"
	result, err := ExecuteToolWithPermissions(context.Background(), executableTool(), call, permissions.StaticPolicy{Rules: []config.PermissionRule{
		{Permission: "shell", Pattern: "rm *", Action: config.PermissionActionDeny},
	}})
	if err != nil {
		t.Fatalf("ExecuteToolWithPermissions error = %v", err)
	}
	assertPermissionResult(t, result, "denied", "tool call denied by policy")
}

func TestExecuteToolWithPermissionsOperationalPolicyError(t *testing.T) {
	t.Parallel()

	errOperational := errors.New("policy store unavailable")
	_, err := ExecuteToolWithPermissions(context.Background(), executableTool(), toolCall(), permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{}, errOperational
	}))
	if !errors.Is(err, errOperational) {
		t.Fatalf("ExecuteToolWithPermissions error = %v, want operational error", err)
	}
}

func TestExecuteToolWithPermissionsApprovalOperationalError(t *testing.T) {
	t.Parallel()

	call := toolCall()
	errApproval := errors.New("approval backend unavailable")
	call.Approval = approvalFunc(func(context.Context, ApprovalRequest) error {
		return errApproval
	})
	_, err := ExecuteToolWithPermissions(context.Background(), executableTool(), call, permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		return permissions.Decision{Action: permissions.ActionAsk}, nil
	}))
	if !errors.Is(err, errApproval) {
		t.Fatalf("ExecuteToolWithPermissions error = %v, want approval operational error", err)
	}
}

func executableTool() Tool {
	return Tool{
		Name: "shell",
		Scope: ToolScope{
			Permissions: []string{"shell"},
		},
		Executor: toolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "executed"}, nil
		}),
	}
}

func toolCall() ToolCall {
	return ToolCall{
		ID:        session.ToolCallID("call-1"),
		SessionID: session.ID("session-1"),
		RunID:     session.RunID("run-1"),
		Name:      "shell",
	}
}

func assertPermissionResult(t *testing.T, result ToolResult, status string, message string) {
	t.Helper()
	if result.Output != message {
		t.Fatalf("output = %q, want %q", result.Output, message)
	}
	if result.Metadata["permission_status"] != status {
		t.Fatalf("permission status = %q, want %q", result.Metadata["permission_status"], status)
	}
	var payload map[string]string
	if err := json.Unmarshal(result.Structured, &payload); err != nil {
		t.Fatalf("structured output is not json: %v", err)
	}
	if payload["status"] != status || payload["message"] != message {
		t.Fatalf("payload = %#v, want %s/%s", payload, status, message)
	}
}

type toolExecutorFunc func(context.Context, ToolCall) (ToolResult, error)

func (fn toolExecutorFunc) Execute(ctx context.Context, call ToolCall) (ToolResult, error) {
	return fn(ctx, call)
}

type approvalFunc func(context.Context, ApprovalRequest) error

func (fn approvalFunc) Ask(ctx context.Context, request ApprovalRequest) error {
	return fn(ctx, request)
}
