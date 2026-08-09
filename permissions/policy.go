package permissions

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/mattsp1290/eino-agent/config"
)

var (
	// ErrDenied marks a policy denial that should be visible to the model.
	ErrDenied = errors.New("permission denied")
	// ErrApprovalRequired marks a missing or rejected approval that should be
	// visible to the model.
	ErrApprovalRequired = errors.New("permission approval required")
	// ErrInterrupted marks user interruption during approval.
	ErrInterrupted = errors.New("permission approval interrupted")
	// ErrOperational marks a policy infrastructure failure that should not be
	// presented as ordinary model-visible tool output.
	ErrOperational = errors.New("permission policy operational failure")
)

// Action is the permission policy decision.
type Action string

const (
	ActionAllow Action = "allow"
	ActionDeny  Action = "deny"
	ActionAsk   Action = "ask"
)

// Request is one permission check for a tool call.
type Request struct {
	SessionID  string
	RunID      string
	ToolCallID string
	ToolName   string
	Permission string
	Pattern    string
	Metadata   map[string]string
}

// Decision is the policy answer for one request.
type Decision struct {
	Action  Action
	Reason  string
	Message string
}

// Policy decides whether a tool call permission is allowed, denied, or needs
// host/user approval.
type Policy interface {
	Decide(ctx context.Context, request Request) (Decision, error)
}

// PolicyFunc adapts a function into a Policy.
type PolicyFunc func(context.Context, Request) (Decision, error)

var _ Policy = PolicyFunc(nil)

// Decide calls fn.
func (fn PolicyFunc) Decide(ctx context.Context, request Request) (Decision, error) {
	return fn(ctx, request)
}

// StaticPolicy evaluates config permission rules in order.
type StaticPolicy struct {
	Rules []config.PermissionRule
}

// Decide returns the first matching rule or allows when no rule matches.
func (p StaticPolicy) Decide(ctx context.Context, request Request) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	for _, rule := range p.Rules {
		matched, err := matches(rule, request)
		if err != nil {
			return Decision{}, err
		}
		if !matched {
			continue
		}
		switch rule.Action {
		case config.PermissionActionAllow:
			return Decision{Action: ActionAllow}, nil
		case config.PermissionActionDeny:
			return Decision{Action: ActionDeny, Reason: "rule_denied", Message: "tool call denied by policy"}, nil
		case config.PermissionActionAsk, "":
			return Decision{Action: ActionAsk, Reason: "approval_required", Message: "tool call requires approval"}, nil
		default:
			return Decision{}, fmt.Errorf("%w: unknown action %q", ErrOperational, rule.Action)
		}
	}
	return Decision{Action: ActionAllow}, nil
}

func matches(rule config.PermissionRule, request Request) (bool, error) {
	if rule.Permission != "" && rule.Permission != request.Permission {
		return false, nil
	}
	if rule.Pattern == "" || rule.Pattern == "*" {
		return true, nil
	}
	if rule.Pattern == request.Pattern {
		return true, nil
	}
	ok, err := path.Match(rule.Pattern, request.Pattern)
	if err != nil {
		return false, fmt.Errorf("%w: invalid pattern %q: %v", ErrOperational, rule.Pattern, err)
	}
	return ok, nil
}
