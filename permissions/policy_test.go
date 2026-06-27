package permissions

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
)

func TestStaticPolicyDecisions(t *testing.T) {
	t.Parallel()

	policy := StaticPolicy{Rules: []config.PermissionRule{
		{Permission: "shell", Pattern: "go test *", Action: config.PermissionActionAllow},
		{Permission: "shell", Pattern: "rm *", Action: config.PermissionActionDeny},
		{Permission: "network", Pattern: "*", Action: config.PermissionActionAsk},
	}}

	for _, test := range []struct {
		name    string
		request Request
		action  Action
	}{
		{name: "allow", request: Request{Permission: "shell", Pattern: "go test ./..."}, action: ActionAllow},
		{name: "deny", request: Request{Permission: "shell", Pattern: "rm -rf tmp"}, action: ActionDeny},
		{name: "ask", request: Request{Permission: "network", Pattern: "https://example.test"}, action: ActionAsk},
		{name: "default allow", request: Request{Permission: "read", Pattern: "project://file"}, action: ActionAllow},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			decision, err := policy.Decide(context.Background(), test.request)
			if err != nil {
				t.Fatalf("Decide error = %v", err)
			}
			if decision.Action != test.action {
				t.Fatalf("action = %q, want %q", decision.Action, test.action)
			}
		})
	}
}

func TestStaticPolicyUnknownActionIsOperational(t *testing.T) {
	t.Parallel()

	policy := StaticPolicy{Rules: []config.PermissionRule{{Permission: "shell", Pattern: "*", Action: "later"}}}
	_, err := policy.Decide(context.Background(), Request{Permission: "shell", Pattern: "cmd"})
	if !errors.Is(err, ErrOperational) {
		t.Fatalf("Decide error = %v, want ErrOperational", err)
	}
}

func TestStaticPolicyMalformedPatternIsOperational(t *testing.T) {
	t.Parallel()

	policy := StaticPolicy{Rules: []config.PermissionRule{{Permission: "shell", Pattern: "rm [", Action: config.PermissionActionDeny}}}
	_, err := policy.Decide(context.Background(), Request{Permission: "shell", Pattern: "rm -rf tmp"})
	if !errors.Is(err, ErrOperational) {
		t.Fatalf("Decide error = %v, want ErrOperational", err)
	}
}
