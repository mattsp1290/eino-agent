package agenticgraph

import (
	"context"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"
)

// TestBuildGraphInvokesToolThroughAliasWithOneAuditAndOneDispatch asserts
// exactly what this example proves: one audited model request and one call
// through the fake Dispatch closure per graph invocation. It deliberately
// does NOT claim "one settlement" -- dispatchFor's counters live entirely
// in this example's own closure (see CallCounters' doc comment), not behind
// a real runtime.BuildToolSettlement + store settle, so it cannot verify a
// durable settlement count.
func TestBuildGraphInvokesToolThroughAliasWithOneAuditAndOneDispatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	counters := &CallCounters{}
	runnable, err := BuildGraph(ctx, counters)
	if err != nil {
		t.Fatalf("BuildGraph: %v", err)
	}

	out, err := runnable.Invoke(ctx, map[string]any{})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("output messages = %d, want 1: %#v", len(out), out)
	}
	if out[0].Role != einoschema.AgenticRoleTypeUser {
		t.Fatalf("output role = %q, want user", out[0].Role)
	}

	if counters.Audited != 1 {
		t.Fatalf("Audited = %d, want 1", counters.Audited)
	}
	if counters.Dispatch != 1 {
		t.Fatalf("Dispatch = %d, want 1", counters.Dispatch)
	}
}

func TestBuildBranchExampleCompilesAndRoutesThroughRegisteredBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	counters := &CallCounters{}
	runnable, err := BuildBranchExample(ctx, counters)
	if err != nil {
		t.Fatalf("BuildBranchExample: %v", err)
	}
	out, err := runnable.Invoke(ctx, map[string]any{})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("output messages = %d, want 1: %#v", len(out), out)
	}
	if counters.Dispatch != 1 {
		t.Fatalf("Dispatch = %d, want 1 (only the branch actually taken should dispatch)", counters.Dispatch)
	}
}

func TestBuildParallelExampleCompilesAndFansOutToBothRegisteredMembers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	counters := &CallCounters{}
	runnable, err := BuildParallelExample(ctx, counters)
	if err != nil {
		t.Fatalf("BuildParallelExample: %v", err)
	}
	out, err := runnable.Invoke(ctx, map[string]any{})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if _, ok := out["result_a"]; !ok {
		t.Fatalf("output missing result_a key: %#v", out)
	}
	if _, ok := out["result_b"]; !ok {
		t.Fatalf("output missing result_b key: %#v", out)
	}
	// Both parallel members are registered under the same alias the fixed
	// model always calls, so both independently resolve and dispatch it.
	if counters.Dispatch != 2 {
		t.Fatalf("Dispatch = %d, want 2", counters.Dispatch)
	}
}
