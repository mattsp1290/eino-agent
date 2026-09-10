package agenticgraph

import (
	"context"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"
)

func TestBuildGraphInvokesToolThroughAliasWithOneAuditAndOneSettlement(t *testing.T) {
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
	if counters.Settled != 1 {
		t.Fatalf("Settled = %d, want 1", counters.Settled)
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
