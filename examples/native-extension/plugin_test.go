package nativeextension

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestScopedMountConcurrentPlansAndQuiescentUnmount(t *testing.T) {
	registry := composition.NewRegistry(nil)
	var cleaned atomic.Bool
	mount, err := Mount(context.Background(), registry, "session-a", func() { cleaned.Store(true) })
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		count int
		err   error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, sessionID := range []string{"session-a", "session-b"} {
		sessionID := sessionID
		wait.Add(1)
		go func() {
			defer wait.Done()
			plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: session.ID(sessionID)})
			if err != nil {
				results <- result{err: err}
				return
			}
			defer plan.Release()
			resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: session.ID(sessionID)})
			results <- result{count: len(resolved), err: err}
		}()
	}
	wait.Wait()
	close(results)
	counts := map[int]int{}
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		counts[result.count]++
	}
	if counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("scoped tool counts = %#v", counts)
	}

	frozen, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	mount.Deactivate()
	newPlan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	tools, err := newPlan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	newPlan.Release()
	if err != nil || len(tools) != 0 {
		t.Fatalf("post-deactivate tools = %#v, %v", tools, err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := mount.Close(closeCtx); err == nil || cleaned.Load() {
		t.Fatalf("close while leased = %v cleaned=%t", err, cleaned.Load())
	}
	frozen.Release()
	if err := mount.Close(context.Background()); err != nil || !cleaned.Load() {
		t.Fatalf("final close = %v cleaned=%t", err, cleaned.Load())
	}
}
