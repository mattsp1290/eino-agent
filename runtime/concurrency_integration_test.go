package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestConcurrentSessionsCompleteWithSQLiteStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()
	var executions atomic.Int64
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		executions.Add(1)
		return ToolResult{Output: "tool ok"}, nil
	})}}}
	orch := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
			for _, msg := range request.Messages {
				if msg.Role == einoschema.Tool {
					return []*einoschema.Message{einoschema.AssistantMessage("ok:"+request.Identity.SessionID, nil)}, nil
				}
			}
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
				ID:   "call-" + request.Identity.SessionID,
				Type: "function",
				Function: einoschema.FunctionCall{
					Name:      "echo",
					Arguments: `{}`,
				},
			}})}, nil
		})}),
		WithClock(func() time.Time { return time.Date(2026, 6, 28, 15, 0, 0, 0, time.UTC) }),
		WithOwnerID("owner"),
		WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
	)

	const sessions = 12
	var wg sync.WaitGroup
	errs := make(chan error, sessions)
	for i := range sessions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sessionID := session.ID("concurrent-session-" + string(rune('a'+i)))
			handle, err := orch.Start(ctx, Request{
				SessionID: sessionID,
				Message:   UserMessage{Content: "hello"},
				Config:    orchestratorConfig(),
			})
			if err != nil {
				errs <- err
				return
			}
			result := <-handle.Done()
			if result.Error != nil || result.Status != session.RunCompleted {
				errs <- fmt.Errorf("session %s result = %+v", sessionID, result)
				return
			}
			if active, err := store.ActiveRun(ctx, sessionID); !errors.Is(err, session.ErrNotFound) {
				errs <- fmt.Errorf("session %s active run = %+v err=%v, want ErrNotFound", sessionID, active, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent session error: %v", err)
		}
	}
	if executions.Load() != sessions {
		t.Fatalf("tool executions = %d, want %d", executions.Load(), sessions)
	}
}

func TestConcurrentInterruptsSettleDurableRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()
	started := make(chan struct{}, 16)
	orch := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(ctx context.Context, _ model.Request) ([]*einoschema.Message, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		})}),
		WithClock(func() time.Time { return time.Date(2026, 6, 28, 15, 30, 0, 0, time.UTC) }),
		WithOwnerID("owner"),
	)

	const sessions = 8
	handles := make([]Handle, 0, sessions)
	for i := range sessions {
		handle, err := orch.Start(ctx, Request{
			SessionID: session.ID("interrupt-session-" + string(rune('a'+i))),
			Message:   UserMessage{Content: "hello"},
			Config:    orchestratorConfig(),
		})
		if err != nil {
			t.Fatalf("Start %d: %v", i, err)
		}
		handles = append(handles, handle)
	}
	for range sessions {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for provider start")
		}
	}
	if _, err := orch.Start(ctx, Request{
		SessionID: "interrupt-session-a",
		Message:   UserMessage{Content: "stale contender"},
		Config:    orchestratorConfig(),
	}); !errors.Is(err, session.ErrSessionBusy) {
		t.Fatalf("contending Start error = %v, want ErrSessionBusy", err)
	}
	contended, err := store.ListMessages(ctx, "interrupt-session-a", session.ReplayCursor{Limit: 10})
	if err != nil || len(contended.Messages) != 2 || len(contended.Parts) != 1 {
		t.Fatalf("contended history = %#v, %v; want only first admission pair", contended, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(handles))
	for _, handle := range handles {
		wg.Add(1)
		go func(handle Handle) {
			defer wg.Done()
			if err := handle.Interrupt(context.Background(), "disconnect"); err != nil {
				errs <- err
			}
		}(handle)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Interrupt error: %v", err)
		}
	}
	for _, handle := range handles {
		result := <-handle.Done()
		if result.Status != session.RunInterrupted || !result.Interrupted {
			t.Fatalf("result = %+v, want interrupted", result)
		}
		run, err := store.GetRun(ctx, result.RunID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if run.Status != session.RunInterrupted {
			t.Fatalf("durable run status = %s, want interrupted", run.Status)
		}
		if _, err := store.ActiveRun(ctx, run.SessionID); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("active run err = %v, want ErrNotFound", err)
		}
		batch, err := store.ListMessages(ctx, run.SessionID, session.ReplayCursor{Limit: 10})
		if err != nil || len(batch.Messages) != 2 || batch.Messages[0].Role != session.RoleUser || batch.Messages[1].Role != session.RoleAssistant || len(batch.Parts) != 1 || string(batch.Parts[0].Payload) != `{"text":"hello"}` {
			t.Fatalf("interrupted history = %#v, %v; want admitted user/assistant pair", batch, err)
		}
	}
}
