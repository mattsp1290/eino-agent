package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
)

func TestSessionWatchRuntimeTerminalPaths(t *testing.T) {
	for _, mode := range []string{"empty", "model-error", "model-panic", "persistence-error", "tool-error", "tool-denied", "tool-cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			st, stPool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "watch.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stPool.Close() }()
			options := watch.Options{Snapshot: session.ObservationLimits{MaxMessages: 20, MaxTools: 20, MaxParts: 40, MaxSnapshotBytes: 64000, MaxTextBytes: 1000}, PollInterval: time.Millisecond, ReadTimeout: time.Second, MaxSubscriptions: 10, MaxWatchedSessions: 10, MaxLiveRuns: 10, MaxLiveTextBytes: 1000, PendingUpdates: 20}
			service, err := watch.NewService(st, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = service.Close(context.Background()) }()
			var store session.Store = st
			if mode == "persistence-error" {
				store = &finalizationFailureStore{Store: st}
			}
			var modelCalls atomic.Int32
			streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
				n := modelCalls.Add(1)
				switch mode {
				case "model-error":
					return nil, errors.New("PRIVATE_MODEL_ERROR")
				case "model-panic":
					panic("PRIVATE_MODEL_PANIC")
				}
				if strings.HasPrefix(mode, "tool-") && n == 1 {
					return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"secret":"PRIVATE_ARGUMENT"}`}}})}, nil
				}
				return []*einoschema.Message{einoschema.AssistantMessage("", nil)}, nil
			})
			toolStarted := make(chan struct{})
			tool := Tool{Name: "echo", Executor: orchestratorToolExecutorFunc(func(ctx context.Context, _ ToolCall) (ToolResult, error) {
				if mode == "tool-cancel" {
					close(toolStarted)
					<-ctx.Done()
					return ToolResult{}, ctx.Err()
				}
				return ToolResult{}, errors.New("PRIVATE_TOOL_ERROR")
			})}
			if mode == "tool-denied" {
				tool.Scope.Permissions = []string{"test.tool"}
			}
			opts := []Option{WithStore(store), WithModelResolver(resolvedModel{streamer}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{tools: []Tool{tool}})}), WithSessionObserver(service)}
			if mode == "tool-denied" {
				opts = append(opts, WithPermissions(permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
					return permissions.Decision{Action: permissions.ActionDeny, Message: "PRIVATE_DENIAL"}, nil
				})))
			}
			orchestrator, err := NewStreamingOrchestrator(opts...)
			if err != nil {
				t.Fatal(err)
			}
			observer, err := service.Watch(ctx, "watched")
			if err != nil {
				t.Fatal(err)
			}
			defer observer.Close()
			handle, err := orchestrator.Start(ctx, Request{SessionID: "watched", Message: UserMessage{Content: "user"}, Config: orchestratorConfig()})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "tool-cancel" {
				select {
				case <-toolStarted:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if err = handle.Interrupt(ctx, "fixture"); err != nil {
					t.Fatal(err)
				}
			}
			var result Result
			select {
			case result = <-handle.Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			want := session.RunCompleted
			if strings.HasPrefix(mode, "model-") || mode == "persistence-error" {
				want = session.RunFailed
			}
			if mode == "tool-cancel" {
				want = session.RunInterrupted
			}
			if result.Status != want {
				t.Fatal(result)
			}
			var snapshot session.ObservationSnapshot
			for {
				u, err := observer.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(u)
				if strings.Contains(string(raw), "PRIVATE_") {
					t.Fatal("private data in observation")
				}
				if u.Kind != watch.Durable {
					continue
				}
				terminal := false
				for _, r := range u.Snapshot.Runs {
					if r.ID == handle.RunID() && r.Terminal() {
						terminal = true
					}
				}
				if terminal {
					snapshot = u.Snapshot
					break
				}
			}
			assistant := snapshot.Messages[len(snapshot.Messages)-1]
			if want == session.RunCompleted && !assistant.Finalized {
				t.Fatal("empty completion not finalized")
			}
			if mode == "persistence-error" && assistant.Finalized {
				t.Fatal("failed transaction finalized placeholder")
			}
			if strings.HasPrefix(mode, "tool-") && (len(snapshot.Tools) != 1 || !session.TerminalToolCall(snapshot.Tools[0].Status)) {
				t.Fatal(snapshot)
			}
		})
	}
}

type finalizationFailureStore struct{ session.Store }

func (s *finalizationFailureStore) Execution(f session.RunFence) session.ExecutionStore {
	return &finalizationFailureExecution{ExecutionStore: s.Store.Execution(f)}
}

type finalizationFailureExecution struct{ session.ExecutionStore }

func (s *finalizationFailureExecution) WithinTx(ctx context.Context, fn func(context.Context, session.ExecutionStore) error) error {
	return s.ExecutionStore.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
		return fn(ctx, &finalizationFailureExecution{ExecutionStore: tx})
	})
}
func (s *finalizationFailureExecution) FinalizeAssistantMessage(context.Context, session.MessageID) error {
	return errors.New("PRIVATE_PERSISTENCE_ERROR")
}
