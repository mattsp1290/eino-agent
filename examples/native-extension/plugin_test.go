package nativeextension

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
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

func TestNativeContextContributionReachesProviderBeforeHistory(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), t.TempDir()+"/native-context.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	registry := composition.NewRegistry(nil)
	mount, err := Mount(context.Background(), registry, "session-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	streamer := &capturingStreamer{}
	selection := model.Selection{ProviderID: "fake", ModelID: "test"}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store), runtime.WithRunPlanProvider(registry), runtime.WithIDGenerator(&testIDs{}),
		runtime.WithOwnerID("native-test"), runtime.WithModelResolver(testResolver{streamer: streamer}),
	)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := orchestrator.Start(context.Background(), runtime.Request{
		SessionID: "session-a", Input: []*einoschema.Message{einoschema.UserMessage("base-user")},
		Config: config.Snapshot{Agent: config.Agent{Name: "agent", Model: selection, Options: map[string]string{}}, Model: selection, Metadata: map[string]string{"workspace_root": t.TempDir()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-handle.Done(); result.Error != nil {
		t.Fatal(result.Error)
	}
	streamer.mu.Lock()
	defer streamer.mu.Unlock()
	if len(streamer.messages) < 2 || streamer.messages[0] != "Native example extension is active for this session." || streamer.messages[1] != "base-user" {
		t.Fatalf("provider messages = %v", streamer.messages)
	}
}

type testResolver struct{ streamer model.Streamer }

func (r testResolver) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{Provider: model.Provider{ID: "fake"}, Model: model.Descriptor{ID: "test", ProviderID: "fake"}, Streamer: r.streamer}, nil
}

type capturingStreamer struct {
	mu       sync.Mutex
	messages []string
}

func (s *capturingStreamer) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	s.mu.Lock()
	for _, message := range request.Messages {
		s.messages = append(s.messages, message.Content)
	}
	s.mu.Unlock()
	reader, writer := einoschema.Pipe[*einoschema.Message](1)
	go func() {
		defer writer.Close()
		writer.Send(einoschema.AssistantMessage("done", nil), nil)
	}()
	return reader, nil
}

type testIDs struct{ next atomic.Int64 }

func (i *testIDs) id(prefix string) string           { return fmt.Sprintf("%s-%d", prefix, i.next.Add(1)) }
func (i *testIDs) NewRunID() session.RunID           { return session.RunID(i.id("run")) }
func (i *testIDs) NewMessageID() session.MessageID   { return session.MessageID(i.id("message")) }
func (i *testIDs) NewPartID() session.PartID         { return session.PartID(i.id("part")) }
func (i *testIDs) NewToolCallID() session.ToolCallID { return session.ToolCallID(i.id("tool-call")) }
func (i *testIDs) NewEventID() session.EventID       { return session.EventID(i.id("event")) }
func (i *testIDs) NewEpochID() session.EpochID       { return session.EpochID(i.id("epoch")) }
