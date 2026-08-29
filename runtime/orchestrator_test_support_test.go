package runtime

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func newTestOrchestrator(store *admissionStore, streamer model.Streamer, extra ...Option) *StreamingOrchestrator {
	options := []Option{
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}),
		WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("owner-1"),
		WithQueueSize(2),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{})}),
	}
	return mustConfiguredOrchestrator(append(options, extra...)...)
}

func mustConfiguredOrchestrator(extra ...Option) *StreamingOrchestrator {
	options := []Option{
		WithStore(newAdmissionStore()),
		WithModelResolver(resolvedModel{}),
		WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	}
	orchestrator, err := NewStreamingOrchestrator(append(options, extra...)...)
	if err != nil {
		panic(err)
	}
	return orchestrator
}

func newTestRunExecution(host *StreamingOrchestrator, plan *RunPlan) *runExecution {
	return newRunExecution(host, plan, session.Run{
		ID: "test-run", SessionID: "test-session", ClaimToken: "test-claim", Status: session.RunRunning,
	})
}

func orchestratorConfig() config.Snapshot {
	selection := model.Selection{ProviderID: "fake", ModelID: "test"}
	return config.Snapshot{
		Agent: config.Agent{
			Name:    "agent",
			Model:   selection,
			Options: map[string]string{"temperature": "0"},
		},
		Model: selection,
		Metadata: map[string]string{
			"workspace_id":   "workspace-1",
			"workspace_root": os.TempDir(),
		},
	}
}

type resolvedModel struct {
	streamer model.Streamer
}

func (r resolvedModel) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{
		Provider: model.Provider{ID: "fake"},
		Model:    model.Descriptor{ID: "test", ProviderID: "fake"},
		Streamer: r.streamer,
	}, nil
}

type scriptedStreamer func(context.Context, model.Request) ([]*einoschema.Message, error)

func (s scriptedStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	messages, err := s(ctx, request)
	if err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[model.StreamDelta](len(messages))
	go func() {
		defer writer.Close()
		for _, msg := range messages {
			if writer.Send(model.StreamDelta{Message: msg, Usage: model.UsageFromMessage(msg)}, nil) {
				return
			}
		}
	}()
	return reader, nil
}

type deltaStreamerFunc func(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error)

func (f deltaStreamerFunc) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	return f(ctx, request)
}

type sequenceIDs struct {
	mu sync.Mutex
	n  int
}

func (s *sequenceIDs) next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return prefix + "-" + strconv.Itoa(s.n)
}

func (s *sequenceIDs) NewRunID() session.RunID         { return session.RunID(s.next("run")) }
func (s *sequenceIDs) NewMessageID() session.MessageID { return session.MessageID(s.next("message")) }
func (s *sequenceIDs) NewPartID() session.PartID       { return session.PartID(s.next("part")) }
func (s *sequenceIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next("tool-call"))
}
func (s *sequenceIDs) NewEventID() session.EventID { return session.EventID(s.next("event")) }
func (s *sequenceIDs) NewEpochID() session.EpochID { return session.EpochID(s.next("epoch")) }

type blockingSink struct {
	mu     sync.Mutex
	events []Event
	delay  time.Duration
}

func (s *blockingSink) Emit(_ context.Context, event Event) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *blockingSink) count(kind EventKind) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, event := range s.events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

type blockingSinkFunc func(context.Context, Event) error

func (f blockingSinkFunc) Emit(ctx context.Context, event Event) error {
	return f(ctx, event)
}

type staticToolRegistry struct {
	tools []Tool
}

func (r staticToolRegistry) ResolveTools(context.Context, ToolScopeContext) ([]Tool, error) {
	return r.tools, nil
}

type orchestratorToolExecutorFunc func(context.Context, ToolCall) (ToolResult, error)

func (f orchestratorToolExecutorFunc) Execute(ctx context.Context, call ToolCall) (ToolResult, error) {
	return f(ctx, call)
}

func permissionPatternField(field string) PermissionPatternResolver {
	return PermissionPatternResolverFunc(func(_ context.Context, input json.RawMessage) (string, error) {
		var object map[string]string
		if err := json.Unmarshal(input, &object); err != nil {
			return "", err
		}
		return object[field], nil
	})
}
