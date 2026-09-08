package consumer

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	"github.com/mattsp1290/eino-agent/store/sqlite"
)

type discoveryModel struct {
	mu       sync.Mutex
	requests []model.Request
}

func (m *discoveryModel) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	m.mu.Lock()
	m.requests = append(m.requests, request)
	m.mu.Unlock()
	prompt := request.Messages[len(request.Messages)-1].Content
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("reply:"+prompt, nil)}, nil)
	writer.Close()
	return reader, nil
}
func (m *discoveryModel) recorded() []model.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]model.Request(nil), m.requests...)
}

type discoveryResolver struct{ streamer model.Streamer }

func (r discoveryResolver) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{Provider: model.Provider{ID: "discovery"}, Model: model.Descriptor{ID: "deterministic", ProviderID: "discovery"}, Streamer: r.streamer}, nil
}
func discoveryRuntime(t *testing.T, st session.Store, m *discoveryModel) *runtime.StreamingOrchestrator {
	t.Helper()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}), runtime.WithModelResolver(discoveryResolver{m}), runtime.WithRunPlanProvider(registry))
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator
}
func discoveryTurn(t *testing.T, ctx context.Context, o *runtime.StreamingOrchestrator, id session.ID, workspace, prompt string) session.RunID {
	t.Helper()
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	handle, err := o.Start(ctx, runtime.Request{SessionID: id, Message: runtime.UserMessage{Content: prompt}, Config: config.Snapshot{Agent: config.Agent{Name: "discovery-consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": workspace}}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-handle.Done():
		if result.Status != session.RunCompleted || result.Error != nil {
			t.Fatal(result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return handle.RunID()
}
func discoveredIDs(t *testing.T, ctx context.Context, st session.Store) []session.ID {
	t.Helper()
	reader, ok := st.(session.SessionDiscoveryReader)
	if !ok {
		t.Fatal("discovery unavailable")
	}
	q := session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1}
	var ids []session.ID
	for i := 0; ; i++ {
		if i > 2 {
			t.Fatal("unbounded traversal")
		}
		p, err := reader.ListSessions(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range p.Sessions {
			if s.WorkspaceID != "A" {
				t.Fatal(s)
			}
			ids = append(ids, s.ID)
		}
		if p.NextCursor == "" {
			return ids
		}
		q.Cursor = p.NextCursor
	}
}
func discoveryHistory(t *testing.T, ctx context.Context, st session.Store, id session.ID) []*einoschema.Message {
	t.Helper()
	batch, err := history.LoadBatch(ctx, st, id)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := history.Project(batch, history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return messages
}
func assertDiscoveryMessages(t *testing.T, messages []*einoschema.Message, contents ...string) {
	t.Helper()
	if len(messages) != len(contents) {
		t.Fatalf("messages=%+v want contents=%v", messages, contents)
	}
	for i, m := range messages {
		role := einoschema.User
		if i%2 == 1 {
			role = einoschema.Assistant
		}
		if m.Content != contents[i] || m.Role != role {
			t.Fatalf("message %d = %+v want %s %q", i, m, role, contents[i])
		}
	}
}
func TestPublicSessionDiscoveryReopenAndIndependentContinuation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "discovery.db")
	st, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	initialModel := &discoveryModel{}
	orchestrator := discoveryRuntime(t, st, initialModel)
	at := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	for i, id := range []session.ID{"a1", "a2", "b1"} {
		ws := "A"
		if id == "b1" {
			ws = "B"
		}
		_, err = st.CreateSession(ctx, session.Session{ID: id, WorkspaceID: ws, Title: string(id), CreatedAt: at.Add(time.Duration(i)), UpdatedAt: at})
		if err != nil {
			t.Fatal(err)
		}
	}
	selected := discoveredIDs(t, ctx, st)
	if !reflect.DeepEqual(selected, []session.ID{"a2", "a1"}) || len(initialModel.recorded()) != 0 {
		t.Fatal(selected, "provider called during discovery")
	}
	for _, id := range selected {
		if len(discoveryHistory(t, ctx, st, id)) != 0 {
			t.Fatal("empty discovery depended on execution")
		}
	}
	var runIDs []session.RunID
	for _, id := range append(selected, session.ID("b1")) {
		ws := "A"
		if id == "b1" {
			ws = "B"
		}
		runIDs = append(runIDs, discoveryTurn(t, ctx, orchestrator, id, ws, string(id)+"-first"))
		assertDiscoveryMessages(t, discoveryHistory(t, ctx, st, id), string(id)+"-first", "reply:"+string(id)+"-first")
	}
	if !reflect.DeepEqual(discoveredIDs(t, ctx, st), selected) {
		t.Fatal("completed sessions disappeared")
	}
	bBefore := discoveryHistory(t, ctx, st, "b1")
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	freshModel := &discoveryModel{}
	freshRuntime := discoveryRuntime(t, reopened, freshModel)
	rediscovered := discoveredIDs(t, ctx, reopened)
	if !reflect.DeepEqual(rediscovered, []session.ID{"a2", "a1"}) || len(freshModel.recorded()) != 0 {
		t.Fatal(rediscovered)
	}
	for i, id := range rediscovered {
		runIDs = append(runIDs, discoveryTurn(t, ctx, freshRuntime, id, "A", string(id)+"-second"))
		requests := freshModel.recorded()
		if len(requests) != i+1 {
			t.Fatal("unexpected provider calls")
		}
		assertDiscoveryMessages(t, requests[i].Messages, string(id)+"-first", "reply:"+string(id)+"-first", string(id)+"-second")
		assertDiscoveryMessages(t, discoveryHistory(t, ctx, reopened, id), string(id)+"-first", "reply:"+string(id)+"-first", string(id)+"-second", "reply:"+string(id)+"-second")
	}
	if !reflect.DeepEqual(bBefore, discoveryHistory(t, ctx, reopened, "b1")) {
		t.Fatal("B history changed")
	}
	durable := func() string {
		t.Helper()
		var values []any
		for _, id := range []session.ID{"a1", "a2", "b1"} {
			w, err := reopened.ReadObservationRevision(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, w)
		}
		for _, id := range runIDs {
			r, err := reopened.GetRun(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, r)
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	before := durable()
	calls := len(freshModel.recorded())
	for range 3 {
		discoveredIDs(t, ctx, reopened)
	}
	if durable() != before || len(freshModel.recorded()) != calls {
		t.Fatal("discovery changed execution or revision state")
	}
}

// A reconstructed host allocates fresh IDs without an in-memory sequence.
type discoveryIDs struct{}

func (discoveryIDs) NewRunID() session.RunID           { return session.RunID(rand.Text()) }
func (discoveryIDs) NewMessageID() session.MessageID   { return session.MessageID(rand.Text()) }
func (discoveryIDs) NewPartID() session.PartID         { return session.PartID(rand.Text()) }
func (discoveryIDs) NewToolCallID() session.ToolCallID { return session.ToolCallID(rand.Text()) }
func (discoveryIDs) NewEventID() session.EventID       { return session.EventID(rand.Text()) }
func (discoveryIDs) NewEpochID() session.EpochID       { return session.EpochID(rand.Text()) }
