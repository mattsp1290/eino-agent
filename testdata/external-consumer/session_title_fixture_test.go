package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	"github.com/mattsp1290/eino-agent/tools"
)

func TestPublicSessionTitleReopenAndIndependentHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "titles.db")
	st, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	initial := []session.Session{
		{ID: "a1", WorkspaceID: "A", Title: "Alpha one", CreatedAt: at.Add(time.Second), UpdatedAt: at},
		{ID: "a2", WorkspaceID: "A", Title: "Alpha two", CreatedAt: at.Add(2 * time.Second), UpdatedAt: at},
		{ID: "b1", WorkspaceID: "B", Title: "Beta sentinel", CreatedAt: at.Add(3 * time.Second), UpdatedAt: at},
	}
	for _, record := range initial {
		if err := session.ValidateSessionTitle(record.Title); err != nil {
			t.Fatal(err)
		}
		if _, err := st.CreateSession(ctx, record); err != nil {
			t.Fatal(err)
		}
	}

	m := &discoveryModel{}
	o := discoveryRuntime(t, st, m)
	discoveryTurn(t, ctx, o, "a1", "A", "a1-first")
	historyBeforeRename, err := st.ListMessages(ctx, "a1", session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	eventsBeforeRename, err := st.ListEvents(ctx, "a1", session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	modelCallsBeforeRename := len(m.recorded())
	if _, err := st.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "a1", WorkspaceID: "A", Title: "Renamed A1"}); err != nil {
		t.Fatal(err)
	}
	historyAfterRename, _ := st.ListMessages(ctx, "a1", session.ReplayCursor{Limit: 100})
	eventsAfterRename, _ := st.ListEvents(ctx, "a1", session.EventCursor{Limit: 100})
	if !reflect.DeepEqual(historyAfterRename, historyBeforeRename) || !reflect.DeepEqual(eventsAfterRename, eventsBeforeRename) || len(m.recorded()) != modelCallsBeforeRename {
		t.Fatal("standalone rename dispatched or changed conversation history")
	}
	discoveryTurn(t, ctx, o, "a1", "A", "a1-second")
	if _, err := st.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "a2", WorkspaceID: "A", Title: "Renamed A2"}); err != nil {
		t.Fatal(err)
	}
	discoveryTurn(t, ctx, o, "a2", "A", "a2-first")
	bBefore, err := st.GetSession(ctx, "b1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "b1", WorkspaceID: "A", Title: "forged"}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("cross-workspace error = %v", err)
	}
	if bAfter, _ := st.GetSession(ctx, "b1"); !reflect.DeepEqual(bAfter, bBefore) {
		t.Fatal("cross-workspace request changed sentinel")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()
	for id, title := range map[session.ID]string{"a1": "Renamed A1", "a2": "Renamed A2", "b1": "Beta sentinel"} {
		got, err := reopened.GetSession(ctx, id)
		if err != nil || got.Title != title {
			t.Fatalf("session %s = %#v, error = %v", id, got, err)
		}
	}
	freshModel := &discoveryModel{}
	freshRuntime := discoveryRuntime(t, reopened, freshModel)
	discoveryTurn(t, ctx, freshRuntime, "a1", "A", "a1-third")
	requests := freshModel.recorded()
	if len(requests) != 1 {
		t.Fatalf("model requests = %d", len(requests))
	}
	assertDiscoveryMessages(t, requests[0].Messages, "a1-first", "reply:a1-first", "a1-second", "reply:a1-second", "a1-third")
	assertDiscoveryMessages(t, discoveryHistory(t, ctx, reopened, "a1"), "a1-first", "reply:a1-first", "a1-second", "reply:a1-second", "a1-third", "reply:a1-third")
	assertDiscoveryMessages(t, discoveryHistory(t, ctx, reopened, "a2"), "a2-first", "reply:a2-first")
	page, err := reopened.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1})
	if err != nil || len(page.Sessions) != 1 || page.Sessions[0].ID != "a2" || page.Sessions[0].Title != "Renamed A2" || page.NextCursor == "" {
		t.Fatalf("first page = %#v, error = %v", page, err)
	}
	next, err := reopened.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1, Cursor: page.NextCursor})
	if err != nil || len(next.Sessions) != 1 || next.Sessions[0].ID != "a1" || next.Sessions[0].Title != "Renamed A1" || next.NextCursor != "" {
		t.Fatalf("second page = %#v, error = %v", next, err)
	}
}

type titleToolModel struct{ calls atomic.Int32 }

func (m *titleToolModel) StreamProvider(_ context.Context, _ model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	if m.calls.Add(1) == 1 {
		writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "rename-call", Type: "function", Function: einoschema.FunctionCall{Name: "rename_session", Arguments: `{"title":"Agent chosen"}`}}})}, nil)
	} else {
		writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("renamed", nil)}, nil)
	}
	writer.Close()
	return reader, nil
}

func TestPublicExecutorOnlySessionTitleWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	st, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "agent-title.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "agent", WorkspaceID: "A", Title: "Initial", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := composition.NewToolSourceIdentity(strings.Repeat("a", 64), strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	var retained runtime.SessionTitleWriter
	mount, err := registry.Mount(ctx, extension.Component{InstanceID: "title-tool", Artifact: extension.Artifact{Name: "title-tool", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}, composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
		return registrar.Tool(composition.ToolRegistration{ID: "rename", Scope: extension.GlobalScope(), SourceIdentity: identity, Definition: tools.Definition{
			Name: "rename_session", AllowSessionTitle: true,
			Execute: func(ctx context.Context, execution tools.Execution) (json.RawMessage, error) {
				var input struct {
					Title string `json:"title"`
				}
				if err := json.Unmarshal(execution.Input, &input); err != nil {
					return nil, err
				}
				retained = execution.Call.SessionTitle
				if retained == nil {
					return nil, errors.New("missing session title writer")
				}
				_, err := retained.SetTitle(ctx, input.Title)
				return json.RawMessage(`{"ok":true}`), err
			},
		}})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	m := &titleToolModel{}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	o, err := runtime.NewStreamingOrchestrator(runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}), runtime.WithModelResolver(discoveryResolver{m}), runtime.WithRunPlanProvider(registry))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := o.Start(ctx, runtime.Request{SessionID: "agent", Message: runtime.UserMessage{Content: "rename"}, Config: config.Snapshot{Agent: config.Agent{Name: "consumer", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "A"}}})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Error != nil || result.Status != session.RunCompleted || retained == nil {
		t.Fatalf("result = %#v", result)
	}
	if got, _ := st.GetSession(ctx, "agent"); got.Title != "Agent chosen" {
		t.Fatalf("title = %q", got.Title)
	}
	if _, err := retained.SetTitle(context.Background(), "stale"); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("post-settlement error = %v", err)
	}
}

const titleStateKey = "consumer:title_state"

var titleStateSentinel = json.RawMessage(`{"continuation":"TITLE_STATE_SENTINEL"}`)

type titleStateModel struct {
	mu        sync.Mutex
	responses []*einoschema.Message
	inputs    [][]*einoschema.Message
}

func (m *titleStateModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return nil, errors.New("unused")
}

func (m *titleStateModel) Stream(_ context.Context, input []*einoschema.Message, _ ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inputs = append(m.inputs, input)
	if len(m.responses) == 0 {
		return nil, errors.New("unexpected model call")
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return einoschema.StreamReaderFromArray([]*einoschema.Message{response}), nil
}

func (m *titleStateModel) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	return m, nil
}

func titleStateContract() model.ProviderStateContract {
	return model.ProviderStateContract{
		CodecID: "example.com/consumer/title-state", Version: 1, CompatibilityKey: "title-state-v1",
		Limits: model.ProviderStateLimits{MaxItems: 2, MaxItemBytes: 1024, MaxMessageBytes: 2048, MaxEnvelopeBytes: 4096, MaxStoredMessageBytes: 8192},
	}
}

func titleStateRuntime(t *testing.T, st session.Store, client *titleStateModel) *runtime.StreamingOrchestrator {
	t.Helper()
	codec, err := model.NewEinoJSONExtraStateCodec(model.EinoJSONExtraStateConfig{ExtraKey: titleStateKey, Contract: titleStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	streamer, err := model.NewEinoStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(runtime.WithStore(st), runtime.WithIDGenerator(discoveryIDs{}), runtime.WithModelResolver(discoveryResolver{streamer}), runtime.WithRunPlanProvider(registry))
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator
}

func TestPublicSessionTitlePreservesProviderStateAcrossReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "title-state.db")
	st, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "stateful", WorkspaceID: "A", Title: "before", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	firstAnswer := einoschema.AssistantMessage("first answer", nil)
	firstAnswer.Extra = map[string]any{titleStateKey: []json.RawMessage{append(json.RawMessage(nil), titleStateSentinel...)}}
	firstClient := &titleStateModel{responses: []*einoschema.Message{firstAnswer}}
	discoveryTurn(t, ctx, titleStateRuntime(t, st, firstClient), "stateful", "A", "first question")
	before := titleStatePayloads(t, ctx, st, "stateful")
	if len(before) != 1 {
		t.Fatalf("provider-state payloads = %d", len(before))
	}
	if _, err := st.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "stateful", WorkspaceID: "A", Title: "after"}); err != nil {
		t.Fatal(err)
	}
	if after := titleStatePayloads(t, ctx, st, "stateful"); !reflect.DeepEqual(after, before) {
		t.Fatal("rename changed provider-private bytes")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()
	secondClient := &titleStateModel{responses: []*einoschema.Message{einoschema.AssistantMessage("second answer", nil)}}
	discoveryTurn(t, ctx, titleStateRuntime(t, reopened, secondClient), "stateful", "A", "second question")
	if len(secondClient.inputs) != 1 {
		t.Fatalf("second model inputs = %d", len(secondClient.inputs))
	}
	var restored []json.RawMessage
	for _, message := range secondClient.inputs[0] {
		if items, ok := message.Extra[titleStateKey].([]json.RawMessage); ok {
			restored = items
		}
	}
	if len(restored) != 1 || !bytes.Equal(restored[0], titleStateSentinel) {
		t.Fatalf("restored provider state = %q", restored)
	}
	got, err := reopened.GetSession(ctx, "stateful")
	if err != nil || got.Title != "after" {
		t.Fatalf("reopened session = %#v, error = %v", got, err)
	}
}

func titleStatePayloads(t *testing.T, ctx context.Context, st session.Store, id session.ID) [][]byte {
	t.Helper()
	batch, err := history.LoadBatch(ctx, st, id)
	if err != nil {
		t.Fatal(err)
	}
	var payloads [][]byte
	for _, part := range batch.Parts {
		if part.Kind == session.PartProviderState {
			payloads = append(payloads, append([]byte(nil), part.Payload...))
		}
	}
	return payloads
}
