package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
	"github.com/mattsp1290/eino-agent/transport"
	"github.com/mattsp1290/eino-agent/watch"
)

func consumerWatchOptions() watch.Options {
	return watch.Options{Snapshot: session.ObservationLimits{MaxMessages: 20, MaxTools: 10, MaxParts: 30, MaxSnapshotBytes: 1 << 20, MaxTextBytes: 10000}, PollInterval: 5 * time.Millisecond, ReadTimeout: time.Second, MaxSubscriptions: 10, MaxWatchedSessions: 10, MaxLiveRuns: 10, MaxLiveTextBytes: 10000, PendingUpdates: 10}
}

type watchScript struct {
	partial, release, toolStarted, toolRelease chan struct{}
	cancelStarted                              chan struct{}
	partialOnce, toolOnce                      sync.Once
	toolCalls                                  atomic.Int32
	cancelRun                                  atomic.Bool
}

func (m *watchScript) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	go func() {
		defer writer.Close()
		if m.cancelRun.Load() {
			close(m.cancelStarted)
			<-ctx.Done()
			writer.Send(model.StreamDelta{}, ctx.Err())
			return
		}
		for i := len(request.Messages) - 1; i >= 0; i-- {
			if request.Messages[i].Role == einoschema.User {
				break
			}
			if request.Messages[i].Role == einoschema.Tool {
				writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("durable final", nil)}, nil)
				return
			}
		}
		writer.Send(model.StreamDelta{Message: &einoschema.Message{Role: einoschema.Assistant, Content: "paused prefix", ReasoningContent: "PRIVATE_REASONING"}}, nil)
		m.partialOnce.Do(func() { close(m.partial) })
		select {
		case <-ctx.Done():
			return
		case <-m.release:
		}
		writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "watch-call", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"text":"PRIVATE_ARGUMENT"}`}}})}, nil)
	}()
	return reader, nil
}

type watchEcho struct {
	Text string `json:"text"`
}

func mountWatchTool(t *testing.T, m *watchScript) (*composition.Registry, *composition.Mount) {
	t.Helper()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(t.Context(), extension.Component{InstanceID: "watch-echo", Artifact: extension.Artifact{Name: "watch-echo", Version: "v1", Hash: "watch-echo-v1", ConfigHash: "default", SourceKind: extension.SourceNative}}, composition.InstallerFunc(func(_ context.Context, r *composition.Registrar) error {
		return r.Tool(composition.ToolRegistration{ID: "echo", Scope: extension.GlobalScope(), Definition: tools.Definition{
			Name: "echo", Description: "Safe native fixture echo.",
			Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"text": {Type: einoschema.String, Required: true}}),
			Execute: tools.TypedExecutor[watchEcho, watchEcho](func(ctx context.Context, e tools.TypedExecution[watchEcho]) (watchEcho, error) {
				m.toolCalls.Add(1)
				m.toolOnce.Do(func() { close(m.toolStarted) })
				select {
				case <-ctx.Done():
					return watchEcho{}, ctx.Err()
				case <-m.toolRelease:
					return watchEcho{Text: "PRIVATE_RESULT"}, nil
				}
			}),
		}})
	}))
	if err != nil {
		t.Fatal(err)
	}
	return registry, mount
}
func watchRead(t *testing.T, sub *watch.Subscription, predicate func(watch.Update) bool) watch.Update {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var seq uint64
	for {
		u, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if u.DeliverySequence <= seq {
			t.Fatal("delivery regression")
		}
		seq = u.DeliverySequence
		raw, _ := json.Marshal(u)
		if strings.Contains(string(raw), "PRIVATE_") {
			t.Fatal("private data crossed observation")
		}
		if predicate(u) {
			return u
		}
	}
}
func watchTerminal(t *testing.T, sub *watch.Subscription, id session.RunID) session.ObservationSnapshot {
	return watchRead(t, sub, func(u watch.Update) bool {
		if u.Kind != watch.Durable {
			return false
		}
		for _, r := range u.Snapshot.Runs {
			if r.ID == id {
				return r.Terminal()
			}
		}
		return false
	}).Snapshot
}

type blockedWatchSink struct {
	entered, release chan struct{}
	once             sync.Once
}

func (s *blockedWatchSink) Emit(context.Context, session.EventRecord) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
}

func TestPublicSessionWatchConstructionExecutionAndReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "watch.db")
	store, storePool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storePool.Close() }()
	service, err := watch.NewService(store, consumerWatchOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.Close(context.Background()) }()
	httpServer := httptest.NewServer(transport.SessionWatchHandler(transport.SessionWatchConfig{
		Service: service, WriteTimeout: time.Second,
		Auth:    func(ctx context.Context, _ *http.Request) (context.Context, error) { return ctx, nil },
		Session: func(*http.Request) (session.ID, error) { return "watch-session", nil },
	}))
	defer httpServer.Close()
	script := &watchScript{partial: make(chan struct{}), release: make(chan struct{}), toolStarted: make(chan struct{}), toolRelease: make(chan struct{})}
	registry, mount := mountWatchTool(t, script)
	defer func() { mount.Deactivate(); _ = mount.Close(context.Background()) }()
	sink := &blockedWatchSink{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(sink.release)
	orchestrator, err := runtime.NewStreamingOrchestrator(runtime.WithStore(store), runtime.WithModelResolver(delegatedModelResolver{script}), runtime.WithIDGenerator(&delegatedSearchIDs{}), runtime.WithRunPlanProvider(registry), runtime.WithSessionObserver(service), runtime.WithEventSink(sink), runtime.WithQueueSize(1))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := service.Watch(ctx, "watch-session")
	if err != nil {
		t.Fatal(err)
	}
	if sub.Initial().Exists {
		t.Fatal("attach created session")
	}
	config := delegatedRuntimeConfig()
	config.Agent.SystemPrompt = "PRIVATE_SYSTEM"
	config.Metadata["secret"] = "PRIVATE_METADATA"
	admission, err := orchestrator.Start(ctx, runtime.Request{SessionID: "watch-session", Message: runtime.UserMessage{Content: "new submission"}, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-script.partial:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-sink.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	watchRead(t, sub, func(u watch.Update) bool { return u.Kind == watch.Live && u.Live.Text == "paused prefix" })
	second, err := service.Watch(ctx, "watch-session")
	if err != nil {
		t.Fatal(err)
	}
	live := watchRead(t, second, func(u watch.Update) bool { return u.Kind == watch.Live && u.Live.Text == "paused prefix" })
	httpLive := watchHTTPMessages(t, httpServer.URL, "paused prefix")
	if httpLive[len(httpLive)-1].ID != string(live.Live.Identity.MessageID) {
		t.Fatal("HTTP changed live durable message identity")
	}
	if _, err = second.Resnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	again := watchRead(t, second, func(u watch.Update) bool { return u.Kind == watch.Live })
	if again.Live.Identity != live.Live.Identity || again.Live.Text != live.Live.Text {
		t.Fatal("resnapshot lost paused text")
	}
	sub.Close()
	close(script.release)
	select {
	case <-script.toolStarted:
	case result := <-admission.Handle.Done():
		t.Fatalf("run finished before tool: %+v", result)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Detachment has left the native tool independently running.
	select {
	case <-admission.Handle.Done():
		t.Fatal("run ended while tool paused")
	default:
	}
	close(script.toolRelease)
	select {
	case result := <-admission.Handle.Done():
		if result.Status != session.RunCompleted {
			t.Fatal(result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	final := watchTerminal(t, second, admission.Handle.RunID())
	if final.Messages[len(final.Messages)-1].Text != "durable final" || !final.Messages[len(final.Messages)-1].Finalized || len(final.Tools) != 1 || final.Tools[0].Status != session.ToolCallCompleted || script.toolCalls.Load() != 1 {
		t.Fatal(final)
	}
	httpFinal := watchHTTPMessages(t, httpServer.URL, "durable final")
	if httpFinal[1].ID != httpLive[1].ID {
		t.Fatal("HTTP reconnect changed durable identity")
	}
	second.Close()
	// Observe interruption through the same public runtime while its native sink
	// remains blocked. Done and durable settlement cannot wait for that sink.
	script.cancelStarted = make(chan struct{})
	script.cancelRun.Store(true)
	interrupted, err := orchestrator.Start(ctx, runtime.Request{SessionID: "watch-session", Message: runtime.UserMessage{Content: "interrupt next"}, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	observer, err := service.Watch(ctx, "watch-session")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-script.cancelStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = interrupted.Handle.Interrupt(ctx, "fixture"); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-interrupted.Handle.Done():
		if result.Status != session.RunInterrupted {
			t.Fatal(result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	terminal := watchTerminal(t, observer, interrupted.Handle.RunID())
	observer.Close()
	mount.Deactivate()
	if err = mount.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = storePool.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()
	persisted, err := reopened.ReadObservationSnapshot(ctx, "watch-session", consumerWatchOptions().Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(terminal, persisted) {
		t.Fatal("reopen changed durable identity or state")
	}
	fresh, err := watch.NewService(reopened, consumerWatchOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fresh.Close(context.Background()) }()
	attached, err := fresh.Watch(ctx, "watch-session")
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Close()
	if !reflect.DeepEqual(attached.Initial(), persisted) {
		t.Fatal("fresh service changed snapshot")
	}
}
func TestPublicWatchOverflowAndReattach(t *testing.T) {
	st, stPool, err := openTestSQLite(t.Context(), filepath.Join(t.TempDir(), "overflow.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stPool.Close() }()
	options := consumerWatchOptions()
	options.PendingUpdates = 1
	service, err := watch.NewService(st, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.Close(context.Background()) }()
	sub, err := service.Watch(t.Context(), "overflow")
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.CreateSession(t.Context(), session.Session{ID: "overflow"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := st.AdmitRun(t.Context(), session.Run{ID: "r", SessionID: "overflow", Status: session.RunRunning, ClaimToken: "f"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Execution(session.RunFence{RunID: r.ID, ClaimToken: r.ClaimToken}).AppendMessage(t.Context(), session.Message{ID: "m", SessionID: r.SessionID, RunID: r.ID, Role: session.RoleAssistant})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		_, err = sub.Next(ctx)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, watch.ErrResyncRequired) {
		t.Fatal(err)
	}
	recovered, err := service.Watch(ctx, "overflow")
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if len(recovered.Initial().Messages) != 1 {
		t.Fatal("reattach lost state")
	}
	u, err := recovered.Next(ctx)
	if err != nil || u.Kind != watch.LiveUnavailable {
		t.Fatal(u, err)
	}
}

func TestPublicWatchStrictResumeDoesNotDuplicateTool(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, stPool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "resume.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stPool.Close() }()
	script := &watchScript{toolStarted: make(chan struct{}), toolRelease: make(chan struct{})}
	close(script.toolRelease)
	registry, mount := mountWatchTool(t, script)
	defer func() { mount.Deactivate(); _ = mount.Close(context.Background()) }()
	config := delegatedRuntimeConfig()
	plan, err := registry.AcquireRunPlan(ctx, runtime.RunPlanRequest{SessionID: "resume", Config: config})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	plan.Release()
	if _, err = st.CreateSession(ctx, session.Session{ID: "resume"}); err != nil {
		t.Fatal(err)
	}
	r, err := st.AdmitRun(ctx, session.Run{ID: "resume-run", SessionID: "resume", OwnerID: "old", ClaimToken: "old-fence", Status: session.RunRunning, Agent: config.Agent.Name, ProviderID: "fixture", ModelID: "scripted", Config: config.Metadata, ExtensionPlan: descriptor}, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	ex := st.Execution(session.RunFence{RunID: r.ID, ClaimToken: r.ClaimToken})
	m := session.Message{ID: "assistant", SessionID: r.SessionID, RunID: r.ID, Role: session.RoleAssistant}
	if _, err = ex.AppendMessage(ctx, m); err != nil {
		t.Fatal(err)
	}
	call := session.ToolCall{ID: "resume-call", SessionID: r.SessionID, RunID: r.ID, MessageID: m.ID, RequestPartID: "request", ResultMessageID: "result-message", ResultPartID: "result-part", Name: "echo", Pattern: "echo", Input: []byte(`{"text":"PRIVATE_ARGUMENT"}`), Status: session.ToolCallPending}
	if _, err = ex.CreateToolCall(ctx, session.CreateToolCallRequest{Call: call, RequestPart: session.Part{ID: call.RequestPartID, MessageID: m.ID, SessionID: r.SessionID, RunID: r.ID, Kind: session.PartToolCall, Payload: []byte(`{"id":"resume-call","name":"echo","arguments":{"text":"PRIVATE_ARGUMENT"}}`)}, Event: session.ToolTransitionEvent{ID: "pending-event", CreatedAt: time.Now()}}); err != nil {
		t.Fatal(err)
	}
	if err = ex.FinalizeAssistantMessage(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	service, err := watch.NewService(st, consumerWatchOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.Close(context.Background()) }()
	observer, err := service.Watch(ctx, r.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	if script.toolCalls.Load() != 0 {
		t.Fatal("attachment executed tool")
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(runtime.WithStore(st), runtime.WithModelResolver(delegatedModelResolver{script}), runtime.WithIDGenerator(&delegatedSearchIDs{}), runtime.WithRunPlanProvider(registry), runtime.WithSessionObserver(service))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := orchestrator.Resume(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-handle.Done():
		if result.Status != session.RunInterrupted || result.Error != nil {
			t.Fatal(result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	final := watchTerminal(t, observer, r.ID)
	if len(final.Tools) != 1 || final.Tools[0].Status != session.ToolCallCompleted || script.toolCalls.Load() != 1 {
		t.Fatal(final)
	}
	if err = ex.FinalizeAssistantMessage(ctx, m.ID); err == nil {
		t.Fatal("stale fence survived resume")
	}
	repeated, err := orchestrator.Resume(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	<-repeated.Done()
	if script.toolCalls.Load() != 1 {
		t.Fatal("duplicate tool on terminal resume")
	}
}

type watchHTTPMessage struct{ ID, Role, Content string }

func watchHTTPMessages(t *testing.T, url, content string) []watchHTTPMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Last-Event-ID", "ignored-historical-cursor")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatal(response.Status)
	}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "PRIVATE_") {
			t.Fatal("private data crossed HTTP observation")
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var frame struct {
			Type     string
			Messages []watchHTTPMessage
		}
		if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Type != "MESSAGES_SNAPSHOT" {
			continue
		}
		for _, m := range frame.Messages {
			if m.Content == content {
				return frame.Messages
			}
		}
	}
	t.Fatal("HTTP did not deliver current text", scanner.Err())
	return nil
}
