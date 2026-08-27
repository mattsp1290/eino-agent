//go:build unix

package einotools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-tools/catalog"
	"github.com/mattsp1290/eino-tools/fileops"
	"github.com/mattsp1290/eino-tools/result"
	"github.com/mattsp1290/eino-tools/tracker"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func TestMountStandardPublishesCatalogOrderAndExecutesFileRead(t *testing.T) {
	registry := composition.NewRegistry(nil)
	component := standardComponent("standard")
	mount, err := MountStandard(context.Background(), registry, component, Options{Scope: extension.GlobalScope()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	root := canonicalTempDir(t)
	if err := os.WriteFile(filepath.Join(root, "hello.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{WorkspaceID: "workspace", WorkspaceRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(resolved))
	for index := range resolved {
		got[index] = resolved[index].Name
	}
	want := []string{"file_read", "file_write", "file_edit", "file_list", "glob", "search", "apply_patch", "shell", "url_fetch", "user_interact"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tool order = %#v, want %#v", got, want)
	}

	input, err := resolved[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"path":"dir/../hello.txt"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(input) != `{"path":"hello.txt"}` {
		t.Fatalf("normalized input = %s", input)
	}
	pattern, err := resolved[0].Pattern.ResolvePermissionPattern(context.Background(), input)
	if err != nil || pattern != "hello.txt" {
		t.Fatalf("pattern = %q, %v", pattern, err)
	}
	output, err := resolved[0].Executor.Execute(context.Background(), runtime.ToolCall{Input: input})
	if err != nil {
		t.Fatal(err)
	}
	var read fileops.ReadResult
	if err := json.Unmarshal(output.Structured, &read); err != nil {
		t.Fatal(err)
	}
	if read.Outcome != result.OutcomeSucceeded || read.Content != "hello" {
		t.Fatalf("read result = %+v", read)
	}

	descriptor := plan.Descriptor()
	if len(descriptor.Tools) != 10 {
		t.Fatalf("descriptor tools = %d", len(descriptor.Tools))
	}
	for _, identity := range descriptor.Tools {
		if len(identity.SchemaHash) != 64 || len(identity.ExecutorHash) != 64 || identity.Artifact.ConfigHash != component.Artifact.ConfigHash {
			t.Fatalf("invalid tool identity = %#v", identity)
		}
	}
}

func TestMountStandardRunsThroughOrchestratorAndDurableSettlement(t *testing.T) {
	ctx := context.Background()
	root := canonicalTempDir(t)
	if err := os.WriteFile(filepath.Join(root, "hello.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	plans := composition.NewRegistry(nil)
	mount, err := MountStandard(ctx, plans, standardComponent("runtime"), Options{
		Scope: extension.GlobalScope(),
		Permissions: map[string][]string{
			catalog.IDFileRead: {"workspace.read"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	wantOrder := []string{"file_read", "file_write", "file_edit", "file_list", "glob", "search", "apply_patch", "shell", "url_fetch", "user_interact"}
	streamer := &catalogRuntimeStreamer{wantOrder: wantOrder}
	var permissionRequest permissions.Request
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store),
		runtime.WithModelResolver(catalogModelResolver{streamer: streamer}),
		runtime.WithRunPlanProvider(plans),
		runtime.WithPermissions(permissions.PolicyFunc(func(_ context.Context, request permissions.Request) (permissions.Decision, error) {
			permissionRequest = request
			return permissions.Decision{Action: permissions.ActionAllow}, nil
		})),
		runtime.WithIDGenerator(&catalogSequenceIDs{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "fake", ModelID: "test"}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "catalog-runtime",
		Input:     []*einoschema.Message{einoschema.UserMessage("read the fixture")},
		Config: config.Snapshot{
			Agent: config.Agent{Name: "agent", Model: selection}, Model: selection,
			Metadata: map[string]string{"workspace_id": "workspace", "workspace_root": root},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("runtime result = %+v", result)
	}
	if streamer.err != nil {
		t.Fatal(streamer.err)
	}
	if permissionRequest.Permission != "workspace.read" || permissionRequest.Pattern != "hello.txt" || permissionRequest.ToolName != "file_read" {
		t.Fatalf("permission request = %+v", permissionRequest)
	}
	call, err := store.GetToolCall(ctx, "catalog-call")
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != session.ToolCallCompleted || call.Pattern != "hello.txt" || string(call.Input) != `{"path":"hello.txt"}` || !strings.Contains(string(call.Output), `"content":"hello"`) {
		t.Fatalf("durable tool call = %+v", call)
	}
}

func TestMountStandardTrackerAndMCPPending(t *testing.T) {
	registry := composition.NewRegistry(nil)
	writer := &closeWriter{}
	mount, err := MountStandard(context.Background(), registry, standardComponent("tracker"), Options{
		Scope: extension.GlobalScope(), Catalog: catalog.Options{TrackerWriter: writer},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	tools, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{WorkspaceRoot: canonicalTempDir(t)})
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 11 || tools[10].Name != "tracker_write" {
		t.Fatalf("tools = %#v", toolNames(tools))
	}
	trackerInput, err := tools[10].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"op":"close","id":"issue-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	trackerPattern, err := tools[10].Pattern.ResolvePermissionPattern(context.Background(), trackerInput)
	if err != nil || trackerPattern != "issue-1" {
		t.Fatalf("tracker pattern = %q, %v", trackerPattern, err)
	}
	if _, err := tools[10].Executor.Execute(context.Background(), runtime.ToolCall{Input: trackerInput}); err != nil || writer.calls != 1 {
		t.Fatalf("tracker execution = calls %d, err %v", writer.calls, err)
	}

	user := tools[9]
	userInput, err := user.InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"question":"Continue?"}`))
	if err != nil {
		t.Fatal(err)
	}
	pending, err := user.Executor.Execute(context.Background(), runtime.ToolCall{Input: userInput})
	if err != nil || !strings.Contains(pending.Output, `"outcome":"pending"`) {
		t.Fatalf("MCP pending = %s, %v", pending.Output, err)
	}
}

func TestMountStandardErrorsDoNotPublish(t *testing.T) {
	registry := composition.NewRegistry(nil)
	unsupported := errors.New("wrapped: " + catalog.ErrUnsupportedPlatform.Error())
	_, err := mountStandard(context.Background(), registry, standardComponent("unsupported"), Options{Scope: extension.GlobalScope()}, func(catalog.Options) ([]catalog.Definition, error) {
		return nil, errors.Join(unsupported, catalog.ErrUnsupportedPlatform)
	})
	if !errors.Is(err, catalog.ErrUnsupportedPlatform) {
		t.Fatalf("unsupported error = %v", err)
	}
	if diagnostics := registry.Diagnostics(); len(diagnostics.Tools) != 0 || len(diagnostics.Components) != 0 {
		t.Fatalf("published diagnostics = %#v", diagnostics)
	}

	_, err = MountStandard(context.Background(), registry, standardComponent("unknown-policy"), Options{
		Scope: extension.GlobalScope(), Permissions: map[string][]string{"standard.missing": {"read"}},
	})
	if !errors.Is(err, agenttools.ErrInvalidDefinition) {
		t.Fatalf("unknown permissions error = %v", err)
	}
	if diagnostics := registry.Diagnostics(); len(diagnostics.Tools) != 0 || len(diagnostics.Components) != 0 {
		t.Fatalf("published after translation error = %#v", diagnostics)
	}

	for _, test := range []struct {
		name   string
		mutate func([]catalog.Definition) []catalog.Definition
	}{
		{name: "duplicate catalog ID", mutate: func(definitions []catalog.Definition) []catalog.Definition {
			return append(definitions, definitions[0])
		}},
		{name: "unsupported catalog ID", mutate: func(definitions []catalog.Definition) []catalog.Definition {
			definitions[0].ID = "standard.future"
			return definitions
		}},
		{name: "invalid source identity", mutate: func(definitions []catalog.Definition) []catalog.Definition {
			definitions[0].SchemaHash = "invalid"
			return definitions
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := mountStandard(context.Background(), registry, standardComponent(test.name), Options{Scope: extension.GlobalScope()}, func(options catalog.Options) ([]catalog.Definition, error) {
				definitions, err := catalog.Standard(options)
				if err != nil {
					return nil, err
				}
				return test.mutate(definitions), nil
			})
			if err == nil {
				t.Fatal("expected mount error")
			}
			if diagnostics := registry.Diagnostics(); len(diagnostics.Tools) != 0 || len(diagnostics.Components) != 0 {
				t.Fatalf("published after %s = %#v", test.name, diagnostics)
			}
		})
	}
}

func TestPermissionPatternBoundariesAndPathValidation(t *testing.T) {
	tests := []struct {
		id      string
		input   string
		want    string
		wantErr bool
	}{
		{id: catalog.IDFileList, input: `{}`, want: "."},
		{id: catalog.IDFileRead, input: `{"path":"a/../secret/file"}`, want: "secret/file"},
		{id: catalog.IDFileRead, input: `{"path":"../secret"}`, wantErr: true},
		{id: catalog.IDFileRead, input: `{"path":"/secret"}`, wantErr: true},
		{id: catalog.IDGlob, input: `{"path":"","pattern":"*.go"}`, want: "*.go"},
		{id: catalog.IDShell, input: `{"cmd":"` + strings.Repeat("x", maxPermissionPatternBytes) + `"}`, want: strings.Repeat("x", maxPermissionPatternBytes)},
		{id: catalog.IDShell, input: `{"cmd":"` + strings.Repeat("x", maxPermissionPatternBytes+1) + `"}`, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.id+test.want, func(t *testing.T) {
			normalized, err := normalizeCatalogInput(test.id, json.RawMessage(test.input))
			if err == nil {
				_, err = permissionPattern(test.id, normalized)
			}
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			pattern, err := permissionPattern(test.id, normalized)
			if err != nil || pattern != test.want {
				t.Fatalf("pattern = %q, %v", pattern, err)
			}
		})
	}
}

func TestKeyedLockerSerializesAndReclaims(t *testing.T) {
	var locker keyedLocker
	entered := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	var active, maximum int
	var mu sync.Mutex
	call := func(key string) <-chan error {
		done := make(chan error, 1)
		go func() {
			done <- locker.Do(context.Background(), key, func() error {
				mu.Lock()
				active++
				if active > maximum {
					maximum = active
				}
				mu.Unlock()
				entered <- struct{}{}
				<-release
				mu.Lock()
				active--
				mu.Unlock()
				return nil
			})
		}()
		return done
	}
	first, second := call("same"), call("same")
	<-entered
	select {
	case <-entered:
		t.Fatal("same key overlapped")
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	<-entered
	release <- struct{}{}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if maximum != 1 || locker.idleEntries() != 0 {
		t.Fatalf("maximum=%d idle=%d", maximum, locker.idleEntries())
	}

	ctx, cancel := context.WithCancel(context.Background())
	block := call("cancel")
	<-entered
	waiter := make(chan error, 1)
	go func() { waiter <- locker.Do(ctx, "cancel", func() error { return nil }) }()
	cancel()
	if !errors.Is(<-waiter, context.Canceled) {
		t.Fatal("canceled waiter did not cancel")
	}
	release <- struct{}{}
	if err := <-block; err != nil {
		t.Fatal(err)
	}
	if locker.idleEntries() != 0 {
		t.Fatalf("idle entries = %d", locker.idleEntries())
	}
}

type closeWriter struct{ calls int }

var _ tracker.CloseWriter = (*closeWriter)(nil)

func (w *closeWriter) Close(context.Context, string, string) error {
	w.calls++
	return nil
}

func standardComponent(instance string) extension.Component {
	return extension.Component{InstanceID: instance, Artifact: extension.Artifact{
		Name: "eino-tools-standard", Version: "63a3c99", Hash: "agent-adapter-v1", ConfigHash: "catalog-options-v1", SourceKind: extension.SourceNative,
	}}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func toolNames(tools []runtime.Tool) []string {
	names := make([]string, len(tools))
	for index := range tools {
		names[index] = tools[index].Name
	}
	return names
}

type catalogModelResolver struct{ streamer model.Streamer }

func (r catalogModelResolver) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{
		Provider: model.Provider{ID: "fake"},
		Model:    model.Descriptor{ID: "test", ProviderID: "fake"},
		Streamer: r.streamer,
	}, nil
}

type catalogRuntimeStreamer struct {
	wantOrder []string
	turn      int
	err       error
}

func (s *catalogRuntimeStreamer) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	s.turn++
	var response *einoschema.Message
	if s.turn == 1 {
		got := make([]string, len(request.Tools))
		for index := range request.Tools {
			got[index] = request.Tools[index].Name
		}
		if !reflect.DeepEqual(got, s.wantOrder) {
			s.err = fmt.Errorf("provider tool order = %#v, want %#v", got, s.wantOrder)
			return nil, s.err
		}
		response = einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID: "catalog-call", Type: "function",
			Function: einoschema.FunctionCall{Name: "file_read", Arguments: `{"path":"dir/../hello.txt"}`},
		}})
	} else {
		response = einoschema.AssistantMessage("done", nil)
	}
	reader, writer := einoschema.Pipe[*einoschema.Message](1)
	_ = writer.Send(response, nil)
	writer.Close()
	return reader, nil
}

type catalogSequenceIDs struct {
	mu sync.Mutex
	n  int
}

func (s *catalogSequenceIDs) next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return prefix + "-" + strconv.Itoa(s.n)
}

func (s *catalogSequenceIDs) NewRunID() session.RunID { return session.RunID(s.next("run")) }
func (s *catalogSequenceIDs) NewMessageID() session.MessageID {
	return session.MessageID(s.next("message"))
}
func (s *catalogSequenceIDs) NewPartID() session.PartID { return session.PartID(s.next("part")) }
func (s *catalogSequenceIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next("tool-call"))
}
func (s *catalogSequenceIDs) NewEventID() session.EventID { return session.EventID(s.next("event")) }
func (s *catalogSequenceIDs) NewEpochID() session.EpochID { return session.EpochID(s.next("epoch")) }
