package einotools

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-tools/fileops"
	"github.com/mattsp1290/eino-tools/result"
	"github.com/mattsp1290/eino-tools/urlfetch"

	"github.com/mattsp1290/eino-agent/internal/workspace"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func TestRegisterDefaultsMaterializesLeafTools(t *testing.T) {
	t.Parallel()

	registry := agenttools.NewRegistry()
	registrations, err := RegisterDefaults(context.Background(), registry, Options{UserStdin: nil, UserStderr: nil})
	if err != nil {
		t.Fatalf("RegisterDefaults error = %v", err)
	}
	if len(registrations) != 10 {
		t.Fatalf("registered %d tools, want 10", len(registrations))
	}
	root := t.TempDir()
	canonicalRoot, err := workspace.CanonicalRoot(root)
	if err != nil {
		t.Fatalf("canonical root: %v", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot(root, "session-1"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	got := make([]string, 0, len(materialized))
	for _, tool := range materialized {
		got = append(got, tool.Name)
		if tool.Name == fileops.NameRead {
			if tool.Scope.Root != canonicalRoot {
				t.Fatalf("file_read root = %q, want %q", tool.Scope.Root, canonicalRoot)
			}
		}
	}
	sort.Strings(got)
	want := []string{"apply_patch", "file_edit", "file_list", "file_read", "file_write", "glob", "search", "shell", "url_fetch", "user_interact"}
	if !equalStrings(got, want) {
		t.Fatalf("materialized names = %#v, want %#v", got, want)
	}
}

func TestFileReadWrapperPreservesEinoToolsContract(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "hello.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	registry := agenttools.NewRegistry()
	if _, err := registerWorkspace(context.Background(), registry, workspaceSpec{
		name:    fileops.NameRead,
		factory: func(root string) (invokableTool, error) { return fileops.NewReadTool(root) },
		locker:  &workspace.Locker{},
	}); err != nil {
		t.Fatalf("register file_read: %v", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot(root, "session-1"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	normalized, err := materialized[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"path":"hello.txt"}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	out, err := materialized[0].Executor.Execute(context.Background(), runtime.ToolCall{Input: normalized})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	var decoded fileops.ReadResult
	if err := json.Unmarshal(out.Structured, &decoded); err != nil {
		t.Fatalf("unmarshal file_read result: %v", err)
	}
	if decoded.Outcome != result.OutcomeSucceeded || decoded.Content != "hello" {
		t.Fatalf("file_read result = %+v", decoded)
	}
	if int64(len(out.Output)) > materialized[0].Retention.MaxInlineBytes {
		t.Fatalf("output length %d exceeds retention %d", len(out.Output), materialized[0].Retention.MaxInlineBytes)
	}
}

func TestWorkspaceToolsSerializeSameCanonicalRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	locker := &workspace.Locker{}
	probe := newConcurrencyProbe()
	registry := agenttools.NewRegistry()
	if _, err := registerWorkspace(context.Background(), registry, workspaceSpec{
		name:    "probe",
		factory: probe.factory,
		locker:  locker,
	}); err != nil {
		t.Fatalf("register probe: %v", err)
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot(root, "session-1"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	startTool(t, materialized[0])
	probe.waitEnter(t)
	startTool(t, materialized[0])
	probe.assertNoEnter(t)
	probe.releaseOne()
	probe.waitEnter(t)
	probe.releaseOne()
	probe.waitDone(t, 2)
	if probe.maxActive() != 1 {
		t.Fatalf("max active = %d, want 1", probe.maxActive())
	}
}

func TestWorkspaceToolsAllowDifferentCanonicalRootsInParallel(t *testing.T) {
	t.Parallel()

	rootA := t.TempDir()
	rootB := t.TempDir()
	locker := &workspace.Locker{}
	probe := newConcurrencyProbe()
	registry := agenttools.NewRegistry()
	if _, err := registerWorkspace(context.Background(), registry, workspaceSpec{
		name:    "probe",
		factory: probe.factory,
		locker:  locker,
	}); err != nil {
		t.Fatalf("register probe: %v", err)
	}
	toolA, err := registry.ResolveTools(context.Background(), snapshot(rootA, "session-a"))
	if err != nil {
		t.Fatalf("ResolveTools A error = %v", err)
	}
	toolB, err := registry.ResolveTools(context.Background(), snapshot(rootB, "session-b"))
	if err != nil {
		t.Fatalf("ResolveTools B error = %v", err)
	}
	startTool(t, toolA[0])
	probe.waitEnter(t)
	startTool(t, toolB[0])
	probe.waitEnter(t)
	if probe.maxActive() != 2 {
		t.Fatalf("max active = %d, want 2", probe.maxActive())
	}
	probe.releaseOne()
	probe.releaseOne()
	probe.waitDone(t, 2)
}

func TestWorkspaceToolsSerializeSymlinkAliasRoots(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	parent := t.TempDir()
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatalf("symlink fixture: %v", err)
	}
	locker := &workspace.Locker{}
	probe := newConcurrencyProbe()
	registry := agenttools.NewRegistry()
	if _, err := registerWorkspace(context.Background(), registry, workspaceSpec{
		name:    "probe",
		factory: probe.factory,
		locker:  locker,
	}); err != nil {
		t.Fatalf("register probe: %v", err)
	}
	realTools, err := registry.ResolveTools(context.Background(), snapshot(root, "session-real"))
	if err != nil {
		t.Fatalf("ResolveTools real error = %v", err)
	}
	aliasTools, err := registry.ResolveTools(context.Background(), snapshot(alias, "session-alias"))
	if err != nil {
		t.Fatalf("ResolveTools alias error = %v", err)
	}
	startTool(t, realTools[0])
	probe.waitEnter(t)
	startTool(t, aliasTools[0])
	probe.assertNoEnter(t)
	probe.releaseOne()
	probe.waitEnter(t)
	probe.releaseOne()
	probe.waitDone(t, 2)
	if probe.maxActive() != 1 {
		t.Fatalf("max active = %d, want 1", probe.maxActive())
	}
}

type concurrencyProbe struct {
	enter   chan struct{}
	release chan struct{}
	done    chan struct{}

	mu     sync.Mutex
	active int
	max    int
}

func newConcurrencyProbe() *concurrencyProbe {
	return &concurrencyProbe{
		enter:   make(chan struct{}, 4),
		release: make(chan struct{}, 4),
		done:    make(chan struct{}, 4),
	}
}

func (p *concurrencyProbe) factory(string) (invokableTool, error) {
	return fakeInvokable{run: p.run}, nil
}

func (p *concurrencyProbe) run(context.Context, string) (string, error) {
	p.mu.Lock()
	p.active++
	if p.active > p.max {
		p.max = p.active
	}
	p.mu.Unlock()
	p.enter <- struct{}{}
	<-p.release
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
	p.done <- struct{}{}
	return `{"outcome":"succeeded"}`, nil
}

func (p *concurrencyProbe) waitEnter(t *testing.T) {
	t.Helper()
	select {
	case <-p.enter:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tool entry")
	}
}

func (p *concurrencyProbe) assertNoEnter(t *testing.T) {
	t.Helper()
	select {
	case <-p.enter:
		t.Fatal("tool entered while same-root execution should be serialized")
	case <-time.After(50 * time.Millisecond):
	}
}

func (p *concurrencyProbe) releaseOne() {
	p.release <- struct{}{}
}

func (p *concurrencyProbe) waitDone(t *testing.T, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-p.done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for tool completion")
		}
	}
}

func (p *concurrencyProbe) maxActive() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.max
}

type fakeInvokable struct {
	run func(context.Context, string) (string, error)
}

func (f fakeInvokable) Info(context.Context) (*einoschema.ToolInfo, error) {
	return &einoschema.ToolInfo{Name: "probe", Desc: "probe"}, nil
}

func (f fakeInvokable) InvokableRun(ctx context.Context, input string, _ ...tool.Option) (string, error) {
	return f.run(ctx, input)
}

func startTool(t *testing.T, tool runtime.Tool) {
	t.Helper()
	go func() {
		_, err := tool.Executor.Execute(context.Background(), runtime.ToolCall{Input: json.RawMessage(`{}`)})
		if err != nil {
			t.Errorf("Execute error = %v", err)
		}
	}()
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRegisterDefaultsThreadsURLFetchHTTPClient(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "from-injected-client")
	}))
	defer srv.Close()

	hit := false
	client := srv.Client() // trusts the test server's TLS cert
	base := client.Transport
	client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		hit = true
		return base.RoundTrip(req)
	})

	registry := agenttools.NewRegistry()
	if _, err := RegisterDefaults(context.Background(), registry, Options{
		URLFetchOptions: &urlfetch.Options{HTTPClient: client},
	}); err != nil {
		t.Fatalf("RegisterDefaults error = %v", err)
	}

	materialized, err := registry.ResolveTools(context.Background(), snapshot(t.TempDir(), "session-urlfetch"))
	if err != nil {
		t.Fatalf("ResolveTools error = %v", err)
	}
	var fetch *runtime.Tool
	for i := range materialized {
		if materialized[i].Name == urlfetch.Name {
			fetch = &materialized[i]
			break
		}
	}
	if fetch == nil {
		t.Fatalf("url_fetch not materialized; got %d tools", len(materialized))
	}

	input := []byte(`{"url":` + strconv.Quote(srv.URL) + `}`)
	normalized, err := fetch.InputDecoder.DecodeToolInput(context.Background(), input)
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	out, err := fetch.Executor.Execute(context.Background(), runtime.ToolCall{Input: normalized})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}

	var decoded urlfetch.Result
	if err := json.Unmarshal(out.Structured, &decoded); err != nil {
		t.Fatalf("unmarshal url_fetch result: %v", err)
	}
	if decoded.Outcome != result.OutcomeSucceeded {
		t.Fatalf("url_fetch outcome = %q, want succeeded (error=%+v)", decoded.Outcome, decoded.Error)
	}
	if decoded.Content != "from-injected-client" {
		t.Fatalf("url_fetch content = %q, want body served by injected client", decoded.Content)
	}
	if !hit {
		t.Fatal("injected HTTPClient was not used by url_fetch")
	}
}

func TestRegisterDefaultsNilURLFetchOptionsRegisters(t *testing.T) {
	t.Parallel()

	registry := agenttools.NewRegistry()
	registrations, err := RegisterDefaults(context.Background(), registry, Options{})
	if err != nil {
		t.Fatalf("RegisterDefaults error = %v", err)
	}
	found := false
	for _, reg := range registrations {
		if reg.Name == urlfetch.Name {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("url_fetch not registered with nil URLFetchOptions")
	}
}

func snapshot(root string, id session.ID) runtime.ToolScopeContext {
	return runtime.ToolScopeContext{
		SessionID: id, WorkspaceID: "workspace-" + string(id), WorkspaceRoot: root,
	}
}

func equalStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
