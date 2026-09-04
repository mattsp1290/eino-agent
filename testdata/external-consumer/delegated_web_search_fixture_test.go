package consumer

// This fixture is intentionally private and non-normative. The canonical
// production web_search schema and host adapter belong to eino-agent-extensions.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/sqlite"
	"github.com/mattsp1290/eino-agent/tools"
)

const (
	delegatedToolName       = "web_search"
	delegatedPermissionName = "network.web.search"
	delegatedPattern        = "web_search"
	delegatedQueryMaxBytes  = 32
	delegatedResultMax      = 2
	delegatedTitleMaxBytes  = 12
	delegatedURLMaxBytes    = 64
	delegatedSnippetMax     = 24
	delegatedFailureClass   = "search backend unavailable"
)

var errDelegatedBackendUnavailable = errors.New(delegatedFailureClass)

type delegatedSearchRequest struct {
	Query string `json:"query"`
}

type delegatedSearchRecord struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet"`
}

type delegatedSearchResult struct {
	Results []delegatedSearchRecord `json:"results"`
}

type delegatedSearchFixture interface {
	Search(context.Context, string) ([]delegatedSearchRecord, error)
}

type delegatedSearchFunc func(context.Context, string) ([]delegatedSearchRecord, error)

func (fn delegatedSearchFunc) Search(ctx context.Context, query string) ([]delegatedSearchRecord, error) {
	return fn(ctx, query)
}

func TestDelegatedWebSearchUsesPublishedRuntimeContract(t *testing.T) {
	t.Run("bounded durable execution and quiescent lifecycle", testDelegatedSearchExecution)
	t.Run("layered malformed input", testDelegatedSearchValidation)
	t.Run("cancellation reaches host once", testDelegatedSearchCancellation)
	t.Run("backend diagnostics stay private", testDelegatedSearchFailureRedaction)
	t.Run("equivalent identity and strict drift", testDelegatedSearchPlanIdentity)
}

func testDelegatedSearchExecution(t *testing.T) {
	ctx := context.Background()
	var backendCalls atomic.Int32
	searcher := delegatedSearchFunc(func(_ context.Context, query string) ([]delegatedSearchRecord, error) {
		backendCalls.Add(1)
		if query != "bounded test" {
			t.Fatalf("backend query = %q", query)
		}
		return oversizedDelegatedRecords(), nil
	})
	component := delegatedComponent("artifact-hash-v1", "config-hash-v1")
	registry, mount := mountDelegatedSearchFixture(t, component, searcher)

	frozen, err := registry.AcquireRunPlan(ctx, runtime.RunPlanRequest{SessionID: "delegated-success"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := frozen.Descriptor()
	if descriptor.Fingerprint == "" {
		t.Fatal("frozen plan has no fingerprint")
	}
	toolsFromPlan, err := frozen.ResolveTools(ctx, runtime.ToolScopeContext{SessionID: "delegated-success"})
	if err != nil || len(toolsFromPlan) != 1 || toolsFromPlan[0].Name != delegatedToolName {
		t.Fatalf("frozen plan tools = %#v, err = %v", toolsFromPlan, err)
	}
	frozen.Release()

	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	modelFixture := &delegatedSearchModel{callID: "delegated-success-call"}
	policy := &recordingDelegatedPolicy{}
	events := &recordingDelegatedEvents{}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store),
		runtime.WithModelResolver(delegatedModelResolver{streamer: modelFixture}),
		runtime.WithRunPlanProvider(registry),
		runtime.WithPermissions(policy),
		runtime.WithEventSink(events),
		runtime.WithIDGenerator(&delegatedSearchIDs{}),
		runtime.WithClock(func() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) }),
		runtime.WithOwnerID("external-consumer-proof"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := orchestrator.Start(ctx, runtime.Request{
		SessionID: "delegated-success",
		Message:   runtime.UserMessage{Content: "search once"},
		Config:    delegatedRuntimeConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := waitDelegatedResult(t, handle)
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("run result = %+v", result)
	}
	if backendCalls.Load() != 1 {
		t.Fatalf("backend calls = %d, want 1", backendCalls.Load())
	}

	call, err := store.GetToolCall(ctx, "delegated-success-call")
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != session.ToolCallCompleted || call.Name != delegatedToolName || call.Pattern != delegatedPattern || call.RetrySafe {
		t.Fatalf("durable call = %+v", call)
	}
	if string(call.Input) != `{"query":"bounded test"}` {
		t.Fatalf("canonical input = %s", call.Input)
	}

	requests := policy.snapshot()
	if len(requests) != 1 || requests[0].ToolName != delegatedToolName || requests[0].Permission != delegatedPermissionName || requests[0].Pattern != delegatedPattern {
		t.Fatalf("permission requests = %#v", requests)
	}

	var output runtime.ToolOutput
	if err := json.Unmarshal(call.Output, &output); err != nil {
		t.Fatal(err)
	}
	if output.Status != "completed" || output.Truncated || output.External || output.Redacted {
		t.Fatalf("bounded output = %+v", output)
	}
	encodedResult := []byte(output.Content)
	if !bytes.Equal(output.Structured, encodedResult) {
		t.Fatalf("structured copy = %s, content = %s", output.Structured, encodedResult)
	}
	if output.InlineSize != 2*int64(len(encodedResult)) || output.OriginalSize != output.InlineSize {
		t.Fatalf("output sizes = original %d inline %d encoded %d", output.OriginalSize, output.InlineSize, len(encodedResult))
	}
	worstCase, err := maximumDelegatedResultBytes()
	if err != nil {
		t.Fatal(err)
	}
	if toolsFromPlan[0].Retention.MaxInlineBytes < 2*worstCase || toolsFromPlan[0].Retention.MaxInlineBytes < output.InlineSize || toolsFromPlan[0].Retention.StoreExternal || toolsFromPlan[0].Retention.Redact {
		t.Fatalf("retention = %+v, worst-case result bytes = %d", toolsFromPlan[0].Retention, worstCase)
	}

	var bounded delegatedSearchResult
	if err := json.Unmarshal(output.Structured, &bounded); err != nil {
		t.Fatal(err)
	}
	if len(bounded.Results) != delegatedResultMax {
		t.Fatalf("result count = %d", len(bounded.Results))
	}
	for _, record := range bounded.Results {
		if !utf8.ValidString(record.Title) || !utf8.ValidString(record.URL) || !utf8.ValidString(record.Snippet) ||
			len(record.Title) > delegatedTitleMaxBytes || len(record.URL) > delegatedURLMaxBytes || len(record.Snippet) > delegatedSnippetMax {
			t.Fatalf("unbounded record = %#v", record)
		}
		parsed, parseErr := url.Parse(record.URL)
		if parseErr != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			t.Fatalf("invalid source URL %q: %v", record.URL, parseErr)
		}
		raw, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(raw, &keys); err != nil || len(keys) != 3 || keys["title"] == nil || keys["url"] == nil || keys["snippet"] == nil {
			t.Fatalf("record fields = %s, err = %v", raw, err)
		}
	}
	for _, forbidden := range []string{"credential", "provider_name", "diagnostic", "generated_answer"} {
		if strings.Contains(string(output.Structured), forbidden) {
			t.Fatalf("result contains forbidden field %q: %s", forbidden, output.Structured)
		}
	}
	if got := modelFixture.toolResult(); got != string(call.Output) {
		t.Fatalf("model observed tool result %q, want %q", got, call.Output)
	}

	replay, err := store.ListMessages(ctx, "delegated-success", session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var foundResult bool
	for _, part := range replay.Parts {
		if part.ID == call.ResultPartID {
			foundResult = part.Kind == session.PartToolResult && bytes.Equal(part.Payload, call.Output)
		}
	}
	if !foundResult {
		t.Fatalf("durable result part missing from replay: %#v", replay.Parts)
	}

	lifecyclePlan, err := registry.AcquireRunPlan(ctx, runtime.RunPlanRequest{SessionID: "delegated-lifecycle"})
	if err != nil {
		t.Fatal(err)
	}
	mount.Deactivate()
	laterPlan, err := registry.AcquireRunPlan(ctx, runtime.RunPlanRequest{SessionID: "delegated-lifecycle"})
	if err != nil {
		lifecyclePlan.Release()
		t.Fatal(err)
	}
	laterTools, err := laterPlan.ResolveTools(ctx, runtime.ToolScopeContext{SessionID: "delegated-lifecycle"})
	if err != nil || len(laterTools) != 0 {
		t.Fatalf("post-deactivation tools = %#v, err = %v", laterTools, err)
	}
	retainedTools, err := lifecyclePlan.ResolveTools(ctx, runtime.ToolScopeContext{SessionID: "delegated-lifecycle"})
	if err != nil || len(retainedTools) != 1 || retainedTools[0].Name != delegatedToolName {
		t.Fatalf("retained lifecycle tools = %#v, err = %v", retainedTools, err)
	}
	laterPlan.Release()
	lifecyclePlan.Release()
	closeDelegatedMount(t, mount)
}

func testDelegatedSearchValidation(t *testing.T) {
	var backendCalls atomic.Int32
	registry, mount := mountDelegatedSearchFixture(t, delegatedComponent("artifact-validation", "config-validation"), delegatedSearchFunc(func(context.Context, string) ([]delegatedSearchRecord, error) {
		backendCalls.Add(1)
		return nil, nil
	}))
	defer closeDelegatedMount(t, mount)
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "validation-session"})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "validation-session"})
	if err != nil || len(resolved) != 1 {
		t.Fatalf("resolved tools = %#v, err = %v", resolved, err)
	}
	decoder := resolved[0].InputDecoder
	targetOwned := []json.RawMessage{
		json.RawMessage(`{`),
		json.RawMessage(`null`),
		json.RawMessage(`[]`),
		json.RawMessage(`{"query":"one","query":"two"}`),
		json.RawMessage(`{"query":"one"} trailing`),
	}
	for _, raw := range targetOwned {
		if _, err := decoder.DecodeToolInput(context.Background(), raw); !errors.Is(err, tools.ErrMalformedInput) {
			t.Errorf("target-owned decode %q = %v", raw, err)
		}
	}
	invalidUTF8 := json.RawMessage([]byte{'{', '"', 'q', 'u', 'e', 'r', 'y', '"', ':', '"', 0xff, '"', '}'})
	canonicalUTF8, err := decoder.DecodeToolInput(context.Background(), invalidUTF8)
	if err != nil || !utf8.Valid(canonicalUTF8) || bytes.Contains(canonicalUTF8, []byte{0xff}) {
		t.Fatalf("invalid original UTF-8 remained observable: %q, err = %v", canonicalUTF8, err)
	}
	extensionOwned := []json.RawMessage{
		json.RawMessage(`{"query":"ok","unknown":true}`),
		json.RawMessage(`{"query":"bad\u0000query"}`),
		json.RawMessage(`{"query":"   "}`),
		json.RawMessage(fmt.Sprintf(`{"query":%q}`, strings.Repeat("q", delegatedQueryMaxBytes+1))),
	}
	for _, raw := range extensionOwned {
		if _, err := decoder.DecodeToolInput(context.Background(), raw); !errors.Is(err, tools.ErrMalformedInput) {
			t.Errorf("extension-owned decode %q = %v", raw, err)
		}
	}
	canonical, err := decoder.DecodeToolInput(context.Background(), json.RawMessage(` { "query" : "ok" } `))
	if err != nil || string(canonical) != `{"query":"ok"}` {
		t.Fatalf("valid canonical input = %s, err = %v", canonical, err)
	}
	if backendCalls.Load() != 0 {
		t.Fatalf("validation invoked backend %d times", backendCalls.Load())
	}
}

func testDelegatedSearchCancellation(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	searcher := delegatedSearchFunc(func(ctx context.Context, _ string) ([]delegatedSearchRecord, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	})
	registry, mount := mountDelegatedSearchFixture(t, delegatedComponent("artifact-cancel", "config-cancel"), searcher)
	defer closeDelegatedMount(t, mount)
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "cancel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	modelFixture := &delegatedSearchModel{callID: "delegated-cancel-call"}
	orchestrator := newDelegatedOrchestrator(t, store, registry, modelFixture, permissions.StaticPolicy{}, nil)
	runCtx, cancel := context.WithCancel(ctx)
	handle, err := orchestrator.Start(runCtx, runtime.Request{SessionID: "delegated-cancel", Message: runtime.UserMessage{Content: "cancel search"}, Config: delegatedRuntimeConfig()})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("backend did not start")
	}
	cancel()
	result := waitDelegatedResult(t, handle)
	if result.Status != session.RunInterrupted || !result.Interrupted {
		t.Fatalf("canceled result = %+v", result)
	}
	call, err := store.GetToolCall(ctx, "delegated-cancel-call")
	if err != nil || call.Status != session.ToolCallInterrupted {
		t.Fatalf("canceled durable call = %+v, err = %v", call, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("canceled backend calls = %d, want 1", calls.Load())
	}
}

func testDelegatedSearchFailureRedaction(t *testing.T) {
	const sentinel = "SENTINEL_PROVIDER_BODY_SECRET"
	ctx := context.Background()
	var calls atomic.Int32
	searcher := delegatedSearchFunc(func(context.Context, string) ([]delegatedSearchRecord, error) {
		calls.Add(1)
		return nil, fmt.Errorf("provider response contained %s", sentinel)
	})
	registry, mount := mountDelegatedSearchFixture(t, delegatedComponent("artifact-failure", "config-failure"), searcher)
	defer closeDelegatedMount(t, mount)
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "failure.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	events := &recordingDelegatedEvents{}
	modelFixture := &delegatedSearchModel{callID: "delegated-failure-call"}
	orchestrator := newDelegatedOrchestrator(t, store, registry, modelFixture, permissions.StaticPolicy{}, events)
	handle, err := orchestrator.Start(ctx, runtime.Request{SessionID: "delegated-failure", Message: runtime.UserMessage{Content: "fail search"}, Config: delegatedRuntimeConfig()})
	if err != nil {
		t.Fatal(err)
	}
	result := waitDelegatedResult(t, handle)
	if result.Status != session.RunCompleted || result.Error != nil || calls.Load() != 1 {
		t.Fatalf("failure-handling result = %+v, calls = %d", result, calls.Load())
	}
	call, err := store.GetToolCall(ctx, "delegated-failure-call")
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != session.ToolCallFailed || call.Error != delegatedFailureClass {
		t.Fatalf("classified durable call = %+v", call)
	}
	replay, err := store.ListMessages(ctx, "delegated-failure", session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	durableEvents, err := store.ListEvents(ctx, "delegated-failure", session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	emitted := events.waitForRunFinished(t)
	allExported, err := json.Marshal(struct {
		Call    session.ToolCall
		Replay  session.ReplayBatch
		Events  session.EventBatch
		Emitted []session.EventRecord
	}{call, replay, durableEvents, emitted})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(allExported), sentinel) || strings.Contains(string(call.Output), sentinel) || strings.Contains(call.Error, sentinel) {
		t.Fatalf("backend sentinel escaped into a durable or emitted surface: %s", allExported)
	}
	if result.Error != nil && strings.Contains(result.Error.Error(), sentinel) {
		t.Fatalf("backend sentinel escaped into public run error: %v", result.Error)
	}
	var foundResult bool
	for _, part := range replay.Parts {
		if part.ID == call.ResultPartID {
			foundResult = part.Kind == session.PartToolResult && bytes.Equal(part.Payload, call.Output)
		}
	}
	if !foundResult {
		t.Fatalf("classified failure result part missing: %#v", replay.Parts)
	}
}

func testDelegatedSearchPlanIdentity(t *testing.T) {
	ctx := context.Background()
	var backendCalls atomic.Int32
	searcher := delegatedSearchFunc(func(context.Context, string) ([]delegatedSearchRecord, error) {
		backendCalls.Add(1)
		return nil, nil
	})
	base := delegatedComponent("artifact-identity-v1", "config-identity-v1")
	firstRegistry, firstMount := mountDelegatedSearchFixture(t, base, searcher)
	first, err := firstRegistry.AcquireRunPlan(ctx, runtime.RunPlanRequest{SessionID: "identity-session"})
	if err != nil {
		t.Fatal(err)
	}
	persisted := first.Descriptor()
	first.Release()
	closeDelegatedMount(t, firstMount)

	equivalentRegistry, equivalentMount := mountDelegatedSearchFixture(t, base, searcher)
	equivalent, err := equivalentRegistry.AcquireRunPlan(ctx, runtime.RunPlanRequest{SessionID: "identity-session"})
	if err != nil {
		t.Fatal(err)
	}
	if equivalent.Descriptor().Fingerprint != persisted.Fingerprint {
		t.Fatalf("equivalent fingerprints differ: %s != %s", equivalent.Descriptor().Fingerprint, persisted.Fingerprint)
	}
	equivalent.Release()
	closeDelegatedMount(t, equivalentMount)

	sealed, err := session.VerifyExtensionPlanForSession("identity-session", persisted)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		component extension.Component
	}{
		{name: "artifact hash", component: delegatedComponent("artifact-identity-v2", "config-identity-v1")},
		{name: "configuration hash", component: delegatedComponent("artifact-identity-v1", "config-identity-v2")},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, mount := mountDelegatedSearchFixture(t, test.component, searcher)
			defer closeDelegatedMount(t, mount)
			resumed, err := registry.AcquireResumePlan(ctx, runtime.ResumePlanRequest{SessionID: "identity-session", Plan: sealed})
			if resumed != nil {
				resumed.Release()
			}
			if !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
				t.Fatalf("strict resume = %v, want ErrExtensionPlanMismatch", err)
			}
		})
	}
	if backendCalls.Load() != 0 {
		t.Fatalf("descriptor verification invoked backend %d times", backendCalls.Load())
	}
}

func mountDelegatedSearchFixture(t *testing.T, component extension.Component, searcher delegatedSearchFixture) (*composition.Registry, *composition.Mount) {
	t.Helper()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := delegatedSearchDefinition(searcher)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), component, composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
		return registrar.Tool(composition.ToolRegistration{
			ID: delegatedToolName, Order: runtime.OrderApplication,
			Scope: extension.GlobalScope(), Definition: definition,
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	return registry, mount
}

func delegatedSearchDefinition(searcher delegatedSearchFixture) (tools.Definition, error) {
	worstCase, err := maximumDelegatedResultBytes()
	if err != nil {
		return tools.Definition{}, err
	}
	retained, err := checkedDelegatedMul(worstCase, 2)
	if err != nil {
		return tools.Definition{}, err
	}
	return tools.Definition{
		Name:        delegatedToolName,
		Description: "Return bounded source records for one search query.",
		Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
			"query": {Type: einoschema.String, Required: true, Desc: "One bounded search query."},
		}),
		Normalize: strictDelegatedSearchInput,
		Pattern: tools.TypedPermissionPattern[delegatedSearchRequest](func(context.Context, delegatedSearchRequest) (string, error) {
			return delegatedPattern, nil
		}),
		Execute: tools.TypedExecutor[delegatedSearchRequest, delegatedSearchResult](func(ctx context.Context, execution tools.TypedExecution[delegatedSearchRequest]) (delegatedSearchResult, error) {
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			records, err := searcher.Search(callCtx, execution.Input.Query)
			if err != nil {
				if callCtx.Err() != nil && errors.Is(err, callCtx.Err()) {
					return delegatedSearchResult{}, callCtx.Err()
				}
				return delegatedSearchResult{}, errDelegatedBackendUnavailable
			}
			return delegatedSearchResult{Results: boundDelegatedRecords(records)}, nil
		}),
		RetrySafe:   false,
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: retained, StoreExternal: false, Redact: false},
		Permissions: []string{delegatedPermissionName},
	}, nil
}

func strictDelegatedSearchInput(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request delegatedSearchRequest
	if err := decoder.Decode(&request); err != nil {
		return nil, fmt.Errorf("%w: strict query: %v", tools.ErrMalformedInput, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing query content", tools.ErrMalformedInput)
	}
	request.Query = strings.TrimSpace(request.Query)
	if request.Query == "" || strings.ContainsRune(request.Query, '\x00') || len(request.Query) > delegatedQueryMaxBytes {
		return nil, fmt.Errorf("%w: query must be non-blank, NUL-free, and at most %d bytes", tools.ErrMalformedInput, delegatedQueryMaxBytes)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("%w: encode query", tools.ErrMalformedInput)
	}
	return encoded, nil
}

func boundDelegatedRecords(records []delegatedSearchRecord) []delegatedSearchRecord {
	bounded := make([]delegatedSearchRecord, 0, delegatedResultMax)
	for _, record := range records {
		if len(bounded) == delegatedResultMax {
			break
		}
		record.Title = delegatedUTF8Prefix(record.Title, delegatedTitleMaxBytes)
		record.URL = delegatedUTF8Prefix(record.URL, delegatedURLMaxBytes)
		record.Snippet = delegatedUTF8Prefix(record.Snippet, delegatedSnippetMax)
		parsed, err := url.Parse(record.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			continue
		}
		bounded = append(bounded, record)
	}
	return bounded
}

func delegatedUTF8Prefix(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit]
}

func oversizedDelegatedRecords() []delegatedSearchRecord {
	urlPrefix := "https://example.test/"
	longURL := urlPrefix + strings.Repeat("a", delegatedURLMaxBytes-len(urlPrefix)) + "overflow"
	record := delegatedSearchRecord{
		Title:   strings.Repeat("\"\\", delegatedTitleMaxBytes/2) + "overflow",
		URL:     longURL,
		Snippet: strings.Repeat("\n\t\\\"", delegatedSnippetMax/4) + "overflow",
	}
	return []delegatedSearchRecord{record, record, record}
}

func maximumDelegatedResultBytes() (int64, error) {
	baseline := delegatedSearchResult{Results: make([]delegatedSearchRecord, delegatedResultMax)}
	raw, err := json.Marshal(baseline)
	if err != nil {
		return 0, err
	}
	fieldBytes, err := checkedDelegatedAdd(delegatedTitleMaxBytes, delegatedURLMaxBytes)
	if err != nil {
		return 0, err
	}
	fieldBytes, err = checkedDelegatedAdd(fieldBytes, delegatedSnippetMax)
	if err != nil {
		return 0, err
	}
	escapedPerRecord, err := checkedDelegatedMul(fieldBytes, 6)
	if err != nil {
		return 0, err
	}
	escapedAll, err := checkedDelegatedMul(escapedPerRecord, delegatedResultMax)
	if err != nil {
		return 0, err
	}
	return checkedDelegatedAdd(int64(len(raw)), escapedAll)
}

func checkedDelegatedAdd(left, right int64) (int64, error) {
	if left < 0 || right < 0 || left > int64(^uint64(0)>>1)-right {
		return 0, errors.New("delegated result bound overflow")
	}
	return left + right, nil
}

func checkedDelegatedMul(left, right int64) (int64, error) {
	if left < 0 || right < 0 || (left != 0 && right > int64(^uint64(0)>>1)/left) {
		return 0, errors.New("delegated result bound overflow")
	}
	return left * right, nil
}

func delegatedComponent(artifactHash, configHash string) extension.Component {
	return extension.Component{
		InstanceID: "delegated-web-search-proof",
		Artifact: extension.Artifact{
			Name:       "eino-agent-extensions-web-search",
			Version:    "fixture-v1",
			Hash:       artifactHash,
			ConfigHash: configHash,
			SourceKind: extension.SourceNative,
		},
	}
}

type delegatedSearchModel struct {
	callID string
	mu     sync.Mutex
	result string
}

func (m *delegatedSearchModel) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	var response *einoschema.Message
	for _, message := range request.Messages {
		if message.Role == einoschema.Tool {
			m.mu.Lock()
			m.result = message.Content
			m.mu.Unlock()
			response = einoschema.AssistantMessage("done", nil)
			break
		}
	}
	if response == nil {
		response = einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID: m.callID, Type: "function",
			Function: einoschema.FunctionCall{Name: delegatedToolName, Arguments: `{"query":"bounded test"}`},
		}})
	}
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	go func() {
		defer writer.Close()
		writer.Send(model.StreamDelta{Message: response}, nil)
	}()
	return reader, nil
}

func (m *delegatedSearchModel) toolResult() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.result
}

type delegatedModelResolver struct{ streamer model.Streamer }

func (r delegatedModelResolver) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{
		Provider: model.Provider{ID: "fixture"},
		Model:    model.Descriptor{ID: "scripted", ProviderID: "fixture"},
		Streamer: r.streamer,
	}, nil
}

type delegatedSearchIDs struct {
	mu sync.Mutex
	n  int
}

func (i *delegatedSearchIDs) next(prefix string) string {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.n++
	return fmt.Sprintf("%s-%03d", prefix, i.n)
}

func (i *delegatedSearchIDs) NewRunID() session.RunID { return session.RunID(i.next("run")) }
func (i *delegatedSearchIDs) NewMessageID() session.MessageID {
	return session.MessageID(i.next("message"))
}
func (i *delegatedSearchIDs) NewPartID() session.PartID { return session.PartID(i.next("part")) }
func (i *delegatedSearchIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(i.next("tool-call"))
}
func (i *delegatedSearchIDs) NewEventID() session.EventID { return session.EventID(i.next("event")) }
func (i *delegatedSearchIDs) NewEpochID() session.EpochID { return session.EpochID(i.next("epoch")) }

func delegatedRuntimeConfig() config.Snapshot {
	selection := model.Selection{ProviderID: "fixture", ModelID: "scripted"}
	return config.Snapshot{
		Agent: config.Agent{Name: "external-consumer", Model: selection},
		Model: selection,
		Metadata: map[string]string{
			"workspace_id":   "fixture-workspace",
			"workspace_root": os.TempDir(),
		},
	}
}

type recordingDelegatedPolicy struct {
	mu       sync.Mutex
	requests []permissions.Request
}

func (p *recordingDelegatedPolicy) Decide(_ context.Context, request permissions.Request) (permissions.Decision, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	return permissions.Decision{Action: permissions.ActionAllow}, nil
}

func (p *recordingDelegatedPolicy) snapshot() []permissions.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]permissions.Request(nil), p.requests...)
}

type recordingDelegatedEvents struct {
	mu     sync.Mutex
	events []session.EventRecord
}

func (s *recordingDelegatedEvents) Emit(_ context.Context, event session.EventRecord) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func (s *recordingDelegatedEvents) waitForRunFinished(t *testing.T) []session.EventRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		result := append([]session.EventRecord(nil), s.events...)
		s.mu.Unlock()
		for _, event := range result {
			if event.Kind == runtime.EventRunFinished {
				return result
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("emitted events never reached run_finished: %#v", result)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newDelegatedOrchestrator(t *testing.T, store session.Store, registry *composition.Registry, streamer model.Streamer, policy permissions.Policy, events runtime.EventSink) *runtime.StreamingOrchestrator {
	t.Helper()
	options := []runtime.Option{
		runtime.WithStore(store),
		runtime.WithModelResolver(delegatedModelResolver{streamer: streamer}),
		runtime.WithRunPlanProvider(registry),
		runtime.WithPermissions(policy),
		runtime.WithIDGenerator(&delegatedSearchIDs{}),
		runtime.WithClock(func() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) }),
		runtime.WithOwnerID("external-consumer-proof"),
	}
	if events != nil {
		options = append(options, runtime.WithEventSink(events))
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(options...)
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator
}

func waitDelegatedResult(t *testing.T, handle runtime.Handle) runtime.Result {
	t.Helper()
	select {
	case result := <-handle.Done():
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("orchestrator did not settle")
		return runtime.Result{}
	}
}

func closeDelegatedMount(t *testing.T, mount *composition.Mount) {
	t.Helper()
	mount.Deactivate()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mount.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
