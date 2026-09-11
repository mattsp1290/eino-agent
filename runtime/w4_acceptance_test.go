package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// --- helpers ----------------------------------------------------------

// testToolPlanWithSearch builds a *RunPlan carrying exactly tools (with
// whatever Aliases/ArgumentAliases/Deferred each already sets) and, if
// non-nil, search as the plan's tool-search configuration.
func testToolPlanWithSearch(t *testing.T, tools []Tool, search *ToolSearchConfig) *RunPlan {
	t.Helper()
	capabilities := make([]PlanTool, len(tools))
	for index, tool := range tools {
		frozen, err := cloneToolChecked(tool)
		if err != nil {
			t.Fatalf("clone tool: %v", err)
		}
		capabilities[index] = PlanTool{
			Name: tool.Name, RegistrationID: "test", Scope: extension.GlobalScope(),
			SchemaHash: "schema-" + tool.Name, ExecutorHash: "executor-" + tool.Name, Order: index,
			Aliases: tool.Aliases, ArgumentAliases: tool.ArgumentAliases, Deferred: tool.Deferred,
			Resolve: func(context.Context, ToolScopeContext) (Tool, error) { return cloneToolChecked(frozen) },
		}
	}
	spec := RunPlanSpec{ToolSearch: search}
	if len(capabilities) > 0 {
		spec.Components = []PlanComponent{{Component: testPlanComponent("test-tools"), Tools: capabilities}}
	}
	plan, err := NewRunPlan(spec)
	if err != nil {
		t.Fatalf("NewRunPlan: %v", err)
	}
	return plan
}

func isToolSearchResultMessage(msg *einoschema.AgenticMessage) bool {
	if msg == nil {
		return false
	}
	for _, block := range msg.ContentBlocks {
		if block != nil && block.Type == einoschema.ContentBlockTypeToolSearchResult {
			return true
		}
	}
	return false
}

// hasAnyFunctionToolResult/hasAnyToolSearchResult scan every message (not
// just the most recent one) since earlier turns' history messages remain in
// request.Messages on every later request: a per-message loop that checks
// "is this a search result" before ever checking "does ANY message carry a
// function result" would wrongly match an earlier turn's search-result
// message again on a later turn. Callers must check function-result
// presence across the whole slice first.
func hasAnyFunctionToolResult(messages []*einoschema.AgenticMessage) bool {
	for _, msg := range messages {
		if msg != nil && msg.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(msg) {
			return true
		}
	}
	return false
}

func hasAnyToolSearchResult(messages []*einoschema.AgenticMessage) bool {
	for _, msg := range messages {
		if msg != nil && msg.Role == einoschema.AgenticRoleTypeUser && isToolSearchResultMessage(msg) {
			return true
		}
	}
	return false
}

// --- alias resolution (item 3) -----------------------------------------

func TestResolveToolCallResolvesAliasAndRemapsArguments(t *testing.T) {
	t.Parallel()
	snapshot := TurnSnapshot{Tools: []Tool{{
		Name: "search", Aliases: []string{"find"},
		ArgumentAliases: map[string][]string{"query": {"q"}},
		Executor:        orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}}
	tool, canonical, args, requested, err := resolveToolCall(snapshot, "find", json.RawMessage(`{"q":"cats"}`))
	if err != nil {
		t.Fatalf("resolveToolCall error = %v", err)
	}
	if tool.Name != "search" || canonical != "search" || requested != "find" {
		t.Fatalf("tool/canonical/requested = %q/%q/%q", tool.Name, canonical, requested)
	}
	if string(args) != `{"query":"cats"}` {
		t.Fatalf("remapped args = %s", args)
	}
}

func TestResolveToolCallLeavesAliasUntouchedWhenCanonicalKeyAlreadyPresent(t *testing.T) {
	t.Parallel()
	snapshot := TurnSnapshot{Tools: []Tool{{
		Name: "search", ArgumentAliases: map[string][]string{"query": {"q"}},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}}
	// Both "q" and "query" present: upstream semantics keep "q" as-is
	// (an unrecognized field) rather than overwriting "query".
	_, _, args, _, err := resolveToolCall(snapshot, "search", json.RawMessage(`{"q":"cats","query":"dogs"}`))
	if err != nil {
		t.Fatalf("resolveToolCall error = %v", err)
	}
	var decoded map[string]string
	if err := json.Unmarshal(args, &decoded); err != nil {
		t.Fatalf("decode remapped args: %v", err)
	}
	if decoded["query"] != "dogs" || decoded["q"] != "cats" {
		t.Fatalf("remapped args = %#v", decoded)
	}
}

func TestResolveToolCallRejectsUnknownName(t *testing.T) {
	t.Parallel()
	snapshot := TurnSnapshot{Tools: []Tool{{Name: "search", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })}}}
	_, _, _, _, err := resolveToolCall(snapshot, "unknown", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), `"unknown" unavailable`) {
		t.Fatalf("resolveToolCall error = %v", err)
	}
}

func TestBuildTurnToolAliasIndexRejectsAliasCollidingWithCanonicalName(t *testing.T) {
	t.Parallel()
	tools := []Tool{
		{Name: "search"},
		{Name: "other", Aliases: []string{"search"}},
	}
	if _, err := buildTurnToolAliasIndex(tools); err == nil {
		t.Fatal("expected collision error")
	}
}

// --- RunPlan compile-time alias collision (item 2) ----------------------

func TestNewRunPlanRejectsAliasCollidingWithAnotherToolName(t *testing.T) {
	t.Parallel()
	_, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{{
		Component: testPlanComponent("tools"),
		Tools: []PlanTool{
			{Name: "search", RegistrationID: "t1", Scope: extension.GlobalScope(), SchemaHash: "h1", ExecutorHash: "e1", Resolve: nopResolve},
			{Name: "other", RegistrationID: "t2", Scope: extension.GlobalScope(), SchemaHash: "h2", ExecutorHash: "e2", Aliases: []string{"search"}, Resolve: nopResolve},
		},
	}}})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("err = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestNewRunPlanRejectsDuplicateAliasAcrossTools(t *testing.T) {
	t.Parallel()
	_, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{{
		Component: testPlanComponent("tools"),
		Tools: []PlanTool{
			{Name: "a", RegistrationID: "t1", Scope: extension.GlobalScope(), SchemaHash: "h1", ExecutorHash: "e1", Aliases: []string{"shared"}, Resolve: nopResolve},
			{Name: "b", RegistrationID: "t2", Scope: extension.GlobalScope(), SchemaHash: "h2", ExecutorHash: "e2", Aliases: []string{"shared"}, Resolve: nopResolve},
		},
	}}})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("err = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestNewRunPlanAcceptsNonCollidingAliasesAndResolveToolNameWorks(t *testing.T) {
	t.Parallel()
	plan, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{{
		Component: testPlanComponent("tools"),
		Tools: []PlanTool{
			{Name: "search", RegistrationID: "t1", Scope: extension.GlobalScope(), SchemaHash: "h1", ExecutorHash: "e1", Aliases: []string{"find"}, Resolve: nopResolve},
		},
	}}})
	if err != nil {
		t.Fatalf("NewRunPlan error = %v", err)
	}
	if canonical, ok := plan.ResolveToolName("find"); !ok || canonical != "search" {
		t.Fatalf("ResolveToolName(find) = %q, %v", canonical, ok)
	}
	if canonical, ok := plan.ResolveToolName("search"); !ok || canonical != "search" {
		t.Fatalf("ResolveToolName(search) = %q, %v", canonical, ok)
	}
	if _, ok := plan.ResolveToolName("nope"); ok {
		t.Fatal("expected ResolveToolName(nope) to fail")
	}
}

func nopResolve(context.Context, ToolScopeContext) (Tool, error) { return Tool{}, errors.New("unused") }

// --- enhanced results (item 4) ------------------------------------------

func TestToolOutputToResultContentScalarKeepsExactJSONTextShape(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage(`{"tool_call_id":"c1","status":"completed","content":"hi"}`)
	content := toolOutputToResultContent(raw, ToolOutput{ToolCallID: "c1", Status: "completed", Content: "hi"})
	if len(content) != 1 || content[0].Type != session.ResultContentText || content[0].Text != string(raw) {
		t.Fatalf("content = %#v", content)
	}
}

func TestToolOutputToResultContentEnhancedMirrorsPartsAsSeparateContentItems(t *testing.T) {
	t.Parallel()
	output := ToolOutput{
		ToolCallID: "c1", Status: "completed",
		Parts: []ToolOutputPart{
			{Type: "text", Text: "hello"},
			{Type: "image", Media: &ToolOutputMedia{URL: "https://example/x.png", MIMEType: "image/png"}},
			{Type: "image", Omitted: true, OriginalSize: 999},
		},
	}
	content := toolOutputToResultContent(json.RawMessage(`{}`), output)
	if len(content) != 3 {
		t.Fatalf("content = %#v", content)
	}
	if content[0].Type != session.ResultContentText || content[0].Text != "hello" {
		t.Fatalf("content[0] = %#v", content[0])
	}
	if content[1].Type != session.ResultContentImage || content[1].Media == nil || content[1].Media.URL != "https://example/x.png" {
		t.Fatalf("content[1] = %#v", content[1])
	}
	if content[2].Type != session.ResultContentText || !strings.Contains(content[2].Text, `"omitted":true`) || !strings.Contains(content[2].Text, "999") {
		t.Fatalf("content[2] (omission record) = %#v", content[2])
	}
}

func TestApplyToolOutputPartsBoundsProducesOmissionRecordForOversizedMedia(t *testing.T) {
	t.Parallel()
	result := ToolResult{Parts: []ToolResultPart{
		{Type: ToolResultPartText, Text: "ok"},
		{Type: ToolResultPartImage, Media: &ToolResultMedia{Base64Data: strings.Repeat("A", 1000)}},
	}}
	var output ToolOutput
	applyToolOutputPartsBounds(&output, result, RetentionPolicy{MaxInlineBytes: 100, StoreExternal: true})
	if len(output.Parts) != 2 {
		t.Fatalf("parts = %#v", output.Parts)
	}
	if output.Parts[0].Omitted {
		t.Fatalf("text part should not be omitted: %#v", output.Parts[0])
	}
	if !output.Parts[1].Omitted || output.Parts[1].OriginalSize != 1000 || output.Parts[1].Media != nil {
		t.Fatalf("oversized media part = %#v, want omission record", output.Parts[1])
	}
	if !output.Truncated || !output.External {
		t.Fatalf("output truncated/external = %v/%v, want true/true", output.Truncated, output.External)
	}
}

func TestApplyToolOutputPartsBoundsRedactsEveryPartAsOmissionRecord(t *testing.T) {
	t.Parallel()
	result := ToolResult{Parts: []ToolResultPart{{Type: ToolResultPartText, Text: "secret"}}}
	var output ToolOutput
	applyToolOutputPartsBounds(&output, result, RetentionPolicy{Redact: true})
	if len(output.Parts) != 1 || !output.Parts[0].Omitted || output.Parts[0].Text != "" {
		t.Fatalf("redacted parts = %#v", output.Parts)
	}
	if !output.Redacted {
		t.Fatal("expected output.Redacted = true")
	}
}

// --- deferred tools and search (item 6) ----------------------------------

func TestSearchDeferredToolsCaseInsensitiveSubstringAndBoundedTopN(t *testing.T) {
	t.Parallel()
	var tools []Tool
	for i := 0; i < 12; i++ {
		tools = append(tools, Tool{Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool", Desc: "get the WEATHER forecast"}})
	}
	tools = append(tools, Tool{Name: "eager_tool", Info: &einoschema.ToolInfo{Name: "eager_tool", Desc: "not deferred, never a search candidate"}})
	tools = append(tools, Tool{Name: "unrelated_deferred", Deferred: true, Info: &einoschema.ToolInfo{Name: "unrelated_deferred", Desc: "does something else entirely"}})
	snapshot := TurnSnapshot{Tools: tools}

	matches := searchDeferredTools(snapshot, "WEATHER", toolSearchMaxResults)
	if len(matches) != toolSearchMaxResults {
		t.Fatalf("matches = %d, want bounded to %d", len(matches), toolSearchMaxResults)
	}
	for _, m := range matches {
		if m.Name != "weather_tool" {
			t.Fatalf("unexpected match %q", m.Name)
		}
	}
	if len(searchDeferredTools(snapshot, "nonexistent-query", toolSearchMaxResults)) != 0 {
		t.Fatal("expected a miss for a non-matching query")
	}
	if got := len(searchDeferredTools(snapshot, "WEATHER", 2)); got != 2 {
		t.Fatalf("matches bounded by an explicit max_results = %d, want 2", got)
	}
}

// TestSearchDeferredToolsSelectMatchesCanonicalNameOrAlias guards
// composition-search-reviewer I7: a "select:<name>" query returns exactly
// the deferred tool named, by canonical name or alias, ignoring
// max_results (the model has already named the exact tool it wants).
func TestSearchDeferredToolsSelectMatchesCanonicalNameOrAlias(t *testing.T) {
	t.Parallel()
	snapshot := TurnSnapshot{Tools: []Tool{
		{Name: "weather_tool", Deferred: true, Aliases: []string{"weather"}, Info: &einoschema.ToolInfo{Name: "weather_tool"}},
		{Name: "map_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "map_tool"}},
		{Name: "eager_tool", Info: &einoschema.ToolInfo{Name: "eager_tool"}},
	}}

	byCanonical := searchDeferredTools(snapshot, "select:weather_tool", 1)
	if len(byCanonical) != 1 || byCanonical[0].Name != "weather_tool" {
		t.Fatalf("select by canonical name = %#v", byCanonical)
	}

	byAlias := searchDeferredTools(snapshot, "select:weather", 1)
	if len(byAlias) != 1 || byAlias[0].Name != "weather_tool" {
		t.Fatalf("select by alias = %#v", byAlias)
	}

	multi := searchDeferredTools(snapshot, "select:weather_tool,map_tool", 1)
	if len(multi) != 2 {
		t.Fatalf("select:name,name = %#v, want 2 matches despite max_results=1", multi)
	}

	if got := searchDeferredTools(snapshot, "select:eager_tool", 1); len(got) != 0 {
		t.Fatalf("select must not match a non-deferred tool: %#v", got)
	}
	if got := searchDeferredTools(snapshot, "select:nonexistent", 1); len(got) != 0 {
		t.Fatalf("select of an unknown name = %#v, want no matches", got)
	}
}

// TestToolSearchToolInfoAdvertisesQueryAndMaxResults guards
// composition-search-reviewer I7: the advertised tool-search ToolInfo must
// carry a required "query" string and optional "max_results" integer, and
// fall back to a default description mentioning select: when none is
// configured.
func TestToolSearchToolInfoAdvertisesQueryAndMaxResults(t *testing.T) {
	t.Parallel()
	info := toolSearchToolInfo(&ToolSearchConfig{Name: "tool_search"})
	if info == nil || info.ParamsOneOf == nil {
		t.Fatalf("info = %+v, want a non-nil ParamsOneOf", info)
	}
	if !strings.Contains(info.Desc, "select:") {
		t.Fatalf("default description = %q, want it to mention select:", info.Desc)
	}
	schema, err := info.ToJSONSchema()
	if err != nil {
		t.Fatalf("ToJSONSchema: %v", err)
	}
	query, ok := schema.Properties.Get("query")
	if !ok || query == nil {
		t.Fatal("schema missing query property")
	}
	found := false
	for _, name := range schema.Required {
		found = found || name == "query"
	}
	if !found {
		t.Fatalf("schema.Required = %#v, want query required", schema.Required)
	}
	maxResults, ok := schema.Properties.Get("max_results")
	if !ok || maxResults == nil {
		t.Fatal("schema missing max_results property")
	}
	for _, name := range schema.Required {
		if name == "max_results" {
			t.Fatal("max_results must not be required")
		}
	}

	custom := toolSearchToolInfo(&ToolSearchConfig{Name: "tool_search", Description: "custom description"})
	if custom.Desc != "custom description" {
		t.Fatalf("custom description = %q", custom.Desc)
	}
}

func TestDiscoveredToolsFromHistoryExtractsNamesFromToolSearchResultBlocks(t *testing.T) {
	t.Parallel()
	block, err := toolSearchResultBlock("block-1", "call-1", "tool_search", []Tool{
		{Name: "weather_tool", Info: &einoschema.ToolInfo{Name: "weather_tool"}},
		{Name: "map_tool", Info: &einoschema.ToolInfo{Name: "map_tool"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	limits := session.DefaultContentLimits()
	content := session.Content{Role: session.RoleUser, Blocks: []session.ContentBlock{block}}
	parts, err := session.EncodeContentParts(content, func() session.PartID { return "part-1" }, "msg-1", "session-1", "run-1", time.Now(), limits)
	if err != nil {
		t.Fatal(err)
	}
	names := discoveredToolsFromHistory(session.ReplayBatch{Parts: parts}, limits)
	if len(names) != 2 || names[0] != "weather_tool" || names[1] != "map_tool" {
		t.Fatalf("names = %#v", names)
	}
}

func TestProviderRequestPartitionsEagerDeferredAndExcludesSearchTool(t *testing.T) {
	t.Parallel()
	snapshot := TurnSnapshot{
		ToolSearch: &ToolSearchConfig{Name: "tool_search", Description: "search for tools"},
		Tools: []Tool{
			{Name: "eager", Info: &einoschema.ToolInfo{Name: "eager"}},
			{Name: "deferred_undiscovered", Deferred: true, Info: &einoschema.ToolInfo{Name: "deferred_undiscovered"}},
			{Name: "deferred_discovered", Deferred: true, Info: &einoschema.ToolInfo{Name: "deferred_discovered"}},
		},
	}
	request := snapshot.ProviderRequest("msg-1", agentcontext.TraceContext{}, nil, map[string]bool{"deferred_discovered": true})
	if len(request.Controls.Tools) != 2 {
		t.Fatalf("eager tools = %#v", request.Controls.Tools)
	}
	toolNames := map[string]bool{}
	for _, info := range request.Controls.Tools {
		toolNames[info.Name] = true
	}
	if !toolNames["eager"] || !toolNames["deferred_discovered"] {
		t.Fatalf("eager tool names = %#v", toolNames)
	}
	if len(request.Controls.DeferredTools) != 1 || request.Controls.DeferredTools[0].Name != "deferred_undiscovered" {
		t.Fatalf("deferred tools = %#v", request.Controls.DeferredTools)
	}
	if request.Controls.ToolSearchTool == nil || request.Controls.ToolSearchTool.Name != "tool_search" {
		t.Fatalf("tool search tool = %#v", request.Controls.ToolSearchTool)
	}
}

// --- end-to-end orchestrator acceptance tests ----------------------------

func TestOrchestratorResolvesAliasAndCorrelatesResultOnRequestedName(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if hasAnyFunctionToolResult(request.Messages) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "echo_alias", `{"text":"hi"}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "echo", Aliases: []string{"echo_alias"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: `{"ok":true}`}, nil }),
	}}, nil)}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall: %v", err)
	}
	if call.Name != "echo" || call.RequestedName != "echo_alias" {
		t.Fatalf("call.Name/RequestedName = %q/%q", call.Name, call.RequestedName)
	}

	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var sawResultName string
	for _, part := range batch.Parts {
		if part.Kind != session.PartFunctionToolResult {
			continue
		}
		content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.DefaultContentLimits())
		if err != nil || len(content.Blocks) != 1 || content.Blocks[0].FunctionResult == nil {
			t.Fatalf("decode function result part: %v", err)
		}
		sawResultName = content.Blocks[0].FunctionResult.Name
	}
	if sawResultName != "echo_alias" {
		t.Fatalf("persisted function_tool_result Name = %q, want echo_alias (RequestedName)", sawResultName)
	}
}

func TestOrchestratorRejectsUnknownAliasBeforeAnyClaim(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "not_an_alias", `{}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "echo", Aliases: []string{"echo_alias"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, nil)}
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v", result)
	}
	if _, err := store.GetToolCall(context.Background(), "call-1"); err == nil {
		t.Fatal("expected no tool call record for a rejected unknown alias (rejected before any claim)")
	}
}

func TestOrchestratorDeniesToolThroughAliasOnCanonicalIdentity(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if hasAnyFunctionToolResult(request.Messages) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "echo_alias", `{}`))}, nil
	}), WithPermissions(permissions.PolicyFunc(func(_ context.Context, request permissions.Request) (permissions.Decision, error) {
		if request.ToolName == "echo" {
			return permissions.Decision{Action: permissions.ActionDeny, Message: "denied by policy"}, nil
		}
		return permissions.Decision{Action: permissions.ActionAllow}, nil
	})))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "echo", Aliases: []string{"echo_alias"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: `{"ok":true}`}, nil }),
	}}, nil)}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall: %v", err)
	}
	if call.Status != session.ToolCallFailed {
		t.Fatalf("call.Status = %q, want failed (denied)", call.Status)
	}
}

func TestOrchestratorDeferredSearchHitDiscoveryThenInvocation(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	turn := 0
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		turn++
		switch {
		case hasAnyFunctionToolResult(request.Messages):
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		case hasAnyToolSearchResult(request.Messages):
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-2", "weather_tool", `{}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "tool_search", `{"query":"weather"}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool", Desc: "the weather forecast"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: `{"forecast":"sunny"}`}, nil
		}),
	}}, &ToolSearchConfig{Name: "tool_search"})}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	searchCall, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil || searchCall.Status != session.ToolCallCompleted {
		t.Fatalf("search call = %+v, err=%v", searchCall, err)
	}
	discoveredCall, err := store.GetToolCall(context.Background(), "call-2")
	if err != nil || discoveredCall.Status != session.ToolCallCompleted || discoveredCall.Name != "weather_tool" {
		t.Fatalf("discovered call = %+v, err=%v", discoveredCall, err)
	}
	if turn != 3 {
		t.Fatalf("turns = %d, want 3 (search, discovered call, final text)", turn)
	}
}

func TestOrchestratorDeferredSearchMissLeavesNothingDiscovered(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if hasAnyToolSearchResult(request.Messages) {
			return []*einoschema.AgenticMessage{agenticAssistantText("no tools found")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "tool_search", `{"query":"nonexistent-capability"}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool", Desc: "the weather forecast"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, &ToolSearchConfig{Name: "tool_search"})}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	searchCall, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatal(err)
	}
	var output ToolOutput
	if err := json.Unmarshal(searchCall.Output, &output); err != nil {
		t.Fatal(err)
	}
	// A miss settles successfully with zero discovered tools, not a failure.
	if searchCall.Status != session.ToolCallCompleted {
		t.Fatalf("search call status = %q, want completed", searchCall.Status)
	}
}

// TestOrchestratorSettlesUndiscoveredDeferredCallAsFailedAndContinues guards
// against composition-search-reviewer I3: a deferred tool called before it
// has been discovered via tool search must settle as a terminal,
// model-visible failed call -- exactly like a guard denial -- and the turn
// must continue (not abort the whole run), since this is a condition an
// otherwise well-behaved model can walk into legitimately (e.g. emitting
// tool_search and a deferred call in the same assistant message).
func TestOrchestratorSettlesUndiscoveredDeferredCallAsFailedAndContinues(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if hasAnyFunctionToolResult(request.Messages) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		// Privilege escalation attempt: call the deferred tool directly,
		// skipping tool_search discovery.
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "weather_tool", `{}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool"},
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor:  orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, &ToolSearchConfig{Name: "tool_search"})}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed (turn continues past an undiscovered-deferred denial)", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall: %v", err)
	}
	if call.Status != session.ToolCallFailed {
		t.Fatalf("call.Status = %q, want failed (denied)", call.Status)
	}
	var output ToolOutput
	if err := json.Unmarshal(call.Output, &output); err != nil {
		t.Fatalf("unmarshal call output: %v", err)
	}
	if !strings.Contains(output.Content, "call tool_search first") {
		t.Fatalf("call output content = %q, want it to tell the model to call tool_search first", output.Content)
	}
}

// TestOrchestratorUnknownToolNameStaysFailClosed guards the pre-existing
// invariant a hallucinated/unregistered tool name must keep: unlike the
// undiscovered-deferred case above, there is no registered tool at all to
// settle a denial against, so this aborts the whole run (unchanged by W4;
// see reconciliation.md's rejected CS-I3 for unknown tool names).
func TestOrchestratorUnknownToolNameStaysFailClosed(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "nonexistent_tool", `{}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, &ToolSearchConfig{Name: "tool_search"})}
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil || !strings.Contains(result.Error.Error(), "unavailable") {
		t.Fatalf("result = %+v", result)
	}
	if _, err := store.GetToolCall(context.Background(), "call-1"); err == nil {
		t.Fatal("expected no tool call record for a rejected unknown tool name")
	}
}

func TestOrchestratorSearchResultReferencesOnlyFrozenRegistryTools(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if hasAnyToolSearchResult(request.Messages) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "tool_search", `{"query":""}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, &ToolSearchConfig{Name: "tool_search"})}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	names := discoveredToolsFromHistory(batch, session.DefaultContentLimits())
	if len(names) != 1 || names[0] != "weather_tool" {
		t.Fatalf("discovered names = %#v (call=%+v)", names, call)
	}
}

// --- tool search name collision (item 3 / CS-I1) ------------------------

func TestNewRunPlanRejectsToolSearchNameCollidingWithToolName(t *testing.T) {
	t.Parallel()
	_, err := NewRunPlan(RunPlanSpec{
		ToolSearch: &ToolSearchConfig{Name: "weather_tool"},
		Components: []PlanComponent{{Component: testPlanComponent("tools"), Tools: []PlanTool{{
			Name: "weather_tool", RegistrationID: "weather_tool", Scope: extension.GlobalScope(),
			SchemaHash: "schema", ExecutorHash: "executor",
			Resolve: func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: "weather_tool"}, nil },
		}}}},
	})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("err = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestNewRunPlanRejectsToolSearchNameCollidingWithToolAlias(t *testing.T) {
	t.Parallel()
	_, err := NewRunPlan(RunPlanSpec{
		ToolSearch: &ToolSearchConfig{Name: "weather"},
		Components: []PlanComponent{{Component: testPlanComponent("tools"), Tools: []PlanTool{{
			Name: "weather_tool", RegistrationID: "weather_tool", Scope: extension.GlobalScope(),
			SchemaHash: "schema", ExecutorHash: "executor", Aliases: []string{"weather"},
			Resolve: func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: "weather_tool"}, nil },
		}}}},
	})
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("err = %v, want ErrExtensionPlanMismatch", err)
	}
}

// --- restriction alias matching and search-name denial (item 8 / CS-I2) -

// TestPlanRestrictionDeniesToolByAlias guards composition-search-reviewer
// I2: a Denied entry naming a tool's alias (not its canonical name) must
// deny the tool under both names, not silently do nothing.
func TestPlanRestrictionDeniesToolByAlias(t *testing.T) {
	t.Parallel()
	rules, err := CanonicalizeRestrictionRules(nil, []string{"say"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewRunPlan(RunPlanSpec{
		Components: []PlanComponent{{Component: testPlanComponent("tools"), Tools: []PlanTool{{
			Name: "echo", RegistrationID: "echo", Scope: extension.GlobalScope(),
			SchemaHash: "schema", ExecutorHash: "executor", Aliases: []string{"say"},
			Resolve: func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: "echo"}, nil },
		}}, Restrictions: []PlanRestriction{{
			RegistrationID: "restriction", Scope: extension.GlobalScope(), Denied: rules.Denied,
		}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	tools, err := plan.ResolveTools(context.Background(), ToolScopeContext{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 0 {
		t.Fatalf("tools = %#v, want echo denied via its alias \"say\"", tools)
	}
}

// TestNewRunPlanDisablesToolSearchWhenRestrictionDeniesSearchName guards
// composition-search-reviewer I2: a restriction that denies the search
// tool's own name disables tool search for the plan entirely (ToolSearch()
// returns nil), exactly as it would for an ordinary tool.
func TestNewRunPlanDisablesToolSearchWhenRestrictionDeniesSearchName(t *testing.T) {
	t.Parallel()
	rules, err := CanonicalizeRestrictionRules(nil, []string{"tool_search"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewRunPlan(RunPlanSpec{
		ToolSearch: &ToolSearchConfig{Name: "tool_search"},
		Components: []PlanComponent{{Component: testPlanComponent("restrictions"), Restrictions: []PlanRestriction{{
			RegistrationID: "restriction", Scope: extension.GlobalScope(), Denied: rules.Denied,
		}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	if plan.ToolSearch() != nil {
		t.Fatalf("ToolSearch() = %+v, want nil (denied by restriction)", plan.ToolSearch())
	}
}

// --- resume of an interrupted tool_search call (item 6 / CS-C2) ---------

// TestStreamingOrchestratorResumesPendingToolSearchCall guards
// composition-search-reviewer C2: an interrupted (pending) tool_search call
// must resume by re-executing the search -- it is side-effect free -- and
// settle successfully, never failing the whole run with
// `tool "tool_search" unavailable` (the search tool is deliberately not a
// plan tool, so a naive `tools[call.Name]` lookup misses it).
func TestStreamingOrchestratorResumesPendingToolSearchCall(t *testing.T) {
	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-resume-search", WorkspaceID: "workspace-1", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID: "run-resume-search", SessionID: "session-resume-search", OwnerID: "dead-owner", ClaimToken: "old-claim",
		Agent: "agent", ProviderID: "fake", ModelID: "test", Status: session.RunPending,
		Config:        map[string]string{"workspace_id": "workspace-1", "workspace_root": "/workspace"},
		ExtensionPlan: testEchoPlanDescriptor(), CreatedAt: now,
	}, time.Millisecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	time.Sleep(2 * time.Millisecond)
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: "assistant-resume-search", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{
		ID: "call-search-resume", SessionID: run.SessionID, RunID: run.ID, MessageID: "assistant-resume-search",
		ResultMessageID: "result-search-resume", ResultPartID: "part-search-resume",
		Name: "tool_search", Pattern: "tool_search", Input: json.RawMessage(`{"query":"weather"}`), Status: session.ToolCallPending,
	}
	if _, err := execution.CreateToolCall(ctx, testCreateToolRequest(call, "event-create-search-resume", now)); err != nil {
		t.Fatalf("create tool call: %v", err)
	}

	plan := testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool", Desc: "weather forecast"},
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor:  orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, &ToolSearchConfig{Name: "tool_search"})
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store), WithOwnerID("owner-1"), WithClock(func() time.Time { return now.Add(time.Hour) }),
	)
	done := make(chan Result, 1)
	orchestrator.executeResume(ctx, newRunExecution(orchestrator, plan, run), run, done)
	result := <-done
	if result.Status != session.RunInterrupted || result.Error != nil {
		t.Fatalf("result = %+v, want interrupted with no error (never \"tool_search unavailable\")", result)
	}
	settled, err := store.GetToolCall(ctx, call.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != session.ToolCallCompleted {
		t.Fatalf("resumed search call status = %q, want completed", settled.Status)
	}
	var searchOutput struct {
		DiscoveredToolNames []string `json:"discovered_tool_names"`
	}
	if err := json.Unmarshal(settled.Output, &searchOutput); err != nil {
		t.Fatalf("unmarshal search output: %v", err)
	}
	if len(searchOutput.DiscoveredToolNames) != 1 || searchOutput.DiscoveredToolNames[0] != "weather_tool" {
		t.Fatalf("discovered_tool_names = %#v", searchOutput.DiscoveredToolNames)
	}
}

// --- discovered-set carries across runs in one session (item 5 / CS-C1) -

// TestOrchestratorDiscoveredToolCarriesToSecondRunInSameSession guards
// composition-search-reviewer C1: a deferred tool discovered via tool
// search in run 1 must remain callable in a LATER run in the same session
// without the model re-discovering it. executeTurn seeds
// execution.discovered from the projected turn messages
// (discoveredToolsFromMessages) at the start of every turn, not just this
// execution's own in-memory history, so a fresh run's advertised set
// already reflects every discovery visible in the projected history.
func TestOrchestratorDiscoveredToolCarriesToSecondRunInSameSession(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	totalCalls := 0
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		totalCalls++
		switch totalCalls {
		case 1: // run 1, turn 1: discover weather_tool.
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "tool_search", `{"query":"weather"}`))}, nil
		case 2: // run 1, turn 2: call the just-discovered tool.
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-2", "weather_tool", `{}`))}, nil
		case 3: // run 1, turn 3: done.
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		case 4:
			// Run 2, turn 1: call the tool discovered in run 1 directly,
			// WITHOUT calling tool_search again. If run 2's advertised set
			// does not carry run 1's discovery forward, prepareToolCalls
			// rejects this as "not yet discovered" and the run fails.
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-3", "weather_tool", `{}`))}, nil
		default: // run 2, turn 2: done.
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool", Desc: "the weather forecast"},
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: `{"forecast":"sunny"}`}, nil
		}),
	}}, &ToolSearchConfig{Name: "tool_search"})}

	const sessionID session.ID = "discover-then-reuse-session"
	first := startAndWaitRequest(t, orch, Request{SessionID: sessionID, Message: TextUserMessage("what's the weather"), Config: orchestratorConfig()})
	if first.Status != session.RunCompleted {
		t.Fatalf("first run = %+v", first)
	}
	second := startAndWaitRequest(t, orch, Request{SessionID: sessionID, Message: TextUserMessage("call it again"), Config: orchestratorConfig()})
	if second.Status != session.RunCompleted || second.Error != nil {
		t.Fatalf("second run = %+v, want completed (a tool discovered in run 1 must be callable in run 2)", second)
	}
	call3, err := store.GetToolCall(context.Background(), "call-3")
	if err != nil || call3.Status != session.ToolCallCompleted || call3.Name != "weather_tool" {
		t.Fatalf("call-3 = %+v, err=%v", call3, err)
	}
}

// --- enhanced tool result: persisted == model-visible == replay (item 11 / ID-I4) ---

// einoFunctionResultSummary extracts (text item 0, image item 1's URL, text
// item 2) from an einoschema function_tool_result content list, for
// comparison against the durable/replayed session.ResultContent shape.
func einoFunctionResultSummary(items []*einoschema.FunctionToolResultContentBlock) (text0, imageURL1, text2 string) {
	if len(items) > 0 && items[0] != nil && items[0].Text != nil {
		text0 = items[0].Text.Text
	}
	if len(items) > 1 && items[1] != nil && items[1].Image != nil {
		imageURL1 = items[1].Image.URL
	}
	if len(items) > 2 && items[2] != nil && items[2].Text != nil {
		text2 = items[2].Text.Text
	}
	return
}

// sessionResultContentSummary is the session.ResultContent counterpart of
// einoFunctionResultSummary.
func sessionResultContentSummary(items []session.ResultContent) (text0, imageURL1, text2 string) {
	if len(items) > 0 {
		text0 = items[0].Text
	}
	if len(items) > 1 && items[1].Media != nil {
		imageURL1 = items[1].Media.URL
	}
	if len(items) > 2 {
		text2 = items[2].Text
	}
	return
}

// functionResultContentForCall finds the function_tool_result block bound
// to callID across every message (agentic history keeps every prior turn's
// messages in request.Messages), and returns its content items.
func functionResultContentForCall(messages []*einoschema.AgenticMessage, callID string) []*einoschema.FunctionToolResultContentBlock {
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block != nil && block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil && block.FunctionToolResult.CallID == callID {
				return block.FunctionToolResult.Content
			}
		}
	}
	return nil
}

// TestOrchestratorEnhancedToolResultPersistedMatchesModelVisibleAndReplay
// guards ID-I4 and, more importantly, the C1/I1 bugs this reconciliation
// fixes: an enhanced (text + image + oversized-media) tool result must
// round-trip identically through (a) the same-turn model-visible message
// the next model call receives, (b) the durable content decoded back out of
// the store, and (c) session/history.ProjectAgentic's replay of the same
// session -- text item 0 ("summary"), image item 1 (its URL), and the
// oversized item 2 degraded to an {type,omitted,original_size} text record
// (see toolOutputPartToResultContent) must all agree.
func TestOrchestratorEnhancedToolResultPersistedMatchesModelVisibleAndReplay(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	oversizedBase64 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), 500))
	var turn2Messages []*einoschema.AgenticMessage
	turn := 0
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		turn++
		if turn == 2 {
			turn2Messages = request.Messages
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-enhanced", "enhanced_tool", `{}`))}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "enhanced_tool", Info: &einoschema.ToolInfo{Name: "enhanced_tool"},
		Retention: RetentionPolicy{MaxInlineBytes: 100},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Parts: []ToolResultPart{
				{Type: ToolResultPartText, Text: "summary"},
				{Type: ToolResultPartImage, Media: &ToolResultMedia{URL: "https://example.com/enhanced.png", MIMEType: "image/png"}},
				{Type: ToolResultPartVideo, Media: &ToolResultMedia{Base64Data: oversizedBase64, MIMEType: "video/mp4"}},
			}}, nil
		}),
	}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}

	// (a) same-turn model-visible content, from the next model request.
	modelVisible := functionResultContentForCall(turn2Messages, "call-enhanced")
	if len(modelVisible) != 3 {
		t.Fatalf("model-visible content = %#v, want 3 items", modelVisible)
	}
	mvText0, mvImage1, mvText2 := einoFunctionResultSummary(modelVisible)

	// (b) durable content decoded back out of the store.
	call, err := store.GetToolCall(context.Background(), "call-enhanced")
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var persisted []session.ResultContent
	for _, part := range batch.Parts {
		if part.ID != call.ResultPartID {
			continue
		}
		content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.DefaultContentLimits())
		if err != nil || len(content.Blocks) != 1 || content.Blocks[0].FunctionResult == nil {
			t.Fatalf("decode persisted function result: %v", err)
		}
		persisted = content.Blocks[0].FunctionResult.Content
	}
	if len(persisted) != 3 {
		t.Fatalf("persisted content = %#v, want 3 items", persisted)
	}
	pText0, pImage1, pText2 := sessionResultContentSummary(persisted)

	// (c) session/history.ProjectAgentic's replay of the same session.
	projection, err := history.LoadAgentic(context.Background(), store, "session-1", history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	replayed := functionResultContentForCall(projection.Messages, "call-enhanced")
	if len(replayed) != 3 {
		t.Fatalf("replayed content = %#v, want 3 items", replayed)
	}
	rText0, rImage1, rText2 := einoFunctionResultSummary(replayed)

	if mvText0 != "summary" || pText0 != "summary" || rText0 != "summary" {
		t.Fatalf("text item 0: model-visible=%q persisted=%q replayed=%q, want \"summary\"", mvText0, pText0, rText0)
	}
	if mvImage1 != "https://example.com/enhanced.png" || pImage1 != mvImage1 || rImage1 != mvImage1 {
		t.Fatalf("image item 1 URL: model-visible=%q persisted=%q replayed=%q", mvImage1, pImage1, rImage1)
	}
	if !strings.Contains(pText2, `"omitted":true`) {
		t.Fatalf("text item 2 (oversized video) = %q, want an omission record", pText2)
	}
	if mvText2 != pText2 || rText2 != pText2 {
		t.Fatalf("text item 2 (omission record) disagrees: model-visible=%q persisted=%q replayed=%q", mvText2, pText2, rText2)
	}
}

// --- resume never re-runs prepare/alias-remap middleware (item 11 / ID-I4) ---

// TestResumeNeverReappliesToolPreparePoint guards ID-I4's "replayed
// preparation count of one" acceptance case: alias resolution and argument
// remapping both happen inside prepareToolCalls, strictly before
// ToolPreparePoint fires (see runtime/tool_preparation.go); resumeRun never
// calls prepareToolCalls at all. So if resume ever regressed to
// re-preparing an already-persisted, already-canonical call, ToolPreparePoint
// would fire again -- this asserts it fires exactly zero times across a
// resume, and that the persisted Input is untouched by it.
func TestResumeNeverReappliesToolPreparePoint(t *testing.T) {
	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "dead-owner", session.ToolCallPending)
	prepareCount := 0
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "prepare-counter", Artifact: extension.Artifact{Name: "prepare-counter", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(ctx, component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnTransform(registrar, ToolPreparePoint, extension.Registration{ID: "count", Scope: extension.GlobalScope()}, func(_ context.Context, input PreparedToolCall) (PreparedToolCall, error) {
			prepareCount++
			return input, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mount.Close(context.Background()) })
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name: "echo", Aliases: []string{"say"}, ArgumentAliases: map[string][]string{"text": {"message"}},
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor:  orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "ok"}, nil }),
	}}}
	plan := newTestToolPlanWithDispatch(toolRegistry, dispatch)
	t.Cleanup(func() { plan.release() })

	before, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatal(err)
	}

	orchestrator := mustConfiguredOrchestrator(
		WithStore(store), WithOwnerID("owner-1"), WithClock(func() time.Time { return time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC) }),
	)
	done := make(chan Result, 1)
	orchestrator.executeResume(ctx, newRunExecution(orchestrator, plan, run), run, done)
	result := <-done
	if result.Error != nil {
		t.Fatalf("resume result = %+v", result)
	}
	if prepareCount != 0 {
		t.Fatalf("ToolPreparePoint fired %d times during resume, want 0 (resume must never re-run prepare/alias-remap middleware)", prepareCount)
	}
	after, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatal(err)
	}
	if string(after.Input) != string(before.Input) {
		t.Fatalf("resumed call Input changed: before=%s after=%s (resume must never re-apply argument-alias remapping)", before.Input, after.Input)
	}
}

// resumeHistoryErrorStore fails only the full-history paging shape
// discoveredToolsFromHistoryPaged uses (Limit: 1000) so the test below
// exercises a resume-time discovery read failure in isolation.
type resumeHistoryErrorStore struct {
	session.Store
	err error
}

func (s *resumeHistoryErrorStore) ListMessages(ctx context.Context, sessionID session.ID, cursor session.ReplayCursor) (session.ReplayBatch, error) {
	if cursor.Limit == 1000 {
		return session.ReplayBatch{}, s.err
	}
	return s.Store.ListMessages(ctx, sessionID, cursor)
}

// TestResumeHistoryReadFailureStillTerminalizesOutstandingCalls guards the
// fix-pass finding that a discovery-history read failure on resume must not
// orphan the outstanding tool calls it was about to resume: the run fails,
// but every non-terminal call is terminalized before the run settles, so no
// row stays pending forever behind a terminal run.
func TestResumeHistoryReadFailureStillTerminalizesOutstandingCalls(t *testing.T) {
	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-resume-history-err", WorkspaceID: "workspace-1", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID: "run-resume-history-err", SessionID: "session-resume-history-err", OwnerID: "dead-owner", ClaimToken: "old-claim",
		Agent: "agent", ProviderID: "fake", ModelID: "test", Status: session.RunPending,
		Config:        map[string]string{"workspace_id": "workspace-1", "workspace_root": "/workspace"},
		ExtensionPlan: testEchoPlanDescriptor(), CreatedAt: now,
	}, time.Millisecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	time.Sleep(2 * time.Millisecond)
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: "assistant-resume-history-err", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{
		ID: "call-resume-history-err", SessionID: run.SessionID, RunID: run.ID, MessageID: "assistant-resume-history-err",
		ResultMessageID: "result-resume-history-err", ResultPartID: "part-resume-history-err",
		Name: "tool_search", Pattern: "tool_search", Input: json.RawMessage(`{"query":"weather"}`), Status: session.ToolCallPending,
	}
	if _, err := execution.CreateToolCall(ctx, testCreateToolRequest(call, "event-create-resume-history-err", now)); err != nil {
		t.Fatalf("create tool call: %v", err)
	}

	historyErr := errors.New("history unavailable")
	plan := testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool", Desc: "weather forecast"},
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor:  orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, &ToolSearchConfig{Name: "tool_search"})
	orchestrator := mustConfiguredOrchestrator(
		WithStore(&resumeHistoryErrorStore{Store: store, err: historyErr}), WithOwnerID("owner-1"),
		WithClock(func() time.Time { return now.Add(time.Hour) }),
	)
	done := make(chan Result, 1)
	orchestrator.executeResume(ctx, newRunExecution(orchestrator, plan, run), run, done)
	result := <-done
	if result.Status != session.RunFailed || !errors.Is(result.Error, historyErr) {
		t.Fatalf("result = %+v, want failed with the history error", result)
	}
	settled, err := store.GetToolCall(ctx, call.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !session.TerminalToolCall(settled.Status) {
		t.Fatalf("outstanding call status = %q, want terminal after a failed resume", settled.Status)
	}
	reopened, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Terminal() {
		t.Fatalf("run status = %q, want terminal", reopened.Status)
	}
}
