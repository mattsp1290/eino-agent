package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
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

	matches := searchDeferredTools(snapshot, "WEATHER")
	if len(matches) != toolSearchMaxResults {
		t.Fatalf("matches = %d, want bounded to %d", len(matches), toolSearchMaxResults)
	}
	for _, m := range matches {
		if m.Name != "weather_tool" {
			t.Fatalf("unexpected match %q", m.Name)
		}
	}
	if len(searchDeferredTools(snapshot, "nonexistent-query")) != 0 {
		t.Fatal("expected a miss for a non-matching query")
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

func TestOrchestratorRejectsCallingDeferredToolBeforeDiscovery(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		// Privilege escalation attempt: call the deferred tool directly,
		// skipping tool_search discovery.
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "weather_tool", `{}`))}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: testToolPlanWithSearch(t, []Tool{{
		Name: "weather_tool", Deferred: true, Info: &einoschema.ToolInfo{Name: "weather_tool"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "{}"}, nil }),
	}}, &ToolSearchConfig{Name: "tool_search"})}
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil || !strings.Contains(result.Error.Error(), "not yet discovered") {
		t.Fatalf("result = %+v", result)
	}
	if _, err := store.GetToolCall(context.Background(), "call-1"); err == nil {
		t.Fatal("expected no tool call record for a rejected undiscovered deferred call")
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
