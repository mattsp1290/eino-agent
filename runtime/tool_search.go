package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

// toolSearchMaxResults bounds how many deferred tools one tool-search call
// can surface.
const toolSearchMaxResults = 8

// isToolSearchCall reports whether name is this turn's configured
// tool-search tool name (see RunPlan.ToolSearch / TurnSnapshot.ToolSearch).
func (s TurnSnapshot) isToolSearchCall(name string) bool {
	return s.ToolSearch != nil && s.ToolSearch.Name == name
}

// toolSearchQueryDesc and defaultToolSearchDesc are the model-facing
// parameter/tool descriptions advertised on the tool-search tool (see
// toolSearchToolInfo) and mirror upstream Eino's own wording
// (adk/middlewares/dynamictool/toolsearch/toolsearch.go's
// getToolSearchToolInfo) so a model already familiar with the upstream
// tool-search protocol recognizes this one.
const (
	toolSearchQueryDesc    = `Query to find deferred tools. Use "select:<tool_name>" for direct selection, or keywords to search.`
	defaultToolSearchDesc  = `Search for or select deferred tools to make them available for use. Use "select:<tool_name>" for direct selection (comma-separated for more than one), or keywords to search.`
	toolSearchSelectPrefix = "select:"
)

// toolSearchQuery is the canonical argument shape for a tool-search call:
// {"query": "...", "max_results": N}. A missing or malformed query degrades
// to an empty keyword query (every deferred tool matches, bounded by
// toolSearchMaxResults) rather than failing the call.
type toolSearchQuery struct {
	Query      string `json:"query"`
	MaxResults *int   `json:"max_results,omitempty"`
}

// toolSearchToolInfo builds the advertised ToolInfo for cfg's tool-search
// tool: a required "query" string and an optional "max_results" integer,
// with a default description documenting the select:<tool_name> direct
// selection protocol when cfg.Description is empty. Mirrors upstream
// Eino's getToolSearchToolInfo (eino@v0.9.19
// adk/middlewares/dynamictool/toolsearch/toolsearch.go:416-433).
func toolSearchToolInfo(cfg *ToolSearchConfig) *einoschema.ToolInfo {
	if cfg == nil {
		return nil
	}
	desc := cfg.Description
	if desc == "" {
		desc = defaultToolSearchDesc
	}
	return &einoschema.ToolInfo{
		Name: cfg.Name,
		Desc: desc,
		ParamsOneOf: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
			"query": {Type: einoschema.String, Desc: toolSearchQueryDesc, Required: true},
			"max_results": {
				Type:     einoschema.Integer,
				Desc:     fmt.Sprintf("Maximum number of results to return (default: %d)", toolSearchMaxResults),
				Required: false,
			},
		}),
	}
}

// searchDeferredTools matches query against every Deferred tool in
// snapshot.Tools. A "select:<name>[,<name>...]" query performs direct
// selection (see selectDeferredTools); anything else performs a
// case-insensitive substring search on name or description, bounded to the
// first limit matches in snapshot.Tools order (limit is clamped to
// [1, toolSearchMaxResults]). It references only snapshot.Tools — the
// turn's frozen, resolved tool registry — so a search result can never name
// a tool absent from it.
func searchDeferredTools(snapshot TurnSnapshot, query string, limit int) []Tool {
	trimmed := strings.TrimSpace(query)
	if rest, ok := strings.CutPrefix(trimmed, toolSearchSelectPrefix); ok {
		return selectDeferredTools(snapshot, rest)
	}
	if limit <= 0 || limit > toolSearchMaxResults {
		limit = toolSearchMaxResults
	}
	needle := strings.ToLower(trimmed)
	matches := make([]Tool, 0, limit)
	for _, tool := range snapshot.Tools {
		if !tool.Deferred || tool.Info == nil {
			continue
		}
		if needle != "" && !strings.Contains(strings.ToLower(tool.Name), needle) && !strings.Contains(strings.ToLower(tool.Info.Desc), needle) {
			continue
		}
		matches = append(matches, tool)
		if len(matches) >= limit {
			break
		}
	}
	return matches
}

// selectDeferredTools implements the "select:<name>[,<name>...]" direct
// selection protocol mirrored from upstream Eino's toolsearch.search
// (eino@v0.9.19 adk/middlewares/dynamictool/toolsearch/toolsearch.go:438-476):
// each comma-separated name is matched against a deferred tool's canonical
// name or any of its Aliases (aliases are an eino-agent extension over
// upstream, which has no alias concept). max_results does not bound direct
// selection -- the model has already named the exact tools it wants --
// matching upstream's stated rationale.
func selectDeferredTools(snapshot TurnSnapshot, names string) []Tool {
	seen := make(map[string]bool)
	var matches []Tool
	for _, requested := range strings.Split(names, ",") {
		requested = strings.TrimSpace(requested)
		if requested == "" {
			continue
		}
		for _, tool := range snapshot.Tools {
			if !tool.Deferred || tool.Info == nil || seen[tool.Name] {
				continue
			}
			if tool.Name != requested && !sliceContainsString(tool.Aliases, requested) {
				continue
			}
			seen[tool.Name] = true
			matches = append(matches, tool)
			break
		}
	}
	return matches
}

func sliceContainsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func toolNamesOf(tools []Tool) []string {
	names := make([]string, len(tools))
	for index, tool := range tools {
		names[index] = tool.Name
	}
	return names
}

// toolSearchResultBlock builds the durable/model-visible tool_search_result
// content block shared by settlement (buildTerminalToolSearchEnvelope) and
// the same-turn outgoing model message (executePreparedTools), mirroring
// toolOutputToResultContent's role for ordinary function results (item 4).
// Each entry in the block is the exact frozen schema.ToolInfo JSON
// (ToolInfo.MarshalJSON) of one matched tool.
func toolSearchResultBlock(blockID, callID, name string, matches []Tool) (session.ContentBlock, error) {
	tools := make([]json.RawMessage, 0, len(matches))
	for _, tool := range matches {
		raw, err := json.Marshal(tool.Info)
		if err != nil {
			return session.ContentBlock{}, fmt.Errorf("encode discovered tool info %q: %w", tool.Name, err)
		}
		tools = append(tools, raw)
	}
	return session.ContentBlock{
		ID:         blockID,
		Kind:       session.BlockKindToolSearchResult,
		ToolSearch: &session.ToolSearchBlock{CallID: callID, Name: name, Tools: tools},
	}, nil
}

type terminalToolSearchEnvelopeInput struct {
	Claimed       session.ToolCall
	ModelID       string
	CompletedAt   time.Time
	MessageAt     time.Time
	BlockID       string
	ContentLimits session.ContentLimits
	Matches       []Tool
}

// buildTerminalToolSearchEnvelope is the tool-search analogue of
// buildTerminalToolEnvelope: it persists a tool_search_result content block
// (session.BlockKindToolSearchResult) on a user-role result message instead
// of a function_tool_result block, since a search result is not itself a
// function tool's output.
func buildTerminalToolSearchEnvelope(input terminalToolSearchEnvelopeInput) (session.ToolSettlement, error) {
	call := input.Claimed
	if call.ID == "" || call.ClaimedBy == "" || call.ClaimToken == "" || call.ResultMessageID == "" || call.ResultPartID == "" {
		return session.ToolSettlement{}, errors.New("tool search settlement requires claim identity and reserved result IDs")
	}
	if input.CompletedAt.IsZero() || input.MessageAt.IsZero() || input.BlockID == "" {
		return session.ToolSettlement{}, errors.New("tool search settlement requires completion time and a content block id")
	}
	// call.Name is the canonical search tool name (see prepareToolCalls);
	// RequestedName carries the model-facing name (always equal here, since
	// the search tool is never aliased).
	block, err := toolSearchResultBlock(input.BlockID, string(call.ID), call.Name, input.Matches)
	if err != nil {
		return session.ToolSettlement{}, err
	}
	content := session.Content{Role: session.RoleUser, Blocks: []session.ContentBlock{block}}
	idUsed := false
	parts, err := session.EncodeContentParts(content, func() session.PartID {
		if idUsed {
			return ""
		}
		idUsed = true
		return call.ResultPartID
	}, call.ResultMessageID, call.SessionID, call.RunID, input.MessageAt, input.ContentLimits)
	if err != nil {
		return session.ToolSettlement{}, fmt.Errorf("encode tool search result content: %w", err)
	}
	if len(parts) != 1 {
		return session.ToolSettlement{}, errors.New("encode tool search result content: unexpected part count")
	}
	output, err := json.Marshal(struct {
		ToolCallID          string   `json:"tool_call_id"`
		Status              string   `json:"status"`
		DiscoveredToolNames []string `json:"discovered_tool_names"`
	}{ToolCallID: string(call.ID), Status: "completed", DiscoveredToolNames: toolNamesOf(input.Matches)})
	if err != nil {
		return session.ToolSettlement{}, fmt.Errorf("encode tool search output: %w", err)
	}
	return session.ToolSettlement{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted,
		Output: output, CompletedAt: input.CompletedAt.UTC(),
		ResultMessage: session.Message{
			ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID,
			Role: session.RoleUser, ModelID: input.ModelID, CreatedAt: input.MessageAt.UTC(), UpdatedAt: input.MessageAt.UTC(),
		},
		ResultPart: parts[0],
	}, nil
}

// executeToolSearchCall claims call's persisted record, matches the query
// against snapshot's deferred tools, settles a tool_search_result envelope,
// and adds every matched tool's name to the execution's per-run advertised
// set (e.discovered) so the next provider request lists it in
// Controls.Tools (see TurnSnapshot.ProviderRequest). It never consults
// anything but snapshot.Tools for candidate tools.
func (e *runExecution) executeToolSearchCall(ctx context.Context, snapshot TurnSnapshot, call ToolCall, record session.ToolCall) ([]Tool, error) {
	startedAt := e.host.now()
	claimed, err := e.persistToolClaim(ctx, session.ClaimToolCallRequest{
		ID: record.ID, ClaimedBy: e.host.ownerID(), ClaimToken: string(e.host.ids.NewEventID()), StartedAt: startedAt,
		LeaseDuration: e.host.lease(), Event: toolTransitionEnvelope(e.host, snapshot, startedAt),
	})
	if err != nil {
		return nil, err
	}
	extension.Notify(e.dispatch(), context.WithoutCancel(ctx), ToolStartedPoint, ToolStartedNotice{
		SessionID: call.SessionID, RunID: call.RunID, ToolCallID: claimed.Call.ID, ToolName: claimed.Call.Name, Time: claimed.Call.StartedAt,
	})
	// A tool-search call carries no execution authority of its own -- every
	// discovered tool is still validated against the frozen registry at
	// claim time -- but it is still a durable, model-initiated action that
	// expands the model's advertised tool surface, so it goes through the
	// same observability points as an ordinary tool call, and through the
	// mounted guard chain (not permissions, which are pattern/scope based
	// and meaningless for a search) on a synthetic search Tool
	// (composition-search-reviewer I8).
	searchTool := Tool{Name: claimed.Call.Name, Info: toolSearchToolInfo(snapshot.ToolSearch)}
	e.host.observeToolMaterialized(ctx, snapshot, searchTool, call)
	observedTool := e.host.startObservedToolCall(ctx, snapshot, searchTool, call)
	failSettlement := func(settleErr error) (settled []Tool, err error) {
		e.host.finishObservedToolCall(observedTool, session.ToolCallFailed, settleErr, nil)
		e.host.observeToolSettled(context.WithoutCancel(ctx), snapshot, searchTool, call, session.ToolCallFailed, e.host.now().Sub(startedAt), settleErr, nil)
		return nil, settleErr
	}

	var matches []Tool
	guard, guardErr := evaluateToolGuards(ctx, e.plan, searchTool, call)
	if guardErr != nil || guard.Decision == ToolGuardDeny {
		// Fail-safe rather than fail-closed: a guard denial or evaluation
		// error degrades to zero discovered tools -- the model sees a
		// legitimate, empty-match tool_search_result -- instead of aborting
		// the run. A guard that wants to hard-stop a specific tool should
		// deny that *discovered* tool's own calls, which still goes
		// through the ordinary guard-checked execution pipeline.
		matches = nil
	} else {
		var query toolSearchQuery
		_ = json.Unmarshal(call.Input, &query) // malformed/missing query degrades to an empty query, not a failure
		limit := toolSearchMaxResults
		if query.MaxResults != nil && *query.MaxResults > 0 && *query.MaxResults < limit {
			limit = *query.MaxResults
		}
		matches = searchDeferredTools(snapshot, query.Query, limit)
	}
	completedAt := e.host.now()
	messageAt, err := e.nextDurableMessageTime(ctx, snapshot.SessionID, completedAt)
	if err != nil {
		return failSettlement(err)
	}
	settlement, err := buildTerminalToolSearchEnvelope(terminalToolSearchEnvelopeInput{
		Claimed: claimed.Call, ModelID: string(snapshot.Model.Model.ID), CompletedAt: completedAt, MessageAt: messageAt,
		BlockID: string(e.host.ids.NewPartID()), ContentLimits: e.host.contentLimits, Matches: matches,
	})
	if err != nil {
		return failSettlement(err)
	}
	if _, err := e.persistToolSettlement(ctx, claimed.Call, settlement, toolTransitionEnvelope(e.host, snapshot, completedAt)); err != nil {
		return failSettlement(err)
	}
	extension.Notify(e.dispatch(), context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{
		SessionID: call.SessionID, RunID: call.RunID, ToolCallID: claimed.Call.ID, ToolName: claimed.Call.Name, Status: settlement.Status,
	})
	e.host.finishObservedToolCall(observedTool, settlement.Status, nil, nil)
	e.host.observeToolSettled(context.WithoutCancel(ctx), snapshot, searchTool, call, settlement.Status, completedAt.Sub(startedAt), nil, nil)
	e.markDiscovered(toolNamesOf(matches)...)
	return matches, nil
}

// discoveredToolsFromMessages rebuilds the advertised set of deferred tool
// names discovered via tool search from the projected turn messages the
// model is about to see (snapshot.Messages, built via
// session/history.ProjectAgentic -- see runtime/provider_state.go): every
// ContentBlockTypeToolSearchResult block contributes the names of the tools
// its ToolSearchFunctionToolResult.Result carries. admitTurn
// (runtime/turn_loop.go) and prepareFreshTurn (runtime/orchestrator.go) seed
// execution.discovered from this at the start of every turn (fresh or a
// later run in the same session), mirroring upstream Eino's client-side
// forward selection (adk/middlewares/dynamictool/toolsearch/toolsearch.go's
// BeforeModelRewriteState), which rebuilds the visible set by scanning the
// whole conversation on every model call rather than trusting a per-run
// in-memory set alone.
func discoveredToolsFromMessages(messages []*einoschema.AgenticMessage) []string {
	seen := make(map[string]bool)
	var names []string
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeToolSearchResult || block.ToolSearchFunctionToolResult == nil {
				continue
			}
			result := block.ToolSearchFunctionToolResult.Result
			if result == nil {
				continue
			}
			for _, info := range result.Tools {
				if info == nil || info.Name == "" || seen[info.Name] {
					continue
				}
				seen[info.Name] = true
				names = append(names, info.Name)
			}
		}
	}
	return names
}

// discoveredToolsFromHistoryPaged is the Resume-path counterpart of
// discoveredToolsFromMessages: it rebuilds the advertised set from a
// session's full durable history rather than one turn's projected messages,
// paging until every message has been read (a single bounded page would
// silently drop the most recent discoveries in a long session) and
// decoding with session.MaxContentLimits() (the widest the durable content
// contract ever allows), since a page may contain content admitted under a
// writer's own raised session.ContentLimits.
func discoveredToolsFromHistoryPaged(ctx context.Context, store session.Store, sessionID session.ID) ([]string, error) {
	cursor := session.ReplayCursor{Limit: 1000}
	seen := make(map[string]bool)
	var names []string
	for {
		batch, err := store.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			return nil, err
		}
		for _, name := range discoveredToolsFromHistory(batch, session.MaxContentLimits()) {
			if seen[name] {
				continue
			}
			seen[name] = true
			names = append(names, name)
		}
		if batch.Next == (session.ReplayCursor{}) {
			return names, nil
		}
		cursor = batch.Next
	}
}

// discoveredToolsFromHistory rebuilds the advertised set of deferred tool
// names discovered via tool search from a run's durable history: every
// tool_search_result content block (session.PartToolSearchResult /
// session.BlockKindToolSearchResult) contributes the names of the tools it
// carries. Used by the classic Resume path (runtime/interrupt.go) to seed
// execution.discovered before resuming outstanding tool calls.
func discoveredToolsFromHistory(batch session.ReplayBatch, limits session.ContentLimits) []string {
	seen := make(map[string]bool)
	var names []string
	for _, part := range batch.Parts {
		if part.Kind != session.PartToolSearchResult {
			continue
		}
		content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, limits)
		if err != nil || len(content.Blocks) != 1 || content.Blocks[0].ToolSearch == nil {
			continue
		}
		infos, err := content.Blocks[0].ToolSearch.ToolInfos()
		if err != nil {
			continue
		}
		for _, info := range infos {
			if info == nil || info.Name == "" || seen[info.Name] {
				continue
			}
			seen[info.Name] = true
			names = append(names, info.Name)
		}
	}
	return names
}
