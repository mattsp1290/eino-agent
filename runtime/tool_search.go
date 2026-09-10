package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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

// toolSearchQuery is the canonical argument shape for a tool-search call:
// {"query": "..."}. A missing or malformed query degrades to an empty query
// (every deferred tool matches, bounded by toolSearchMaxResults) rather than
// failing the call.
type toolSearchQuery struct {
	Query string `json:"query"`
}

// searchDeferredTools matches query against every Deferred tool in
// snapshot.Tools (case-insensitive substring on name or description),
// bounded to the first toolSearchMaxResults matches in snapshot.Tools order.
// It references only snapshot.Tools — the turn's frozen, resolved tool
// registry — so a search result can never name a tool absent from it.
func searchDeferredTools(snapshot TurnSnapshot, query string) []Tool {
	needle := strings.ToLower(strings.TrimSpace(query))
	matches := make([]Tool, 0, toolSearchMaxResults)
	for _, tool := range snapshot.Tools {
		if !tool.Deferred || tool.Info == nil {
			continue
		}
		if needle != "" && !strings.Contains(strings.ToLower(tool.Name), needle) && !strings.Contains(strings.ToLower(tool.Info.Desc), needle) {
			continue
		}
		matches = append(matches, tool)
		if len(matches) >= toolSearchMaxResults {
			break
		}
	}
	return matches
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
	var query toolSearchQuery
	_ = json.Unmarshal(call.Input, &query) // malformed/missing query degrades to an empty query, not a failure
	matches := searchDeferredTools(snapshot, query.Query)
	completedAt := e.host.now()
	messageAt, err := e.nextDurableMessageTime(ctx, snapshot.SessionID, completedAt)
	if err != nil {
		return nil, err
	}
	settlement, err := buildTerminalToolSearchEnvelope(terminalToolSearchEnvelopeInput{
		Claimed: claimed.Call, ModelID: string(snapshot.Model.Model.ID), CompletedAt: completedAt, MessageAt: messageAt,
		BlockID: string(e.host.ids.NewPartID()), ContentLimits: e.host.contentLimits, Matches: matches,
	})
	if err != nil {
		return nil, err
	}
	if _, err := e.persistToolSettlement(ctx, claimed.Call, settlement, toolTransitionEnvelope(e.host, snapshot, completedAt)); err != nil {
		return nil, err
	}
	e.markDiscovered(toolNamesOf(matches)...)
	return matches, nil
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
