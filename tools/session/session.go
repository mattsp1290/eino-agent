package sessiontools

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

const (
	NamePlanSet        = "session_plan_set"
	NamePlanGet        = "session_plan_get"
	NameRetainedOutput = "session_retain_output"
	NameSubagent       = "session_subagent"
	NameSkillLoad      = "session_skill_load"

	defaultMaxRetainedBytes = 64 * 1024
)

type State struct {
	mu               sync.RWMutex
	plans            map[session.ID][]PlanItem
	outputs          map[session.ID]map[string]RetainedOutput
	outputBytes      map[session.ID]int64
	MaxRetainedBytes int64
	MaxSessionBytes  int64
	maxSet           bool
}

type PlanItem struct {
	ID     string `json:"id"`
	Text   string `json:"text"`
	Status string `json:"status"`
}

type RetainedOutput struct {
	ID           string `json:"id"`
	Content      string `json:"content,omitempty"`
	OriginalSize int64  `json:"original_size"`
	InlineSize   int64  `json:"inline_size"`
	Truncated    bool   `json:"truncated,omitempty"`
}

type SubagentRunner interface {
	RunSubagent(ctx context.Context, request SubagentRequest) (SubagentResult, error)
}

type SkillLoader interface {
	LoadSkill(ctx context.Context, request SkillRequest) (SkillResult, error)
}

type SubagentRequest struct {
	SessionID session.ID `json:"session_id"`
	Task      string     `json:"task"`
}

type SubagentResult struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type SkillRequest struct {
	SessionID session.ID `json:"session_id"`
	Name      string     `json:"name"`
}

type SkillResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type Options struct {
	State    *State
	Subagent SubagentRunner
	Skills   SkillLoader
}

func NewState() *State {
	return &State{
		plans:   map[session.ID][]PlanItem{},
		outputs: map[session.ID]map[string]RetainedOutput{},
	}
}

func (s *State) SetMaxRetainedBytes(limit int64) {
	s.mu.Lock()
	s.MaxRetainedBytes = limit
	s.maxSet = true
	s.mu.Unlock()
}

func (s *State) GetRetainedOutput(sessionID session.ID, id string) (RetainedOutput, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	output, ok := s.outputs[sessionID][id]
	return output, ok
}

func (s *State) ListRetainedOutputs(sessionID session.ID) []RetainedOutput {
	s.mu.RLock()
	defer s.mu.RUnlock()
	outputs := s.outputs[sessionID]
	result := make([]RetainedOutput, 0, len(outputs))
	for _, output := range outputs {
		result = append(result, output)
	}
	return result
}

func Register(ctx context.Context, registry *agenttools.Registry, options Options) ([]agenttools.Registration, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: registry required", agenttools.ErrInvalidDefinition)
	}
	if options.State == nil {
		options.State = NewState()
	}
	definitions := []agenttools.Definition{
		planSetDefinition(options.State),
		planGetDefinition(options.State),
		retainOutputDefinition(options.State),
	}
	if options.Subagent != nil {
		definitions = append(definitions, subagentDefinition(options.Subagent))
	}
	if options.Skills != nil {
		definitions = append(definitions, skillLoadDefinition(options.Skills))
	}
	registrations := make([]agenttools.Registration, 0, len(definitions))
	for _, definition := range definitions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		registration, err := registry.Register(definition)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	return registrations, nil
}

func planSetDefinition(state *State) agenttools.Definition {
	return agenttools.Definition{
		Name:        NamePlanSet,
		Description: "Replace the current session plan state.",
		Decode:      decodeJSON[planSetInput],
		Encode:      encodeJSON,
		Execute: func(_ context.Context, execution agenttools.Execution) (any, error) {
			input := execution.Input.(planSetInput)
			items := clonePlan(input.Items)
			state.mu.Lock()
			state.ensure()
			state.plans[execution.Context.Turn.SessionID] = items
			state.mu.Unlock()
			return map[string]any{"items": items}, nil
		},
		RetrySafe:   true,
		Scope:       sessionScope("plan"),
		Permissions: []string{"session.plan"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096},
		Metadata:    map[string]string{"source": "session"},
	}
}

func planGetDefinition(state *State) agenttools.Definition {
	return agenttools.Definition{
		Name:        NamePlanGet,
		Description: "Read the current session plan state.",
		Decode:      decodeJSON[struct{}],
		Encode:      encodeJSON,
		Execute: func(_ context.Context, execution agenttools.Execution) (any, error) {
			state.mu.RLock()
			items := clonePlan(state.plans[execution.Context.Turn.SessionID])
			state.mu.RUnlock()
			return map[string]any{"items": items}, nil
		},
		RetrySafe:   true,
		Scope:       sessionScope("plan"),
		Permissions: []string{"session.plan"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096},
		Metadata:    map[string]string{"source": "session"},
	}
}

func retainOutputDefinition(state *State) agenttools.Definition {
	return agenttools.Definition{
		Name:        NameRetainedOutput,
		Description: "Retain bounded session output for later host retrieval.",
		Decode:      decodeJSON[retainInput],
		Encode:      encodeJSON,
		Execute: func(_ context.Context, execution agenttools.Execution) (any, error) {
			input := execution.Input.(retainInput)
			if input.ID == "" {
				return nil, fmt.Errorf("id required")
			}
			limit := state.maxRetainedBytes()
			output := RetainedOutput{ID: input.ID, OriginalSize: int64(len(input.Content))}
			content := input.Content
			if limit >= 0 && int64(len(content)) > limit {
				content = validUTF8Prefix(content, int(limit))
				output.Truncated = true
			}
			output.Content = content
			output.InlineSize = int64(len(content))
			state.mu.Lock()
			state.ensure()
			sessionID := execution.Context.Turn.SessionID
			if state.outputs[sessionID] == nil {
				state.outputs[sessionID] = map[string]RetainedOutput{}
			}
			if previous, ok := state.outputs[sessionID][input.ID]; ok {
				state.outputBytes[sessionID] -= previous.InlineSize
			}
			maxSessionBytes := state.maxSessionBytes()
			if maxSessionBytes >= 0 {
				available := maxSessionBytes - state.outputBytes[sessionID]
				if available < output.InlineSize {
					output.Content = validUTF8Prefix(output.Content, int(available))
					output.InlineSize = int64(len(output.Content))
					output.Truncated = true
				}
			}
			state.outputs[sessionID][input.ID] = output
			state.outputBytes[sessionID] += output.InlineSize
			state.mu.Unlock()
			return output, nil
		},
		RetrySafe:   true,
		Scope:       sessionScope("retained_output"),
		Permissions: []string{"session.retained_output"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096, StoreExternal: true},
		Metadata:    map[string]string{"source": "session"},
	}
}

func subagentDefinition(runner SubagentRunner) agenttools.Definition {
	return agenttools.Definition{
		Name:        NameSubagent,
		Description: "Request a host-provided subagent task for this session.",
		Decode:      decodeJSON[subagentInput],
		Normalize:   normalizeSubagentInput,
		Encode:      encodeJSON,
		Execute: func(ctx context.Context, execution agenttools.Execution) (any, error) {
			input := execution.Input.(subagentInput)
			if input.Task == "" {
				return nil, fmt.Errorf("task required")
			}
			return runner.RunSubagent(ctx, SubagentRequest{SessionID: execution.Context.Turn.SessionID, Task: input.Task})
		},
		Scope:       sessionScope("subagent"),
		Permissions: []string{"session.subagent"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096},
		Metadata:    map[string]string{"source": "session"},
	}
}

func skillLoadDefinition(loader SkillLoader) agenttools.Definition {
	return agenttools.Definition{
		Name:        NameSkillLoad,
		Description: "Request host-provided skill loading for this session.",
		Decode:      decodeJSON[skillInput],
		Normalize:   normalizeSkillInput,
		Encode:      encodeJSON,
		Execute: func(ctx context.Context, execution agenttools.Execution) (any, error) {
			input := execution.Input.(skillInput)
			if input.Name == "" {
				return nil, fmt.Errorf("name required")
			}
			return loader.LoadSkill(ctx, SkillRequest{SessionID: execution.Context.Turn.SessionID, Name: input.Name})
		},
		Scope:       sessionScope("skill"),
		Permissions: []string{"session.skill"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096},
		Metadata:    map[string]string{"source": "session"},
	}
}

type planSetInput struct {
	Items []PlanItem `json:"items"`
}

type retainInput struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

type subagentInput struct {
	Task string `json:"task"`
}

type skillInput struct {
	Name string `json:"name"`
}

func decodeJSON[T any](_ context.Context, raw json.RawMessage) (any, error) {
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func encodeJSON(_ context.Context, value any) (json.RawMessage, error) {
	return json.Marshal(value)
}

func sessionScope(_ string) agenttools.ScopeResolver {
	return func(context runtime.ToolScopeContext) runtime.ToolScope {
		return runtime.ToolScope{
			WorkspaceID: context.WorkspaceID,
			Root:        "session://" + string(context.SessionID),
		}
	}
}

func (s *State) ensure() {
	if s.plans == nil {
		s.plans = map[session.ID][]PlanItem{}
	}
	if s.outputs == nil {
		s.outputs = map[session.ID]map[string]RetainedOutput{}
	}
	if s.outputBytes == nil {
		s.outputBytes = map[session.ID]int64{}
	}
}

func (s *State) maxRetainedBytes() int64 {
	if s.maxSet {
		return s.MaxRetainedBytes
	}
	if s.MaxRetainedBytes > 0 {
		return s.MaxRetainedBytes
	}
	return defaultMaxRetainedBytes
}

func (s *State) maxSessionBytes() int64 {
	if s.MaxSessionBytes != 0 {
		return s.MaxSessionBytes
	}
	return defaultMaxRetainedBytes * 4
}

func clonePlan(src []PlanItem) []PlanItem {
	if src == nil {
		return nil
	}
	dst := make([]PlanItem, len(src))
	copy(dst, src)
	return dst
}

func validUTF8Prefix(content string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if limit > len(content) {
		limit = len(content)
	}
	for limit > 0 && !utf8.ValidString(content[:limit]) {
		limit--
	}
	return content[:limit]
}

func normalizeSubagentInput(_ context.Context, input any) (json.RawMessage, error) {
	value := input.(subagentInput)
	if value.Task == "" {
		return nil, fmt.Errorf("task required")
	}
	return json.Marshal(map[string]string{
		"task":               value.Task,
		"permission_pattern": value.Task,
	})
}

func normalizeSkillInput(_ context.Context, input any) (json.RawMessage, error) {
	value := input.(skillInput)
	if value.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	return json.Marshal(map[string]string{
		"name":               value.Name,
		"permission_pattern": value.Name,
	})
}
