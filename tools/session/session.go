package sessiontools

import (
	"context"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
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

// Mount publishes the session tools atomically through the canonical composition registry.
func Mount(ctx context.Context, registry *composition.Registry, component extension.Component, scope extension.Scope, options Options) (*composition.Mount, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: composition registry required", agenttools.ErrInvalidDefinition)
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
	return registry.Mount(ctx, component, composition.InstallerFunc(func(ctx context.Context, registrar *composition.Registrar) error {
		for index, definition := range definitions {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := registrar.Tool(composition.ToolRegistration{
				ID: definition.Name, Order: runtime.OrderApplication + index,
				Scope: scope, Definition: definition,
			}); err != nil {
				return err
			}
		}
		return nil
	}))
}

func planSetDefinition(state *State) agenttools.Definition {
	return agenttools.Definition{
		Name:        NamePlanSet,
		Description: "Replace the current session plan state.",
		Execute: agenttools.TypedExecutor[planSetInput, map[string]any](func(_ context.Context, execution agenttools.TypedExecution[planSetInput]) (map[string]any, error) {
			input := execution.Input
			items := clonePlan(input.Items)
			state.mu.Lock()
			state.ensure()
			state.plans[execution.Context.Turn.SessionID] = items
			state.mu.Unlock()
			return map[string]any{"items": items}, nil
		}),
		RetrySafe:   true,
		Scope:       sessionScope(),
		Permissions: []string{"session.plan"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096},
		Metadata:    map[string]string{"source": "session"},
	}
}

func planGetDefinition(state *State) agenttools.Definition {
	return agenttools.Definition{
		Name:        NamePlanGet,
		Description: "Read the current session plan state.",
		Execute: agenttools.TypedExecutor[struct{}, map[string]any](func(_ context.Context, execution agenttools.TypedExecution[struct{}]) (map[string]any, error) {
			state.mu.RLock()
			items := clonePlan(state.plans[execution.Context.Turn.SessionID])
			state.mu.RUnlock()
			return map[string]any{"items": items}, nil
		}),
		RetrySafe:   true,
		Scope:       sessionScope(),
		Permissions: []string{"session.plan"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096},
		Metadata:    map[string]string{"source": "session"},
	}
}

func retainOutputDefinition(state *State) agenttools.Definition {
	return agenttools.Definition{
		Name:        NameRetainedOutput,
		Description: "Retain bounded session output for later host retrieval.",
		Execute: agenttools.TypedExecutor[retainInput, RetainedOutput](func(_ context.Context, execution agenttools.TypedExecution[retainInput]) (RetainedOutput, error) {
			input := execution.Input
			if input.ID == "" {
				return RetainedOutput{}, fmt.Errorf("id required")
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
		}),
		RetrySafe:   true,
		Scope:       sessionScope(),
		Permissions: []string{"session.retained_output"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096, StoreExternal: true},
		Metadata:    map[string]string{"source": "session"},
	}
}

func subagentDefinition(runner SubagentRunner) agenttools.Definition {
	return agenttools.Definition{
		Name:        NameSubagent,
		Description: "Request a host-provided subagent task for this session.",
		Normalize:   agenttools.TypedNormalizer(normalizeSubagentInput),
		Pattern: agenttools.TypedPermissionPattern(func(_ context.Context, input subagentInput) (string, error) {
			return input.Task, nil
		}),
		Execute: agenttools.TypedExecutor[subagentInput, SubagentResult](func(ctx context.Context, execution agenttools.TypedExecution[subagentInput]) (SubagentResult, error) {
			input := execution.Input
			if input.Task == "" {
				return SubagentResult{}, fmt.Errorf("task required")
			}
			return runner.RunSubagent(ctx, SubagentRequest{SessionID: execution.Context.Turn.SessionID, Task: input.Task})
		}),
		Scope:       sessionScope(),
		Permissions: []string{"session.subagent"},
		Retention:   runtime.RetentionPolicy{MaxInlineBytes: 4096},
		Metadata:    map[string]string{"source": "session"},
	}
}

func skillLoadDefinition(loader SkillLoader) agenttools.Definition {
	return agenttools.Definition{
		Name:        NameSkillLoad,
		Description: "Request host-provided skill loading for this session.",
		Normalize:   agenttools.TypedNormalizer(normalizeSkillInput),
		Pattern: agenttools.TypedPermissionPattern(func(_ context.Context, input skillInput) (string, error) {
			return input.Name, nil
		}),
		Execute: agenttools.TypedExecutor[skillInput, SkillResult](func(ctx context.Context, execution agenttools.TypedExecution[skillInput]) (SkillResult, error) {
			input := execution.Input
			if input.Name == "" {
				return SkillResult{}, fmt.Errorf("name required")
			}
			return loader.LoadSkill(ctx, SkillRequest{SessionID: execution.Context.Turn.SessionID, Name: input.Name})
		}),
		Scope:       sessionScope(),
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

func sessionScope() agenttools.ScopeResolver {
	return func(_ context.Context, context runtime.ToolScopeContext) runtime.ToolScope {
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

func normalizeSubagentInput(_ context.Context, value subagentInput) (subagentInput, error) {
	if value.Task == "" {
		return subagentInput{}, fmt.Errorf("task required")
	}
	return value, nil
}

func normalizeSkillInput(_ context.Context, value skillInput) (skillInput, error) {
	if value.Name == "" {
		return skillInput{}, fmt.Errorf("name required")
	}
	return value, nil
}
