package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

const (
	OrderHostPolicy  = -1000
	OrderRuntime     = 0
	OrderApplication = 1000
)

type PromptContext struct {
	SessionID  session.ID
	RunID      session.RunID
	EpochID    session.EpochID
	Attempt    int
	Step       int
	AgentName  string
	ProviderID string
	ModelID    string
}

type promptSection struct {
	Name       string
	Order      int
	Text       string
	InstanceID string
}

type PromptProvider interface {
	ProvidePrompt(context.Context, PromptContext) (string, error)
}

type PromptProviderFunc func(context.Context, PromptContext) (string, error)

func (f PromptProviderFunc) ProvidePrompt(ctx context.Context, prompt PromptContext) (string, error) {
	return f(ctx, prompt)
}

type MountedPrompt struct {
	Name       string
	Order      int
	InstanceID string
	Provider   PromptProvider
}

type RunGateInput struct {
	SessionID  session.ID
	RunID      session.RunID
	ProviderID string
	ModelID    string
}

type RunDecisionKind string

const (
	RunContinue RunDecisionKind = "continue"
	RunReject   RunDecisionKind = "reject"
)

type RunDecision struct {
	Kind    RunDecisionKind
	Code    string
	Message string
}

type ContextAssembly struct {
	SessionID     session.ID
	RunID         session.RunID
	EpochID       session.EpochID
	Metadata      BoundedTurnMetadata
	Base          []*einoschema.Message
	Contributions []ContextContribution
}

type ContextContribution struct {
	Source  string
	Order   int
	Message *einoschema.Message
}

// BoundedTurnMetadata is the content-free turn projection exposed through
// extension points. It deliberately excludes messages, prompts, config maps,
// provider clients, tool executors, and every other callable runtime value.
type BoundedTurnMetadata struct {
	RunID           session.RunID
	SessionID       session.ID
	EpochID         session.EpochID
	AgentName       string
	AgentMode       string
	ProviderID      string
	ModelID         string
	ToolNames       []string
	MessageCount    uint32
	RoleCounts      MessageRoleCounts
	HasSystemPrompt bool
}

type MessageRoleCounts struct {
	System    uint32
	User      uint32
	Assistant uint32
	Tool      uint32
}

func (o *StreamingOrchestrator) renderSystemPrompt(ctx context.Context, plan *RunPlan, snapshot TurnSnapshot, attempt, step int) (string, error) {
	sections := make([]promptSection, 0)
	if snapshot.SystemPrompt != "" {
		sections = append(sections, promptSection{Name: "agent/system", Order: OrderRuntime, Text: snapshot.SystemPrompt, InstanceID: "runtime"})
	}
	if plan != nil {
		promptContext := PromptContext{SessionID: snapshot.SessionID, RunID: snapshot.RunID, EpochID: snapshot.EpochID, Attempt: attempt, Step: step, AgentName: snapshot.Config.Agent.Name, ProviderID: string(snapshot.Model.Provider.ID), ModelID: string(snapshot.Model.Model.ID)}
		for _, mounted := range plan.prompts {
			if mounted.Provider == nil {
				return "", errors.New("nil prompt provider")
			}
			text, err := mounted.Provider.ProvidePrompt(ctx, promptContext)
			if err != nil {
				return "", err
			}
			if text != "" {
				sections = append(sections, promptSection{Name: mounted.Name, Order: mounted.Order, Text: text, InstanceID: mounted.InstanceID})
			}
		}
	}
	sort.Slice(sections, func(i, j int) bool {
		if sections[i].Order != sections[j].Order {
			return sections[i].Order < sections[j].Order
		}
		if sections[i].Name != sections[j].Name {
			return sections[i].Name < sections[j].Name
		}
		return sections[i].InstanceID < sections[j].InstanceID
	})
	parts := make([]string, 0, len(sections))
	seen := make(map[string]bool, len(sections))
	for _, section := range sections {
		if seen[section.Name] {
			return "", fmt.Errorf("duplicate prompt section %q", section.Name)
		}
		seen[section.Name] = true
		parts = append(parts, section.Text)
	}
	return strings.Join(parts, "\n\n"), nil
}

func validateRunGateInput(original, candidate RunGateInput) error {
	if original != candidate {
		return extension.ErrProtectedMutation
	}
	return nil
}

func validateRunDecision(decision RunDecision) error {
	switch decision.Kind {
	case RunContinue:
		if decision.Code != "" || decision.Message != "" {
			return errors.New("continue decision cannot carry rejection detail")
		}
	case RunReject:
		if decision.Code == "" {
			return errors.New("reject decision requires a code")
		}
	default:
		return errors.New("invalid run decision")
	}
	return nil
}

func cloneContextAssembly(value ContextAssembly) (ContextAssembly, error) {
	value.Metadata = cloneBoundedTurnMetadata(value.Metadata)
	var err error
	value.Base, err = cloneProtectedMessages(value.Base)
	if err != nil {
		return ContextAssembly{}, err
	}
	contributions := value.Contributions
	value.Contributions = make([]ContextContribution, len(contributions))
	for index, contribution := range contributions {
		value.Contributions[index] = contribution
		value.Contributions[index].Message, err = cloneMessageDeep(contribution.Message)
		if err != nil {
			return ContextAssembly{}, err
		}
	}
	return value, nil
}

func validateContextAssemblyInput(original, candidate ContextAssembly) error {
	if original.SessionID != candidate.SessionID || original.RunID != candidate.RunID || original.EpochID != candidate.EpochID || !reflect.DeepEqual(original.Metadata, candidate.Metadata) || !reflect.DeepEqual(original.Base, candidate.Base) {
		return extension.ErrProtectedMutation
	}
	return validateContextAssembly(candidate)
}

func validateContextAssembly(value ContextAssembly) error {
	seen := make(map[string]bool, len(value.Contributions))
	for _, contribution := range value.Contributions {
		if contribution.Source == "" {
			return errors.New("context contribution source required")
		}
		if contribution.Message == nil {
			return fmt.Errorf("context contribution %q message required", contribution.Source)
		}
		if err := validateContextContributionMessage(contribution.Message); err != nil {
			return fmt.Errorf("context contribution %q: %w", contribution.Source, err)
		}
		if _, err := cloneMessageDeep(contribution.Message); err != nil {
			return err
		}
		if seen[contribution.Source] {
			return fmt.Errorf("duplicate context contribution %q", contribution.Source)
		}
		seen[contribution.Source] = true
	}
	return nil
}

func validateContextContributionMessage(message *einoschema.Message) error {
	if message == nil {
		return errors.New("message required")
	}
	if message.Role != einoschema.System && message.Role != einoschema.User {
		return fmt.Errorf("unsupported role %q", message.Role)
	}
	if message.Name != "" || len(message.ToolCalls) != 0 || message.ToolCallID != "" || message.ToolName != "" || message.ResponseMeta != nil || message.ReasoningContent != "" || len(message.Extra) != 0 || len(message.UserInputMultiContent) != 0 || len(message.AssistantGenMultiContent) != 0 {
		return errors.New("only role and text content are supported")
	}
	//nolint:staticcheck // This boundary rejects the dependency's deprecated field.
	if len(message.MultiContent) != 0 {
		return errors.New("deprecated MultiContent is unsupported")
	}
	return nil
}

func cloneBoundedTurnMetadata(value BoundedTurnMetadata) BoundedTurnMetadata {
	value.ToolNames = cloneSlice(value.ToolNames)
	return value
}

// NewToolScopeContext projects trusted runtime state into the data-only value
// accepted by tool materializers.
func NewToolScopeContext(snapshot TurnSnapshot) ToolScopeContext {
	return ToolScopeContext{
		SessionID:     snapshot.SessionID,
		WorkspaceID:   snapshot.Config.Metadata["workspace_id"],
		WorkspaceRoot: snapshot.Config.Metadata["workspace_root"],
		EnabledTools:  cloneSlice(snapshot.Config.Tools.Enabled),
		DisabledTools: cloneSlice(snapshot.Config.Tools.Disabled),
	}
}

func toolContext(snapshot TurnSnapshot, tools []Tool) ToolContext {
	projected := snapshot
	projected.Tools = cloneSlice(tools)
	return ToolContext{
		Turn:          boundedTurnMetadata(projected),
		WorkspaceID:   snapshot.Config.Metadata["workspace_id"],
		WorkspaceRoot: snapshot.Config.Metadata["workspace_root"],
	}.Clone()
}

func boundedTurnMetadata(snapshot TurnSnapshot) BoundedTurnMetadata {
	toolNames := make([]string, 0, len(snapshot.Tools))
	for _, tool := range snapshot.Tools {
		toolNames = append(toolNames, tool.Name)
	}
	counts := MessageRoleCounts{}
	for _, message := range snapshot.Messages {
		if message == nil {
			continue
		}
		switch message.Role {
		case einoschema.System:
			counts.System = saturatingUint32Increment(counts.System)
		case einoschema.User:
			counts.User = saturatingUint32Increment(counts.User)
		case einoschema.Assistant:
			counts.Assistant = saturatingUint32Increment(counts.Assistant)
		case einoschema.Tool:
			counts.Tool = saturatingUint32Increment(counts.Tool)
		}
	}
	messageCount := len(snapshot.Messages)
	if messageCount > math.MaxUint32 {
		messageCount = math.MaxUint32
	}
	return BoundedTurnMetadata{
		RunID: snapshot.RunID, SessionID: snapshot.SessionID, EpochID: snapshot.EpochID,
		AgentName: snapshot.Config.Agent.Name, AgentMode: snapshot.Config.Agent.Mode,
		ProviderID: string(snapshot.Model.Provider.ID), ModelID: string(snapshot.Model.Model.ID),
		ToolNames: toolNames, MessageCount: uint32(messageCount), RoleCounts: counts,
		HasSystemPrompt: snapshot.SystemPrompt != "" || snapshot.Config.Agent.SystemPrompt != "",
	}
}

func saturatingUint32Increment(value uint32) uint32 {
	if value == math.MaxUint32 {
		return value
	}
	return value + 1
}

func validateBoundedTurnMetadataInput(original, candidate BoundedTurnMetadata) error {
	if !reflect.DeepEqual(original, candidate) {
		return extension.ErrProtectedMutation
	}
	return nil
}

func materializeContextAssembly(value ContextAssembly) ([]*einoschema.Message, error) {
	if err := validateContextAssembly(value); err != nil {
		return nil, err
	}
	contributions := append([]ContextContribution(nil), value.Contributions...)
	sort.Slice(contributions, func(i, j int) bool {
		if contributions[i].Order != contributions[j].Order {
			return contributions[i].Order < contributions[j].Order
		}
		return contributions[i].Source < contributions[j].Source
	})
	prelude := make([]*einoschema.Message, 0, len(contributions))
	suffix := make([]*einoschema.Message, 0, len(contributions))
	for _, contribution := range contributions {
		message, err := cloneMessageDeep(contribution.Message)
		if err != nil {
			return nil, err
		}
		if message.Role == einoschema.System {
			prelude = append(prelude, message)
		} else {
			suffix = append(suffix, message)
		}
	}
	base, err := cloneProtectedMessages(value.Base)
	if err != nil {
		return nil, err
	}
	messages := make([]*einoschema.Message, 0, len(prelude)+len(base)+len(suffix))
	messages = append(messages, prelude...)
	messages = append(messages, base...)
	messages = append(messages, suffix...)
	return messages, nil
}
