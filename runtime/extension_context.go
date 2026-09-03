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
	OrderHostPolicy         = -1000
	OrderRuntime            = 0
	OrderApplication        = 1000
	systemPromptSectionName = "agent/system"
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
	ID         string
	Scope      extension.Scope
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

type contextAssembly struct {
	SessionID     session.ID
	RunID         session.RunID
	EpochID       session.EpochID
	Metadata      BoundedTurnMetadata
	Base          []*einoschema.Message
	Contributions []contextContribution
}

type contextContribution struct {
	Source  string
	Order   int
	Message *einoschema.Message
}

// ContextSourceInput is the bounded, read-only runtime state exposed to a
// context source. It excludes the base conversation and other sources' output.
type ContextSourceInput struct {
	SessionID session.ID
	RunID     session.RunID
	EpochID   session.EpochID
	Metadata  BoundedTurnMetadata
}

// ContextSource returns independent context messages. The host owns their
// source identity and ordering.
type ContextSource func(context.Context, ContextSourceInput) ([]*einoschema.Message, error)

// OnContextSource registers a context source without exposing cumulative
// context assembly to the extension.
func OnContextSource(registrar extension.Registrar, spec extension.Registration, source ContextSource) error {
	if registrar == nil || source == nil {
		return fmt.Errorf("%w: nil registrar or context source", extension.ErrInvalidRegistration)
	}
	instanceID := registrar.InstanceID()
	return extension.OnTransform(registrar, contextAssemblePoint, spec, func(ctx context.Context, assembly contextAssembly) (contextAssembly, error) {
		input := ContextSourceInput{
			SessionID: assembly.SessionID,
			RunID:     assembly.RunID,
			EpochID:   assembly.EpochID,
			Metadata:  cloneBoundedTurnMetadata(assembly.Metadata),
		}
		messages, err := source(ctx, input)
		if err != nil {
			return contextAssembly{}, err
		}
		for index, message := range messages {
			assembly.Contributions = append(assembly.Contributions, contextContribution{
				Source:  contextContributionSource(instanceID, spec, index),
				Order:   spec.Order,
				Message: message,
			})
		}
		return assembly, nil
	})
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
		sections = append(sections, promptSection{Name: systemPromptSectionName, Order: OrderRuntime, Text: snapshot.SystemPrompt, InstanceID: "runtime"})
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
	for _, section := range sections {
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

func cloneContextAssembly(value contextAssembly) (contextAssembly, error) {
	value.Metadata = cloneBoundedTurnMetadata(value.Metadata)
	var err error
	value.Base, err = cloneProtectedMessages(value.Base)
	if err != nil {
		return contextAssembly{}, err
	}
	contributions := value.Contributions
	value.Contributions = make([]contextContribution, len(contributions))
	for index, contribution := range contributions {
		value.Contributions[index] = contribution
		value.Contributions[index].Message, err = cloneMessageDeep(contribution.Message)
		if err != nil {
			return contextAssembly{}, err
		}
	}
	return value, nil
}

func validateContextAssemblyInput(original, candidate contextAssembly) error {
	if original.SessionID != candidate.SessionID || original.RunID != candidate.RunID || original.EpochID != candidate.EpochID || !reflect.DeepEqual(original.Metadata, candidate.Metadata) || !reflect.DeepEqual(original.Base, candidate.Base) {
		return extension.ErrProtectedMutation
	}
	return validateContextAssembly(candidate)
}

func validateContextAssembly(value contextAssembly) error {
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

func contextContributionSource(instanceID string, spec extension.Registration, index int) string {
	parts := []string{instanceID, spec.ID, string(spec.Scope.Kind), spec.Scope.Key}
	return fmt.Sprintf("context/%d:%s/%d:%s/%d:%s/%d:%s/%06d", len(parts[0]), parts[0], len(parts[1]), parts[1], len(parts[2]), parts[2], len(parts[3]), parts[3], index)
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

func materializeContextAssembly(value contextAssembly) ([]*einoschema.Message, error) {
	materialized, err := materializeContextAssemblyWithMapping(value)
	return materialized.Messages, err
}

type materializedContext struct {
	Messages    []*einoschema.Message
	BaseToFinal []int
}

func materializeContextAssemblyWithMapping(value contextAssembly) (materializedContext, error) {
	if err := validateContextAssembly(value); err != nil {
		return materializedContext{}, err
	}
	contributions := append([]contextContribution(nil), value.Contributions...)
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
			return materializedContext{}, err
		}
		if message.Role == einoschema.System {
			prelude = append(prelude, message)
		} else {
			suffix = append(suffix, message)
		}
	}
	base, err := cloneProtectedMessages(value.Base)
	if err != nil {
		return materializedContext{}, err
	}
	messages := make([]*einoschema.Message, 0, len(prelude)+len(base)+len(suffix))
	messages = append(messages, prelude...)
	messages = append(messages, base...)
	messages = append(messages, suffix...)
	mapping := make([]int, len(base))
	for index := range base {
		mapping[index] = len(prelude) + index
	}
	return materializedContext{Messages: messages, BaseToFinal: mapping}, nil
}
