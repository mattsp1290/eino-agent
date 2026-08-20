package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

var ErrExtensionPlanMismatch = errors.New("extension plan mismatch")

type RunPlanRequest struct {
	SessionID session.ID
	Config    config.Snapshot
}

type RunPlanProvider interface {
	AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error)
	AcquireResumePlan(context.Context, session.ExtensionPlanDescriptor) (*RunPlan, error)
}

type RunPlan struct {
	Dispatch   *extension.Plan
	Tools      ToolRegistry
	Prompts    []MountedPrompt
	Guards     []MountedToolGuard
	Descriptor session.ExtensionPlanDescriptor
	// RequiresToolSettlement is set when the frozen plan contains a mounted
	// tool. Strict plans reject stores without atomic settlement before durable
	// admission or resume mutation.
	RequiresToolSettlement bool
	Release                func()
	once                   sync.Once
}

const (
	OrderHostPolicy    = -1000
	OrderCompatibility = 0
	OrderApplication   = 1000
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

type PromptSection struct {
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

type ToolGuardDecision string

const (
	ToolGuardAbstain ToolGuardDecision = "abstain"
	ToolGuardDeny    ToolGuardDecision = "deny"
)

type ToolGuardRequest struct {
	SessionID session.ID
	RunID     session.RunID
	Call      ToolCall
	ToolName  string
}

type ToolGuardResult struct {
	Decision ToolGuardDecision
	Code     string
	Message  string
}

type ToolGuard interface {
	GuardTool(context.Context, ToolGuardRequest) (ToolGuardResult, error)
}

type ToolGuardFunc func(context.Context, ToolGuardRequest) (ToolGuardResult, error)

func (f ToolGuardFunc) GuardTool(ctx context.Context, request ToolGuardRequest) (ToolGuardResult, error) {
	return f(ctx, request)
}

type MountedToolGuard struct {
	ID         string
	Order      int
	InstanceID string
	Guard      ToolGuard
}

func (p *RunPlan) release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.Dispatch != nil {
			p.Dispatch.Release()
		}
		if p.Release != nil {
			p.Release()
		}
	})
}

type ClassifiedError struct {
	Code      string
	Message   string
	Retryable bool
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
	Base          []*einoschema.Message
	Contributions []ContextContribution
}

type ContextContribution struct {
	Source  string
	Order   int
	Message *einoschema.Message
}

type ModelStreamInput struct {
	Resolved model.Resolved
	Request  model.Request
}

type PreparedToolCall struct {
	Tool Tool
	Call ToolCall
}

type ToolDisposition string

const (
	ToolExecuted         ToolDisposition = "executed"
	ToolDenied           ToolDisposition = "denied"
	ToolApprovalRequired ToolDisposition = "approval-required"
	ToolInterrupted      ToolDisposition = "interrupted"
	ToolFailed           ToolDisposition = "failed"
)

type ToolExecution struct {
	Tool Tool
	Call ToolCall
}

type ToolOutcome struct {
	Call               ToolCall
	Disposition        ToolDisposition
	Result             ToolResult
	RawError           error
	Error              ClassifiedError
	PermissionMetadata map[string]string
	seal               *toolOutcomeSeal
}

type toolOutcomeSeal struct {
	Call               ToolCall
	Disposition        ToolDisposition
	RawError           error
	Error              ClassifiedError
	PermissionMetadata map[string]string
}

type RunAdmittedNotice struct {
	SessionID session.ID
	RunID     session.RunID
	Plan      session.ExtensionPlanDescriptor
	Time      time.Time
}

type RunStartedNotice struct {
	SessionID session.ID
	RunID     session.RunID
	Time      time.Time
}

type RunSettledNotice struct {
	SessionID session.ID
	Result    Result
	Duration  time.Duration
	Error     ClassifiedError
}

type ModelRequestedNotice struct {
	SessionID       session.ID
	RunID           session.RunID
	MessageID       session.MessageID
	Attempt         int
	Step            int
	ProviderID      string
	ModelID         string
	RequestRecordID session.ModelRequestID
	MessageCount    int
	ToolCount       int
	ContentHash     string
}

type ModelCompletedNotice struct {
	SessionID session.ID
	RunID     session.RunID
	MessageID session.MessageID
	Attempt   int
	Step      int
	Usage     Usage
	Error     ClassifiedError
}

type ToolPreparedNotice struct {
	SessionID  session.ID
	RunID      session.RunID
	MessageID  session.MessageID
	ToolCallID session.ToolCallID
	ToolName   string
	Input      json.RawMessage
	Component  map[string]string
}

type ToolStartedNotice struct {
	SessionID  session.ID
	RunID      session.RunID
	ToolCallID session.ToolCallID
	ToolName   string
	Time       time.Time
}

type ToolSettledNotice struct {
	SessionID  session.ID
	RunID      session.RunID
	ToolCallID session.ToolCallID
	ToolName   string
	Status     session.ToolCallStatus
	Result     ToolResult
	Error      ClassifiedError
}

var (
	RunBeforeExecutePoint    = extension.NewInterceptor(extension.Contract{ID: "eino-agent/runtime/run-before-execute", Version: "1"}, func(value RunGateInput) RunGateInput { return value }, validateRunGateInput, validateRunDecision)
	ContextAssemblePoint     = extension.NewInterceptorWithResultValidation(extension.Contract{ID: "eino-agent/runtime/context-assemble", Version: "1"}, cloneContextAssembly, validateContextAssemblyInput, validateContextAssembly, validateContextAssemblyResult)
	ModelStreamPoint         = extension.NewRequiredDelegatingInterceptor(extension.Contract{ID: "eino-agent/runtime/model-stream", Version: "1"}, cloneModelStreamInput, validateModelStreamInput, validateStreamReader, validateDelegatedStreamReader)
	ToolPreparePoint         = extension.NewInterceptorWithResultValidation(extension.Contract{ID: "eino-agent/runtime/tool-prepare", Version: "1"}, clonePreparedToolCall, validatePreparedToolCallInput, validatePreparedToolCall, validatePreparedToolCallResult)
	ToolExecutePoint         = extension.NewRequiredInterceptorWithResultValidation(extension.Contract{ID: "eino-agent/runtime/tool-execute", Version: "1"}, cloneToolExecution, validateToolExecutionInput, validateToolOutcome, validateToolExecutionResult)
	ToolResultTransformPoint = extension.NewInterceptorWithResultValidation(extension.Contract{ID: "eino-agent/runtime/tool-result-transform", Version: "1"}, cloneToolOutcome, validateToolOutcomeInput, validateToolOutcome, validateToolOutcomeResult)

	RunAdmittedPoint    = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/run-admitted", Version: "1"}, extension.NotificationContained, cloneRunAdmittedNotice)
	RunStartedPoint     = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/run-started", Version: "1"}, extension.NotificationContained, func(value RunStartedNotice) RunStartedNotice { return value })
	RunSettledPoint     = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/run-settled", Version: "1"}, extension.NotificationContained, cloneRunSettledNotice)
	ModelRequestedPoint = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/model-requested", Version: "1"}, extension.NotificationContained, func(value ModelRequestedNotice) ModelRequestedNotice { return value })
	ModelCompletedPoint = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/model-completed", Version: "1"}, extension.NotificationContained, func(value ModelCompletedNotice) ModelCompletedNotice { return value })
	ToolPreparedPoint   = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/tool-prepared", Version: "1"}, extension.NotificationContained, cloneToolPreparedNotice)
	ToolStartedPoint    = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/tool-started", Version: "1"}, extension.NotificationContained, func(value ToolStartedNotice) ToolStartedNotice { return value })
	ToolSettledPoint    = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/tool-settled", Version: "1"}, extension.NotificationContained, cloneToolSettledNotice)
	EventPublishedPoint = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/event-published", Version: "1"}, extension.NotificationContained, cloneEvent)
)

type runPlanContextKey struct{}

func withRunPlan(ctx context.Context, plan *RunPlan) context.Context {
	if plan == nil {
		return ctx
	}
	return context.WithValue(ctx, runPlanContextKey{}, plan)
}

func runPlanFromContext(ctx context.Context) *RunPlan {
	plan, _ := ctx.Value(runPlanContextKey{}).(*RunPlan)
	return plan
}

func (o *StreamingOrchestrator) acquireRunPlan(ctx context.Context, request RunPlanRequest) (*RunPlan, error) {
	if o.Plans == nil {
		descriptor := session.ExtensionPlanDescriptor{SchemaVersion: 1, Mode: session.PlanLegacy}
		descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
		return &RunPlan{Descriptor: descriptor}, nil
	}
	plan, err := o.Plans.AcquireRunPlan(ctx, RunPlanRequest{SessionID: request.SessionID, Config: request.Config.Clone()})
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, fmt.Errorf("%w: provider returned nil plan", ErrExtensionPlanMismatch)
	}
	if plan.Descriptor.SchemaVersion == 0 {
		plan.Descriptor.SchemaVersion = 1
	}
	if plan.Descriptor.Mode == "" {
		plan.Descriptor.Mode = session.PlanStrict
	}
	providedFingerprint := plan.Descriptor.Fingerprint
	plan.Descriptor.Fingerprint = ""
	fingerprint, fingerprintErr := session.FingerprintExtensionPlan(plan.Descriptor)
	if fingerprintErr != nil {
		plan.release()
		return nil, fingerprintErr
	}
	if providedFingerprint != "" && providedFingerprint != fingerprint {
		plan.release()
		return nil, fmt.Errorf("%w: invalid fresh descriptor fingerprint", ErrExtensionPlanMismatch)
	}
	if o.Tools != nil || len(o.Context) != 0 || len(o.Hooks) != 0 || len(o.Middleware) != 0 {
		plan.Descriptor.Mode = session.PlanPartialLegacy
	}
	fingerprint, fingerprintErr = session.FingerprintExtensionPlan(plan.Descriptor)
	if fingerprintErr != nil {
		plan.release()
		return nil, fingerprintErr
	}
	plan.Descriptor.Fingerprint = fingerprint
	if plan.Descriptor.Mode == session.PlanStrict && plan.RequiresToolSettlement {
		if _, ok := o.Store.(session.ToolSettlementStore); !ok {
			plan.release()
			return nil, fmt.Errorf("%w: strict tool plan requires ToolSettlementStore", ErrInvalidOrchestrator)
		}
	}
	return plan, nil
}

func (o *StreamingOrchestrator) acquireResumePlan(ctx context.Context, descriptor session.ExtensionPlanDescriptor) (*RunPlan, error) {
	if descriptor.SchemaVersion == 0 || descriptor.Mode == "" || descriptor.Mode == session.PlanLegacy {
		return &RunPlan{Descriptor: descriptor.Clone()}, nil
	}
	if o.Plans == nil {
		return nil, fmt.Errorf("%w: run requires a plan provider", ErrExtensionPlanMismatch)
	}
	plan, err := o.Plans.AcquireResumePlan(ctx, descriptor.Clone())
	if err != nil {
		return nil, err
	}
	if plan == nil || plan.Descriptor.Fingerprint != descriptor.Fingerprint {
		if plan != nil {
			plan.release()
		}
		return nil, ErrExtensionPlanMismatch
	}
	if descriptorHasTools(descriptor) {
		if _, ok := o.Store.(session.ToolSettlementStore); !ok {
			plan.release()
			return nil, fmt.Errorf("%w: strict tool plan requires ToolSettlementStore", ErrInvalidOrchestrator)
		}
	}
	return plan, nil
}

func descriptorHasTools(descriptor session.ExtensionPlanDescriptor) bool {
	for _, entry := range descriptor.Entries {
		if entry.Kind == session.ExtensionTool && entry.Required {
			return true
		}
	}
	return false
}

func (o *StreamingOrchestrator) eventSink(ctx context.Context) EventSink {
	plan := runPlanFromContext(ctx)
	if plan == nil || plan.Dispatch == nil {
		return o.Events
	}
	return compositeEventSink{infrastructure: o.Events, plan: plan.Dispatch}
}

type compositeEventSink struct {
	infrastructure EventSink
	plan           *extension.Plan
}

func (s compositeEventSink) Emit(ctx context.Context, event Event) error {
	var err error
	if s.infrastructure != nil {
		err = s.infrastructure.Emit(ctx, cloneEvent(event))
	}
	_ = extension.Notify(s.plan, ctx, EventPublishedPoint, event)
	return err
}

func cloneRunAdmittedNotice(value RunAdmittedNotice) RunAdmittedNotice {
	value.Plan = value.Plan.Clone()
	return value
}

func cloneRunSettledNotice(value RunSettledNotice) RunSettledNotice {
	return value
}

func cloneToolPreparedNotice(value ToolPreparedNotice) ToolPreparedNotice {
	value.Input = cloneJSON(value.Input)
	value.Component = cloneStringMap(value.Component)
	return value
}

func cloneToolSettledNotice(value ToolSettledNotice) ToolSettledNotice {
	value.Result.Structured = cloneJSON(value.Result.Structured)
	value.Result.Attachments = cloneSlice(value.Result.Attachments)
	value.Result.Metadata = cloneStringMap(value.Result.Metadata)
	return value
}

func cloneEvent(value Event) Event {
	value.Payload = cloneJSON(value.Payload)
	return value
}

func classifyExtensionError(err error) ClassifiedError {
	if err == nil {
		return ClassifiedError{}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassifiedError{Code: "interrupted", Message: "operation interrupted", Retryable: true}
	}
	var callback *extension.CallbackError
	if errors.As(err, &callback) {
		return ClassifiedError{Code: callback.Code, Message: "extension callback failed"}
	}
	return ClassifiedError{Code: "operation_failed", Message: "operation failed"}
}

func modelRequestContentHash(request model.Request) string {
	raw, _ := json.Marshal(struct {
		Messages any
		System   string
		Tools    any
	}{Messages: request.Messages, System: request.System, Tools: request.Tools})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (o *StreamingOrchestrator) renderSystemPrompt(ctx context.Context, snapshot TurnSnapshot, attempt, step int) (string, error) {
	sections := make([]PromptSection, 0)
	if o.SystemPromptMaterialization && snapshot.SystemPrompt != "" {
		sections = append(sections, PromptSection{Name: "agent/system", Order: OrderCompatibility, Text: snapshot.SystemPrompt, InstanceID: "runtime"})
	}
	if plan := runPlanFromContext(ctx); plan != nil {
		promptContext := PromptContext{SessionID: snapshot.SessionID, RunID: snapshot.RunID, EpochID: snapshot.EpochID, Attempt: attempt, Step: step, AgentName: snapshot.Config.Agent.Name, ProviderID: string(snapshot.Model.Provider.ID), ModelID: string(snapshot.Model.Model.ID)}
		for _, mounted := range plan.Prompts {
			if mounted.Provider == nil {
				return "", errors.New("nil prompt provider")
			}
			text, err := mounted.Provider.ProvidePrompt(ctx, promptContext)
			if err != nil {
				return "", err
			}
			if text != "" {
				sections = append(sections, PromptSection{Name: mounted.Name, Order: mounted.Order, Text: text, InstanceID: mounted.InstanceID})
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

func evaluateToolGuards(ctx context.Context, plan *RunPlan, tool Tool, call ToolCall) (ToolGuardResult, error) {
	if plan == nil || len(plan.Guards) == 0 {
		return ToolGuardResult{Decision: ToolGuardAbstain}, nil
	}
	request := ToolGuardRequest{SessionID: call.SessionID, RunID: call.RunID, Call: cloneToolCall(call), ToolName: tool.Name}
	denial := ToolGuardResult{Decision: ToolGuardAbstain}
	for _, mounted := range plan.Guards {
		if mounted.Guard == nil {
			return ToolGuardResult{}, errors.New("nil tool guard")
		}
		result, err := mounted.Guard.GuardTool(ctx, request)
		if err != nil {
			return ToolGuardResult{}, err
		}
		switch result.Decision {
		case ToolGuardAbstain:
		case ToolGuardDeny:
			if denial.Decision != ToolGuardDeny {
				denial = result
				if denial.Code == "" {
					denial.Code = "denied"
				}
				if denial.Message == "" {
					denial.Message = "tool call denied by mounted guard"
				}
			}
		default:
			return ToolGuardResult{}, errors.New("invalid tool guard decision")
		}
	}
	return denial, nil
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

func cloneContextAssembly(value ContextAssembly) ContextAssembly {
	value.Base = cloneMessages(value.Base)
	contributions := value.Contributions
	value.Contributions = make([]ContextContribution, len(contributions))
	for index, contribution := range contributions {
		value.Contributions[index] = contribution
		value.Contributions[index].Message = cloneMessageDeep(contribution.Message)
	}
	return value
}

func validateContextAssemblyInput(original, candidate ContextAssembly) error {
	if original.SessionID != candidate.SessionID || original.RunID != candidate.RunID || original.EpochID != candidate.EpochID || !reflect.DeepEqual(original.Base, candidate.Base) {
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
		if seen[contribution.Source] {
			return fmt.Errorf("duplicate context contribution %q", contribution.Source)
		}
		seen[contribution.Source] = true
	}
	return nil
}

func validateContextAssemblyResult(original ContextAssembly, output ContextAssembly) error {
	return validateContextAssemblyInput(original, output)
}

func materializeContextAssembly(value ContextAssembly) []*einoschema.Message {
	sort.Slice(value.Contributions, func(i, j int) bool {
		if value.Contributions[i].Order != value.Contributions[j].Order {
			return value.Contributions[i].Order < value.Contributions[j].Order
		}
		return value.Contributions[i].Source < value.Contributions[j].Source
	})
	messages := cloneMessages(value.Base)
	for _, contribution := range value.Contributions {
		messages = append(messages, cloneMessageDeep(contribution.Message))
	}
	return messages
}

func cloneModelStreamInput(value ModelStreamInput) ModelStreamInput {
	value.Request = value.Request.Clone()
	value.Resolved.Provider.Options = cloneStringMap(value.Resolved.Provider.Options)
	value.Resolved.Provider.Environment = cloneSlice(value.Resolved.Provider.Environment)
	value.Resolved.Model.Options = cloneStringMap(value.Resolved.Model.Options)
	if value.Resolved.Model.Capabilities != nil {
		capabilities := make(map[string]bool, len(value.Resolved.Model.Capabilities))
		for key, enabled := range value.Resolved.Model.Capabilities {
			capabilities[key] = enabled
		}
		value.Resolved.Model.Capabilities = capabilities
	}
	return value
}

func validateModelStreamInput(original, candidate ModelStreamInput) error {
	if !reflect.DeepEqual(original, candidate) {
		return extension.ErrProtectedMutation
	}
	return nil
}

func validateStreamReader(reader *einoschema.StreamReader[*einoschema.Message]) error {
	if reader == nil {
		return errors.New("nil provider stream")
	}
	return nil
}

func validateDelegatedStreamReader(delegated, returned *einoschema.StreamReader[*einoschema.Message]) error {
	if delegated != returned {
		return extension.ErrProtectedMutation
	}
	return nil
}

func clonePreparedToolCall(value PreparedToolCall) PreparedToolCall {
	value.Tool = cloneTool(value.Tool)
	value.Call = cloneToolCall(value.Call)
	return value
}

func validatePreparedToolCallInput(original, candidate PreparedToolCall) error {
	left, right := clonePreparedToolCall(original), clonePreparedToolCall(candidate)
	left.Call.Input, right.Call.Input = nil, nil
	left.Call.Pattern, right.Call.Pattern = "", ""
	if !reflect.DeepEqual(left, right) || !json.Valid(candidate.Call.Input) {
		return extension.ErrProtectedMutation
	}
	return nil
}

func validatePreparedToolCall(value PreparedToolCall) error {
	if value.Call.ID == "" || value.Call.Name == "" || !json.Valid(value.Call.Input) {
		return errors.New("invalid prepared tool call")
	}
	return nil
}

func validatePreparedToolCallResult(original PreparedToolCall, output PreparedToolCall) error {
	return validatePreparedToolCallInput(original, output)
}

func cloneToolExecution(value ToolExecution) ToolExecution {
	value.Tool = cloneTool(value.Tool)
	value.Call = cloneToolCall(value.Call)
	return value
}

func validateToolExecutionInput(original, candidate ToolExecution) error {
	if !reflect.DeepEqual(cloneToolExecution(original), cloneToolExecution(candidate)) {
		return extension.ErrProtectedMutation
	}
	return nil
}

func validateToolExecutionResult(original ToolExecution, output ToolOutcome) error {
	if output.seal == nil || !reflect.DeepEqual(original.Call, output.Call) {
		return extension.ErrProtectedMutation
	}
	return validateToolOutcome(output)
}

func cloneToolOutcome(value ToolOutcome) ToolOutcome {
	value.Call = cloneToolCall(value.Call)
	value.Result = cloneRuntimeToolResult(value.Result)
	value.PermissionMetadata = cloneStringMap(value.PermissionMetadata)
	return value
}

func sealToolOutcome(value ToolOutcome) ToolOutcome {
	value.seal = &toolOutcomeSeal{Call: cloneToolCall(value.Call), Disposition: value.Disposition, RawError: value.RawError, Error: value.Error, PermissionMetadata: cloneStringMap(value.PermissionMetadata)}
	return value
}

func validateToolOutcome(value ToolOutcome) error {
	switch value.Disposition {
	case ToolExecuted, ToolDenied, ToolApprovalRequired, ToolInterrupted, ToolFailed:
	default:
		return errors.New("invalid tool outcome disposition")
	}
	if value.seal != nil {
		protected := toolOutcomeSeal{Call: value.Call, Disposition: value.Disposition, RawError: value.RawError, Error: value.Error, PermissionMetadata: value.PermissionMetadata}
		if !reflect.DeepEqual(*value.seal, protected) {
			return extension.ErrProtectedMutation
		}
	}
	return nil
}

func validateToolOutcomeInput(original, candidate ToolOutcome) error {
	left, right := cloneToolOutcome(original), cloneToolOutcome(candidate)
	left.Result, right.Result = ToolResult{}, ToolResult{}
	if !reflect.DeepEqual(left, right) {
		return extension.ErrProtectedMutation
	}
	return validateToolOutcome(candidate)
}

func validateToolOutcomeResult(original ToolOutcome, output ToolOutcome) error {
	return validateToolOutcomeInput(original, output)
}

func cloneTool(tool Tool) Tool {
	tool.Scope.Permissions = cloneSlice(tool.Scope.Permissions)
	tool.Metadata = cloneStringMap(tool.Metadata)
	if tool.Info != nil {
		raw, _ := json.Marshal(tool.Info)
		var info einoschema.ToolInfo
		if json.Unmarshal(raw, &info) == nil {
			tool.Info = &info
		}
	}
	return tool
}

func cloneToolCall(call ToolCall) ToolCall {
	call.Input = cloneJSON(call.Input)
	call.Scope.Permissions = cloneSlice(call.Scope.Permissions)
	return call
}

func cloneRuntimeToolResult(result ToolResult) ToolResult {
	result.Structured = cloneJSON(result.Structured)
	result.Attachments = cloneSlice(result.Attachments)
	result.Metadata = cloneStringMap(result.Metadata)
	return result
}

func cloneMessageDeep(message *einoschema.Message) *einoschema.Message {
	if message == nil {
		return nil
	}
	raw, err := json.Marshal(message)
	if err != nil {
		clone := *message
		return &clone
	}
	var clone einoschema.Message
	if json.Unmarshal(raw, &clone) != nil {
		fallback := *message
		return &fallback
	}
	return &clone
}
