package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

type ClassifiedError struct {
	Code      string
	Message   string
	Retryable bool
}

type RunAdmittedNotice struct {
	SessionID session.ID
	RunID     session.RunID
	Plan      session.ExtensionPlanDescriptor
	Metadata  BoundedTurnMetadata
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
	Metadata  BoundedTurnMetadata
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
	Usage     session.Usage
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
	RunBeforeExecutePoint = extension.NewGate(extension.Contract{ID: "eino-agent/runtime/run-before-execute", Version: "1"}, infallibleClone(func(value RunGateInput) RunGateInput { return value }), validateRunGateInput, validateRunDecision, RunDecision{Kind: RunContinue}, func(decision RunDecision) bool {
		return decision.Kind == RunContinue
	})
	ContextAssemblePoint = extension.NewTransform(extension.Contract{ID: "eino-agent/runtime/context-assemble", Version: "1"}, cloneContextAssembly, validateContextAssemblyInput)
	TurnPreparePoint     = extension.NewHook(extension.Contract{ID: "eino-agent/runtime/turn-prepare", Version: "1"}, infallibleClone(cloneBoundedTurnMetadata), validateBoundedTurnMetadataInput)
	ModelStreamPoint     = extension.NewRequiredAround(extension.Contract{ID: "eino-agent/runtime/model-stream", Version: "1"}, cloneModelStreamInput, validateModelStreamInput, validateStreamReader, validateDelegatedStreamReader)
	ToolPreparePoint     = extension.NewTransform(extension.Contract{ID: "eino-agent/runtime/tool-prepare", Version: "1"}, clonePreparedToolCallChecked, validatePreparedToolCallInput)
	ToolExecutePoint     = extension.NewRequiredAround(extension.Contract{ID: "eino-agent/runtime/tool-execute", Version: "1"}, cloneToolExecutionChecked, validateToolExecutionInput, validateToolResult, func(_ ToolResult, returned ToolResult) error {
		return validateToolResult(returned)
	})
	ToolResultTransformPoint = extension.NewTransform(extension.Contract{ID: "eino-agent/runtime/tool-result-transform", Version: "1"}, cloneToolResultTransformChecked, validateToolResultTransformInput)

	RunAdmittedPoint    = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/run-admitted", Version: "1"}, infallibleClone(cloneRunAdmittedNotice))
	RunStartedPoint     = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/run-started", Version: "1"}, identityClone[RunStartedNotice])
	RunSettledPoint     = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/run-settled", Version: "1"}, infallibleClone(cloneRunSettledNotice))
	ModelRequestedPoint = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/model-requested", Version: "1"}, identityClone[ModelRequestedNotice])
	ModelCompletedPoint = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/model-completed", Version: "1"}, identityClone[ModelCompletedNotice])
	ToolPreparedPoint   = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/tool-prepared", Version: "1"}, infallibleClone(cloneToolPreparedNotice))
	ToolStartedPoint    = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/tool-started", Version: "1"}, identityClone[ToolStartedNotice])
	ToolSettledPoint    = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/tool-settled", Version: "1"}, infallibleClone(cloneToolSettledNotice))
	EventPublishedPoint = extension.NewNotification(extension.Contract{ID: "eino-agent/runtime/event-published", Version: "1"}, infallibleClone(cloneEvent))
)

func identityClone[T any](value T) (T, error) { return value, nil }

func infallibleClone[T any](clone func(T) T) extension.CloneFunc[T] {
	return func(value T) (T, error) { return clone(value), nil }
}

func cloneRunAdmittedNotice(value RunAdmittedNotice) RunAdmittedNotice {
	value.Plan = value.Plan.Clone()
	value.Metadata = cloneBoundedTurnMetadata(value.Metadata)
	return value
}

func cloneRunSettledNotice(value RunSettledNotice) RunSettledNotice {
	value.Metadata = cloneBoundedTurnMetadata(value.Metadata)
	return value
}

func cloneToolPreparedNotice(value ToolPreparedNotice) ToolPreparedNotice {
	value.Input = cloneJSON(value.Input)
	value.Component = cloneStringMap(value.Component)
	return value
}

func cloneToolSettledNotice(value ToolSettledNotice) ToolSettledNotice {
	value.Result.Structured = cloneJSON(value.Result.Structured)
	value.Result.Attachments = cloneAttachments(value.Result.Attachments)
	value.Result.Metadata = cloneStringMap(value.Result.Metadata)
	return value
}

func cloneEvent(value session.EventRecord) session.EventRecord {
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
