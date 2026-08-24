package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

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

func evaluateToolGuards(ctx context.Context, plan *RunPlan, tool Tool, call ToolCall) (ToolGuardResult, error) {
	if plan == nil || len(plan.guards) == 0 {
		return ToolGuardResult{Decision: ToolGuardAbstain}, nil
	}
	denial := ToolGuardResult{Decision: ToolGuardAbstain}
	for _, mounted := range plan.guards {
		if mounted.Guard == nil {
			return ToolGuardResult{}, errors.New("nil tool guard")
		}
		request := ToolGuardRequest{SessionID: call.SessionID, RunID: call.RunID, Call: extensionToolCall(call), ToolName: tool.Name}
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

func clonePreparedToolCall(value PreparedToolCall) PreparedToolCall {
	value.Tool = cloneTool(value.Tool)
	value.Call = cloneToolCall(value.Call)
	return value
}

func clonePreparedToolCallChecked(value PreparedToolCall) (PreparedToolCall, error) {
	var err error
	value.Tool, err = cloneToolChecked(value.Tool)
	if err != nil {
		return PreparedToolCall{}, err
	}
	value.Call = cloneToolCall(value.Call)
	return value, nil
}

func validatePreparedToolCallInput(original, candidate PreparedToolCall) error {
	leftCall, rightCall := cloneToolCall(original.Call), cloneToolCall(candidate.Call)
	leftCall.Input, rightCall.Input = nil, nil
	if !sameProtectedTool(original.Tool, candidate.Tool) || !sameProtectedToolCall(leftCall, rightCall) || !json.Valid(candidate.Call.Input) {
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

func cloneToolExecutionChecked(value ToolExecution) (ToolExecution, error) {
	var err error
	value.Tool, err = cloneToolChecked(value.Tool)
	if err != nil {
		return ToolExecution{}, err
	}
	value.Call = cloneToolCall(value.Call)
	return value, nil
}

func validateToolExecutionInput(original, candidate ToolExecution) error {
	if !sameProtectedTool(original.Tool, candidate.Tool) || !sameProtectedToolCall(original.Call, candidate.Call) {
		return extension.ErrProtectedMutation
	}
	return nil
}

func validateToolExecutionResult(original ToolExecution, output ToolOutcome) error {
	if output.seal == nil || !sameProtectedToolCall(original.Call, output.Call) {
		return extension.ErrProtectedMutation
	}
	return validateToolOutcome(output)
}

func sameProtectedTool(left, right Tool) bool {
	leftInfo, rightInfo := left.Info, right.Info
	if left.Executor != nil || right.Executor != nil || left.InputDecoder != nil || right.InputDecoder != nil {
		return false
	}
	left.Executor, right.Executor = nil, nil
	left.InputDecoder, right.InputDecoder = nil, nil
	left.Info, right.Info = nil, nil
	return reflect.DeepEqual(left, right) && sameProtectedToolInfo(leftInfo, rightInfo)
}

func sameProtectedToolInfo(left, right *einoschema.ToolInfo) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	leftSchema, leftSchemaErr := protectedParamsOneOfJSON(left.ParamsOneOf)
	rightSchema, rightSchemaErr := protectedParamsOneOfJSON(right.ParamsOneOf)
	return leftErr == nil && rightErr == nil && leftSchemaErr == nil && rightSchemaErr == nil && bytes.Equal(leftRaw, rightRaw) && bytes.Equal(leftSchema, rightSchema)
}

func sameProtectedToolCall(left, right ToolCall) bool {
	if left.Approval != nil || right.Approval != nil {
		return false
	}
	left.Approval, right.Approval = nil, nil
	return reflect.DeepEqual(cloneToolCall(left), cloneToolCall(right))
}

func cloneToolOutcome(value ToolOutcome) ToolOutcome {
	value.Call = cloneToolCall(value.Call)
	value.Result = cloneRuntimeToolResult(value.Result)
	value.PermissionMetadata = cloneStringMap(value.PermissionMetadata)
	return value
}

func sealToolOutcome(value ToolOutcome) ToolOutcome {
	value.Call = extensionToolCall(value.Call)
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
	cloned, err := cloneToolChecked(tool)
	if err != nil {
		return Tool{}
	}
	return cloned
}

func cloneToolChecked(tool Tool) (Tool, error) {
	tool.Scope.Permissions = cloneSlice(tool.Scope.Permissions)
	tool.Metadata = cloneStringMap(tool.Metadata)
	if tool.Info != nil {
		params, paramsErr := cloneProtectedParamsOneOf(tool.Info.ParamsOneOf)
		raw, err := json.Marshal(tool.Info)
		var info einoschema.ToolInfo
		if paramsErr != nil {
			return Tool{}, paramsErr
		}
		if err != nil {
			return Tool{}, err
		}
		if err := json.Unmarshal(raw, &info); err != nil {
			return Tool{}, err
		}
		info.ParamsOneOf = params
		tool.Info = &info
	}
	return tool, nil
}

func extensionTool(tool Tool) Tool {
	tool.Executor = nil
	tool.InputDecoder = nil
	return tool
}

func extensionToolCall(call ToolCall) ToolCall {
	call = cloneToolCall(call)
	call.Approval = nil
	return call
}

func cloneProtectedParamsOneOf(src *einoschema.ParamsOneOf) (*einoschema.ParamsOneOf, error) {
	if src == nil {
		return nil, nil
	}
	raw, err := protectedParamsOneOfJSON(src)
	if err != nil {
		return nil, err
	}
	var cloned jsonschema.Schema
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return nil, err
	}
	return einoschema.NewParamsOneOfByJSONSchema(&cloned), nil
}

func protectedParamsOneOfJSON(src *einoschema.ParamsOneOf) (raw []byte, err error) {
	if src == nil {
		return nil, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			raw = nil
			err = fmt.Errorf("tool parameter schema conversion panic: %v", recovered)
		}
	}()
	schema, err := src.ToJSONSchema()
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, errors.New("tool parameter schema conversion returned nil")
	}
	return json.Marshal(schema)
}

func cloneToolCall(call ToolCall) ToolCall {
	call.Input = cloneJSON(call.Input)
	call.Scope.Permissions = cloneSlice(call.Scope.Permissions)
	call.Context = call.Context.Clone()
	return call
}

func cloneRuntimeToolResult(result ToolResult) ToolResult {
	result.Structured = cloneJSON(result.Structured)
	result.Attachments = cloneAttachments(result.Attachments)
	result.Metadata = cloneStringMap(result.Metadata)
	return result
}

func cloneAttachments(attachments []Attachment) []Attachment {
	cloned := cloneSlice(attachments)
	for index := range cloned {
		cloned[index].Metadata = cloneStringMap(cloned[index].Metadata)
	}
	return cloned
}
