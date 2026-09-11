//go:build cgo

package wasmext

/*
#include "wasmtime_abi.h"
*/
import "C"

import (
	"context"
	"errors"

	"go.bytecodealliance.org/cm"

	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.2.0/types"
)

func (c *wasmtimeComponent) ToolMetadata(ctx context.Context) (wittypes.ToolMetadata, error) {
	var output wittypes.ToolMetadata
	err := c.invokeABI(ctx, "metadata", nil, func(result *C.wasmtime_component_val_t) error {
		payload, err := componentResult(result)
		if err != nil {
			return err
		}
		return decodeToolMetadata(payload, &output, c.limits.MaxOutputBytes)
	})
	return output, err
}

func (c *wasmtimeComponent) ToolPermissionPattern(ctx context.Context, inputJSON string) (string, error) {
	arguments := make([]C.wasmtime_component_val_t, 1)
	setComponentString(&arguments[0], inputJSON)
	limit := min(c.limits.MaxOutputBytes, maxPermissionPatternBytes)
	var output string
	err := c.invokeABI(ctx, "permission-pattern", arguments, func(result *C.wasmtime_component_val_t) error {
		payload, err := componentResult(result)
		if err != nil {
			return err
		}
		output, err = componentString(payload, limit)
		return err
	})
	return output, err
}

func (c *wasmtimeComponent) ExecuteTool(ctx context.Context, request toolExecuteRequest) (string, error) {
	arguments := make([]C.wasmtime_component_val_t, 3)
	setComponentString(&arguments[0], request.ToolCallID)
	setComponentString(&arguments[1], request.InputJSON)
	arguments[2] = componentTurnMetadata(request.Turn)
	var output string
	err := c.invokeABI(ctx, "execute", arguments, func(result *C.wasmtime_component_val_t) error {
		payload, err := componentResult(result)
		if err != nil {
			return err
		}
		output, err = componentString(payload, c.limits.MaxOutputBytes)
		return err
	})
	return output, err
}

func (c *wasmtimeComponent) DecidePermissions(ctx context.Context, request wittypes.PermissionRequest) (wittypes.PermissionDecision, error) {
	arguments := []C.wasmtime_component_val_t{componentPermissionRequest(request)}
	var output wittypes.PermissionDecision
	err := c.invokeABI(ctx, "decide", arguments, func(result *C.wasmtime_component_val_t) error {
		payload, err := componentResult(result)
		if err != nil {
			return err
		}
		return decodePermissionDecision(payload, &output, c.limits.MaxOutputBytes)
	})
	return output, err
}

func (c *wasmtimeComponent) LoadContext(ctx context.Context, turn wittypes.TurnMetadata) ([]wittypes.Message, error) {
	arguments := []C.wasmtime_component_val_t{componentTurnMetadata(turn)}
	var output []wittypes.Message
	err := c.invokeABI(ctx, "load-context", arguments, func(result *C.wasmtime_component_val_t) error {
		payload, err := componentResult(result)
		if err != nil {
			return err
		}
		return decodeMessages(payload, &output, c.limits.MaxOutputBytes)
	})
	return output, err
}

func (c *wasmtimeComponent) EmitEvent(ctx context.Context, event wittypes.BoundedEvent) error {
	return c.invokeABI(ctx, "emit", []C.wasmtime_component_val_t{componentBoundedEvent(event)}, decodeVoidResult)
}

func (c *wasmtimeComponent) BeforeRun(ctx context.Context, turn wittypes.TurnMetadata) error {
	return c.invokeTurn(ctx, "before-run", turn)
}

func (c *wasmtimeComponent) AfterRun(ctx context.Context, turn wittypes.TurnMetadata) error {
	return c.invokeTurn(ctx, "after-run", turn)
}

func (c *wasmtimeComponent) invokeTurn(ctx context.Context, functionName string, turn wittypes.TurnMetadata) error {
	return c.invokeABI(ctx, functionName, []C.wasmtime_component_val_t{componentTurnMetadata(turn)}, decodeVoidResult)
}

func (c *wasmtimeComponent) BeforeToolCall(ctx context.Context, request toolMiddlewareBeforeRequest) (wittypes.Replacement, error) {
	arguments := make([]C.wasmtime_component_val_t, 4)
	setComponentString(&arguments[0], request.ToolName)
	setComponentString(&arguments[1], request.ToolCallID)
	setComponentString(&arguments[2], request.InputJSON)
	arguments[3] = componentTurnMetadata(request.Turn)
	return c.invokeReplacement(ctx, "before-tool-call", arguments)
}

func (c *wasmtimeComponent) AfterToolCall(ctx context.Context, request toolMiddlewareAfterRequest) (wittypes.Replacement, error) {
	arguments := make([]C.wasmtime_component_val_t, 5)
	setComponentString(&arguments[0], request.ToolName)
	setComponentString(&arguments[1], request.ToolCallID)
	setComponentString(&arguments[2], request.InputJSON)
	setComponentString(&arguments[3], request.OutputJSON)
	arguments[4] = componentTurnMetadata(request.Turn)
	return c.invokeReplacement(ctx, "after-tool-call", arguments)
}

func (c *wasmtimeComponent) invokeReplacement(ctx context.Context, functionName string, arguments []C.wasmtime_component_val_t) (wittypes.Replacement, error) {
	var output wittypes.Replacement
	err := c.invokeABI(ctx, functionName, arguments, func(result *C.wasmtime_component_val_t) error {
		return decodeReplacement(result, &output, c.limits.MaxOutputBytes)
	})
	return output, err
}

func decodeVoidResult(result *C.wasmtime_component_val_t) error {
	_, err := componentResult(result)
	return err
}

func componentTurnMetadata(turn wittypes.TurnMetadata) C.wasmtime_component_val_t {
	record := newComponentRecord([]string{
		"run-id", "session-id", "epoch-id", "agent-name", "agent-mode",
		"provider-id", "model-id", "tool-names", "message-count", "role-counts",
		"has-system-prompt",
	})
	setComponentString(componentRecordFieldForSet(&record, 0), turn.RunID)
	setComponentString(componentRecordFieldForSet(&record, 1), turn.SessionID)
	setComponentString(componentRecordFieldForSet(&record, 2), turn.EpochID)
	setComponentString(componentRecordFieldForSet(&record, 3), turn.AgentName)
	setComponentString(componentRecordFieldForSet(&record, 4), turn.AgentMode)
	setComponentString(componentRecordFieldForSet(&record, 5), turn.ProviderID)
	setComponentString(componentRecordFieldForSet(&record, 6), turn.ModelID)
	setComponentStringList(componentRecordFieldForSet(&record, 7), turn.ToolNames.Slice())
	setComponentU32(componentRecordFieldForSet(&record, 8), turn.MessageCount)
	roleCounts := newComponentRecord([]string{"system", "user", "assistant", "tool"})
	setComponentU32(componentRecordFieldForSet(&roleCounts, 0), turn.RoleCounts.System)
	setComponentU32(componentRecordFieldForSet(&roleCounts, 1), turn.RoleCounts.User)
	setComponentU32(componentRecordFieldForSet(&roleCounts, 2), turn.RoleCounts.Assistant)
	setComponentU32(componentRecordFieldForSet(&roleCounts, 3), turn.RoleCounts.Tool)
	*componentRecordFieldForSet(&record, 9) = roleCounts
	setComponentBool(componentRecordFieldForSet(&record, 10), turn.HasSystemPrompt)
	return record
}

func componentPermissionRequest(request wittypes.PermissionRequest) C.wasmtime_component_val_t {
	record := newComponentRecord([]string{
		"tool-name", "tool-call-id", "permission", "arguments-summary", "session-id", "run-id",
	})
	setComponentString(componentRecordFieldForSet(&record, 0), request.ToolName)
	setComponentString(componentRecordFieldForSet(&record, 1), request.ToolCallID)
	setComponentString(componentRecordFieldForSet(&record, 2), request.Permission)
	setComponentString(componentRecordFieldForSet(&record, 3), request.ArgumentsSummary)
	setComponentString(componentRecordFieldForSet(&record, 4), request.SessionID)
	setComponentString(componentRecordFieldForSet(&record, 5), request.RunID)
	return record
}

func componentBoundedEvent(event wittypes.BoundedEvent) C.wasmtime_component_val_t {
	record := newComponentRecord([]string{"kind", "session-id", "run-id", "message-id", "tool-call-id", "epoch-id", "timestamp-unix-millis", "payload-summary"})
	setComponentString(componentRecordFieldForSet(&record, 0), event.Kind)
	setComponentString(componentRecordFieldForSet(&record, 1), event.SessionID)
	setComponentString(componentRecordFieldForSet(&record, 2), event.RunID)
	setComponentString(componentRecordFieldForSet(&record, 3), event.MessageID)
	setComponentString(componentRecordFieldForSet(&record, 4), event.ToolCallID)
	setComponentString(componentRecordFieldForSet(&record, 5), event.EpochID)
	setComponentS64(componentRecordFieldForSet(&record, 6), event.TimestampUnixMillis)
	setComponentString(componentRecordFieldForSet(&record, 7), event.PayloadSummary)
	return record
}

// decodeMessages decodes a component-model `list<message>` result, where
// each message is a `role` enum plus an ordered `blocks: list<content-block>`
// (see decodeContentBlock). It replaces the old flat decodeTextMessages: a
// guest built against the prior (v0.1.0) text-message shape exports a
// DIFFERENTLY-VERSIONED world interface name
// ("eino-agent:extensions/context-source-api@0.1.0" vs this package's
// "...@0.2.0"), so it is rejected at Compile -- engine_wasmtime.go's
// GetExportIndex(nil, contract.exportName) simply finds no export by that
// name and fails closed ("required world export missing", classified
// ErrorContract) -- long before this decoder, or any call, ever runs. This
// is a lookup-by-name failure, not a canonical-ABI/signature type check:
// two components sharing the SAME versioned export name but a genuinely
// incompatible function signature is not a case this mechanism catches --
// signature compatibility within one package version relies on
// WIT/package-version discipline, not a runtime check -- see the
// content-block doc comment in wit/eino-agent-extensions.wit.
func decodeMessages(value *C.wasmtime_component_val_t, output *[]wittypes.Message, limit int64) error {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindList {
		return errors.New("component returned an unexpected message list")
	}
	count := int(C.wasmext_list_size(value))
	if count > 4096 {
		return errModuleTooLarge
	}
	messages := make([]wittypes.Message, 0, count)
	var total int64
	for index := 0; index < count; index++ {
		item := C.wasmext_list_value(value, C.size_t(index))
		roleValue, err := componentRecordField(item, 0, 2)
		if err != nil {
			return err
		}
		blocksValue, err := componentRecordField(item, 1, 2)
		if err != nil {
			return err
		}
		role, err := componentEnum(roleValue)
		if err != nil {
			return err
		}
		message := wittypes.Message{}
		switch role {
		case "system":
			message.Role = wittypes.TextRoleSystem
		case "user":
			message.Role = wittypes.TextRoleUser
		default:
			return errors.New("component returned an invalid text role")
		}
		blocks, consumed, err := decodeContentBlocks(blocksValue, limit-total)
		if err != nil {
			return err
		}
		total += consumed
		message.Blocks = cm.ToList(blocks)
		messages = append(messages, message)
	}
	*output = messages
	return nil
}

// decodeContentBlocks decodes a `list<content-block>` value, bounding both
// the number of blocks and the cumulative byte size of their string/JSON
// payloads against limit (the same MaxOutputBytes budget applied elsewhere
// in this file), and returns the bytes it consumed so a caller decoding
// multiple messages can carry the remaining budget forward.
func decodeContentBlocks(value *C.wasmtime_component_val_t, limit int64) ([]wittypes.ContentBlock, int64, error) {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindList {
		return nil, 0, errors.New("component returned an unexpected content block list")
	}
	count := int(C.wasmext_list_size(value))
	if count > 4096 {
		return nil, 0, errModuleTooLarge
	}
	blocks := make([]wittypes.ContentBlock, 0, count)
	var total int64
	for index := 0; index < count; index++ {
		item := C.wasmext_list_value(value, C.size_t(index))
		block, consumed, err := decodeContentBlock(item, limit-total)
		if err != nil {
			return nil, 0, err
		}
		total += consumed
		blocks = append(blocks, block)
	}
	return blocks, total, nil
}

// decodeContentBlock decodes one `content-block` variant case, modeled on
// the proven decodeReplacement pattern: the case name is read from the
// variant discriminant, and the payload is decoded according to that case's
// known shape. An unrecognized case name fails closed rather than guessing
// at an unknown payload's shape -- this is a real, reachable path (not
// prevented by anything at compile time): a genuinely incompatible guest
// still compiles successfully whenever it happens to export the same
// versioned world interface name (see decodeMessages's doc comment for why
// the differently-versioned v0.1.0 fixture is rejected earlier, at Compile,
// and never reaches this decoder at all).
func decodeContentBlock(value *C.wasmtime_component_val_t, limit int64) (wittypes.ContentBlock, int64, error) {
	var zero wittypes.ContentBlock
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindVariant {
		return zero, 0, errors.New("component returned an unexpected content block")
	}
	name := C.GoStringN(C.wasmext_variant_data(value), C.int(C.wasmext_variant_size(value)))
	payload := C.wasmext_variant_value(value)
	switch name {
	case "text":
		text, err := componentString(payload, limit)
		if err != nil {
			return zero, 0, err
		}
		return wittypes.ContentBlockText(text), int64(len(text)), nil
	case "media-reference":
		uriValue, err := componentRecordField(payload, 0, 2)
		if err != nil {
			return zero, 0, err
		}
		mimeValue, err := componentRecordField(payload, 1, 2)
		if err != nil {
			return zero, 0, err
		}
		uri, err := componentString(uriValue, limit)
		if err != nil {
			return zero, 0, err
		}
		mimeType, err := componentString(mimeValue, limit-int64(len(uri)))
		if err != nil {
			return zero, 0, err
		}
		ref := wittypes.MediaReference{URI: uri, MIMEType: mimeType}
		return wittypes.ContentBlockMediaReference(ref), int64(len(uri) + len(mimeType)), nil
	case "function-call":
		callIDValue, err := componentRecordField(payload, 0, 3)
		if err != nil {
			return zero, 0, err
		}
		nameValue, err := componentRecordField(payload, 1, 3)
		if err != nil {
			return zero, 0, err
		}
		argsValue, err := componentRecordField(payload, 2, 3)
		if err != nil {
			return zero, 0, err
		}
		callID, err := componentString(callIDValue, limit)
		if err != nil {
			return zero, 0, err
		}
		fnName, err := componentString(nameValue, limit-int64(len(callID)))
		if err != nil {
			return zero, 0, err
		}
		argumentsJSON, err := componentString(argsValue, limit-int64(len(callID)+len(fnName)))
		if err != nil {
			return zero, 0, err
		}
		call := wittypes.FunctionCallBlock{CallID: callID, Name: fnName, ArgumentsJSON: argumentsJSON}
		return wittypes.ContentBlockFunctionCall(call), int64(len(callID) + len(fnName) + len(argumentsJSON)), nil
	case "function-result-text":
		callIDValue, err := componentRecordField(payload, 0, 2)
		if err != nil {
			return zero, 0, err
		}
		textValue, err := componentRecordField(payload, 1, 2)
		if err != nil {
			return zero, 0, err
		}
		callID, err := componentString(callIDValue, limit)
		if err != nil {
			return zero, 0, err
		}
		text, err := componentString(textValue, limit-int64(len(callID)))
		if err != nil {
			return zero, 0, err
		}
		result := wittypes.FunctionResultTextBlock{CallID: callID, Text: text}
		return wittypes.ContentBlockFunctionResultText(result), int64(len(callID) + len(text)), nil
	default:
		return zero, 0, errors.New("component returned an invalid content block case")
	}
}

func decodeReplacement(value *C.wasmtime_component_val_t, output *wittypes.Replacement, limit int64) error {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindVariant {
		return errors.New("component returned an unexpected replacement")
	}
	name := C.GoStringN(C.wasmext_variant_data(value), C.int(C.wasmext_variant_size(value)))
	payload := C.wasmext_variant_value(value)
	switch name {
	case "unchanged":
		*output = wittypes.ReplacementUnchanged()
		return nil
	case "json":
		text, err := componentString(payload, limit)
		if err != nil {
			return err
		}
		*output = wittypes.ReplacementJSON(text)
		return nil
	case "error":
		guestErr, err := decodeStructuredError(payload, limit)
		if err != nil {
			return err
		}
		*output = wittypes.ReplacementError(guestErr)
		return nil
	default:
		return errors.New("component returned an invalid replacement case")
	}
}

func decodeStructuredError(value *C.wasmtime_component_val_t, limit int64) (wittypes.StructuredError, error) {
	codeValue, err := componentRecordField(value, 0, 3)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	messageValue, err := componentRecordField(value, 1, 3)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	retryableValue, err := componentRecordField(value, 2, 3)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	code, err := componentString(codeValue, limit)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	message, err := componentString(messageValue, limit-int64(len(code)))
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	retryable, err := componentBool(retryableValue)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	return wittypes.StructuredError{Code: code, Message: message, Retryable: retryable}, nil
}

func decodeToolMetadata(payload *C.wasmtime_component_val_t, metadata *wittypes.ToolMetadata, limit int64) error {
	fields := make([]*C.wasmtime_component_val_t, 5)
	for index := range fields {
		field, err := componentRecordField(payload, index, len(fields))
		if err != nil {
			return err
		}
		fields[index] = field
	}
	var err error
	if metadata.Name, err = componentString(fields[0], limit); err != nil {
		return err
	}
	remaining := limit - int64(len(metadata.Name))
	if metadata.Description, err = componentString(fields[1], remaining); err != nil {
		return err
	}
	remaining -= int64(len(metadata.Description))
	if metadata.ParametersJSONSchema, err = componentString(fields[2], remaining); err != nil {
		return err
	}
	if metadata.RetrySafe, err = componentBool(fields[3]); err != nil {
		return err
	}
	remaining -= int64(len(metadata.ParametersJSONSchema))
	permissions, _, err := componentStringList(fields[4], remaining, 1024)
	if err != nil {
		return err
	}
	metadata.RequiredPermissions = cm.ToList(permissions)
	return nil
}

func decodePermissionDecision(payload *C.wasmtime_component_val_t, decision *wittypes.PermissionDecision, limit int64) error {
	actionValue, err := componentRecordField(payload, 0, 2)
	if err != nil {
		return err
	}
	reasonValue, err := componentRecordField(payload, 1, 2)
	if err != nil {
		return err
	}
	action, err := componentEnum(actionValue)
	if err != nil {
		return err
	}
	switch action {
	case "allow":
		decision.Action = wittypes.PermissionActionAllow
	case "deny":
		decision.Action = wittypes.PermissionActionDeny
	case "ask":
		decision.Action = wittypes.PermissionActionAsk
	default:
		return errors.New("component returned an invalid permission action")
	}
	decision.Reason, err = componentString(reasonValue, limit)
	return err
}
