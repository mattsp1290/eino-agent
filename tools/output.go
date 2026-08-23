package tools

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

const (
	// MetadataOutputStatus records the stable settlement class in durable
	// metadata and model-facing payloads.
	MetadataOutputStatus = runtime.ToolMetadataOutputStatus
	// MetadataPermissionStatus is emitted by runtime permission hooks.
	MetadataPermissionStatus = "permission_status"
)

const (
	outputStatusCompleted          = "completed"
	outputStatusExpectedFailure    = "expected_failure"
	outputStatusOperationalFailure = "operational_failure"
	outputStatusInterrupted        = "interrupted"
)

// ModelOutput is the canonical bounded tool result payload stored in
// PartToolResult and sent back to the model.
type ModelOutput = runtime.ToolOutput

// EncodeModelOutput bounds a runtime tool result according to policy and
// returns the JSON payload used for replayable model-facing history.
func EncodeModelOutput(call runtime.ToolCall, result runtime.ToolResult, policy runtime.RetentionPolicy) (json.RawMessage, ModelOutput, error) {
	disposition := dispositionFromResult(result, nil)
	raw, output := runtime.EncodeToolOutput(call.ID, result, policy, disposition, nil)
	return raw, output, nil
}

// BuildToolSettlement delegates to the runtime's canonical durable settlement
// builder and returns its replayable result part for convenience.
func BuildToolSettlement(input runtime.ToolSettlementInput) (session.ToolSettlement, session.Part, error) {
	settlement, _, err := runtime.BuildToolSettlement(input)
	return settlement, settlement.ResultPart, err
}

func dispositionFromResult(result runtime.ToolResult, err error) runtime.ToolDisposition {
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return runtime.ToolInterrupted
		}
		return runtime.ToolFailed
	}
	switch result.Metadata[MetadataPermissionStatus] {
	case "interrupted":
		return runtime.ToolInterrupted
	case "denied":
		return runtime.ToolDenied
	case "approval_required":
		return runtime.ToolApprovalRequired
	default:
		return runtime.ToolExecuted
	}
}
