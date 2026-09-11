package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

func init() {
	einoschema.RegisterName[*adkApprovalInfo]("eino_agent_adk_approval_info")
	einoschema.RegisterName[*adkApprovalState]("eino_agent_adk_approval_state")
}

// adkApprovalBinding is the native MCP approval adapter promoted from the W1
// proof (runtime/adk_approval_proof_test.go). It is an execution-local
// wrapper paired with the mandatory model adapter (adkModel.approval) at
// every permitted model-agent boundary: it recognizes a completed
// approval-bearing model result -- schema.MCPToolApprovalRequest, a real W2
// content block kind (session.BlockKindMCPToolApprovalRequest) -- before the
// inner ReAct loop can schedule any sibling local function calls, and raises
// a durable ADK pause carrying only bounded record IDs.
//
// The public approval request block is committed as an ordinary part of the
// assistant message by adkModel.commit (persistAssistantTurn already
// understands the block kind; no special-casing is required there). This
// binding additionally records a private native continuation -- the exact
// dispatch input and committed transcript needed to reconstruct the paused
// call -- as a session.PartState part on that same message, then raises
// adk.TypedStatefulInterrupt via compose.StatefulInterrupt.
type adkApprovalBinding struct {
	mu sync.Mutex
}

// adkApprovalInfo is the user-facing interrupt info (not persisted; exposed
// to hosts via InterruptCtx.Info).
type adkApprovalInfo struct {
	ApprovalRequestID string
	MessageID         string
}

// adkApprovalState is the internal state ADK persists in the checkpoint and
// hands back to this binding on resume.
type adkApprovalState struct {
	ApprovalRequestID string
	MessageID         string
	PartID            string
}

// adkApprovalPartType marks a session.PartState payload as this binding's
// private native-continuation record.
const adkApprovalPartType = "eino_agent_mcp_approval"

// adkApprovalRecord is the durable private continuation record: the exact
// dispatch input and committed transcript needed to reconstruct the paused
// call, plus the decision CAS state. It is never exposed as public content;
// the public fact is the committed mcp_tool_approval_request block itself.
type adkApprovalRecord struct {
	Type              string          `json:"type"`
	ApprovalRequestID string          `json:"approval_request_id"`
	Status            string          `json:"status"`
	Decision          string          `json:"decision,omitempty"`
	Transcript        json.RawMessage `json:"transcript"`
	Input             json.RawMessage `json:"input"`
}

func approvalRequestBlock(msg *einoschema.AgenticMessage) *einoschema.MCPToolApprovalRequest {
	if msg == nil {
		return nil
	}
	for _, block := range msg.ContentBlocks {
		if block != nil && block.Type == einoschema.ContentBlockTypeMCPToolApprovalRequest && block.MCPToolApprovalRequest != nil {
			return block.MCPToolApprovalRequest
		}
	}
	return nil
}

// pause commits the private native-continuation record and raises the
// durable interrupt for a completed approval-bearing result. Sibling
// function calls (a mixed function-call/approval result) are already
// persisted as pending records by commit, but ADK's tools node never runs
// them: this method returns an error instead of the committed message.
func (b *adkApprovalBinding) pause(ctx context.Context, m *adkModel, dispatch *adkDispatch, committed *einoschema.AgenticMessage) error {
	request := approvalRequestBlock(committed)
	if request == nil {
		return nil
	}
	transcript, err := json.Marshal(committed)
	if err != nil {
		return err
	}
	input, err := json.Marshal(dispatch.input)
	if err != nil {
		return err
	}
	partID := m.host.ids.NewPartID()
	now := m.host.now()
	payload := mustJSON(adkApprovalRecord{Type: adkApprovalPartType, ApprovalRequestID: request.ID, Status: "pending", Transcript: transcript, Input: input})
	if _, err := m.execution.store.AppendPart(ctx, session.Part{
		ID: partID, MessageID: dispatch.messageID, SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID,
		Kind: session.PartState, Ordinal: 1000, Payload: payload, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	return compose.StatefulInterrupt(ctx,
		&adkApprovalInfo{ApprovalRequestID: request.ID, MessageID: string(dispatch.messageID)},
		&adkApprovalState{ApprovalRequestID: request.ID, MessageID: string(dispatch.messageID), PartID: string(partID)})
}

// prepare runs before any ledger row exists on a re-executed model node. An
// untargeted leaf re-raises the same interrupt (matching upstream's
// ResumeWithParams contract: a leaf not in Targets must re-interrupt to
// preserve its state); a targeted leaf CASes the durable record once (under
// the run fence and this binding's mutex -- see the accepted simplification
// noted in docs/architecture/eino-feature-support.md's W5 section) and
// returns the exact native continuation: the original dispatch input, the
// committed transcript, and the matching user-role MCPToolApprovalResponse.
func (b *adkApprovalBinding) prepare(ctx context.Context, m *adkModel, input []*einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
	wasInterrupted, hasState, state := compose.GetInterruptState[*adkApprovalState](ctx)
	if !wasInterrupted || !hasState || state == nil {
		return input, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	isTarget, hasData, decision := compose.GetResumeContext[string](ctx)
	if !isTarget || !hasData {
		return nil, compose.StatefulInterrupt(ctx, &adkApprovalInfo{ApprovalRequestID: state.ApprovalRequestID, MessageID: state.MessageID}, state)
	}
	if decision != "approve" && decision != "deny" {
		return nil, fmt.Errorf("invalid approval decision %q", decision)
	}
	record, part, err := b.load(ctx, m, session.MessageID(state.MessageID), session.PartID(state.PartID))
	if err != nil {
		return nil, err
	}
	if record.ApprovalRequestID != state.ApprovalRequestID {
		return nil, errors.New("approval record does not match interrupt state")
	}
	if record.Status != "pending" {
		return nil, fmt.Errorf("approval %s already decided: %s", state.ApprovalRequestID, record.Decision)
	}
	record.Status = "decided"
	record.Decision = decision
	part.Payload = mustJSON(record)
	part.UpdatedAt = m.host.now()
	if err := m.execution.store.UpdatePart(ctx, part); err != nil {
		return nil, err
	}
	var transcript einoschema.AgenticMessage
	if err := json.Unmarshal(record.Transcript, &transcript); err != nil {
		return nil, err
	}
	var original []*einoschema.AgenticMessage
	if err := json.Unmarshal(record.Input, &original); err != nil {
		return nil, err
	}
	response := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{
		einoschema.NewContentBlock(&einoschema.MCPToolApprovalResponse{ApprovalRequestID: state.ApprovalRequestID, Approve: decision == "approve", Reason: "host decision"}),
	}}
	continuation := append(append([]*einoschema.AgenticMessage{}, original...), &transcript, response)
	m.needMessage = true
	return continuation, nil
}

func (b *adkApprovalBinding) load(ctx context.Context, m *adkModel, messageID session.MessageID, partID session.PartID) (adkApprovalRecord, session.Part, error) {
	cursor := session.ReplayCursor{Limit: 1000}
	for {
		batch, err := m.host.store.ListMessages(ctx, m.engine.snapshot.SessionID, cursor)
		if err != nil {
			return adkApprovalRecord{}, session.Part{}, err
		}
		for _, part := range batch.Parts {
			if part.ID != partID || part.MessageID != messageID || part.Kind != session.PartState {
				continue
			}
			var record adkApprovalRecord
			if err := json.Unmarshal(part.Payload, &record); err != nil {
				return adkApprovalRecord{}, session.Part{}, err
			}
			if record.Type != adkApprovalPartType {
				return adkApprovalRecord{}, session.Part{}, errors.New("unexpected approval part type")
			}
			return record, part, nil
		}
		if batch.Next == (session.ReplayCursor{}) {
			return adkApprovalRecord{}, session.Part{}, fmt.Errorf("approval record %s not found", partID)
		}
		cursor = batch.Next
	}
}
