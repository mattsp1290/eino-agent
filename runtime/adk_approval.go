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
// Both the public approval request block (committed by adkModel.commit; no
// special-casing needed since persistAssistantTurn already understands the
// block kind) and, on a decided resume, the public response block are
// ordinary durable content. This binding therefore needs no private native
// continuation blob: adkModel.durableProjection reconstructs the model's
// next input from committed history, so once the response message is
// committed here it is simply part of that projection like any other fact,
// exactly as replay would see it.
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
// decision-CAS record.
const adkApprovalPartType = "eino_agent_mcp_approval"

// adkApprovalRecord is the durable CAS record guarding a one-time decision.
// It is never exposed as public content; the public facts are the committed
// mcp_tool_approval_request/response blocks themselves.
type adkApprovalRecord struct {
	Type              string `json:"type"`
	ApprovalRequestID string `json:"approval_request_id"`
	Status            string `json:"status"`
	Decision          string `json:"decision,omitempty"`
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

// pause commits the decision-CAS record and raises the durable interrupt for
// a completed approval-bearing result. Sibling function calls (a mixed
// function-call/approval result) are already persisted as pending records by
// commit, but ADK's tools node never runs them: this method returns an error
// instead of the committed message.
func (b *adkApprovalBinding) pause(ctx context.Context, m *adkModel, dispatch *adkDispatch, committed *einoschema.AgenticMessage) error {
	request := approvalRequestBlock(committed)
	if request == nil {
		return nil
	}
	// A sibling function_tool_call on this same message is permanently
	// orphaned: ADK's tools node never dispatches it (the model node itself
	// errors with the StatefulInterrupt raised below before the tools node
	// ever runs), and a decided resume's continuation is a fresh physical
	// dispatch, not a resumption of this ReAct iteration -- see
	// durableProjection's approvalOrphanedToolCallIDs exclusion. Left
	// pending forever it would (a) never settle, so SettleRun (which
	// refuses any non-terminal tool call) could never terminally settle
	// this run, and (b) never execute, matching the documented "sibling
	// call is never executed or manufactured" contract. Terminalize it now,
	// the same way the resume path abandons an unstarted call after a fatal
	// tool outcome (terminalizeUnfinishedTools/interruptPendingTool).
	for _, block := range committed.ContentBlocks {
		if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolCall || block.FunctionToolCall == nil {
			continue
		}
		callID := session.ToolCallID(block.FunctionToolCall.CallID)
		if callID == "" {
			continue
		}
		record, err := m.host.store.GetToolCall(ctx, callID)
		if err != nil {
			return err
		}
		if record.Status != session.ToolCallPending {
			continue
		}
		if _, err := m.execution.interruptPendingTool(ctx, m.engine.snapshot, record); err != nil {
			return err
		}
	}
	partID := m.host.ids.NewPartID()
	now := m.host.now()
	payload := mustJSON(adkApprovalRecord{Type: adkApprovalPartType, ApprovalRequestID: request.ID, Status: "pending"})
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
// preserve its state); a targeted leaf CASes the durable decision record
// once (under the run fence and this binding's mutex -- the run fence's own
// CAS in ClaimRun already guarantees only one process holds it at a time,
// so a plain read-then-update here is safe against concurrent resumes, just
// not a store-level conditional write) and commits the durable, public
// user-role MCPToolApprovalResponse bound to the exact request ID. It
// returns no continuation: the caller (adkModel.Generate/Stream) always
// reloads the durable projection afterward, which now includes this
// response like any other committed fact.
func (b *adkApprovalBinding) prepare(ctx context.Context, m *adkModel) error {
	wasInterrupted, hasState, state := compose.GetInterruptState[*adkApprovalState](ctx)
	if !wasInterrupted || !hasState || state == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	isTarget, hasData, decision := compose.GetResumeContext[string](ctx)
	if !isTarget || !hasData {
		return compose.StatefulInterrupt(ctx, &adkApprovalInfo{ApprovalRequestID: state.ApprovalRequestID, MessageID: state.MessageID}, state)
	}
	if decision != "approve" && decision != "deny" {
		return fmt.Errorf("invalid approval decision %q", decision)
	}
	record, part, err := b.load(ctx, m, session.MessageID(state.MessageID), session.PartID(state.PartID))
	if err != nil {
		return err
	}
	if record.ApprovalRequestID != state.ApprovalRequestID {
		return errors.New("approval record does not match interrupt state")
	}
	if record.Status != "pending" {
		return fmt.Errorf("approval %s already decided: %s", state.ApprovalRequestID, record.Decision)
	}
	record.Status = "decided"
	record.Decision = decision
	part.Payload = mustJSON(record)
	part.UpdatedAt = m.host.now()
	if err := m.execution.store.UpdatePart(ctx, part); err != nil {
		return err
	}
	if err := b.commitResponse(ctx, m, state.ApprovalRequestID, decision); err != nil {
		return err
	}
	// The resumed dispatch is a new logical assistant message: the
	// approval-bearing message already consumed the turn's placeholder (or
	// an earlier continuation message already claimed one), so the next
	// commit must mint a fresh one rather than reuse it.
	m.needMessage = true
	return nil
}

// commitResponse durably records the host's decision as an ordinary,
// caller-shaped user-role MCPToolApprovalResponse message (the same block
// kind and role Start/Enqueue accept from a real caller), so it becomes part
// of the durable projection like any other committed fact.
func (b *adkApprovalBinding) commitResponse(ctx context.Context, m *adkModel, approvalRequestID, decision string) error {
	blocks, err := assignContentBlockIDs([]session.ContentBlock{{
		Kind: session.BlockKindMCPToolApprovalResponse,
		MCPApprovalResponse: &session.MCPApprovalResponseBlock{
			ApprovalRequestID: approvalRequestID, Approve: decision == "approve", Reason: "host decision",
		},
	}}, m.host.ids)
	if err != nil {
		return err
	}
	content := session.Content{Role: session.RoleUser, Blocks: blocks}
	messageID := m.host.ids.NewMessageID()
	at, err := m.execution.nextDurableMessageTime(ctx, m.engine.snapshot.SessionID, m.host.now())
	if err != nil {
		return err
	}
	parts, err := session.EncodeContentParts(content, func() session.PartID { return m.host.ids.NewPartID() }, messageID, m.engine.snapshot.SessionID, m.engine.snapshot.RunID, at, m.host.contentLimits)
	if err != nil {
		return err
	}
	if _, err := m.execution.store.AppendMessage(ctx, session.Message{
		ID: messageID, SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID, Role: session.RoleUser, CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		return err
	}
	for _, p := range parts {
		if _, err := m.execution.store.AppendPart(ctx, p); err != nil {
			return err
		}
	}
	return nil
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
