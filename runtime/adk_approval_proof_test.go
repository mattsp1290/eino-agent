package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// adkApprovalBinding is the W1 proof of the native MCP approval pause. It
// recognizes a completed approval-bearing model result at the model boundary,
// commits a durable approval record, and raises a typed stateful interrupt
// before the inner ReAct loop can schedule sibling local function calls.
// Resume rebuilds the continuation from the committed transcript plus the
// exact matching user-role MCPToolApprovalResponse, CASes the durable record
// once, and dispatches one new physical request.
type adkApprovalBinding struct {
	mu sync.Mutex
}

type adkApprovalInfo struct {
	ApprovalRequestID string
	MessageID         string
}

type adkApprovalState struct {
	ApprovalRequestID string
	MessageID         string
	PartID            string
}

const adkApprovalPartType = "proof_mcp_approval"

// adkApprovalRecord is the durable approval record persisted as a PartState
// payload on the owning assistant message. W2 replaces it with the typed
// content contract; the proof only needs a real fenced durable record.
type adkApprovalRecord struct {
	Type              string          `json:"type"`
	ApprovalRequestID string          `json:"approval_request_id"`
	Status            string          `json:"status"`
	Decision          string          `json:"decision,omitempty"`
	Transcript        json.RawMessage `json:"transcript"`
	Input             json.RawMessage `json:"input"`
}

func approvalRequestBlock(msg *schema.AgenticMessage) *schema.MCPToolApprovalRequest {
	if msg == nil {
		return nil
	}
	for _, block := range msg.ContentBlocks {
		if block != nil && block.Type == schema.ContentBlockTypeMCPToolApprovalRequest && block.MCPToolApprovalRequest != nil {
			return block.MCPToolApprovalRequest
		}
	}
	return nil
}

// pause commits the durable approval record and raises the stateful interrupt
// for a completed approval-bearing result. Sibling function calls are already
// persisted as pending records by commit, but the tools node never runs.
func (b *adkApprovalBinding) pause(ctx context.Context, m *adkLedgerModel, dispatch *adkDispatch, committed *schema.AgenticMessage) error {
	request := approvalRequestBlock(committed)
	if request == nil {
		return nil
	}
	p := m.proof
	transcript, err := json.Marshal(committed)
	if err != nil {
		return err
	}
	input, err := json.Marshal(dispatch.input)
	if err != nil {
		return err
	}
	partID := p.host.ids.NewPartID()
	now := p.host.now()
	payload := mustJSON(adkApprovalRecord{Type: adkApprovalPartType, ApprovalRequestID: request.ID, Status: "pending", Transcript: transcript, Input: input})
	if _, err := p.execution.store.AppendPart(ctx, session.Part{
		ID: partID, MessageID: dispatch.messageID, SessionID: p.run.SessionID, RunID: p.run.ID, Kind: session.PartState,
		Ordinal: 1000, Payload: payload, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	p.trace.add("approval.pause request=%s message=%s", request.ID, dispatch.messageID)
	return compose.StatefulInterrupt(ctx,
		&adkApprovalInfo{ApprovalRequestID: request.ID, MessageID: string(dispatch.messageID)},
		&adkApprovalState{ApprovalRequestID: request.ID, MessageID: string(dispatch.messageID), PartID: string(partID)})
}

// prepare runs before any ledger row exists on the re-executed model node. An
// untargeted leaf re-raises the same interrupt; a targeted leaf CASes the
// durable record once and returns the native continuation input.
func (b *adkApprovalBinding) prepare(ctx context.Context, m *adkLedgerModel, input []*schema.AgenticMessage) ([]*schema.AgenticMessage, error) {
	wasInterrupted, hasState, state := compose.GetInterruptState[*adkApprovalState](ctx)
	if !wasInterrupted || !hasState || state == nil {
		return input, nil
	}
	p := m.proof
	b.mu.Lock()
	defer b.mu.Unlock()
	isTarget, hasData, decision := compose.GetResumeContext[string](ctx)
	if !isTarget || !hasData {
		p.trace.add("approval.still_pending request=%s", state.ApprovalRequestID)
		return nil, compose.StatefulInterrupt(ctx, &adkApprovalInfo{ApprovalRequestID: state.ApprovalRequestID, MessageID: state.MessageID}, state)
	}
	if decision != "approve" && decision != "deny" {
		return nil, fmt.Errorf("invalid approval decision %q", decision)
	}
	record, part, err := b.load(ctx, p, session.MessageID(state.MessageID), session.PartID(state.PartID))
	if err != nil {
		return nil, err
	}
	if record.ApprovalRequestID != state.ApprovalRequestID {
		return nil, errors.New("approval record does not match interrupt state")
	}
	if record.Status != "pending" {
		p.trace.add("approval.duplicate request=%s", state.ApprovalRequestID)
		return nil, fmt.Errorf("approval %s already decided: %s", state.ApprovalRequestID, record.Decision)
	}
	record.Status = "decided"
	record.Decision = decision
	part.Payload = mustJSON(record)
	part.UpdatedAt = p.host.now()
	if err := p.execution.store.UpdatePart(ctx, part); err != nil {
		return nil, err
	}
	p.trace.add("approval.decided request=%s decision=%s", state.ApprovalRequestID, decision)
	var transcript schema.AgenticMessage
	if err := json.Unmarshal(record.Transcript, &transcript); err != nil {
		return nil, err
	}
	var original []*schema.AgenticMessage
	if err := json.Unmarshal(record.Input, &original); err != nil {
		return nil, err
	}
	response := &schema.AgenticMessage{Role: schema.AgenticRoleTypeUser, ContentBlocks: []*schema.ContentBlock{
		schema.NewContentBlock(&schema.MCPToolApprovalResponse{ApprovalRequestID: state.ApprovalRequestID, Approve: decision == "approve", Reason: "host decision"}),
	}}
	continuation := append(append([]*schema.AgenticMessage{}, original...), &transcript, response)
	m.needMessage = true
	return continuation, nil
}

func (b *adkApprovalBinding) load(ctx context.Context, p *adkProof, messageID session.MessageID, partID session.PartID) (adkApprovalRecord, session.Part, error) {
	batch, err := p.store.ListMessages(ctx, p.run.SessionID, session.ReplayCursor{Limit: 1000})
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
	return adkApprovalRecord{}, session.Part{}, fmt.Errorf("approval record %s not found", partID)
}

// approvalRecords lists durable approval records for assertions.
func (p *adkProof) approvalRecords() []adkApprovalRecord {
	batch, err := p.store.ListMessages(p.ctx, p.run.SessionID, session.ReplayCursor{Limit: 1000})
	if err != nil {
		p.t.Fatal(err)
	}
	var records []adkApprovalRecord
	for _, part := range batch.Parts {
		if part.Kind != session.PartState {
			continue
		}
		var record adkApprovalRecord
		if err := json.Unmarshal(part.Payload, &record); err == nil && record.Type == adkApprovalPartType {
			records = append(records, record)
		}
	}
	return records
}
