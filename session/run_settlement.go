package session

import (
	"encoding/json"
	"time"
)

// RunSettlementEventKind is the one canonical terminal event phase for a run.
const RunSettlementEventKind = "run_finished"

// RunSettlement contains the caller-owned terminal fields applied to the
// current fenced run. Store-owned identity, ownership, and lease fields remain
// authoritative.
type RunSettlement struct {
	Status     RunStatus
	FinishedAt time.Time
	Error      string
}

// RunSettlementEvent contains the event values that cannot be derived from the
// canonical terminal run.
type RunSettlementEvent struct {
	ID        EventID
	MessageID MessageID
	Usage     Usage
	ErrorCode string
	Retryable bool
}

// SettleRunRequest atomically commits terminal run state and its canonical
// durable event.
type SettleRunRequest struct {
	Settlement RunSettlement
	Event      RunSettlementEvent
}

// RunSettlementResult is the canonical run/event pair committed by settlement.
type RunSettlementResult struct {
	Run   Run
	Event EventRecord
}

type runSettlementPayload struct {
	Status      RunStatus `json:"status"`
	Interrupted bool      `json:"interrupted"`
}

// ApplyRunSettlement derives a canonical terminal run from the currently
// fenced durable run and caller-owned terminal fields.
func ApplyRunSettlement(current Run, settlement RunSettlement) (Run, error) {
	if current.ID == "" || current.SessionID == "" || current.ClaimToken == "" ||
		!terminalRunStatus(settlement.Status) || settlement.FinishedAt.IsZero() ||
		(settlement.Status == RunCompleted && settlement.Error != "") {
		return Run{}, ErrConflict
	}
	current.Status = settlement.Status
	current.FinishedAt = settlement.FinishedAt.UTC()
	current.Error = settlement.Error
	return current, nil
}

// RunSettlementRecord derives the canonical durable terminal event from a
// complete terminal run and its bounded event envelope.
func RunSettlementRecord(run Run, event RunSettlementEvent) (EventRecord, error) {
	if !run.Terminal() || run.ID == "" || run.SessionID == "" || run.ClaimToken == "" ||
		run.FinishedAt.IsZero() || event.ID == "" ||
		(run.Status == RunCompleted && (event.ErrorCode != "" || event.Retryable)) {
		return EventRecord{}, ErrConflict
	}
	payload, err := json.Marshal(runSettlementPayload{Status: run.Status, Interrupted: run.Status == RunInterrupted})
	if err != nil {
		return EventRecord{}, err
	}
	return EventRecord{
		ID: event.ID, SessionID: run.SessionID, RunID: run.ID, MessageID: event.MessageID,
		ProviderID: run.ProviderID, ModelID: run.ModelID, Kind: RunSettlementEventKind,
		Usage: event.Usage, Error: EventError{Code: event.ErrorCode, Message: run.Error, Retryable: event.Retryable},
		Payload: payload, Redaction: RedactionMetadata, CreatedAt: run.FinishedAt.UTC(),
	}, nil
}

func terminalRunStatus(status RunStatus) bool {
	return status == RunInterrupted || status == RunFailed || status == RunCompleted
}
