package session

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrModelRequestTooLarge = errors.New("model request record exceeds retention limit")

type ModelRequestState string

const (
	ModelRequestPrepared        ModelRequestState = "prepared"
	ModelRequestDispatchStarted ModelRequestState = "dispatch_started"
	ModelRequestCompleted       ModelRequestState = "completed"
	ModelRequestFailed          ModelRequestState = "failed"
)

type ModelRequestRecord struct {
	ID                 ModelRequestID
	SessionID          ID
	RunID              RunID
	AssistantMessageID MessageID
	Attempt            int
	Step               int
	ProviderID         string
	ModelID            string
	State              ModelRequestState
	Messages           json.RawMessage
	System             string
	Tools              json.RawMessage
	SafeCallConfig     json.RawMessage
	ContentSHA256      string
	ExtensionPlanHash  string
	ErrorCode          string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type ModelRequestCursor struct {
	AfterID ModelRequestID
	Limit   int
}

type ModelRequestBatch struct {
	Records []ModelRequestRecord
	Next    ModelRequestCursor
}

// ModelRequestReader exposes durable provider-attempt records without granting mutation authority.
type ModelRequestReader interface {
	GetModelRequest(context.Context, ModelRequestID) (ModelRequestRecord, error)
	ListModelRequests(context.Context, RunID, ModelRequestCursor) (ModelRequestBatch, error)
}

// ModelRequestWriter mutates provider-attempt records through a run-fenced execution store.
type ModelRequestWriter interface {
	CreateModelRequest(context.Context, ModelRequestRecord) (ModelRequestRecord, error)
	UpdateModelRequest(context.Context, ModelRequestRecord) error
}

func ValidModelRequestTransition(from, to ModelRequestState) bool {
	return from == ModelRequestPrepared && (to == ModelRequestDispatchStarted || to == ModelRequestFailed) ||
		from == ModelRequestDispatchStarted && (to == ModelRequestCompleted || to == ModelRequestFailed)
}
