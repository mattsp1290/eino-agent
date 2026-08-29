package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

var ErrInvalidAdmission = errors.New("invalid admission")

type admissionIDs struct {
	SessionID          session.ID
	RunID              session.RunID
	AssistantMessageID session.MessageID
	ContextEpochID     session.EpochID
	EventID            session.EventID
	RunClaimToken      string
}

type admissionRequest struct {
	IDs             admissionIDs
	ParentMessageID session.MessageID
	Config          config.Snapshot
	Model           model.Resolved
	Input           []*einoschema.Message
	OwnerID         string
	LeaseDuration   time.Duration
	Metadata        map[string]string
	ExtensionPlan   session.ExtensionPlanDescriptor
}

type admittedRun struct {
	Session          session.Session
	Run              session.Run
	AssistantMessage session.Message
	Event            session.EventRecord
	Snapshot         TurnSnapshot
}

type admitter struct {
	Store session.Store
	Clock func() time.Time
}

func (a admitter) admit(ctx context.Context, request admissionRequest) (admittedRun, error) {
	if a.Store == nil {
		return admittedRun{}, fmt.Errorf("%w: store required", ErrInvalidAdmission)
	}
	if err := model.ValidateResolved(request.Config.Model, request.Model); err != nil {
		return admittedRun{}, fmt.Errorf("%w: %w", ErrInvalidAdmission, err)
	}
	if err := validateAdmissionIdentity(request.IDs); err != nil {
		return admittedRun{}, err
	}
	if request.LeaseDuration <= 0 {
		return admittedRun{}, fmt.Errorf("%w: positive lease duration required", ErrInvalidAdmission)
	}
	request, err := freezeAdmission(request)
	if err != nil {
		return admittedRun{}, fmt.Errorf("%w: freeze request: %v", ErrInvalidAdmission, err)
	}
	now := a.now()
	frozenSnapshot, err := FreezeTurnSnapshot(request.IDs.RunID, request.IDs.SessionID, request.IDs.ContextEpochID, request.Config, request.Model, request.Input, request.Config.Agent.SystemPrompt, now)
	if err != nil {
		return admittedRun{}, fmt.Errorf("%w: freeze snapshot: %v", ErrInvalidAdmission, err)
	}

	var admitted admittedRun
	if err := a.Store.WithinTx(ctx, func(ctx context.Context, store session.Store) error {
		var err error
		admitted, err = admitDurable(ctx, store, request, frozenSnapshot, now)
		return err
	}); err != nil {
		return admittedRun{}, err
	}
	return admitted, nil
}

func cloneJSON(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	cloned := make(json.RawMessage, len(raw))
	copy(cloned, raw)
	return cloned
}

func (a admitter) now() time.Time {
	if a.Clock != nil {
		return a.Clock().UTC()
	}
	return time.Now().UTC()
}

func admitDurable(ctx context.Context, store session.Store, request admissionRequest, snapshot TurnSnapshot, now time.Time) (admittedRun, error) {
	sessionRecord, err := store.CreateSession(ctx, admissionSession(request, now))
	if err != nil {
		return admittedRun{}, err
	}
	runRecord, err := store.AdmitRun(ctx, admissionRun(request, sessionRecord.ID, now), request.LeaseDuration)
	if err != nil {
		return admittedRun{}, err
	}
	executionStore := store.Execution(session.RunFence{RunID: runRecord.ID, ClaimToken: runRecord.ClaimToken})
	if _, err := executionStore.StartContextEpoch(ctx, admissionContextEpoch(request, sessionRecord.ID, now)); err != nil {
		return admittedRun{}, err
	}
	assistantMessage, err := executionStore.AppendMessage(ctx, admissionAssistantMessage(request, sessionRecord.ID, runRecord.ID, now))
	if err != nil {
		return admittedRun{}, err
	}
	event := admissionEvent(request, sessionRecord.ID, runRecord.ID, assistantMessage.ID, now)
	committedEvent, err := executionStore.AppendEvent(ctx, event)
	if err != nil {
		return admittedRun{}, err
	}
	return buildAdmission(sessionRecord, runRecord, assistantMessage, committedEvent, snapshot, now), nil
}

func buildAdmission(sessionRecord session.Session, runRecord session.Run, assistantMessage session.Message, event session.EventRecord, snapshot TurnSnapshot, now time.Time) admittedRun {
	snapshot.RunID = runRecord.ID
	snapshot.SessionID = sessionRecord.ID
	snapshot.EpochID = runRecord.ContextEpoch
	snapshot.CreatedAt = now
	return admittedRun{
		Session:          sessionRecord,
		Run:              runRecord,
		AssistantMessage: assistantMessage,
		Event:            event,
		Snapshot:         snapshot,
	}
}

func freezeAdmission(request admissionRequest) (admissionRequest, error) {
	request.Config = request.Config.Clone()
	root, err := canonicalAdmissionWorkspace(request.Config.Metadata["workspace_root"])
	if err != nil {
		return admissionRequest{}, err
	}
	if request.Config.Metadata == nil && root != "" {
		request.Config.Metadata = make(map[string]string)
	}
	if root != "" {
		request.Config.Metadata["workspace_root"] = root
	}
	request.Model = cloneResolved(request.Model)
	cloned, err := (model.Request{Messages: request.Input}).Clone()
	if err != nil {
		return admissionRequest{}, err
	}
	request.Input = cloned.Messages
	request.Metadata = cloneStringMap(request.Metadata)
	request.ExtensionPlan = request.ExtensionPlan.Clone()
	return request, nil
}

func canonicalAdmissionWorkspace(root string) (string, error) {
	if root == "" {
		return "", nil
	}
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("workspace root must be absolute: %q", root)
	}
	clean := filepath.Clean(root)
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root %q: %w", root, err)
	}
	return resolved, nil
}

func validateAdmissionIdentity(ids admissionIDs) error {
	switch {
	case ids.SessionID == "":
		return fmt.Errorf("%w: session id required", ErrInvalidAdmission)
	case ids.RunID == "":
		return fmt.Errorf("%w: run id required", ErrInvalidAdmission)
	case ids.AssistantMessageID == "":
		return fmt.Errorf("%w: assistant message id required", ErrInvalidAdmission)
	case ids.ContextEpochID == "":
		return fmt.Errorf("%w: context epoch id required", ErrInvalidAdmission)
	case ids.EventID == "":
		return fmt.Errorf("%w: event id required", ErrInvalidAdmission)
	case ids.RunClaimToken == "":
		return fmt.Errorf("%w: run claim token required", ErrInvalidAdmission)
	default:
		return nil
	}
}

func admissionSession(request admissionRequest, now time.Time) session.Session {
	metadata := cloneStringMap(request.Metadata)
	return session.Session{
		ID:          request.IDs.SessionID,
		WorkspaceID: request.Config.Metadata["workspace_id"],
		Directory:   request.Config.Metadata["workspace_root"],
		Title:       string(request.IDs.SessionID),
		Metadata:    metadata,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func admissionRun(request admissionRequest, sessionID session.ID, now time.Time) session.Run {
	return session.Run{
		ID:            request.IDs.RunID,
		SessionID:     sessionID,
		ParentMsgID:   request.ParentMessageID,
		OwnerID:       request.OwnerID,
		ClaimToken:    request.IDs.RunClaimToken,
		Agent:         request.Config.Agent.Name,
		ProviderID:    string(request.Model.Provider.ID),
		ModelID:       string(request.Model.Model.ID),
		ContextEpoch:  request.IDs.ContextEpochID,
		Status:        session.RunPending,
		Config:        admissionConfig(request),
		ExtensionPlan: request.ExtensionPlan.Clone(),
		CreatedAt:     now,
	}
}

func admissionAssistantMessage(request admissionRequest, sessionID session.ID, runID session.RunID, now time.Time) session.Message {
	return session.Message{
		ID:        request.IDs.AssistantMessageID,
		SessionID: sessionID,
		RunID:     runID,
		ParentID:  request.ParentMessageID,
		Role:      session.RoleAssistant,
		Agent:     request.Config.Agent.Name,
		ModelID:   string(request.Model.Model.ID),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func admissionEvent(request admissionRequest, sessionID session.ID, runID session.RunID, messageID session.MessageID, now time.Time) session.EventRecord {
	payload := mustJSON(map[string]string{
		"agent":       request.Config.Agent.Name,
		"provider_id": string(request.Model.Provider.ID),
		"model_id":    string(request.Model.Model.ID),
	})
	return session.EventRecord{
		ID:         request.IDs.EventID,
		SessionID:  sessionID,
		RunID:      runID,
		MessageID:  messageID,
		EpochID:    request.IDs.ContextEpochID,
		ProviderID: string(request.Model.Provider.ID),
		ModelID:    string(request.Model.Model.ID),
		Kind:       string(EventRunStarted),
		Payload:    payload,
		Redaction:  session.RedactionMetadata,
		CreatedAt:  now,
	}
}

func admissionConfig(request admissionRequest) map[string]string {
	snapshot := request.Config
	return map[string]string{
		"agent":          snapshot.Agent.Name,
		"workspace_id":   snapshot.Metadata["workspace_id"],
		"workspace_root": snapshot.Metadata["workspace_root"],
	}
}
