package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

var ErrInvalidAdmission = errors.New("invalid admission")

// AdmissionIDs are caller-chosen durable identifiers for one admitted run.
type AdmissionIDs struct {
	SessionID          session.ID
	RunID              session.RunID
	AssistantMessageID session.MessageID
	ContextEpochID     session.EpochID
	EventID            session.EventID
}

// AdmissionRequest describes the durable records created before execution.
type AdmissionRequest struct {
	IDs             AdmissionIDs
	ParentMessageID session.MessageID
	Config          config.Snapshot
	Model           model.Resolved
	Input           []*einoschema.Message
	OwnerID         string
	LeaseUntil      time.Time
	Metadata        map[string]string
}

// AdmittedRun is the durable state created before model execution begins.
type AdmittedRun struct {
	Session          session.Session
	Run              session.Run
	AssistantMessage session.Message
	Snapshot         TurnSnapshot
}

// Admitter persists the admission boundary before any provider execution.
type Admitter struct {
	Store  session.Store
	Events EventSink
	Hooks  []Hook
	Clock  func() time.Time
}

// Admit creates the durable session, run, assistant message, and run-start
// event before returning a frozen turn snapshot to execution code.
func (a Admitter) Admit(ctx context.Context, request AdmissionRequest) (AdmittedRun, error) {
	if a.Store == nil {
		return AdmittedRun{}, fmt.Errorf("%w: store required", ErrInvalidAdmission)
	}
	if err := validateAdmissionIDs(request.IDs); err != nil {
		return AdmittedRun{}, err
	}
	now := a.now()
	sessionRecord, err := a.Store.CreateSession(ctx, admissionSession(request, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	runRecord, err := a.Store.AdmitRun(ctx, admissionRun(request, sessionRecord.ID, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	for _, hook := range a.Hooks {
		if err := hook.BeforeRun(ctx, runRecord); err != nil {
			return AdmittedRun{}, err
		}
	}
	assistantMessage, err := a.Store.AppendMessage(ctx, admissionAssistantMessage(request, sessionRecord.ID, runRecord.ID, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	event := admissionEvent(request, sessionRecord.ID, runRecord.ID, assistantMessage.ID, now)
	if _, err := a.Store.AppendEvent(ctx, event); err != nil {
		return AdmittedRun{}, err
	}
	if a.Events != nil {
		if err := a.Events.Emit(ctx, Event{
			Kind:       EventRunStarted,
			SessionID:  sessionRecord.ID,
			RunID:      runRecord.ID,
			MessageID:  assistantMessage.ID,
			EpochID:    request.IDs.ContextEpochID,
			ProviderID: event.ProviderID,
			ModelID:    event.ModelID,
			Payload:    cloneJSON(event.Payload),
			Time:       now,
		}); err != nil {
			return AdmittedRun{}, err
		}
	}
	snapshot := FreezeTurnSnapshot(
		runRecord.ID,
		sessionRecord.ID,
		request.IDs.ContextEpochID,
		request.Config,
		request.Model,
		request.Input,
		request.Config.Agent.SystemPrompt,
		now,
	)
	return AdmittedRun{
		Session:          sessionRecord,
		Run:              runRecord,
		AssistantMessage: assistantMessage,
		Snapshot:         snapshot,
	}, nil
}

func cloneJSON(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	cloned := make(json.RawMessage, len(raw))
	copy(cloned, raw)
	return cloned
}

func (a Admitter) now() time.Time {
	if a.Clock != nil {
		return a.Clock().UTC()
	}
	return time.Now().UTC()
}

func validateAdmissionIDs(ids AdmissionIDs) error {
	switch {
	case ids.SessionID == "":
		return fmt.Errorf("%w: session id required", ErrInvalidAdmission)
	case ids.RunID == "":
		return fmt.Errorf("%w: run id required", ErrInvalidAdmission)
	case ids.AssistantMessageID == "":
		return fmt.Errorf("%w: assistant message id required", ErrInvalidAdmission)
	case ids.EventID == "":
		return fmt.Errorf("%w: event id required", ErrInvalidAdmission)
	default:
		return nil
	}
}

func admissionSession(request AdmissionRequest, now time.Time) session.Session {
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

func admissionRun(request AdmissionRequest, sessionID session.ID, now time.Time) session.Run {
	leaseUntil := request.LeaseUntil
	if leaseUntil.IsZero() {
		leaseUntil = now.Add(time.Minute)
	}
	return session.Run{
		ID:           request.IDs.RunID,
		SessionID:    sessionID,
		ParentMsgID:  request.ParentMessageID,
		OwnerID:      request.OwnerID,
		LeaseUntil:   leaseUntil,
		Agent:        request.Config.Agent.Name,
		ProviderID:   string(request.Model.Provider.ID),
		ModelID:      string(request.Model.Model.ID),
		ContextEpoch: request.IDs.ContextEpochID,
		Status:       session.RunPending,
		Config:       admissionConfig(request.Config),
		Components:   admissionComponents(request.Config),
		CreatedAt:    now,
	}
}

func admissionAssistantMessage(request AdmissionRequest, sessionID session.ID, runID session.RunID, now time.Time) session.Message {
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

func admissionEvent(request AdmissionRequest, sessionID session.ID, runID session.RunID, messageID session.MessageID, now time.Time) session.EventRecord {
	payload, _ := json.Marshal(map[string]string{
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

func admissionConfig(snapshot config.Snapshot) map[string]string {
	return map[string]string{
		"agent":          snapshot.Agent.Name,
		"workspace_id":   snapshot.Metadata["workspace_id"],
		"workspace_root": snapshot.Metadata["workspace_root"],
	}
}

func admissionComponents(snapshot config.Snapshot) []session.ComponentVersion {
	components := make([]session.ComponentVersion, 0, len(snapshot.Plugins))
	for _, plugin := range snapshot.Plugins {
		components = append(components, session.ComponentVersion{
			Name:    plugin.Name,
			Version: plugin.Version,
			Hash:    plugin.Hash,
			Source:  plugin.Source,
		})
	}
	return components
}
