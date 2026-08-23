package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
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
	ExtensionPlan   session.ExtensionPlanDescriptor
}

// AdmittedRun is the durable state created before model execution begins.
type AdmittedRun struct {
	Session          session.Session
	Run              session.Run
	AssistantMessage session.Message
	Snapshot         TurnSnapshot
	AlreadyAdmitted  bool
}

// Admitter persists the admission boundary before any provider execution.
type Admitter struct {
	Store      session.Store
	Transactor session.Transactor
	Events     EventSink
	Hooks      []Hook
	Clock      func() time.Time
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
	if existing, err := a.existingAdmission(ctx, request, now); err == nil {
		existing.AlreadyAdmitted = true
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return AdmittedRun{}, err
	}

	var admitted AdmittedRun
	transactor := a.transactor()
	if transactor != nil {
		err := transactor.WithinTx(ctx, func(ctx context.Context, tx session.Tx) error {
			var err error
			admitted, err = admitDurable(ctx, tx, request, now)
			return err
		})
		if err != nil {
			return AdmittedRun{}, err
		}
	} else {
		var err error
		admitted, err = admitDurable(ctx, a.Store, request, now)
		if err != nil {
			return AdmittedRun{}, err
		}
	}
	if err := a.afterDurableAdmission(ctx, admitted, request, now); err != nil {
		return AdmittedRun{}, err
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

func (a Admitter) now() time.Time {
	if a.Clock != nil {
		return a.Clock().UTC()
	}
	return time.Now().UTC()
}

func (a Admitter) transactor() session.Transactor {
	if a.Transactor != nil {
		return a.Transactor
	}
	transactor, _ := a.Store.(session.Transactor)
	return transactor
}

func (a Admitter) existingAdmission(ctx context.Context, request AdmissionRequest, now time.Time) (AdmittedRun, error) {
	runRecord, err := a.Store.GetRun(ctx, request.IDs.RunID)
	if err != nil {
		return AdmittedRun{}, err
	}
	if err := validateMatchingExtensionPlans(runRecord.ExtensionPlan, request.ExtensionPlan); err != nil {
		return AdmittedRun{}, err
	}
	sessionRecord, err := a.Store.GetSession(ctx, runRecord.SessionID)
	if err != nil {
		return AdmittedRun{}, err
	}
	assistantMessage, err := findMessage(ctx, a.Store, sessionRecord.ID, request.IDs.AssistantMessageID)
	if err != nil {
		return AdmittedRun{}, err
	}
	return buildAdmittedRun(sessionRecord, runRecord, assistantMessage, request, now), nil
}

func validateMatchingExtensionPlans(persisted, requested session.ExtensionPlanDescriptor) error {
	if !admissibleExtensionPlanMode(persisted.Mode) || persisted.SchemaVersion == 0 || persisted.Fingerprint == "" || !admissibleExtensionPlanMode(requested.Mode) || requested.SchemaVersion == 0 || requested.Fingerprint == "" {
		return ErrExtensionPlanMismatch
	}
	persistedFingerprint, err := session.FingerprintExtensionPlan(persisted)
	if err != nil || persisted.Fingerprint != persistedFingerprint {
		return ErrExtensionPlanMismatch
	}
	requestedFingerprint, err := session.FingerprintExtensionPlan(requested)
	if err != nil || requested.Fingerprint != requestedFingerprint {
		return ErrExtensionPlanMismatch
	}
	if persistedFingerprint != requestedFingerprint {
		return ErrExtensionPlanMismatch
	}
	return nil
}

func admissibleExtensionPlanMode(mode session.PlanMode) bool {
	return mode == session.PlanStrict || mode == session.PlanPartialLegacy
}

func admitDurable(ctx context.Context, store session.Store, request AdmissionRequest, now time.Time) (AdmittedRun, error) {
	sessionRecord, err := store.CreateSession(ctx, admissionSession(request, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	runRecord, err := store.AdmitRun(ctx, admissionRun(request, sessionRecord.ID, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	if _, err := store.StartContextEpoch(ctx, admissionContextEpoch(request, sessionRecord.ID, now)); err != nil {
		return AdmittedRun{}, err
	}
	assistantMessage, err := store.AppendMessage(ctx, admissionAssistantMessage(request, sessionRecord.ID, runRecord.ID, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	event := admissionEvent(request, sessionRecord.ID, runRecord.ID, assistantMessage.ID, now)
	if _, err := store.AppendEvent(ctx, event); err != nil {
		return AdmittedRun{}, err
	}
	return buildAdmittedRun(sessionRecord, runRecord, assistantMessage, request, now), nil
}

func (a Admitter) afterDurableAdmission(ctx context.Context, admitted AdmittedRun, request AdmissionRequest, now time.Time) error {
	for _, hook := range a.Hooks {
		if err := hook.BeforeRun(ctx, admitted.Run); err != nil {
			return err
		}
	}
	if a.Events != nil {
		event := admissionEvent(request, admitted.Session.ID, admitted.Run.ID, admitted.AssistantMessage.ID, now)
		_ = a.Events.Emit(ctx, Event{
			Kind:       EventRunStarted,
			EventID:    event.ID,
			SessionID:  admitted.Session.ID,
			RunID:      admitted.Run.ID,
			MessageID:  admitted.AssistantMessage.ID,
			EpochID:    request.IDs.ContextEpochID,
			ProviderID: event.ProviderID,
			ModelID:    event.ModelID,
			Payload:    cloneJSON(event.Payload),
			Time:       now,
		})
	}
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		_ = extension.Notify(plan.Dispatch, ctx, RunAdmittedPoint, RunAdmittedNotice{SessionID: admitted.Session.ID, RunID: admitted.Run.ID, Plan: plan.Descriptor, Metadata: boundedTurnMetadata(admitted.Snapshot), Time: now})
	}
	return nil
}

func buildAdmittedRun(sessionRecord session.Session, runRecord session.Run, assistantMessage session.Message, request AdmissionRequest, now time.Time) AdmittedRun {
	return AdmittedRun{
		Session:          sessionRecord,
		Run:              runRecord,
		AssistantMessage: assistantMessage,
		Snapshot: FreezeTurnSnapshot(
			runRecord.ID,
			sessionRecord.ID,
			request.IDs.ContextEpochID,
			request.Config,
			request.Model,
			request.Input,
			request.Config.Agent.SystemPrompt,
			now,
		),
	}
}

func findMessage(ctx context.Context, store session.Store, sessionID session.ID, id session.MessageID) (session.Message, error) {
	cursor := session.ReplayCursor{Limit: 100}
	for {
		batch, err := store.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			return session.Message{}, err
		}
		for _, message := range batch.Messages {
			if message.ID == id {
				return message, nil
			}
		}
		if batch.Next == (session.ReplayCursor{}) {
			return session.Message{}, session.ErrNotFound
		}
		cursor = batch.Next
	}
}

func validateAdmissionIDs(ids AdmissionIDs) error {
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
		ID:            request.IDs.RunID,
		SessionID:     sessionID,
		ParentMsgID:   request.ParentMessageID,
		OwnerID:       request.OwnerID,
		LeaseUntil:    leaseUntil,
		Agent:         request.Config.Agent.Name,
		ProviderID:    admissionProviderID(request),
		ModelID:       admissionModelID(request),
		ContextEpoch:  request.IDs.ContextEpochID,
		Status:        session.RunPending,
		Config:        admissionConfig(request.Config),
		Components:    admissionComponents(request.Config),
		ExtensionPlan: request.ExtensionPlan.Clone(),
		CreatedAt:     now,
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
		ModelID:   admissionModelID(request),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func admissionEvent(request AdmissionRequest, sessionID session.ID, runID session.RunID, messageID session.MessageID, now time.Time) session.EventRecord {
	payload, _ := json.Marshal(map[string]string{
		"agent":       request.Config.Agent.Name,
		"provider_id": admissionProviderID(request),
		"model_id":    admissionModelID(request),
	})
	return session.EventRecord{
		ID:         request.IDs.EventID,
		SessionID:  sessionID,
		RunID:      runID,
		MessageID:  messageID,
		EpochID:    request.IDs.ContextEpochID,
		ProviderID: admissionProviderID(request),
		ModelID:    admissionModelID(request),
		Kind:       string(EventRunStarted),
		Payload:    payload,
		Redaction:  session.RedactionMetadata,
		CreatedAt:  now,
	}
}

func admissionProviderID(request AdmissionRequest) string {
	if request.Model.Provider.ID != "" {
		return string(request.Model.Provider.ID)
	}
	return string(request.Config.Model.ProviderID)
}

func admissionModelID(request AdmissionRequest) string {
	if request.Model.Model.ID != "" {
		return string(request.Model.Model.ID)
	}
	return string(request.Config.Model.ModelID)
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
