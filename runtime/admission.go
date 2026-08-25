package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
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
	RunClaimToken      string
}

// AdmissionRequest describes the durable records created before execution.
type AdmissionRequest struct {
	IDs                  AdmissionIDs
	ParentMessageID      session.MessageID
	Config               config.Snapshot
	Model                model.Resolved
	Input                []*einoschema.Message
	OwnerID              string
	LeaseDuration        time.Duration
	Metadata             map[string]string
	ExtensionPlan        session.ExtensionPlanDescriptor
	admissionFingerprint string
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
	Events     EventSink
	Extensions *extension.Plan
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
	if request.LeaseDuration <= 0 {
		return AdmittedRun{}, fmt.Errorf("%w: positive lease duration required", ErrInvalidAdmission)
	}
	request, err := freezeAdmissionRequest(request)
	if err != nil {
		return AdmittedRun{}, fmt.Errorf("%w: freeze request: %v", ErrInvalidAdmission, err)
	}
	now := a.now()
	frozenSnapshot, err := FreezeTurnSnapshot(request.IDs.RunID, request.IDs.SessionID, request.IDs.ContextEpochID, request.Config, request.Model, request.Input, request.Config.Agent.SystemPrompt, now)
	if err != nil {
		return AdmittedRun{}, fmt.Errorf("%w: freeze snapshot: %v", ErrInvalidAdmission, err)
	}
	if existing, err := a.existingAdmission(ctx, request, frozenSnapshot, now); err == nil {
		existing.AlreadyAdmitted = true
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return AdmittedRun{}, err
	}

	var admitted AdmittedRun
	if err := a.Store.WithinTx(ctx, func(ctx context.Context, store session.Store) error {
		var err error
		admitted, err = admitDurable(ctx, store, request, frozenSnapshot, now)
		return err
	}); err != nil {
		return AdmittedRun{}, err
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

func (a Admitter) existingAdmission(ctx context.Context, request AdmissionRequest, snapshot TurnSnapshot, now time.Time) (AdmittedRun, error) {
	runRecord, err := a.Store.GetRun(ctx, request.IDs.RunID)
	if err != nil {
		return AdmittedRun{}, err
	}
	if err := validateMatchingExtensionPlans(runRecord.ExtensionPlan, request.ExtensionPlan); err != nil {
		return AdmittedRun{}, err
	}
	if err := validateExistingAdmission(runRecord, request); err != nil {
		return AdmittedRun{}, err
	}
	sessionRecord, err := a.Store.GetSession(ctx, runRecord.SessionID)
	if err != nil {
		return AdmittedRun{}, err
	}
	assistantMessage, err := findMessage(ctx, a.Store, sessionRecord.ID, request.IDs.AssistantMessageID)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return AdmittedRun{}, session.ErrConflict
		}
		return AdmittedRun{}, err
	}
	if sessionRecord.ID != request.IDs.SessionID || assistantMessage.ID != request.IDs.AssistantMessageID || assistantMessage.SessionID != sessionRecord.ID || assistantMessage.RunID != runRecord.ID || assistantMessage.ParentID != request.ParentMessageID || assistantMessage.Agent != request.Config.Agent.Name || assistantMessage.ModelID != admissionModelID(request) {
		return AdmittedRun{}, session.ErrConflict
	}
	return buildAdmittedRun(sessionRecord, runRecord, assistantMessage, snapshot, now), nil
}

func validateMatchingExtensionPlans(persisted, requested session.ExtensionPlanDescriptor) error {
	if persisted.SchemaVersion != session.ExtensionPlanSchemaVersion || persisted.Fingerprint == "" || requested.SchemaVersion != session.ExtensionPlanSchemaVersion || requested.Fingerprint == "" {
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

func admitDurable(ctx context.Context, store session.Store, request AdmissionRequest, snapshot TurnSnapshot, now time.Time) (AdmittedRun, error) {
	sessionRecord, err := store.CreateSession(ctx, admissionSession(request, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	runRecord, err := store.AdmitRun(ctx, admissionRun(request, sessionRecord.ID, now), request.LeaseDuration)
	if err != nil {
		return AdmittedRun{}, err
	}
	executionStore := store.Execution(session.RunFence{RunID: runRecord.ID, ClaimToken: runRecord.ClaimToken})
	if _, err := executionStore.StartContextEpoch(ctx, admissionContextEpoch(request, sessionRecord.ID, now)); err != nil {
		return AdmittedRun{}, err
	}
	assistantMessage, err := executionStore.AppendMessage(ctx, admissionAssistantMessage(request, sessionRecord.ID, runRecord.ID, now))
	if err != nil {
		return AdmittedRun{}, err
	}
	event := admissionEvent(request, sessionRecord.ID, runRecord.ID, assistantMessage.ID, now)
	if _, err := executionStore.AppendEvent(ctx, event); err != nil {
		return AdmittedRun{}, err
	}
	return buildAdmittedRun(sessionRecord, runRecord, assistantMessage, snapshot, now), nil
}

func (a Admitter) afterDurableAdmission(ctx context.Context, admitted AdmittedRun, request AdmissionRequest, now time.Time) error {
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
	if a.Extensions != nil {
		_ = extension.Notify(a.Extensions, ctx, RunAdmittedPoint, RunAdmittedNotice{SessionID: admitted.Session.ID, RunID: admitted.Run.ID, Plan: request.ExtensionPlan, Metadata: boundedTurnMetadata(admitted.Snapshot), Time: now})
	}
	return nil
}

func buildAdmittedRun(sessionRecord session.Session, runRecord session.Run, assistantMessage session.Message, snapshot TurnSnapshot, now time.Time) AdmittedRun {
	snapshot.RunID = runRecord.ID
	snapshot.SessionID = sessionRecord.ID
	snapshot.EpochID = runRecord.ContextEpoch
	snapshot.CreatedAt = now
	return AdmittedRun{
		Session:          sessionRecord,
		Run:              runRecord,
		AssistantMessage: assistantMessage,
		Snapshot:         snapshot,
	}
}

const admissionFingerprintKey = "_admission_fingerprint"

func freezeAdmissionRequest(request AdmissionRequest) (AdmissionRequest, error) {
	request.Config = request.Config.Clone()
	root, err := canonicalAdmissionWorkspace(request.Config.Metadata["workspace_root"])
	if err != nil {
		return AdmissionRequest{}, err
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
		return AdmissionRequest{}, err
	}
	request.Input = cloned.Messages
	request.Metadata = cloneStringMap(request.Metadata)
	request.ExtensionPlan = request.ExtensionPlan.Clone()
	payload := struct {
		Config       config.Snapshot
		Provider     model.Provider
		Model        model.Descriptor
		Messages     []*einoschema.Message
		SystemPrompt string
	}{request.Config, request.Model.Provider, request.Model.Model, request.Input, request.Config.Agent.SystemPrompt}
	raw, err := json.Marshal(payload)
	if err != nil {
		return AdmissionRequest{}, err
	}
	sum := sha256.Sum256(raw)
	request.admissionFingerprint = hex.EncodeToString(sum[:])
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

func validateExistingAdmission(run session.Run, request AdmissionRequest) error {
	if run.ID != request.IDs.RunID || run.ClaimToken != request.IDs.RunClaimToken || run.SessionID != request.IDs.SessionID || run.ContextEpoch != request.IDs.ContextEpochID || run.ParentMsgID != request.ParentMessageID || run.Agent != request.Config.Agent.Name || run.ProviderID != admissionProviderID(request) || run.ModelID != admissionModelID(request) || run.Config[admissionFingerprintKey] != request.admissionFingerprint {
		return session.ErrConflict
	}
	return nil
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
	case ids.RunClaimToken == "":
		return fmt.Errorf("%w: run claim token required", ErrInvalidAdmission)
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
	return session.Run{
		ID:            request.IDs.RunID,
		SessionID:     sessionID,
		ParentMsgID:   request.ParentMessageID,
		OwnerID:       request.OwnerID,
		ClaimToken:    request.IDs.RunClaimToken,
		Agent:         request.Config.Agent.Name,
		ProviderID:    admissionProviderID(request),
		ModelID:       admissionModelID(request),
		ContextEpoch:  request.IDs.ContextEpochID,
		Status:        session.RunPending,
		Config:        admissionConfig(request),
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
	payload := mustJSON(map[string]string{
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

func admissionConfig(request AdmissionRequest) map[string]string {
	snapshot := request.Config
	return map[string]string{
		"agent":                 snapshot.Agent.Name,
		"workspace_id":          snapshot.Metadata["workspace_id"],
		"workspace_root":        snapshot.Metadata["workspace_root"],
		admissionFingerprintKey: request.admissionFingerprint,
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
