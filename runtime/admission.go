package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

var ErrInvalidAdmission = errors.New("invalid admission")

type admissionIDs struct {
	SessionID          session.ID
	RunID              session.RunID
	UserMessageID      session.MessageID
	UserPartIDs        []session.PartID
	AssistantMessageID session.MessageID
	ContextEpochID     session.EpochID
	EventID            session.EventID
	RunClaimToken      string
	// TurnID and TurnStartedEventID identify the run's first admitted turn,
	// committed atomically with admission (see admitDurable). Distinct from
	// EventID, which identifies the separate run_started event.
	TurnID             session.TurnID
	TurnStartedEventID session.EventID
}

type admissionRequest struct {
	IDs           admissionIDs
	UserMessage   UserMessage
	History       history.Options
	Config        config.Snapshot
	Model         model.Resolved
	OwnerID       string
	LeaseDuration time.Duration
	Metadata      map[string]string
	ExtensionPlan session.ExtensionPlanDescriptor
	ContentLimits session.ContentLimits
}

type admittedRun struct {
	Session          session.Session
	Run              session.Run
	UserMessage      session.Message
	UserParts        []session.Part
	AssistantMessage session.Message
	Event            session.EventRecord
	Snapshot         TurnSnapshot
	// Turn is the run's first admitted turn (ordinal 1), committed
	// atomically with the run/session/messages/epoch by admitDurable.
	Turn session.Turn
	// HistoryOptions is the options actually used to build Snapshot's
	// projection -- request.History with Epoch resolved to this session's
	// most recently finished summarization epoch, if any (see
	// resolveTurnHistoryOptions) -- so the caller's first-turn adkEngine
	// (which will reload this same session repeatedly across this turn's
	// own ReAct cycles) stays consistent with what Snapshot/baseMessageCount
	// were actually computed against.
	HistoryOptions history.Options
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
	var admitted admittedRun
	if err := a.Store.WithinTx(ctx, func(ctx context.Context, store session.Store) error {
		var err error
		admitted, err = admitDurable(ctx, store, request, now)
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

func admitDurable(ctx context.Context, store session.Store, request admissionRequest, now time.Time) (admittedRun, error) {
	sessionRecord, err := getOrCreateAdmissionSession(ctx, store, request, now)
	if err != nil {
		return admittedRun{}, err
	}
	runRecord, err := store.AdmitRun(ctx, admissionRun(request, sessionRecord.ID, now), request.LeaseDuration)
	if err != nil {
		return admittedRun{}, err
	}
	resolvedHistory, err := resolveTurnHistoryOptions(ctx, store, sessionRecord.ID, request.History)
	if err != nil {
		return admittedRun{}, err
	}
	historyMessages, historySourceIDs, providerState, err := loadProviderHistory(ctx, store, sessionRecord, resolvedHistory, request.Model)
	if err != nil {
		return admittedRun{}, err
	}
	// A prior run on this same session that failed (or was otherwise never
	// finalized) can leave its own assistant placeholder row durably behind
	// with zero content blocks -- AdmitTurn always creates one at admission,
	// and nothing ever deletes it on failure (session.ApplyFailTurn only
	// transitions turn state; see turn.go). Every OTHER admission-time reload
	// in this package (turnLoopCoordinator.admitTurn, resumeEngine) drops
	// such placeholders immediately after loadProviderHistory before using
	// the result for baseMessageCount -- this call site must match, or a
	// leftover placeholder gets counted into THIS run's admitted base here
	// but is then correctly dropped by every later adkEngine.buildDurableBaseline
	// reload within this run's own first turn, permanently tripping its
	// "durable history shrank below this turn's admitted base" guard
	// (eino-agent-978's true root cause: the guard was correct, this count
	// was wrong).
	historyMessages, historySourceIDs, providerState = dropUnfinalizedAssistantPlaceholders(historyMessages, historySourceIDs, providerState)
	admittedUserContent := session.Content{Role: session.RoleUser, Blocks: request.UserMessage.Blocks}
	admittedUserMessage, err := session.ContentToAgenticMessage(admittedUserContent)
	if err != nil {
		return admittedRun{}, fmt.Errorf("%w: convert admitted user message: %v", ErrInvalidAdmission, err)
	}
	providerMessages := make([]*einoschema.AgenticMessage, 0, len(historyMessages)+1)
	providerMessages = append(providerMessages, historyMessages...)
	providerMessages = append(providerMessages, admittedUserMessage)
	// providerMessageSourceIDs is providerMessages' durable-id parallel
	// (round-two W6 review item 8): historySourceIDs for the reloaded
	// prefix, plus request.IDs.UserMessageID for the freshly admitted user
	// message this same admission is about to durably commit (below, as
	// userMessage) -- both are genuinely durable, so both carry real ids.
	providerMessageSourceIDs := make([]session.MessageID, 0, len(historyMessages)+1)
	providerMessageSourceIDs = append(providerMessageSourceIDs, paddedMessageSourceIDs(historySourceIDs, len(historyMessages))...)
	providerMessageSourceIDs = append(providerMessageSourceIDs, request.IDs.UserMessageID)
	snapshot, err := freezeTurnSnapshotWithProviderState(request.IDs.RunID, request.IDs.SessionID, request.IDs.ContextEpochID, request.Config, request.Model, providerMessages, providerState, request.Config.Agent.SystemPrompt, now)
	if err != nil {
		return admittedRun{}, fmt.Errorf("%w: freeze snapshot: %v", ErrInvalidAdmission, err)
	}
	snapshot.MessageSourceIDs = providerMessageSourceIDs
	latestMessageAt, err := latestAdmissionMessageTime(ctx, store, sessionRecord.ID)
	if err != nil {
		return admittedRun{}, err
	}
	userAt := now
	if next := latestMessageAt.Add(time.Nanosecond); next.After(userAt) {
		userAt = next
	}
	assistantAt := userAt.Add(time.Nanosecond)
	executionStore := store.Execution(session.RunFence{RunID: runRecord.ID, ClaimToken: runRecord.ClaimToken})
	if _, err := executionStore.StartContextEpoch(ctx, admissionContextEpoch(request, sessionRecord.ID, now)); err != nil {
		return admittedRun{}, err
	}
	userMessage := admissionUserMessage(request, sessionRecord.ID, runRecord.ID, userAt)
	userParts, err := admissionUserParts(request, sessionRecord.ID, runRecord.ID, userMessage.ID, userAt)
	if err != nil {
		return admittedRun{}, fmt.Errorf("%w: encode user content: %v", ErrInvalidAdmission, err)
	}
	assistantMessage := admissionAssistantMessage(request, sessionRecord.ID, runRecord.ID, assistantAt)
	turn := session.Turn{
		ID: request.IDs.TurnID, RunID: runRecord.ID, SessionID: sessionRecord.ID, Ordinal: 1, State: session.TurnAdmitted,
		UserMessageIDs: []session.MessageID{userMessage.ID}, AssistantMessageID: assistantMessage.ID,
		EpochID: request.IDs.ContextEpochID, CreatedAt: now,
	}
	turnStartedEvent := session.EventRecord{
		ID: request.IDs.TurnStartedEventID, SessionID: sessionRecord.ID, RunID: runRecord.ID, MessageID: assistantMessage.ID,
		EpochID: request.IDs.ContextEpochID, TurnID: turn.ID, Kind: session.TurnStartedEventKind, CreatedAt: now,
	}
	admitted, err := executionStore.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: turn, UserMessages: []session.Message{userMessage}, UserParts: userParts,
		AssistantPlaceholder: assistantMessage, Event: turnStartedEvent,
	})
	if err != nil {
		return admittedRun{}, err
	}
	event := admissionEvent(request, sessionRecord.ID, runRecord.ID, assistantMessage.ID, now)
	committedEvent, err := executionStore.AppendEvent(ctx, event)
	if err != nil {
		return admittedRun{}, err
	}
	result := buildAdmission(sessionRecord, runRecord, userMessage, userParts, assistantMessage, committedEvent, snapshot, now)
	result.Turn = admitted.Turn
	result.HistoryOptions = resolvedHistory
	return result, nil
}

func getOrCreateAdmissionSession(ctx context.Context, store session.Store, request admissionRequest, now time.Time) (session.Session, error) {
	candidate := admissionSession(request, now)
	existing, err := store.GetSession(ctx, candidate.ID)
	switch {
	case err == nil:
		if !sameAdmissionSessionIdentity(existing, candidate) {
			return session.Session{}, session.ErrConflict
		}
		return existing, nil
	case errors.Is(err, session.ErrNotFound):
		return store.CreateSession(ctx, candidate)
	default:
		return session.Session{}, err
	}
}

func sameAdmissionSessionIdentity(left, right session.Session) bool {
	return left.ID == right.ID &&
		left.ParentID == right.ParentID &&
		left.WorkspaceID == right.WorkspaceID &&
		left.Directory == right.Directory &&
		maps.Equal(left.Metadata, right.Metadata)
}

func latestAdmissionMessageTime(ctx context.Context, store session.Store, sessionID session.ID) (time.Time, error) {
	cursor := session.ReplayCursor{Limit: 100}
	var latest time.Time
	for {
		batch, err := store.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			return time.Time{}, err
		}
		for _, message := range batch.Messages {
			if message.CreatedAt.After(latest) {
				latest = message.CreatedAt
			}
		}
		if batch.Next == (session.ReplayCursor{}) {
			return latest, nil
		}
		cursor = batch.Next
	}
}

func buildAdmission(sessionRecord session.Session, runRecord session.Run, userMessage session.Message, userParts []session.Part, assistantMessage session.Message, event session.EventRecord, snapshot TurnSnapshot, now time.Time) admittedRun {
	snapshot.RunID = runRecord.ID
	snapshot.SessionID = sessionRecord.ID
	snapshot.EpochID = runRecord.ContextEpoch
	snapshot.CreatedAt = now
	return admittedRun{
		Session:          sessionRecord,
		Run:              runRecord,
		UserMessage:      userMessage,
		UserParts:        userParts,
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
	request.History = cloneHistoryOptions(request.History)
	request.Metadata = cloneStringMap(request.Metadata)
	request.ExtensionPlan = request.ExtensionPlan.Clone()
	// Deep-clone the caller-supplied block payloads (every *TextBlock,
	// *MediaBlock, json.RawMessage, ...) so a caller mutating its own
	// request concurrently with Start cannot race with, or disagree with,
	// what validate and the encoder inside the admission transaction see.
	frozenBlocks, err := session.Content{Role: session.RoleUser, Blocks: request.UserMessage.Blocks}.Clone()
	if err != nil {
		return admissionRequest{}, fmt.Errorf("%w: freeze user message: %v", ErrInvalidAdmission, err)
	}
	request.UserMessage.Blocks = frozenBlocks.Blocks
	return request, nil
}

func cloneHistoryOptions(options history.Options) history.Options {
	if options.Epoch != nil {
		epoch := *options.Epoch
		options.Epoch = &epoch
	}
	return options
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
	case ids.UserMessageID == "":
		return fmt.Errorf("%w: user message id required", ErrInvalidAdmission)
	case len(ids.UserPartIDs) == 0:
		return fmt.Errorf("%w: user part ids required", ErrInvalidAdmission)
	case ids.AssistantMessageID == "":
		return fmt.Errorf("%w: assistant message id required", ErrInvalidAdmission)
	case ids.ContextEpochID == "":
		return fmt.Errorf("%w: context epoch id required", ErrInvalidAdmission)
	case ids.EventID == "":
		return fmt.Errorf("%w: event id required", ErrInvalidAdmission)
	case ids.RunClaimToken == "":
		return fmt.Errorf("%w: run claim token required", ErrInvalidAdmission)
	case ids.TurnID == "":
		return fmt.Errorf("%w: turn id required", ErrInvalidAdmission)
	case ids.TurnStartedEventID == "":
		return fmt.Errorf("%w: turn started event id required", ErrInvalidAdmission)
	}
	generated := []string{
		string(ids.RunID),
		string(ids.UserMessageID),
		string(ids.AssistantMessageID),
		string(ids.ContextEpochID),
		string(ids.EventID),
		ids.RunClaimToken,
		string(ids.TurnID),
		string(ids.TurnStartedEventID),
	}
	for _, id := range ids.UserPartIDs {
		if id == "" {
			return fmt.Errorf("%w: user part ids required", ErrInvalidAdmission)
		}
		generated = append(generated, string(id))
	}
	seen := make(map[string]struct{}, len(generated))
	for _, id := range generated {
		if _, exists := seen[id]; exists {
			return fmt.Errorf("%w: generated admission ids must be distinct", ErrInvalidAdmission)
		}
		seen[id] = struct{}{}
	}
	return nil
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
		ParentMsgID:   request.IDs.UserMessageID,
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

func admissionUserMessage(request admissionRequest, sessionID session.ID, runID session.RunID, now time.Time) session.Message {
	return session.Message{
		ID:        request.IDs.UserMessageID,
		SessionID: sessionID,
		RunID:     runID,
		Role:      session.RoleUser,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// admissionUserParts encodes the current user submission's content blocks
// into one durable Part per block (plus an optional response_meta part, never
// applicable to a user-role submission), using the pre-generated PartIDs
// assigned to this admission.
func admissionUserParts(request admissionRequest, sessionID session.ID, runID session.RunID, messageID session.MessageID, now time.Time) ([]session.Part, error) {
	content := session.Content{Role: session.RoleUser, Blocks: request.UserMessage.Blocks}
	index := 0
	ids := request.IDs.UserPartIDs
	next := func() session.PartID {
		id := ids[index]
		index++
		return id
	}
	return session.EncodeContentParts(content, next, messageID, sessionID, runID, now, request.ContentLimits)
}

func admissionAssistantMessage(request admissionRequest, sessionID session.ID, runID session.RunID, now time.Time) session.Message {
	return session.Message{
		ID:        request.IDs.AssistantMessageID,
		SessionID: sessionID,
		RunID:     runID,
		ParentID:  request.IDs.UserMessageID,
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

// The run.Config map keys admissionConfig writes and ResumeRun later reads
// back. Named once here, on both the encode and decode side, instead of
// repeating the untyped string literals at every call site: this map
// crosses a JSON serialization boundary (agentOptionsConfigKey) with no
// compile-time check that either side spells a key the same way, exactly
// the casing-mismatch risk a plain literal invites (see the W5 doc and
// reconciliation item 6's "done when": a resumed dispatch's system prompt
// and agent options must equal the fresh-run values, proven by
// TestResumeRunRestoresSystemPromptAndAgentOptions).
const (
	// agentNameConfigKey is written for durable audit only: ResumeRun uses
	// the run row's own Agent field instead (the better source, since it is
	// never JSON-encoded), so this key has no decode side and is
	// deliberately NOT part of the admissionConfig/decodeResumeRunConfig
	// round trip the rest of this block's comment describes.
	agentNameConfigKey     = "agent"
	workspaceIDConfigKey   = "workspace_id"
	workspaceRootConfigKey = "workspace_root"
	systemPromptConfigKey  = "system_prompt"
	agentOptionsConfigKey  = "agent_options"
	// agentModeConfigKey and toolScopeConfigKey round-trip config.Agent.Mode
	// and config.ToolConfig.Enabled/Disabled (round-three reconciliation
	// item 6, RD-2): both are read on every post-resume turn --
	// boundedTurnMetadata publishes AgentMode to every extension, and
	// NewToolScopeContext projects Tools.Enabled/Disabled into what a
	// host's ScopeResolver sees -- so leaving them unrestored would let a
	// resumed run materialize a different effective tool set, or report a
	// different agent mode to extensions, than its own pre-pause turns.
	agentModeConfigKey = "agent_mode"
	toolScopeConfigKey = "tool_scope"
)

// resumeToolScope is the JSON shape toolScopeConfigKey encodes: just the two
// ToolConfig fields a resumed run's snapshot needs (Permissions is not part
// of ToolScopeContext/NewToolScopeContext, so it is not durably needed here).
type resumeToolScope struct {
	Enabled  []string `json:"enabled,omitempty"`
	Disabled []string `json:"disabled,omitempty"`
}

// admissionConfig durably persists the subset of a run's construction config
// ResumeRun later needs to rebuild an equivalent turnLoopCoordinator: without
// system_prompt and agent_options here, a resumed run's post-resume model
// dispatches would silently run with an empty system prompt and a model
// resolved without the agent's options (see ResumeRun's doc comment and the
// W5 doc's Host API bullet). agent_mode and tool_scope close the remaining
// gap RD-2 found: Agent.Mode and Tools.Enabled/Disabled were still silently
// dropped across a resume.
func admissionConfig(request admissionRequest) map[string]string {
	snapshot := request.Config
	cfg := map[string]string{
		agentNameConfigKey:     snapshot.Agent.Name,
		workspaceIDConfigKey:   snapshot.Metadata["workspace_id"],
		workspaceRootConfigKey: snapshot.Metadata["workspace_root"],
		systemPromptConfigKey:  snapshot.Agent.SystemPrompt,
		agentModeConfigKey:     snapshot.Agent.Mode,
	}
	if len(snapshot.Agent.Options) != 0 {
		if raw, err := json.Marshal(snapshot.Agent.Options); err == nil {
			cfg[agentOptionsConfigKey] = string(raw)
		}
	}
	if len(snapshot.Tools.Enabled) != 0 || len(snapshot.Tools.Disabled) != 0 {
		if raw, err := json.Marshal(resumeToolScope{Enabled: snapshot.Tools.Enabled, Disabled: snapshot.Tools.Disabled}); err == nil {
			cfg[toolScopeConfigKey] = string(raw)
		}
	}
	return cfg
}

// decodeToolScope reverses admissionConfig's tool_scope encoding. An empty
// or malformed value decodes to the zero resumeToolScope rather than
// failing ResumeRun, matching decodeAgentOptions' posture for a run
// admitted before this field existed.
func decodeToolScope(raw string) resumeToolScope {
	if raw == "" {
		return resumeToolScope{}
	}
	var scope resumeToolScope
	if err := json.Unmarshal([]byte(raw), &scope); err != nil {
		return resumeToolScope{}
	}
	return scope
}

// decodeAgentOptions reverses admissionConfig's agent_options encoding. An
// empty or malformed value decodes to nil rather than failing ResumeRun: a
// run admitted before this field existed (or one whose options were empty)
// must still resume, just with no audited options to restore.
func decodeAgentOptions(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	var options map[string]string
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return nil
	}
	return options
}

// resumeRunConfig is the typed view of admissionConfig's durable map that
// ResumeRun rebuilds its turnLoopCoordinator from -- the accessor
// reconciliation item 6 asked for, so a rename or a dropped key on either
// side of the admissionConfig/decodeResumeRunConfig pair is a compile error
// at every call site, not a silent empty string at resume time.
type resumeRunConfig struct {
	WorkspaceID   string
	WorkspaceRoot string
	SystemPrompt  string
	AgentOptions  map[string]string
	AgentMode     string
	ToolsEnabled  []string
	ToolsDisabled []string
}

// decodeResumeRunConfig reads resumeRunConfig's fields out of a durable
// run.Config map, using the same keys admissionConfig wrote.
func decodeResumeRunConfig(config map[string]string) resumeRunConfig {
	scope := decodeToolScope(config[toolScopeConfigKey])
	return resumeRunConfig{
		WorkspaceID:   config[workspaceIDConfigKey],
		WorkspaceRoot: config[workspaceRootConfigKey],
		SystemPrompt:  config[systemPromptConfigKey],
		AgentOptions:  decodeAgentOptions(config[agentOptionsConfigKey]),
		AgentMode:     config[agentModeConfigKey],
		ToolsEnabled:  scope.Enabled,
		ToolsDisabled: scope.Disabled,
	}
}
