package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

const providerStateExtraKey = "openaicodex:reasoning_items"

var providerStateRawItems = []json.RawMessage{
	json.RawMessage(`{"type":"reasoning","encrypted_content":"STATE_SENTINEL", "id":1}`),
	json.RawMessage("{\n  \"id\": 2, \"encrypted_content\":\"SECOND\", \"type\":\"reasoning\"\n}"),
}

func runtimeProviderStateContract() model.ProviderStateContract {
	return model.ProviderStateContract{
		CodecID: "github.com/mattsp1290/eino-providers/openaicodex/reasoning-items", Version: 1,
		CompatibilityKey: "openaicodex-responses-reasoning-v1",
		Limits:           model.ProviderStateLimits{MaxItems: 8, MaxItemBytes: 64 * 1024, MaxMessageBytes: 128 * 1024, MaxEnvelopeBytes: 128 * 1024, MaxStoredMessageBytes: 256 * 1024},
	}
}

func TestDurableProviderStateRestoresAcrossSQLiteReopen(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		t.Run(map[bool]string{false: "open store", true: "reopened store"}[reopen], func(t *testing.T) {
			runDurableProviderStateJourney(t, reopen)
		})
	}
}

func runDurableProviderStateJourney(t *testing.T, reopen bool) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ids := &sequenceIDs{}
	firstClient := &runtimeProviderStateModel{responses: []*einoschema.Message{stateBearingAssistant("first answer")}}
	first := providerStateOrchestrator(t, store, ids, firstClient)
	firstResult := startAndWaitRequest(t, first, Request{SessionID: "state-session", Message: UserMessage{Content: "first question"}, Config: orchestratorConfig()})
	if firstResult.Status != session.RunCompleted || firstResult.Error != nil {
		t.Fatalf("first result = %+v", firstResult)
	}
	batch, err := history.LoadBatch(ctx, store, "state-session")
	if err != nil {
		t.Fatal(err)
	}
	var stateParts []session.Part
	for _, part := range batch.Parts {
		if part.Kind == session.PartProviderState {
			stateParts = append(stateParts, part)
		}
	}
	if len(stateParts) != len(providerStateRawItems) {
		t.Fatalf("state parts = %#v", stateParts)
	}
	for index, part := range stateParts {
		envelope, err := session.DecodeProviderStatePayload(part.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if envelope.ItemIndex != index || !bytes.Equal(envelope.Data, providerStateRawItems[index]) || part.MessageID != firstResult.MessageID || part.RunID != firstResult.RunID {
			t.Fatalf("state part %d = %#v envelope %#v", index, part, envelope)
		}
	}
	public, err := history.Load(ctx, store, "state-session", history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	assertProviderStateAbsent(t, public)

	if reopen {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = sqlitestore.Open(ctx, dbPath)
		if err != nil {
			t.Fatal(err)
		}
	}
	secondClient := &runtimeProviderStateModel{responses: []*einoschema.Message{einoschema.AssistantMessage("second answer", nil)}}
	second := providerStateOrchestrator(t, store, ids, secondClient)
	second.plans = staticRunPlanProvider{plan: newTestDispatchPlan(providerStateContextDispatch(t))}
	secondResult := startAndWaitRequest(t, second, Request{SessionID: "state-session", Message: UserMessage{Content: "second question"}, Config: orchestratorConfig()})
	if secondResult.Status != session.RunCompleted || secondResult.Error != nil {
		t.Fatalf("second result = %+v", secondResult)
	}
	inputs := secondClient.Inputs()
	if len(inputs) != 1 || len(inputs[0]) != 5 {
		t.Fatalf("second inputs = %#v", inputs)
	}
	if inputs[0][0].Role != einoschema.System || inputs[0][0].Content != "provider-state-system" ||
		inputs[0][1].Role != einoschema.User || inputs[0][1].Content != "first question" ||
		inputs[0][2].Role != einoschema.Assistant || inputs[0][2].Content != "first answer" ||
		inputs[0][3].Role != einoschema.User || inputs[0][3].Content != "second question" ||
		inputs[0][4].Role != einoschema.User || inputs[0][4].Content != "provider-state-suffix" {
		t.Fatalf("second input ordering = %#v", inputs[0])
	}
	restored, ok := inputs[0][2].Extra[providerStateExtraKey].([]json.RawMessage)
	if !ok || len(restored) != len(providerStateRawItems) {
		t.Fatalf("restored state = %#v", inputs[0][2].Extra)
	}
	for index := range restored {
		if !bytes.Equal(restored[index], providerStateRawItems[index]) {
			t.Fatalf("restored[%d] = %q, want %q", index, restored[index], providerStateRawItems[index])
		}
	}
	assertProviderStateAbsent(t, append(public, inputs[0][0], inputs[0][1], inputs[0][3], inputs[0][4]))
	requests, err := store.ListModelRequests(ctx, secondResult.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(requests.Records) != 1 {
		t.Fatalf("request ledger = %#v, %v", requests, err)
	}
	ledgerRaw, _ := json.Marshal(requests.Records[0])
	if providerStateSensitive(string(ledgerRaw)) {
		t.Fatalf("provider state leaked to ledger: %s", ledgerRaw)
	}
}

func TestActiveProviderStateWithoutCodecRollsBackAdmission(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ids := &sequenceIDs{}
	firstClient := &runtimeProviderStateModel{responses: []*einoschema.Message{stateBearingAssistant("answer")}}
	first := providerStateOrchestrator(t, store, ids, firstClient)
	result := startAndWaitRequest(t, first, Request{SessionID: "state-session", Message: UserMessage{Content: "first"}, Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	before, err := history.LoadBatch(ctx, store, "state-session")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ordinary := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			calls++
			return []*einoschema.Message{einoschema.AssistantMessage("bad", nil)}, nil
		})}), WithIDGenerator(ids), WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	_, err = ordinary.Start(ctx, Request{SessionID: "state-session", Message: UserMessage{Content: "second"}, Config: orchestratorConfig()})
	if !errors.Is(err, model.ErrProviderStateMismatch) || calls != 0 || strings.Contains(err.Error(), "STATE_SENTINEL") {
		t.Fatalf("start = calls %d error %v", calls, err)
	}
	after, loadErr := history.LoadBatch(ctx, store, "state-session")
	if loadErr != nil || len(after.Messages) != len(before.Messages) || len(after.Parts) != len(before.Parts) {
		t.Fatalf("admission did not roll back: before %#v after %#v err %v", before, after, loadErr)
	}
}

func TestSQLiteEmbeddedProviderStateOwnershipCorruptionRollsBackAdmission(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *sql.DB, Result)
	}{
		{name: "part message id", mutate: func(t *testing.T, db *sql.DB, result Result) {
			mutateSQLiteProviderStateParts(t, db, func(part *session.Part) { part.MessageID = "forged-message" })
		}},
		{name: "message id", mutate: func(t *testing.T, db *sql.DB, result Result) {
			mutateSQLiteRecord[session.Message](t, db, "messages", string(result.MessageID), func(message *session.Message) { message.ID = "forged-message" })
		}},
		{name: "coherent embedded session", mutate: func(t *testing.T, db *sql.DB, result Result) {
			mutateSQLiteRecord[session.Message](t, db, "messages", string(result.MessageID), func(message *session.Message) { message.SessionID = "forged-session" })
			mutateSQLiteProviderStateParts(t, db, func(part *session.Part) { part.SessionID = "forged-session" })
			mutateSQLiteRecord[session.Run](t, db, "runs", string(result.RunID), func(run *session.Run) { run.SessionID = "forged-session" })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "corrupt.db")
			store, err := sqlitestore.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			ids := &sequenceIDs{}
			firstClient := &runtimeProviderStateModel{responses: []*einoschema.Message{stateBearingAssistant("answer")}}
			first := providerStateOrchestrator(t, store, ids, firstClient)
			firstResult := startAndWaitRequest(t, first, Request{SessionID: "state-session", Message: UserMessage{Content: "first"}, Config: orchestratorConfig()})
			if firstResult.Error != nil {
				t.Fatal(firstResult.Error)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db, firstResult)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlitestore.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			secondClient := &runtimeProviderStateModel{responses: []*einoschema.Message{einoschema.AssistantMessage("should not run", nil)}}
			second := providerStateOrchestrator(t, store, ids, secondClient)
			_, err = second.Start(ctx, Request{SessionID: "state-session", Message: UserMessage{Content: "second"}, Config: orchestratorConfig()})
			if (!errors.Is(err, model.ErrProviderStateMismatch) && !errors.Is(err, session.ErrConflict)) || clientCallCount(secondClient) != 0 || strings.Contains(err.Error(), "STATE_SENTINEL") {
				t.Fatalf("corrupt admission = calls %d error %v", clientCallCount(secondClient), err)
			}
		})
	}
}

func TestProviderStateContinuesWithinToolLoop(t *testing.T) {
	store := newAdmissionStore()
	ids := &sequenceIDs{}
	first := stateBearingAssistant("")
	first.ToolCalls = []einoschema.ToolCall{{ID: "state-call", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"text":"hi"}`}}}
	client := &runtimeProviderStateModel{responses: []*einoschema.Message{first, einoschema.AssistantMessage("done", nil)}}
	codec, err := model.NewEinoJSONExtraStateCodec(model.EinoJSONExtraStateConfig{ExtraKey: providerStateExtraKey, Contract: runtimeProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	streamer, err := model.NewEinoStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	tools := staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{Output: "echoed"}, nil
	})}}}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(ids),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlanWithDispatch(tools, providerStateContextDispatch(t))}),
	)
	result := startAndWaitRequest(t, orch, Request{SessionID: "tool-state", Message: UserMessage{Content: "go"}, Config: orchestratorConfig()})
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	inputs := client.Inputs()
	if len(inputs) != 2 || len(inputs[1]) != 5 || inputs[1][3].Role != einoschema.Assistant || inputs[1][4].Role != einoschema.Tool {
		t.Fatalf("tool-loop inputs = %#v", inputs)
	}
	restored, ok := inputs[1][3].Extra[providerStateExtraKey].([]json.RawMessage)
	if !ok || len(restored) != 2 || !bytes.Equal(restored[0], providerStateRawItems[0]) || !bytes.Equal(restored[1], providerStateRawItems[1]) {
		t.Fatalf("same-run restored state = %#v", inputs[1][3].Extra)
	}
}

func TestProviderStateCaptureAndPersistenceFailuresLeaveNoAssistantParts(t *testing.T) {
	for _, test := range []struct {
		name        string
		response    *einoschema.Message
		failPartAt  int
		wantErrKind error
	}{
		{name: "unknown Extra", response: func() *einoschema.Message {
			message := stateBearingAssistant("answer")
			message.Extra["unowned"] = "STATE_SENTINEL"
			return message
		}(), wantErrKind: model.ErrProviderStateInvalid},
		{name: "first state write", response: stateBearingAssistant("answer"), failPartAt: 2},
		{name: "second state write", response: stateBearingAssistant("answer"), failPartAt: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newAdmissionStore()
			store.appendPartErrAt = test.failPartAt
			client := &runtimeProviderStateModel{responses: []*einoschema.Message{test.response}}
			orch := providerStateOrchestrator(t, store, &sequenceIDs{}, client)
			result := startAndWaitRequest(t, orch, Request{SessionID: "capture-failure", Message: UserMessage{Content: "go"}, Config: orchestratorConfig()})
			if result.Status != session.RunFailed || result.Error == nil || clientCallCount(client) != 1 || strings.Contains(result.Error.Error(), "STATE_SENTINEL") {
				t.Fatalf("result = %+v calls=%d", result, clientCallCount(client))
			}
			if test.wantErrKind != nil && !errors.Is(result.Error, test.wantErrKind) {
				t.Fatalf("error = %v, want %v", result.Error, test.wantErrKind)
			}
			for _, part := range store.parts {
				if part.Kind != session.PartText || part.MessageID == result.MessageID {
					t.Fatalf("assistant turn partially persisted: %#v", store.parts)
				}
			}
			publicSurfaces, err := json.Marshal(struct {
				Runs          map[session.RunID]session.Run
				Events        map[session.EventID]session.EventRecord
				ModelRequests map[session.ModelRequestID]session.ModelRequestRecord
			}{Runs: store.runs, Events: store.events, ModelRequests: store.modelRequests})
			if err != nil {
				t.Fatal(err)
			}
			if providerStateSensitive(string(publicSurfaces)) {
				t.Fatalf("provider state leaked to public runtime surfaces: %s", publicSurfaces)
			}
		})
	}
}

func TestProviderStateRetriesUseIndependentRestoreCopiesAndCaptureOnce(t *testing.T) {
	t.Run("restore copies", func(t *testing.T) {
		store := newAdmissionStore()
		ids := &sequenceIDs{}
		firstClient := &runtimeProviderStateModel{responses: []*einoschema.Message{stateBearingAssistant("answer")}}
		first := providerStateOrchestrator(t, store, ids, firstClient)
		if result := startAndWaitRequest(t, first, Request{SessionID: "retry-state", Message: UserMessage{Content: "first"}, Config: orchestratorConfig()}); result.Error != nil {
			t.Fatal(result.Error)
		}
		secondClient := &runtimeProviderStateModel{
			streamErrors:          []error{model.Error{Code: "rate_limited", Message: "retry", Retryable: true, Cause: model.ErrProviderRateLimited}},
			mutateRestoredOnError: true,
			responses:             []*einoschema.Message{einoschema.AssistantMessage("done", nil)},
		}
		second := providerStateOrchestrator(t, store, ids, secondClient)
		second.attemptsValue = 2
		result := startAndWaitRequest(t, second, Request{SessionID: "retry-state", Message: UserMessage{Content: "second"}, Config: orchestratorConfig()})
		if result.Error != nil {
			t.Fatal(result.Error)
		}
		inputs := secondClient.Inputs()
		if len(inputs) != 2 {
			t.Fatalf("retry inputs = %d", len(inputs))
		}
		var secondItems []json.RawMessage
		for _, message := range inputs[1] {
			if message.Role == einoschema.Assistant && len(message.Extra) != 0 {
				secondItems, _ = message.Extra[providerStateExtraKey].([]json.RawMessage)
			}
		}
		if len(secondItems) != 2 || !bytes.Equal(secondItems[0], providerStateRawItems[0]) {
			t.Fatalf("second retry state = %q", secondItems)
		}
	})

	t.Run("capture once after retry", func(t *testing.T) {
		store := newAdmissionStore()
		baseCodec, err := model.NewEinoJSONExtraStateCodec(model.EinoJSONExtraStateConfig{ExtraKey: providerStateExtraKey, Contract: runtimeProviderStateContract()})
		if err != nil {
			t.Fatal(err)
		}
		codec := &countingProviderStateCodec{ProviderStateCodec: baseCodec}
		client := &runtimeProviderStateModel{
			streamErrors: []error{model.Error{Code: "rate_limited", Message: "retry", Retryable: true, Cause: model.ErrProviderRateLimited}},
			responses:    []*einoschema.Message{stateBearingAssistant("answer")},
		}
		streamer, err := model.NewEinoStreamerWithProviderState(client, codec)
		if err != nil {
			t.Fatal(err)
		}
		orch := mustConfiguredOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()), WithAttempts(2))
		result := startAndWaitRequest(t, orch, Request{SessionID: "capture-once", Message: UserMessage{Content: "go"}, Config: orchestratorConfig()})
		if result.Error != nil || codec.captures.Load() != 1 || clientCallCount(client) != 2 {
			t.Fatalf("result=%+v captures=%d calls=%d", result, codec.captures.Load(), clientCallCount(client))
		}
	})
}

func providerStateOrchestrator(t *testing.T, store session.Store, ids IDGenerator, client *runtimeProviderStateModel) *StreamingOrchestrator {
	t.Helper()
	codec, err := model.NewEinoJSONExtraStateCodec(model.EinoJSONExtraStateConfig{ExtraKey: providerStateExtraKey, Contract: runtimeProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	streamer, err := model.NewEinoStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	return mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(ids),
		WithRunPlanProvider(emptyTestRunPlanProvider()), WithClock(func() time.Time { return time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC) }),
	)
}

func providerStateContextDispatch(t *testing.T) *extension.Plan {
	t.Helper()
	registry := newTestExtensionRegistry(nil)
	mount, err := registry.Mount(context.Background(), extension.Component{InstanceID: "provider-state-context", Artifact: extension.Artifact{Name: "provider-state-context", Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: extension.SourceNative}}, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return OnContextSource(registrar, extension.Registration{ID: "state-context", Scope: extension.GlobalScope()}, func(context.Context, ContextSourceInput) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.SystemMessage("provider-state-system"), einoschema.UserMessage("provider-state-suffix")}, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mount.Close(context.Background()) })
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	return dispatch
}

func stateBearingAssistant(content string) *einoschema.Message {
	message := einoschema.AssistantMessage(content, nil)
	items := make([]json.RawMessage, len(providerStateRawItems))
	for index := range providerStateRawItems {
		items[index] = append(json.RawMessage(nil), providerStateRawItems[index]...)
	}
	message.Extra = map[string]any{providerStateExtraKey: items}
	return message
}

func assertProviderStateAbsent(t *testing.T, messages []*einoschema.Message) {
	t.Helper()
	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatal(err)
	}
	if providerStateSensitive(string(raw)) {
		t.Fatalf("provider state leaked: %s", raw)
	}
}

func providerStateSensitive(value string) bool {
	digest := sha256.Sum256(providerStateRawItems[0])
	return strings.Contains(value, "STATE_SENTINEL") || strings.Contains(value, providerStateExtraKey) ||
		strings.Contains(value, base64.StdEncoding.EncodeToString(providerStateRawItems[0])) ||
		strings.Contains(value, hex.EncodeToString(digest[:]))
}

type runtimeProviderStateModel struct {
	mu                    sync.Mutex
	responses             []*einoschema.Message
	streamErrors          []error
	mutateRestoredOnError bool
	inputs                [][]*einoschema.Message
}

func (m *runtimeProviderStateModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return nil, errors.New("unused")
}

func (m *runtimeProviderStateModel) Stream(_ context.Context, input []*einoschema.Message, _ ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inputs = append(m.inputs, input)
	if len(m.streamErrors) != 0 {
		err := m.streamErrors[0]
		m.streamErrors = m.streamErrors[1:]
		if m.mutateRestoredOnError {
			for _, message := range input {
				if items, ok := message.Extra[providerStateExtraKey].([]json.RawMessage); ok && len(items) != 0 && len(items[0]) > 2 {
					items[0][2] = 'X'
				}
			}
		}
		return nil, err
	}
	if len(m.responses) == 0 {
		return nil, errors.New("unexpected model call")
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return einoschema.StreamReaderFromArray([]*einoschema.Message{response}), nil
}

type countingProviderStateCodec struct {
	model.ProviderStateCodec
	captures atomic.Int32
}

func (c *countingProviderStateCodec) CaptureAssistant(message *einoschema.Message) (model.ProviderStateCapture, error) {
	c.captures.Add(1)
	return c.ProviderStateCodec.CaptureAssistant(message)
}

func (m *runtimeProviderStateModel) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	return m, nil
}

func (m *runtimeProviderStateModel) Inputs() [][]*einoschema.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]*einoschema.Message(nil), m.inputs...)
}

func clientCallCount(m *runtimeProviderStateModel) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.inputs)
}

func mutateSQLiteProviderStateParts(t *testing.T, db *sql.DB, mutate func(*session.Part)) {
	t.Helper()
	rows, err := db.Query(`SELECT id, record FROM parts`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	type update struct {
		id  string
		raw []byte
	}
	var updates []update
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatal(err)
		}
		var part session.Part
		if err := json.Unmarshal(raw, &part); err != nil {
			t.Fatal(err)
		}
		if part.Kind != session.PartProviderState {
			continue
		}
		mutate(&part)
		raw, err = json.Marshal(part)
		if err != nil {
			t.Fatal(err)
		}
		updates = append(updates, update{id: id, raw: raw})
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, value := range updates {
		if _, err := db.Exec(`UPDATE parts SET record = ? WHERE id = ?`, value.raw, value.id); err != nil {
			t.Fatal(err)
		}
	}
}

func mutateSQLiteRecord[T any](t *testing.T, db *sql.DB, table, id string, mutate func(*T)) {
	t.Helper()
	var raw []byte
	if err := db.QueryRow(`SELECT record FROM `+table+` WHERE id = ?`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var record T
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	mutate(&record)
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE `+table+` SET record = ? WHERE id = ?`, raw, id); err != nil {
		t.Fatal(err)
	}
}
