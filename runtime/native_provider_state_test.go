package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// nativeAgenticProviderStateModel is a minimal einomodel.AgenticModel that
// records every dispatched call and plays back one scripted response per
// call, for exercising model.AgenticProviderStateStreamer end to end
// (as opposed to the classic model.ProviderStateStreamer bridge that
// runtimeProviderStateModel above exercises).
type nativeAgenticProviderStateModel struct {
	mu        sync.Mutex
	responses []*einoschema.AgenticMessage
	inputs    [][]*einoschema.AgenticMessage
}

func (m *nativeAgenticProviderStateModel) Generate(context.Context, []*einoschema.AgenticMessage, ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	return nil, errors.New("unused")
}

func (m *nativeAgenticProviderStateModel) Stream(_ context.Context, input []*einoschema.AgenticMessage, _ ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inputs = append(m.inputs, input)
	if len(m.responses) == 0 {
		return nil, errors.New("unexpected model call")
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return einoschema.StreamReaderFromArray([]*einoschema.AgenticMessage{response}), nil
}

func (m *nativeAgenticProviderStateModel) Inputs() [][]*einoschema.AgenticMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]*einoschema.AgenticMessage(nil), m.inputs...)
}

func nativeProviderStateOrchestrator(t *testing.T, store session.Store, ids IDGenerator, client *nativeAgenticProviderStateModel) *StreamingOrchestrator {
	t.Helper()
	codec, err := model.NewTypedExtensionStateCodec(runtimeProviderStateContract())
	if err != nil {
		t.Fatal(err)
	}
	streamer, err := model.NewAgenticStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	return mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(ids),
		WithRunPlanProvider(emptyTestRunPlanProvider()), WithHistory(history.Options{IncludeReasoning: true}),
	)
}

// TestNativeAgenticProviderStateReasoningSignatureRestoresAcrossReopenWithoutLeakage
// exercises the native, block-bound model.AgenticProviderStateStreamer
// boundary end to end through the runtime (captureAssistantProviderState /
// loadProviderHistory), which TestDurableProviderStateRestoresAcrossSQLiteReopen
// above only exercises via the classic model.ProviderStateStreamer bridge.
//
// A reasoning block's private Signature must: (1) never reach the durable
// ledger row or any public content part, (2) durably capture as a
// block-bound session.ProviderStateEnvelope, and (3) restore onto the
// dispatched clone of the assistant message on a later turn, across a real
// SQLite store close/reopen, without ever touching the caller-visible public
// projection.
func TestNativeAgenticProviderStateReasoningSignatureRestoresAcrossReopenWithoutLeakage(t *testing.T) {
	const sentinel = "SENTINEL-NATIVE-REASONING-SIG"
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "native-state.db")
	store, storePool, err := openTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storePool.Close() }()
	ids := &sequenceIDs{}

	firstAssistant := agenticAssistantReasoning("because")
	firstAssistant.ContentBlocks[0].Reasoning.Signature = sentinel
	firstAssistant.ContentBlocks = append(firstAssistant.ContentBlocks, &einoschema.ContentBlock{
		Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "first answer"},
	})
	firstClient := &nativeAgenticProviderStateModel{responses: []*einoschema.AgenticMessage{firstAssistant}}
	first := nativeProviderStateOrchestrator(t, store, ids, firstClient)
	firstResult := startAndWaitRequest(t, first, Request{SessionID: "native-state-session", Message: TextUserMessage("first question"), Config: orchestratorConfig()})
	if firstResult.Status != session.RunCompleted || firstResult.Error != nil {
		t.Fatalf("first result = %+v", firstResult)
	}

	batch, err := history.LoadBatch(ctx, store, "native-state-session")
	if err != nil {
		t.Fatal(err)
	}
	var stateParts []session.Part
	for _, part := range batch.Parts {
		if part.Kind == session.PartProviderState {
			stateParts = append(stateParts, part)
			continue
		}
		if strings.Contains(string(part.Payload), sentinel) {
			t.Fatalf("public part leaked the private signature: %#v", part)
		}
	}
	if len(stateParts) != 1 {
		t.Fatalf("provider state parts = %#v", stateParts)
	}
	envelope, err := session.DecodeProviderStatePayload(stateParts[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.BlockID == "" {
		t.Fatal("captured provider state is not bound to a content block")
	}
	var payload struct {
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(envelope.Data, &payload); err != nil || payload.Signature != sentinel {
		t.Fatalf("captured state payload = %s, err = %v", envelope.Data, err)
	}

	requests, err := store.ListModelRequests(ctx, firstResult.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(requests.Records) != 1 {
		t.Fatalf("first ledger = %#v, %v", requests, err)
	}
	ledgerRaw, _ := json.Marshal(requests.Records[0])
	if strings.Contains(string(ledgerRaw), sentinel) {
		t.Fatalf("signature leaked into ledger: %s", ledgerRaw)
	}

	if err := storePool.Close(); err != nil {
		t.Fatal(err)
	}
	store, storePool, err = reopenTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}

	secondClient := &nativeAgenticProviderStateModel{responses: []*einoschema.AgenticMessage{agenticAssistantText("second answer")}}
	second := nativeProviderStateOrchestrator(t, store, ids, secondClient)
	secondResult := startAndWaitRequest(t, second, Request{SessionID: "native-state-session", Message: TextUserMessage("second question"), Config: orchestratorConfig()})
	if secondResult.Status != session.RunCompleted || secondResult.Error != nil {
		t.Fatalf("second result = %+v", secondResult)
	}

	inputs := secondClient.Inputs()
	if len(inputs) != 1 {
		t.Fatalf("second inputs = %#v", inputs)
	}
	var restoredOnDispatch bool
	for _, message := range inputs[0] {
		if message == nil || message.Role != einoschema.AgenticRoleTypeAssistant {
			continue
		}
		if agenticReasoningText(message) != "because" {
			continue
		}
		for _, block := range message.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeReasoning || block.Reasoning == nil {
				continue
			}
			if block.Reasoning.Signature != "" {
				if block.Reasoning.Signature != sentinel {
					t.Fatalf("dispatched signature = %q, want %q", block.Reasoning.Signature, sentinel)
				}
				restoredOnDispatch = true
			}
		}
	}
	if !restoredOnDispatch {
		t.Fatal("dispatched clone did not carry the restored signature")
	}

	secondRequests, err := store.ListModelRequests(ctx, secondResult.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(secondRequests.Records) != 1 {
		t.Fatalf("second ledger = %#v, %v", secondRequests, err)
	}
	secondLedgerRaw, _ := json.Marshal(secondRequests.Records[0])
	if strings.Contains(string(secondLedgerRaw), sentinel) {
		t.Fatalf("signature leaked into second ledger: %s", secondLedgerRaw)
	}
	secondBatch, err := history.LoadBatch(ctx, store, "native-state-session")
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range secondBatch.Parts {
		if part.Kind != session.PartProviderState && strings.Contains(string(part.Payload), sentinel) {
			t.Fatalf("public part leaked the signature after reopen: %#v", part)
		}
	}
}
