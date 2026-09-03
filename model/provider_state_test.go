package model

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
)

func testProviderStateContract() ProviderStateContract {
	return ProviderStateContract{
		CodecID: "example.test/reasoning-items", Version: 1, CompatibilityKey: "reasoning-v1",
		Limits: ProviderStateLimits{MaxItems: 4, MaxItemBytes: 1024, MaxMessageBytes: 2048, MaxEnvelopeBytes: 4096, MaxStoredMessageBytes: 8192},
	}
}

func TestRequestCloneOwnsProviderStateBytes(t *testing.T) {
	original := json.RawMessage(`{"b":2, "a":1}`)
	request := Request{ProviderState: []ProviderMessageState{{MessageIndex: 1, Items: []ProviderStateItem{{Data: original}}}}}
	cloned, err := request.Clone()
	if err != nil {
		t.Fatal(err)
	}
	cloned.ProviderState[0].Items[0].Data[2] = 'z'
	if string(original) != `{"b":2, "a":1}` {
		t.Fatalf("source bytes mutated: %q", original)
	}
	request.ProviderState[0].Items[0].Data[2] = 'y'
	if string(cloned.ProviderState[0].Items[0].Data) == string(request.ProviderState[0].Items[0].Data) {
		t.Fatal("clone shares provider-state bytes")
	}
}

func TestProviderStateContractValidation(t *testing.T) {
	valid := testProviderStateContract()
	if err := ValidateProviderStateContract(valid); err != nil {
		t.Fatalf("valid contract: %v", err)
	}
	tests := map[string]func(*ProviderStateContract){
		"codec":            func(c *ProviderStateContract) { c.CodecID = "" },
		"codec whitespace": func(c *ProviderStateContract) { c.CodecID = "bad codec" },
		"version":          func(c *ProviderStateContract) { c.Version = 0 },
		"compatibility":    func(c *ProviderStateContract) { c.CompatibilityKey = "" },
		"items zero":       func(c *ProviderStateContract) { c.Limits.MaxItems = 0 },
		"items ceiling":    func(c *ProviderStateContract) { c.Limits.MaxItems = HardProviderStateMaxItems + 1 },
		"item ceiling":     func(c *ProviderStateContract) { c.Limits.MaxItemBytes = HardProviderStateMaxItemBytes + 1 },
		"message ceiling":  func(c *ProviderStateContract) { c.Limits.MaxMessageBytes = HardProviderStateMaxMessageBytes + 1 },
		"envelope ceiling": func(c *ProviderStateContract) { c.Limits.MaxEnvelopeBytes = HardProviderStateMaxEnvelopeBytes + 1 },
		"stored ceiling": func(c *ProviderStateContract) {
			c.Limits.MaxStoredMessageBytes = HardProviderStateMaxStoredMessageBytes + 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			contract := valid
			mutate(&contract)
			if err := ValidateProviderStateContract(contract); !errors.Is(err, ErrProviderStateInvalid) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if err := ValidateProviderStateItems([]ProviderStateItem{{Data: json.RawMessage(`{"x":1}`)}}, ProviderStateLimits{}); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("invalid item limits error = %v", err)
	}
}

func TestProviderStateIdentityAndRegistrationByteBoundaries(t *testing.T) {
	if err := ValidateProviderStateIdentity(strings.Repeat("p", MaxProviderStateProviderIDBytes), strings.Repeat("m", MaxProviderStateModelIDBytes)); err != nil {
		t.Fatalf("maximum identity rejected: %v", err)
	}
	if err := ValidateProviderStateIdentity(strings.Repeat("p", MaxProviderStateProviderIDBytes+1), "model"); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("oversized provider error = %v", err)
	}
	if err := ValidateProviderStateIdentity("provider", strings.Repeat("m", MaxProviderStateModelIDBytes+1)); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("oversized model error = %v", err)
	}
	contract := testProviderStateContract()
	contract.CodecID = strings.Repeat("c", MaxProviderStateCodecIDBytes)
	contract.CompatibilityKey = strings.Repeat("k", MaxProviderStateCompatibilityKeyBytes)
	if _, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: strings.Repeat("e", MaxProviderStateExtraKeyBytes), Contract: contract}); err != nil {
		t.Fatalf("maximum registration rejected: %v", err)
	}
	contract.CodecID += "c"
	if _, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: "state", Contract: contract}); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("oversized codec error = %v", err)
	}
	contract = testProviderStateContract()
	contract.CompatibilityKey = strings.Repeat("k", MaxProviderStateCompatibilityKeyBytes+1)
	if _, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: "state", Contract: contract}); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("oversized compatibility error = %v", err)
	}
	if _, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: strings.Repeat("e", MaxProviderStateExtraKeyBytes+1), Contract: testProviderStateContract()}); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("oversized Extra key error = %v", err)
	}
}

func TestEinoJSONExtraStateCodecPreservesBytesAndRejectsShape(t *testing.T) {
	codec, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: "openaicodex:reasoning_items", Contract: testProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	raw := []json.RawMessage{json.RawMessage(`{"encrypted":"SENTINEL", "n":1}`), json.RawMessage("{\n  \"n\": 2, \"encrypted\":\"other\"\n}"), json.RawMessage(`{"n":1e400}`)}
	message := einoschema.AssistantMessage("done", nil)
	message.Extra = map[string]any{"openaicodex:reasoning_items": raw}
	stateful := &einoProviderStateStreamer{codec: codec, contract: codec.Contract(), owned: map[string]struct{}{"openaicodex:reasoning_items": {}}}
	capture, err := stateful.CaptureProviderState(message)
	if err != nil {
		t.Fatal(err)
	}
	if message.Extra != nil {
		t.Fatalf("captured message Extra = %#v", message.Extra)
	}
	clean := einoschema.AssistantMessage("done", nil)
	if err := codec.RestoreAssistant(clean, capture.Items); err != nil {
		t.Fatal(err)
	}
	restored, ok := clean.Extra["openaicodex:reasoning_items"].([]json.RawMessage)
	if !ok || !reflect.DeepEqual(restored, raw) {
		t.Fatalf("restored = %#v, want %#v", restored, raw)
	}
	bad := []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(`[]`), json.RawMessage(`1`), json.RawMessage(`{} trailing`)}
	for _, value := range bad {
		msg := einoschema.AssistantMessage("", nil)
		msg.Extra = map[string]any{"openaicodex:reasoning_items": []json.RawMessage{value}}
		if _, err := codec.CaptureAssistant(msg); !errors.Is(err, ErrProviderStateInvalid) {
			t.Fatalf("value %q error = %v", value, err)
		}
	}
}

func TestStateAwareEinoStreamerRestoresOnlyAtDispatch(t *testing.T) {
	codec, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: "openaicodex:reasoning_items", Contract: testProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	client := &providerStateRecordingModel{}
	streamer, err := NewEinoStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	item := json.RawMessage(`{"encrypted":"SENTINEL", "n":1}`)
	request := Request{
		Identity:      Identity{SessionID: "session", ProviderID: "provider", ModelID: "current"},
		Messages:      []*einoschema.Message{einoschema.UserMessage("hi"), einoschema.AssistantMessage("old", nil)},
		ProviderState: []ProviderMessageState{{MessageIndex: 1, MessageID: "message", SourceSessionID: "session", SourceRunID: "run", ProviderID: "provider", SourceModelID: "old-model", CodecID: testProviderStateContract().CodecID, Version: 1, CompatibilityKey: "reasoning-v1", Items: []ProviderStateItem{{Data: item}}}},
	}
	reader, err := streamer.StreamProvider(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	if len(request.Messages[1].Extra) != 0 {
		t.Fatal("request message was mutated")
	}
	if client.calls != 1 || len(client.messages) != 2 {
		t.Fatalf("calls/messages = %d/%d", client.calls, len(client.messages))
	}
	restored := client.messages[1].Extra["openaicodex:reasoning_items"].([]json.RawMessage)
	if !reflect.DeepEqual(restored, []json.RawMessage{item}) {
		t.Fatalf("restored = %q", restored)
	}
	restored[0][2] = 'x'
	if string(request.ProviderState[0].Items[0].Data) != string(item) {
		t.Fatal("provider mutation reached request sidecar")
	}

	invalid := request
	invalid.ProviderState[0].SourceSessionID = "other"
	client.calls = 0
	if _, err := streamer.StreamProvider(context.Background(), invalid); !errors.Is(err, ErrProviderStateMismatch) || client.calls != 0 || strings.Contains(err.Error(), "SENTINEL") {
		t.Fatalf("invalid dispatch = calls %d error %v", client.calls, err)
	}
	if _, err := NewEinoStreamer(client).StreamProvider(context.Background(), request); !errors.Is(err, ErrProviderStateMismatch) {
		t.Fatalf("ordinary streamer error = %v", err)
	}
}

func TestStateAwareEinoStreamerRejectsInvalidSidecarsWithoutDispatch(t *testing.T) {
	codec, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: "state", Contract: testProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	client := &providerStateRecordingModel{}
	streamer, err := NewEinoStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	base := Request{
		Identity:      Identity{SessionID: "session", ProviderID: "provider", ModelID: "model"},
		Messages:      []*einoschema.Message{einoschema.AssistantMessage("old", nil), einoschema.UserMessage("new")},
		ProviderState: []ProviderMessageState{{MessageIndex: 0, MessageID: "message", SourceSessionID: "session", SourceRunID: "run", ProviderID: "provider", SourceModelID: "model", CodecID: testProviderStateContract().CodecID, Version: 1, CompatibilityKey: "reasoning-v1", Items: []ProviderStateItem{{Data: json.RawMessage(`{"x":1}`)}}}},
	}
	tests := map[string]func(*Request){
		"duplicate index":  func(r *Request) { r.ProviderState = append(r.ProviderState, r.ProviderState[0]) },
		"out of range":     func(r *Request) { r.ProviderState[0].MessageIndex = 2 },
		"wrong role":       func(r *Request) { r.ProviderState[0].MessageIndex = 1 },
		"session":          func(r *Request) { r.ProviderState[0].SourceSessionID = "other" },
		"provider":         func(r *Request) { r.ProviderState[0].ProviderID = "other" },
		"codec":            func(r *Request) { r.ProviderState[0].CodecID = "other" },
		"version":          func(r *Request) { r.ProviderState[0].Version = 2 },
		"compatibility":    func(r *Request) { r.ProviderState[0].CompatibilityKey = "other" },
		"message id":       func(r *Request) { r.ProviderState[0].MessageID = "" },
		"run id":           func(r *Request) { r.ProviderState[0].SourceRunID = "" },
		"malformed item":   func(r *Request) { r.ProviderState[0].Items[0].Data = json.RawMessage(`null`) },
		"current identity": func(r *Request) { r.Identity.ModelID = "bad model" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request, err := base.Clone()
			if err != nil {
				t.Fatal(err)
			}
			mutate(&request)
			client.calls = 0
			_, err = streamer.StreamProvider(context.Background(), request)
			if err == nil || client.calls != 0 || strings.Contains(err.Error(), "SENTINEL") {
				t.Fatalf("dispatch = calls %d error %v", client.calls, err)
			}
		})
	}
	withoutState := Request{Identity: Identity{SessionID: "session", ProviderID: "bad provider", ModelID: "model"}, Messages: []*einoschema.Message{einoschema.UserMessage("new")}}
	client.calls = 0
	if _, err := streamer.StreamProvider(context.Background(), withoutState); !errors.Is(err, ErrProviderStateInvalid) || client.calls != 0 {
		t.Fatalf("state-free invalid identity = calls %d error %v", client.calls, err)
	}
}

func TestProviderStateCodecCannotMutateProviderNeutralMessageFields(t *testing.T) {
	base, err := NewEinoJSONExtraStateCodec(EinoJSONExtraStateConfig{ExtraKey: "state", Contract: testProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	client := &providerStateRecordingModel{}
	captureStreamer, err := NewEinoStreamerWithProviderState(client, &mutatingProviderStateCodec{ProviderStateCodec: base, mutateCapture: true})
	if err != nil {
		t.Fatal(err)
	}
	message := einoschema.AssistantMessage("original", nil)
	message.Extra = map[string]any{"state": []json.RawMessage{json.RawMessage(`{"x":1}`)}}
	if _, err := captureStreamer.CaptureProviderState(message); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("capture mutation error = %v", err)
	}

	restoreStreamer, err := NewEinoStreamerWithProviderState(client, &mutatingProviderStateCodec{ProviderStateCodec: base, mutateRestore: true})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		Identity:      Identity{SessionID: "session", ProviderID: "provider", ModelID: "model"},
		Messages:      []*einoschema.Message{einoschema.AssistantMessage("original", nil)},
		ProviderState: []ProviderMessageState{{MessageIndex: 0, MessageID: "message", SourceSessionID: "session", SourceRunID: "run", ProviderID: "provider", SourceModelID: "model", CodecID: testProviderStateContract().CodecID, Version: 1, CompatibilityKey: "reasoning-v1", Items: []ProviderStateItem{{Data: json.RawMessage(`{"x":1}`)}}}},
	}
	client.calls = 0
	if _, err := restoreStreamer.StreamProvider(context.Background(), request); !errors.Is(err, ErrProviderStateInvalid) || client.calls != 0 {
		t.Fatalf("restore mutation = calls %d error %v", client.calls, err)
	}
}

func TestProviderStateCodecPanicsAreContentFree(t *testing.T) {
	client := &providerStateRecordingModel{}
	registration := &panicProviderStateCodec{contract: testProviderStateContract(), keys: []string{"state"}, panicContract: true}
	if _, err := NewEinoStreamerWithProviderState(client, registration); !errors.Is(err, ErrProviderStateInvalid) {
		t.Fatalf("registration panic error = %v", err)
	}
	captureCodec := &panicProviderStateCodec{contract: testProviderStateContract(), keys: []string{"state"}, panicCapture: true}
	streamer, err := NewEinoStreamerWithProviderState(client, captureCodec)
	if err != nil {
		t.Fatal(err)
	}
	message := einoschema.AssistantMessage("", nil)
	message.Extra = map[string]any{"state": []json.RawMessage{json.RawMessage(`{"secret":"SENTINEL"}`)}}
	if _, err := streamer.CaptureProviderState(message); !errors.Is(err, ErrProviderStateInvalid) || strings.Contains(err.Error(), "SENTINEL") {
		t.Fatalf("capture panic error = %v", err)
	}
	restoreCodec := &panicProviderStateCodec{contract: testProviderStateContract(), keys: []string{"state"}, panicRestore: true}
	streamer, err = NewEinoStreamerWithProviderState(client, restoreCodec)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Identity: Identity{SessionID: "session", ProviderID: "provider", ModelID: "model"}, Messages: []*einoschema.Message{einoschema.AssistantMessage("", nil)}, ProviderState: []ProviderMessageState{{MessageIndex: 0, MessageID: "message", SourceSessionID: "session", SourceRunID: "run", ProviderID: "provider", SourceModelID: "model", CodecID: testProviderStateContract().CodecID, Version: 1, CompatibilityKey: "reasoning-v1", Items: []ProviderStateItem{{Data: json.RawMessage(`{"secret":"SENTINEL"}`)}}}}}
	client.calls = 0
	if _, err := streamer.StreamProvider(context.Background(), request); !errors.Is(err, ErrProviderStateInvalid) || client.calls != 0 || strings.Contains(err.Error(), "SENTINEL") {
		t.Fatalf("restore panic = calls %d error %v", client.calls, err)
	}
}

type panicProviderStateCodec struct {
	contract      ProviderStateContract
	keys          []string
	panicContract bool
	panicCapture  bool
	panicRestore  bool
}

type mutatingProviderStateCodec struct {
	ProviderStateCodec
	mutateCapture bool
	mutateRestore bool
}

func (c *mutatingProviderStateCodec) CaptureAssistant(message *einoschema.Message) (ProviderStateCapture, error) {
	capture, err := c.ProviderStateCodec.CaptureAssistant(message)
	if c.mutateCapture {
		message.Content = "mutated"
		message.Role = einoschema.User
	}
	return capture, err
}

func (c *mutatingProviderStateCodec) RestoreAssistant(message *einoschema.Message, items []ProviderStateItem) error {
	if err := c.ProviderStateCodec.RestoreAssistant(message, items); err != nil {
		return err
	}
	if c.mutateRestore {
		message.ReasoningContent = "mutated"
		message.ToolCalls = []einoschema.ToolCall{{ID: "mutated"}}
	}
	return nil
}

func (c *panicProviderStateCodec) Contract() ProviderStateContract {
	if c.panicContract {
		panic("SENTINEL")
	}
	return c.contract
}

func (c *panicProviderStateCodec) OwnedExtraKeys() []string { return c.keys }

func (c *panicProviderStateCodec) CaptureAssistant(*einoschema.Message) (ProviderStateCapture, error) {
	if c.panicCapture {
		panic("SENTINEL")
	}
	return ProviderStateCapture{}, nil
}

func (c *panicProviderStateCodec) RestoreAssistant(*einoschema.Message, []ProviderStateItem) error {
	if c.panicRestore {
		panic("SENTINEL")
	}
	return nil
}

type providerStateRecordingModel struct {
	calls    int
	messages []*einoschema.Message
}

func (m *providerStateRecordingModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return nil, errors.New("unused")
}

func (m *providerStateRecordingModel) Stream(_ context.Context, messages []*einoschema.Message, _ ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	m.calls++
	m.messages = messages
	return einoschema.StreamReaderFromArray([]*einoschema.Message{}), nil
}

func (m *providerStateRecordingModel) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	return m, nil
}
