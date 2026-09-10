package model

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/claude"
	"github.com/cloudwego/eino/schema/gemini"
	"github.com/cloudwego/eino/schema/openai"
)

func typedContract() ProviderStateContract {
	return ProviderStateContract{
		CodecID: "typed.test/extensions", Version: 1, CompatibilityKey: "typed-v1",
		Limits: ProviderStateLimits{MaxItems: 8, MaxItemBytes: 1024, MaxMessageBytes: 4096, MaxEnvelopeBytes: 8192, MaxStoredMessageBytes: 16384},
	}
}

// scriptedAgenticModel already defined in agentic_streamer_test.go is reused
// below via a fresh instance per test.

func TestTypedExtensionCodecReasoningSignatureRoundTrip(t *testing.T) {
	codec, err := NewTypedExtensionStateCodec(typedContract())
	if err != nil {
		t.Fatal(err)
	}
	msg := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlock(&einoschema.Reasoning{Text: "because", Signature: "SENTINEL-SIG"}),
		},
	}
	capture, public, err := codec.Capture(msg, func(i int) string { return "block-0" })
	if err != nil {
		t.Fatal(err)
	}
	if public.ContentBlocks[0].Reasoning.Signature != "" {
		t.Fatalf("public signature = %q, want empty", public.ContentBlocks[0].Reasoning.Signature)
	}
	scanNoSentinel(t, public, "SENTINEL-SIG")
	if len(capture.Items) != 1 || capture.Items[0].BlockID != "block-0" {
		t.Fatalf("items = %#v", capture.Items)
	}
	var payload privateSignature
	if err := json.Unmarshal(capture.Items[0].Data, &payload); err != nil || payload.Signature != "SENTINEL-SIG" {
		t.Fatalf("item payload = %s, err = %v", capture.Items[0].Data, err)
	}

	restored, err := codec.Restore(public, capture.Items, func(i int) string { return "block-0" })
	if err != nil {
		t.Fatal(err)
	}
	if restored.ContentBlocks[0].Reasoning.Signature != "SENTINEL-SIG" {
		t.Fatalf("restored signature = %q", restored.ContentBlocks[0].Reasoning.Signature)
	}
	if !reflect.DeepEqual(restored, msg) {
		t.Fatalf("restore is not the exact inverse:\n got  %#v\n want %#v", restored, msg)
	}
}

func TestTypedExtensionCodecClaudeEncryptedIndexRoundTrip(t *testing.T) {
	codec, err := NewTypedExtensionStateCodec(typedContract())
	if err != nil {
		t.Fatal(err)
	}
	msg := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlock(&einoschema.AssistantGenText{
				Text: "answer",
				OpenAIExtension: &openai.AssistantGenTextExtension{
					Annotations: []*openai.TextAnnotation{{Type: openai.TextAnnotationTypeURLCitation, URLCitation: &openai.TextAnnotationURLCitation{URL: "https://a"}}},
				},
				ClaudeExtension: &claude.AssistantGenTextExtension{
					Citations: []*claude.TextCitation{
						{Type: claude.TextCitationTypeWebSearchResultLocation, WebSearchResultLocation: &claude.CitationWebSearchResultLocation{CitedText: "x", EncryptedIndex: "SENTINEL-ENC"}},
					},
				},
			}),
		},
	}
	capture, public, err := codec.Capture(msg, func(i int) string { return "block-0" })
	if err != nil {
		t.Fatal(err)
	}
	if public.ContentBlocks[0].AssistantGenText.ClaudeExtension.Citations[0].WebSearchResultLocation.EncryptedIndex != "" {
		t.Fatal("public message still carries the encrypted index")
	}
	scanNoSentinel(t, public, "SENTINEL-ENC")
	if len(capture.Items) != 1 {
		t.Fatalf("items = %#v", capture.Items)
	}
	var payload privateEncryptedIndexes
	if err := json.Unmarshal(capture.Items[0].Data, &payload); err != nil {
		t.Fatal(err)
	}
	// One OpenAI annotation precedes the Claude citation, so its annotation
	// index is 1.
	if len(payload.EncryptedIndexes) != 1 || payload.EncryptedIndexes[0].Annotation != 1 || payload.EncryptedIndexes[0].EncryptedIndex != "SENTINEL-ENC" {
		t.Fatalf("payload = %#v", payload)
	}

	restored, err := codec.Restore(public, capture.Items, func(i int) string { return "block-0" })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored, msg) {
		t.Fatalf("restore is not the exact inverse:\n got  %#v\n want %#v", restored, msg)
	}
}

func TestTypedExtensionCodecGeminiSDKBlobRoundTrip(t *testing.T) {
	codec, err := NewTypedExtensionStateCodec(typedContract())
	if err != nil {
		t.Fatal(err)
	}
	msg := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ResponseMeta: &einoschema.AgenticResponseMeta{
			GeminiExtension: &gemini.ResponseMetaExtension{
				GroundingMeta: &gemini.GroundingMetadata{SearchEntryPoint: &gemini.SearchEntryPoint{RenderedContent: "rc", SDKBlob: []byte("SENTINEL-BLOB")}},
			},
		},
	}
	capture, public, err := codec.Capture(msg, func(i int) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if len(public.ResponseMeta.GeminiExtension.GroundingMeta.SearchEntryPoint.SDKBlob) != 0 {
		t.Fatal("public message still carries the SDK blob")
	}
	if public.ResponseMeta.GeminiExtension.GroundingMeta.SearchEntryPoint.RenderedContent != "rc" {
		t.Fatal("public message lost the public rendered content")
	}
	scanNoSentinel(t, public, "SENTINEL-BLOB")
	if len(capture.Items) != 1 || capture.Items[0].BlockID != "" {
		t.Fatalf("items = %#v", capture.Items)
	}

	restored, err := codec.Restore(public, capture.Items, func(i int) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored, msg) {
		t.Fatalf("restore is not the exact inverse:\n got  %#v\n want %#v", restored, msg)
	}
}

func TestTypedExtensionCodecResponseIDsRoundTrip(t *testing.T) {
	tests := map[string]struct {
		build func() *einoschema.AgenticMessage
	}{
		"openai": {build: func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ResponseMeta: &einoschema.AgenticResponseMeta{
				OpenAIExtension: &openai.ResponseMetaExtension{ID: "SENTINEL-ID", PreviousResponseID: "SENTINEL-PREV", CreatedAt: 42},
			}}
		}},
		"claude": {build: func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ResponseMeta: &einoschema.AgenticResponseMeta{
				ClaudeExtension: &claude.ResponseMetaExtension{ID: "SENTINEL-ID", StopReason: "end_turn"},
			}}
		}},
		"gemini": {build: func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ResponseMeta: &einoschema.AgenticResponseMeta{
				GeminiExtension: &gemini.ResponseMetaExtension{ID: "SENTINEL-ID", FinishReason: "STOP"},
			}}
		}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			codec, err := NewTypedExtensionStateCodec(typedContract())
			if err != nil {
				t.Fatal(err)
			}
			msg := test.build()
			capture, public, err := codec.Capture(msg, func(i int) string { return "" })
			if err != nil {
				t.Fatal(err)
			}
			scanNoSentinel(t, public, "SENTINEL-ID")
			scanNoSentinel(t, public, "SENTINEL-PREV")
			if len(capture.Items) != 1 {
				t.Fatalf("items = %#v", capture.Items)
			}
			restored, err := codec.Restore(public, capture.Items, func(i int) string { return "" })
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(restored, msg) {
				t.Fatalf("restore is not the exact inverse:\n got  %#v\n want %#v", restored, msg)
			}
		})
	}
}

// scanNoSentinel fails the test if sentinel appears anywhere in the JSON
// encoding of msg, proving the public projection carries no private bytes.
func scanNoSentinel(t *testing.T, msg *einoschema.AgenticMessage, sentinel string) {
	t.Helper()
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), sentinel) {
		t.Fatalf("public message leaks private sentinel %q: %s", sentinel, raw)
	}
}

func TestAgenticProviderStateStreamerRestoresOnlyDispatchedClone(t *testing.T) {
	codec, err := NewTypedExtensionStateCodec(typedContract())
	if err != nil {
		t.Fatal(err)
	}
	client := &scriptedAgenticModel{chunks: []*einoschema.AgenticMessage{{Role: einoschema.AgenticRoleTypeAssistant}}}
	streamer, err := NewAgenticStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}

	assistant := &einoschema.AgenticMessage{
		Role:          einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{einoschema.NewContentBlock(&einoschema.Reasoning{Text: "because"})},
	}
	item, err := json.Marshal(privateSignature{Signature: "SENTINEL-SIG"})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		Identity: Identity{SessionID: "session", ProviderID: "provider", ModelID: "model"},
		Messages: []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("hi"), assistant},
		ProviderState: []ProviderMessageState{{
			MessageIndex: 1, MessageID: "message", SourceSessionID: "session", SourceRunID: "run",
			ProviderID: "provider", SourceModelID: "model", CodecID: typedContract().CodecID, Version: 1, CompatibilityKey: "typed-v1",
			BlockIDs: []string{"block-0"},
			Items:    []ProviderStateItem{{BlockID: "block-0", Data: item}},
		}},
	}
	reader, err := streamer.StreamProvider(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()

	if request.Messages[1].ContentBlocks[0].Reasoning.Signature != "" {
		t.Fatal("caller's request was mutated")
	}
	if client.callCount() != 1 {
		t.Fatalf("calls = %d, want 1", client.callCount())
	}
	dispatched := client.call(0).messages
	if len(dispatched) != 2 || dispatched[1].ContentBlocks[0].Reasoning.Signature != "SENTINEL-SIG" {
		t.Fatalf("dispatched messages = %#v", dispatched)
	}
}

func TestAgenticProviderStateStreamerRejectsMismatches(t *testing.T) {
	codec, err := NewTypedExtensionStateCodec(typedContract())
	if err != nil {
		t.Fatal(err)
	}
	client := &scriptedAgenticModel{chunks: []*einoschema.AgenticMessage{{Role: einoschema.AgenticRoleTypeAssistant}}}
	streamer, err := NewAgenticStreamerWithProviderState(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	item, err := json.Marshal(privateSignature{Signature: "s"})
	if err != nil {
		t.Fatal(err)
	}
	assistant := &einoschema.AgenticMessage{
		Role:          einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{einoschema.NewContentBlock(&einoschema.Reasoning{Text: "because"})},
	}
	base := Request{
		Identity: Identity{SessionID: "session", ProviderID: "provider", ModelID: "model"},
		Messages: []*einoschema.AgenticMessage{assistant, einoschema.UserAgenticMessage("hi")},
		ProviderState: []ProviderMessageState{{
			MessageIndex: 0, MessageID: "message", SourceSessionID: "session", SourceRunID: "run",
			ProviderID: "provider", SourceModelID: "model", CodecID: typedContract().CodecID, Version: 1, CompatibilityKey: "typed-v1",
			BlockIDs: []string{"block-0"},
			Items:    []ProviderStateItem{{BlockID: "block-0", Data: item}},
		}},
	}
	tests := map[string]func(*Request){
		"wrong role":           func(r *Request) { r.ProviderState[0].MessageIndex = 1 },
		"session mismatch":     func(r *Request) { r.ProviderState[0].SourceSessionID = "other" },
		"provider mismatch":    func(r *Request) { r.ProviderState[0].ProviderID = "other" },
		"codec mismatch":       func(r *Request) { r.ProviderState[0].CodecID = "other" },
		"version mismatch":     func(r *Request) { r.ProviderState[0].Version = 2 },
		"block ids length":     func(r *Request) { r.ProviderState[0].BlockIDs = nil },
		"unknown block id":     func(r *Request) { r.ProviderState[0].Items[0].BlockID = "not-a-block" },
		"current identity bad": func(r *Request) { r.Identity.ModelID = "bad model" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request, err := base.Clone()
			if err != nil {
				t.Fatal(err)
			}
			mutate(&request)
			client.mu.Lock()
			client.calls = nil
			client.mu.Unlock()
			if _, err := streamer.StreamProvider(context.Background(), request); err == nil {
				t.Fatal("expected an error")
			}
			if client.callCount() != 0 {
				t.Fatalf("calls = %d, want 0", client.callCount())
			}
		})
	}
}
