package runtime

import (
	"bytes"
	"fmt"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/claude"
	"github.com/cloudwego/eino/schema/gemini"
	"github.com/cloudwego/eino/schema/openai"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestPrivateStateCaptureAgreesBetweenSessionAndModel is the cross-package
// guard the W3 design required: session.ContentFromAgenticMessage (the
// durable public/private split, session/content.go) and
// model.NewTypedExtensionStateCodec(...).Capture (the native provider-state
// codec, model/agentic_state.go) duplicate the same four private payload
// shapes verbatim so W2's split and W3's codec agree byte-for-byte. Only the
// "signature" shape was covered by an existing cross-package assertion
// (native_provider_state_test.go, via an ad-hoc struct); this test covers
// all four so a JSON-tag rename on either side, without updating the other,
// fails here instead of only at restore time on a real conversation.
func TestPrivateStateCaptureAgreesBetweenSessionAndModel(t *testing.T) {
	tests := map[string]func() *einoschema.AgenticMessage{
		"reasoning signature": func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{
				Role: einoschema.AgenticRoleTypeAssistant,
				ContentBlocks: []*einoschema.ContentBlock{
					einoschema.NewContentBlock(&einoschema.Reasoning{Text: "because", Signature: "SENTINEL-SIG"}),
				},
			}
		},
		"claude encrypted citation index": func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{
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
		},
		"gemini sdk blob": func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{
				Role: einoschema.AgenticRoleTypeAssistant,
				ResponseMeta: &einoschema.AgenticResponseMeta{
					GeminiExtension: &gemini.ResponseMetaExtension{
						GroundingMeta: &gemini.GroundingMetadata{SearchEntryPoint: &gemini.SearchEntryPoint{RenderedContent: "rc", SDKBlob: []byte("SENTINEL-BLOB")}},
					},
				},
			}
		},
		"openai response ids": func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ResponseMeta: &einoschema.AgenticResponseMeta{
				OpenAIExtension: &openai.ResponseMetaExtension{ID: "SENTINEL-ID", PreviousResponseID: "SENTINEL-PREV", CreatedAt: 42},
			}}
		},
		"claude response id": func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ResponseMeta: &einoschema.AgenticResponseMeta{
				ClaudeExtension: &claude.ResponseMetaExtension{ID: "SENTINEL-ID", StopReason: "end_turn"},
			}}
		},
		"gemini response id": func() *einoschema.AgenticMessage {
			return &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ResponseMeta: &einoschema.AgenticResponseMeta{
				GeminiExtension: &gemini.ResponseMetaExtension{ID: "SENTINEL-ID", FinishReason: "STOP"},
			}}
		},
	}

	codec, err := model.NewTypedExtensionStateCodec(runtimeProviderStateContract())
	if err != nil {
		t.Fatal(err)
	}

	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			// Both sides mint the same durable block IDs by position:
			// session's `ids` is called once per content block in order,
			// and the codec's `blockID(index)` is keyed by the same
			// positional index, so "block-<i>" identifies the same block on
			// both sides for these single/zero-block fixtures.
			next := 0
			ids := func() string {
				id := fmt.Sprintf("block-%d", next)
				next++
				return id
			}
			blockID := func(index int) string { return fmt.Sprintf("block-%d", index) }

			_, sessionPrivate, err := session.ContentFromAgenticMessage(build(), ids)
			if err != nil {
				t.Fatalf("session.ContentFromAgenticMessage: %v", err)
			}
			capture, _, err := codec.Capture(build(), blockID)
			if err != nil {
				t.Fatalf("codec.Capture: %v", err)
			}

			if len(sessionPrivate) != len(capture.Items) {
				t.Fatalf("private item count = %d (session) vs %d (codec)", len(sessionPrivate), len(capture.Items))
			}
			for i := range sessionPrivate {
				if sessionPrivate[i].BlockID != capture.Items[i].BlockID {
					t.Fatalf("item %d BlockID = %q (session) vs %q (codec)", i, sessionPrivate[i].BlockID, capture.Items[i].BlockID)
				}
				if !bytes.Equal(sessionPrivate[i].Data, capture.Items[i].Data) {
					t.Fatalf("item %d payload mismatch:\n session = %s\n codec   = %s", i, sessionPrivate[i].Data, capture.Items[i].Data)
				}
			}
		})
	}
}
