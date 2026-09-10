package model

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/claude"
)

// The private item payload shapes below are copied verbatim (field names and
// JSON tags) from github.com/mattsp1290/eino-agent/session (content.go:
// privateSignature, privateEncryptedIndex, privateEncryptedIndexes,
// privateSDKBlob, privateResponseIDs, the payloads PrivateBlockState.Data
// carries) so that W2's durable public/private split and this codec's
// capture agree byte-for-byte. Do not change a field name or JSON tag here
// without updating session/content.go to match.

type privateSignature struct {
	Signature string `json:"signature"`
}

type privateEncryptedIndex struct {
	Annotation     int    `json:"annotation"`
	EncryptedIndex string `json:"encrypted_index"`
}

type privateEncryptedIndexes struct {
	EncryptedIndexes []privateEncryptedIndex `json:"encrypted_indexes"`
}

type privateSDKBlob struct {
	SDKBlobBase64 string `json:"sdk_blob_base64"`
}

type privateResponseIDs struct {
	ResponseID         string `json:"response_id,omitempty"`
	PreviousResponseID string `json:"previous_response_id,omitempty"`
	CreatedAt          int64  `json:"created_at,omitempty"`
}

// AgenticStateCodec captures and privately restores provider-private typed
// extension material carried on a *schema.AgenticMessage (reasoning
// signatures, Claude encrypted citation indexes, the Gemini grounding SDK
// blob and provider response identity). Implementations must be safe for
// concurrent use by multiple in-flight requests.
type AgenticStateCodec interface {
	Contract() ProviderStateContract
	// Capture splits msg into provider-private items and a public message
	// with those fields removed. blockID assigns a durable identity to the
	// content block at each index; it is called at most once per block that
	// carries private state. Items whose BlockID is "" are message-level.
	Capture(msg *einoschema.AgenticMessage, blockID func(index int) string) (ProviderStateCapture, *einoschema.AgenticMessage, error)
	// Restore is the exact inverse of Capture: given the public message
	// Capture produced (or an equivalent one) and the previously captured
	// items, it returns a message with the private fields reinstated.
	//
	// Deviation from the design sketch: Restore additionally takes a
	// blockID function, mirroring Capture's. The design's interface sketch
	// omitted it, but a ProviderStateItem carries only an opaque BlockID
	// string and a schema.ContentBlock carries no identity field (Extra is
	// disallowed), so nothing else lets Restore address a specific content
	// block by the BlockID captured items name. The caller (the state-aware
	// streamer below) already carries this mapping in
	// ProviderMessageState.BlockIDs, so plumbing it through keeps Capture
	// and Restore fully symmetric and deterministic.
	Restore(public *einoschema.AgenticMessage, items []ProviderStateItem, blockID func(index int) string) (*einoschema.AgenticMessage, error)
}

// NewTypedExtensionStateCodec constructs an AgenticStateCodec bound to
// contract. Any non-empty Extra/Extension carried by the message, a content
// block or the response meta is rejected: this codec only ever captures the
// specific typed fields it knows about.
func NewTypedExtensionStateCodec(contract ProviderStateContract) (AgenticStateCodec, error) {
	if err := ValidateProviderStateContract(contract); err != nil {
		return nil, err
	}
	return &typedExtensionStateCodec{contract: contract}, nil
}

type typedExtensionStateCodec struct {
	contract ProviderStateContract
}

func (c *typedExtensionStateCodec) Contract() ProviderStateContract {
	return c.contract
}

func (c *typedExtensionStateCodec) Capture(msg *einoschema.AgenticMessage, blockID func(index int) string) (ProviderStateCapture, *einoschema.AgenticMessage, error) {
	if msg == nil || blockID == nil {
		return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
	}
	cloned, err := cloneAgenticMessages([]*einoschema.AgenticMessage{msg})
	if err != nil || len(cloned) != 1 || cloned[0] == nil {
		return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
	}
	out := cloned[0]
	if len(out.Extra) != 0 {
		return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
	}

	var items []ProviderStateItem
	for index, block := range out.ContentBlocks {
		if block == nil {
			continue
		}
		if len(block.Extra) != 0 {
			return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
		}
		switch block.Type {
		case einoschema.ContentBlockTypeReasoning:
			if block.Reasoning != nil && block.Reasoning.Signature != "" {
				raw, merr := json.Marshal(privateSignature{Signature: block.Reasoning.Signature})
				if merr != nil {
					return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
				}
				items = append(items, ProviderStateItem{BlockID: blockID(index), Data: raw})
				block.Reasoning.Signature = ""
			}
		case einoschema.ContentBlockTypeAssistantGenText:
			if block.AssistantGenText != nil {
				if block.AssistantGenText.Extension != nil {
					return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
				}
				item, ok, cerr := captureClaudeEncryptedIndexes(block.AssistantGenText, blockID(index))
				if cerr != nil {
					return ProviderStateCapture{}, nil, cerr
				}
				if ok {
					items = append(items, item)
				}
			}
		}
	}

	if out.ResponseMeta != nil {
		if out.ResponseMeta.Extension != nil {
			return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
		}
		metaItems, merr := captureResponseMetaPrivate(out.ResponseMeta)
		if merr != nil {
			return ProviderStateCapture{}, nil, merr
		}
		items = append(items, metaItems...)
	}

	if len(items) == 0 {
		return ProviderStateCapture{}, out, nil
	}
	if err := ValidateProviderStateItems(items, c.contract.Limits); err != nil {
		return ProviderStateCapture{}, nil, err
	}
	return ProviderStateCapture{Items: items}, out, nil
}

func captureClaudeEncryptedIndexes(t *einoschema.AssistantGenText, id string) (ProviderStateItem, bool, error) {
	if t.ClaudeExtension == nil {
		return ProviderStateItem{}, false, nil
	}
	openAICount := 0
	if t.OpenAIExtension != nil {
		for _, a := range t.OpenAIExtension.Annotations {
			if a != nil {
				openAICount++
			}
		}
	}
	var encrypted []privateEncryptedIndex
	idx := openAICount
	for _, cit := range t.ClaudeExtension.Citations {
		if cit == nil {
			continue
		}
		if cit.Type == claude.TextCitationTypeWebSearchResultLocation && cit.WebSearchResultLocation != nil && cit.WebSearchResultLocation.EncryptedIndex != "" {
			encrypted = append(encrypted, privateEncryptedIndex{Annotation: idx, EncryptedIndex: cit.WebSearchResultLocation.EncryptedIndex})
			cit.WebSearchResultLocation.EncryptedIndex = ""
		}
		idx++
	}
	if len(encrypted) == 0 {
		return ProviderStateItem{}, false, nil
	}
	raw, err := json.Marshal(privateEncryptedIndexes{EncryptedIndexes: encrypted})
	if err != nil {
		return ProviderStateItem{}, false, providerStateError(ErrProviderStateInvalid)
	}
	return ProviderStateItem{BlockID: id, Data: raw}, true, nil
}

func captureResponseMetaPrivate(rm *einoschema.AgenticResponseMeta) ([]ProviderStateItem, error) {
	var items []ProviderStateItem
	responseID := ""
	previousResponseID := ""
	var createdAt int64

	if rm.OpenAIExtension != nil {
		ext := rm.OpenAIExtension
		if ext.ID != "" {
			responseID = ext.ID
			ext.ID = ""
		}
		previousResponseID = ext.PreviousResponseID
		ext.PreviousResponseID = ""
		createdAt = ext.CreatedAt
		ext.CreatedAt = 0
	}
	if rm.ClaudeExtension != nil {
		ext := rm.ClaudeExtension
		if ext.ID != "" && responseID == "" {
			responseID = ext.ID
			ext.ID = ""
		}
	}
	if rm.GeminiExtension != nil {
		ext := rm.GeminiExtension
		if ext.GroundingMeta != nil && ext.GroundingMeta.SearchEntryPoint != nil && len(ext.GroundingMeta.SearchEntryPoint.SDKBlob) > 0 {
			raw, err := json.Marshal(privateSDKBlob{SDKBlobBase64: base64.StdEncoding.EncodeToString(ext.GroundingMeta.SearchEntryPoint.SDKBlob)})
			if err != nil {
				return nil, providerStateError(ErrProviderStateInvalid)
			}
			items = append(items, ProviderStateItem{Data: raw})
			ext.GroundingMeta.SearchEntryPoint.SDKBlob = nil
		}
		if ext.ID != "" && responseID == "" {
			responseID = ext.ID
			ext.ID = ""
		}
	}
	if responseID != "" || previousResponseID != "" || createdAt != 0 {
		raw, err := json.Marshal(privateResponseIDs{ResponseID: responseID, PreviousResponseID: previousResponseID, CreatedAt: createdAt})
		if err != nil {
			return nil, providerStateError(ErrProviderStateInvalid)
		}
		items = append(items, ProviderStateItem{Data: raw})
	}
	return items, nil
}

func (c *typedExtensionStateCodec) Restore(public *einoschema.AgenticMessage, items []ProviderStateItem, blockID func(index int) string) (*einoschema.AgenticMessage, error) {
	if public == nil || blockID == nil {
		return nil, providerStateError(ErrProviderStateInvalid)
	}
	if len(items) != 0 {
		if err := ValidateProviderStateItems(items, c.contract.Limits); err != nil {
			return nil, err
		}
	}
	cloned, err := cloneAgenticMessages([]*einoschema.AgenticMessage{public})
	if err != nil || len(cloned) != 1 || cloned[0] == nil {
		return nil, providerStateError(ErrProviderStateInvalid)
	}
	out := cloned[0]
	if len(out.Extra) != 0 {
		return nil, providerStateError(ErrProviderStateInvalid)
	}

	blockItems := map[string][]ProviderStateItem{}
	var messageItems []ProviderStateItem
	for _, item := range items {
		if item.BlockID == "" {
			messageItems = append(messageItems, item)
		} else {
			blockItems[item.BlockID] = append(blockItems[item.BlockID], item)
		}
	}

	for index, block := range out.ContentBlocks {
		if block == nil {
			continue
		}
		id := blockID(index)
		pending := blockItems[id]
		if len(pending) == 0 {
			continue
		}
		delete(blockItems, id)
		switch block.Type {
		case einoschema.ContentBlockTypeReasoning:
			if block.Reasoning == nil || len(pending) != 1 {
				return nil, providerStateError(ErrProviderStateMismatch)
			}
			var payload privateSignature
			if err := strictUnmarshal(pending[0].Data, &payload); err != nil {
				return nil, providerStateError(ErrProviderStateInvalid)
			}
			block.Reasoning.Signature = payload.Signature
		case einoschema.ContentBlockTypeAssistantGenText:
			if block.AssistantGenText == nil || len(pending) != 1 {
				return nil, providerStateError(ErrProviderStateMismatch)
			}
			var payload privateEncryptedIndexes
			if err := strictUnmarshal(pending[0].Data, &payload); err != nil {
				return nil, providerStateError(ErrProviderStateInvalid)
			}
			if err := restoreClaudeEncryptedIndexes(block.AssistantGenText, payload); err != nil {
				return nil, err
			}
		default:
			return nil, providerStateError(ErrProviderStateMismatch)
		}
	}
	if len(blockItems) != 0 {
		return nil, providerStateError(ErrProviderStateMismatch)
	}
	if len(messageItems) != 0 {
		if out.ResponseMeta == nil {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if err := restoreResponseMetaPrivate(out.ResponseMeta, messageItems); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func restoreClaudeEncryptedIndexes(t *einoschema.AssistantGenText, payload privateEncryptedIndexes) error {
	if t.ClaudeExtension == nil {
		return providerStateError(ErrProviderStateMismatch)
	}
	openAICount := 0
	if t.OpenAIExtension != nil {
		for _, a := range t.OpenAIExtension.Annotations {
			if a != nil {
				openAICount++
			}
		}
	}
	byAnnotation := make(map[int]*claude.TextCitation)
	idx := openAICount
	for _, cit := range t.ClaudeExtension.Citations {
		if cit == nil {
			continue
		}
		byAnnotation[idx] = cit
		idx++
	}
	for _, e := range payload.EncryptedIndexes {
		cit, ok := byAnnotation[e.Annotation]
		if !ok || cit.Type != claude.TextCitationTypeWebSearchResultLocation || cit.WebSearchResultLocation == nil {
			return providerStateError(ErrProviderStateMismatch)
		}
		cit.WebSearchResultLocation.EncryptedIndex = e.EncryptedIndex
	}
	return nil
}

func restoreResponseMetaPrivate(rm *einoschema.AgenticResponseMeta, items []ProviderStateItem) error {
	for _, item := range items {
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(item.Data, &probe); err != nil {
			return providerStateError(ErrProviderStateInvalid)
		}
		switch {
		case hasKey(probe, "sdk_blob_base64"):
			var payload privateSDKBlob
			if err := strictUnmarshal(item.Data, &payload); err != nil {
				return providerStateError(ErrProviderStateInvalid)
			}
			if err := restoreGeminiSDKBlob(rm, payload); err != nil {
				return err
			}
		case hasKey(probe, "response_id") || hasKey(probe, "previous_response_id") || hasKey(probe, "created_at"):
			var payload privateResponseIDs
			if err := strictUnmarshal(item.Data, &payload); err != nil {
				return providerStateError(ErrProviderStateInvalid)
			}
			if err := restoreResponseIDs(rm, payload); err != nil {
				return err
			}
		default:
			return providerStateError(ErrProviderStateInvalid)
		}
	}
	return nil
}

func restoreGeminiSDKBlob(rm *einoschema.AgenticResponseMeta, payload privateSDKBlob) error {
	if rm.GeminiExtension == nil || rm.GeminiExtension.GroundingMeta == nil || rm.GeminiExtension.GroundingMeta.SearchEntryPoint == nil {
		return providerStateError(ErrProviderStateMismatch)
	}
	blob, err := base64.StdEncoding.DecodeString(payload.SDKBlobBase64)
	if err != nil {
		return providerStateError(ErrProviderStateInvalid)
	}
	rm.GeminiExtension.GroundingMeta.SearchEntryPoint.SDKBlob = blob
	return nil
}

func restoreResponseIDs(rm *einoschema.AgenticResponseMeta, payload privateResponseIDs) error {
	if payload.PreviousResponseID != "" || payload.CreatedAt != 0 {
		if rm.OpenAIExtension == nil {
			return providerStateError(ErrProviderStateMismatch)
		}
		rm.OpenAIExtension.PreviousResponseID = payload.PreviousResponseID
		rm.OpenAIExtension.CreatedAt = payload.CreatedAt
	}
	if payload.ResponseID == "" {
		return nil
	}
	switch {
	case rm.OpenAIExtension != nil:
		rm.OpenAIExtension.ID = payload.ResponseID
	case rm.ClaudeExtension != nil:
		rm.ClaudeExtension.ID = payload.ResponseID
	case rm.GeminiExtension != nil:
		rm.GeminiExtension.ID = payload.ResponseID
	default:
		return providerStateError(ErrProviderStateMismatch)
	}
	return nil
}

func hasKey(m map[string]json.RawMessage, key string) bool {
	_, ok := m[key]
	return ok
}

func strictUnmarshal(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// AgenticProviderStateStreamer is a state-aware agentic provider boundary:
// it can capture provider-private typed extension state from a finalized
// output message, and it restores previously captured state into the
// dispatched clone of a request before calling the upstream model.
type AgenticProviderStateStreamer interface {
	Streamer
	ProviderStateContract() ProviderStateContract
	CaptureProviderState(msg *einoschema.AgenticMessage, blockID func(index int) string) (ProviderStateCapture, *einoschema.AgenticMessage, error)
}

// NewAgenticStreamerWithProviderState adapts an Eino agentic model with one
// immutable typed-extension provider-state contract. State is restored only
// on the dispatched clone; the caller's request is never mutated.
func NewAgenticStreamerWithProviderState(client einomodel.AgenticModel, codec AgenticStateCodec) (AgenticProviderStateStreamer, error) {
	if client == nil || codec == nil {
		return nil, providerStateError(ErrProviderStateInvalid)
	}
	contract := codec.Contract()
	if err := ValidateProviderStateContract(contract); err != nil {
		return nil, err
	}
	return &agenticProviderStateStreamer{client: client, codec: codec, contract: contract}, nil
}

type agenticProviderStateStreamer struct {
	client   einomodel.AgenticModel
	codec    AgenticStateCodec
	contract ProviderStateContract
}

func (s *agenticProviderStateStreamer) ProviderStateContract() ProviderStateContract {
	return s.contract
}

func (s *agenticProviderStateStreamer) CaptureProviderState(msg *einoschema.AgenticMessage, blockID func(index int) string) (ProviderStateCapture, *einoschema.AgenticMessage, error) {
	if s == nil || s.codec == nil {
		return ProviderStateCapture{}, nil, providerStateError(ErrProviderStateInvalid)
	}
	return s.codec.Capture(msg, blockID)
}

func (s *agenticProviderStateStreamer) StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[StreamDelta], error) {
	if s == nil || s.client == nil || s.codec == nil {
		return nil, Error{Code: "model_client_missing", Message: "Eino agentic model client missing", Cause: ErrProviderUnavailable}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req, err := request.Clone()
	if err != nil {
		return nil, err
	}
	if err := ValidateProviderStateIdentity(string(req.Identity.ProviderID), string(req.Identity.ModelID)); err != nil {
		return nil, err
	}
	if err := ValidateControls(req.Controls); err != nil {
		return nil, err
	}
	previousIndex := -1
	for _, state := range req.ProviderState {
		if state.MessageIndex <= previousIndex || state.MessageIndex < 0 || state.MessageIndex >= len(req.Messages) ||
			state.MessageID == "" || state.SourceRunID == "" || state.SourceSessionID == "" ||
			state.SourceSessionID != req.Identity.SessionID || state.ProviderID != string(req.Identity.ProviderID) ||
			state.CodecID != s.contract.CodecID || state.CompatibilityKey != s.contract.CompatibilityKey {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if state.Version != s.contract.Version {
			return nil, providerStateError(ErrProviderStateVersion)
		}
		if err := ValidateProviderStateIdentity(state.ProviderID, state.SourceModelID); err != nil {
			return nil, err
		}
		message := req.Messages[state.MessageIndex]
		if message == nil || message.Role != einoschema.AgenticRoleTypeAssistant || len(message.Extra) != 0 {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if len(state.BlockIDs) != len(message.ContentBlocks) {
			return nil, providerStateError(ErrProviderStateMismatch)
		}
		if err := ValidateProviderStateItems(state.Items, s.contract.Limits); err != nil {
			return nil, err
		}
		blockIDs := state.BlockIDs
		restored, err := s.codec.Restore(message, state.Items, func(index int) string {
			if index < 0 || index >= len(blockIDs) {
				return ""
			}
			return blockIDs[index]
		})
		if err != nil {
			return nil, err
		}
		req.Messages[state.MessageIndex] = restored
		previousIndex = state.MessageIndex
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return streamAgentic(ctx, s.client, req)
}
