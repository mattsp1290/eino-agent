package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

type capturedProviderState struct {
	state    *model.ProviderMessageState
	payloads []json.RawMessage
}

// contractStreamer is the read-only subset of model.ProviderStateStreamer
// and model.AgenticProviderStateStreamer shared by both provider-state
// boundaries: enough to validate a durable envelope's codec identity against
// the currently active provider without committing to either capture shape.
type contractStreamer interface {
	ProviderStateContract() model.ProviderStateContract
}

// blockEnvelopeIdentityProbe mirrors session/history/agentic_projection.go's
// unexported probe of the same name: it reads the "block_id" field common to
// every content block envelope (see session/content.go's
// contentBlockEnvelope) without needing an exported accessor for it.
type blockEnvelopeIdentityProbe struct {
	BlockID string `json:"block_id"`
}

// orderedBlockIDs returns owner's durable content-block IDs, in the same
// ordinal order session.ContentFromAgenticMessage/EncodeContentParts wrote
// them, by reading the block_id field embedded in each content-block part's
// envelope. It ignores provider_state and response_meta parts (neither is
// block-shaped).
func orderedBlockIDs(parts []session.Part, owners []session.MessageID, owner session.MessageID) ([]string, error) {
	type located struct {
		ordinal int64
		id      string
	}
	var found []located
	for index, part := range parts {
		if owners[index] != owner {
			continue
		}
		if _, ok := session.BlockKindForPart(part.Kind); !ok {
			continue
		}
		var probe blockEnvelopeIdentityProbe
		if err := json.Unmarshal(part.Payload, &probe); err != nil || probe.BlockID == "" {
			return nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		found = append(found, located{ordinal: part.Ordinal, id: probe.BlockID})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].ordinal < found[j].ordinal })
	ids := make([]string, len(found))
	for index, item := range found {
		ids[index] = item.id
	}
	return ids, nil
}

func loadProviderHistory(ctx context.Context, store session.Store, sessionRecord session.Session, options history.Options, resolved model.Resolved) ([]*einoschema.AgenticMessage, []session.MessageID, []model.ProviderMessageState, error) {
	batch, err := history.LoadBatch(ctx, store, sessionRecord.ID)
	if err != nil {
		return nil, nil, nil, err
	}
	projection, err := history.ProjectAgentic(batch, options)
	if err != nil {
		return nil, nil, nil, err
	}
	active := make(map[session.MessageID]bool, len(projection.SourceMessageIDs))
	assistantIndexes := make(map[session.MessageID][]int)
	for index, sourceID := range projection.SourceMessageIDs {
		active[sourceID] = true
		if projection.Messages[index] != nil && projection.Messages[index].Role == einoschema.AgenticRoleTypeAssistant {
			assistantIndexes[sourceID] = append(assistantIndexes[sourceID], index)
		}
	}
	messages := make(map[session.MessageID]session.Message, len(batch.Messages))
	for _, message := range batch.Messages {
		messages[message.ID] = message
	}
	type ownedPart struct {
		part  session.Part
		owner session.MessageID
	}
	groups := make(map[session.MessageID][]ownedPart)
	partOwners, err := session.ResolveReplayPartOwners(batch.Parts, batch.PartOwnerMessageIDs)
	if err != nil {
		return nil, nil, nil, err
	}
	for index, part := range batch.Parts {
		if part.Kind != session.PartProviderState {
			continue
		}
		owner := partOwners[index]
		if part.MessageID != owner {
			return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		if active[owner] {
			groups[owner] = append(groups[owner], ownedPart{part: part, owner: owner})
		}
	}
	if len(groups) == 0 {
		return projection.Messages, projection.SourceMessageIDs, nil, nil
	}
	streamer, ok := resolved.Streamer.(contractStreamer)
	if !ok || streamer == nil {
		return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
	}
	// blockBound is true only for the native model.AgenticProviderStateStreamer
	// boundary, where a codec resolves ProviderStateItem.BlockID against a
	// specific dispatched content block via the blockID(index) callback
	// (model/agentic_state.go). The classic model.ProviderStateStreamer
	// bridge never reads ProviderMessageState.BlockIDs or cross-checks an
	// item's BlockID against real content-block identity (see
	// model/classic_streamer.go's StreamProvider) — there, BlockID is an
	// opaque annotation a codec may set to any value it likes. Only the
	// block-bound path may drop or reject items based on block identity.
	_, blockBound := resolved.Streamer.(model.AgenticProviderStateStreamer)
	contract, err := safeProviderStateContract(streamer)
	if err != nil {
		return nil, nil, nil, err
	}
	owners := make([]session.MessageID, 0, len(groups))
	for owner := range groups {
		owners = append(owners, owner)
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
	runs := make(map[session.RunID]session.Run)
	states := make([]model.ProviderMessageState, 0, len(owners))
	for _, owner := range owners {
		message, exists := messages[owner]
		indexes := assistantIndexes[owner]
		if !exists || len(indexes) != 1 || message.ID == "" || message.Role != session.RoleAssistant ||
			message.SessionID != sessionRecord.ID || message.RunID == "" || message.ModelID == "" {
			return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		run, cached := runs[message.RunID]
		if !cached {
			run, err = store.GetRun(ctx, message.RunID)
			if err != nil {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
			}
			runs[message.RunID] = run
		}
		if run.ID != message.RunID || run.SessionID != sessionRecord.ID || run.ProviderID == "" || run.ModelID == "" ||
			run.ModelID != message.ModelID {
			return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		// storedBlockIDs is every durable content-block identity the
		// message actually stored (including, e.g., a reasoning block even
		// when the current history.Options excludes it from the
		// projection); it is used only to distinguish an item bound to a
		// block that was filtered out of this dispatch (dropped, since its
		// continuation is irrelevant when the block is not sent) from an
		// item bound to a block ID that never existed on the message at all
		// (a real mismatch, failed closed).
		storedBlockIDs, err := orderedBlockIDs(batch.Parts, partOwners, owner)
		if err != nil {
			return nil, nil, nil, err
		}
		// dispatchedBlockIDs is parallel to the message the projection
		// actually emits (projection.Messages[indexes[0]].ContentBlocks),
		// i.e. after any reasoning filtering. This is what state.BlockIDs
		// must match, so a block-bound provider-state item resolves to the
		// right position in the dispatched message.
		dispatchedBlockIDs := projection.BlockIDs[indexes[0]]
		if len(dispatchedBlockIDs) != len(projection.Messages[indexes[0]].ContentBlocks) {
			return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		knownBlockIDs := make(map[string]bool, len(storedBlockIDs))
		for _, id := range storedBlockIDs {
			knownBlockIDs[id] = true
		}
		dispatchedBlockIDSet := make(map[string]bool, len(dispatchedBlockIDs))
		for _, id := range dispatchedBlockIDs {
			dispatchedBlockIDSet[id] = true
		}
		parts := groups[owner]
		sort.Slice(parts, func(i, j int) bool {
			if parts[i].part.Ordinal != parts[j].part.Ordinal {
				return parts[i].part.Ordinal < parts[j].part.Ordinal
			}
			return parts[i].part.ID < parts[j].part.ID
		})
		seenPartIDs := make(map[session.PartID]bool, len(parts))
		items := make([]model.ProviderStateItem, 0, len(parts))
		storedBytes := 0
		previousOrdinal := int64(-1)
		for itemIndex, value := range parts {
			part := value.part
			if part.ID == "" || seenPartIDs[part.ID] || part.MessageID != owner || part.SessionID != sessionRecord.ID || part.Ordinal < 0 ||
				part.RunID != message.RunID || (itemIndex > 0 && part.Ordinal <= previousOrdinal) {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
			}
			seenPartIDs[part.ID] = true
			previousOrdinal = part.Ordinal
			storedBytes += len(part.Payload)
			if len(part.Payload) > contract.Limits.MaxEnvelopeBytes || storedBytes > contract.Limits.MaxStoredMessageBytes {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateTooLarge)
			}
			envelope, decodeErr := session.DecodeProviderStatePayload(part.Payload)
			if decodeErr != nil {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			if envelope.ItemIndex != itemIndex {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			if envelope.Version != contract.Version {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateVersion)
			}
			if envelope.ProviderID != run.ProviderID || envelope.SourceModelID != message.ModelID ||
				envelope.CodecID != contract.CodecID || envelope.CompatibilityKey != contract.CompatibilityKey ||
				envelope.ProviderID != string(resolved.Provider.ID) {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
			}
			if err := model.ValidateProviderStateIdentity(envelope.ProviderID, envelope.SourceModelID); err != nil {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			if blockBound && envelope.BlockID != "" {
				if !knownBlockIDs[envelope.BlockID] {
					// The item names a block ID that never existed on this
					// stored message: a real mismatch, fail closed.
					return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
				}
				if !dispatchedBlockIDSet[envelope.BlockID] {
					// The block existed but the projection excluded it from
					// this dispatch (e.g. a reasoning block under
					// IncludeReasoning=false); its private continuation is
					// irrelevant when the block itself is not sent.
					continue
				}
			}
			items = append(items, model.ProviderStateItem{BlockID: envelope.BlockID, Data: append(json.RawMessage(nil), envelope.Data...)})
		}
		if blockBound && len(items) == 0 {
			// Every item this message stored was bound to a block the
			// projection dropped; there is nothing left to restore for this
			// message this turn.
			continue
		}
		if err := model.ValidateProviderStateItems(items, contract.Limits); err != nil {
			if errors.Is(err, model.ErrProviderStateTooLarge) {
				return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateTooLarge)
			}
			return nil, nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
		states = append(states, model.ProviderMessageState{
			MessageIndex: indexes[0], MessageID: string(owner), SourceSessionID: string(sessionRecord.ID), SourceRunID: string(message.RunID),
			ProviderID: run.ProviderID, SourceModelID: message.ModelID, CodecID: contract.CodecID, Version: contract.Version,
			CompatibilityKey: contract.CompatibilityKey, BlockIDs: dispatchedBlockIDs, Items: items,
		})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].MessageIndex < states[j].MessageIndex })
	return projection.Messages, projection.SourceMessageIDs, states, nil
}

// captureAssistantProviderState splits provider-private material out of the
// finalized assistant message before it can ever be persisted or handed to
// an extension. blockIDs is pre-minted, one entry per message.ContentBlocks
// index (see adk_model.go's adkModel.commit), so the same durable block
// identity a block-bound codec captures against is the one
// session.ContentFromAgenticMessage later assigns to the corresponding
// durable content block.
//
// It returns the captured state (nil when the message carries no private
// material) and the public message to persist and hand to extensions in
// place of the raw provider output.
func captureAssistantProviderState(snapshot TurnSnapshot, messageID session.MessageID, message *einoschema.AgenticMessage, blockIDs []string) (capturedProviderState, *einoschema.AgenticMessage, error) {
	switch streamer := snapshot.Model.Streamer.(type) {
	case model.AgenticProviderStateStreamer:
		if message == nil {
			return capturedProviderState{}, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
		contract, err := safeProviderStateContract(streamer)
		if err != nil {
			return capturedProviderState{}, nil, err
		}
		blockIDFn := func(index int) string {
			if index < 0 || index >= len(blockIDs) {
				return ""
			}
			return blockIDs[index]
		}
		capture, public, err := safeCaptureAgenticProviderState(streamer, message, blockIDFn)
		if err != nil {
			return capturedProviderState{}, nil, err
		}
		if public == nil || len(public.Extra) != 0 {
			return capturedProviderState{}, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
		result, err := buildCapturedProviderState(snapshot, messageID, contract, capture.Items, blockIDs)
		if err != nil {
			return capturedProviderState{}, nil, err
		}
		return result, public, nil
	case model.ProviderStateStreamer:
		if message == nil {
			return capturedProviderState{}, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
		contract, err := safeProviderStateContract(streamer)
		if err != nil {
			return capturedProviderState{}, nil, err
		}
		originalKeys := make(map[string]struct{}, len(message.Extra))
		for key := range message.Extra {
			originalKeys[key] = struct{}{}
		}
		capture, err := safeCaptureClassicProviderState(streamer, message)
		if err != nil {
			return capturedProviderState{}, nil, err
		}
		claimed := make(map[string]struct{}, len(capture.ClaimedKeys))
		for _, key := range capture.ClaimedKeys {
			if _, ok := originalKeys[key]; !ok {
				return capturedProviderState{}, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			if _, duplicate := claimed[key]; duplicate {
				return capturedProviderState{}, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			claimed[key] = struct{}{}
		}
		if len(message.Extra) != 0 || len(claimed) != len(originalKeys) ||
			len(capture.Items) == 0 && len(capture.ClaimedKeys) != 0 || len(capture.Items) != 0 && len(claimed) == 0 {
			return capturedProviderState{}, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
		if len(capture.Items) == 0 {
			return capturedProviderState{}, message, nil
		}
		result, err := buildCapturedProviderState(snapshot, messageID, contract, capture.Items, nil)
		if err != nil {
			return capturedProviderState{}, nil, err
		}
		return result, message, nil
	default:
		if message != nil && len(message.Extra) != 0 {
			return capturedProviderState{}, nil, model.Error{Code: "provider_state_unregistered", Message: "provider state codec is not registered", Cause: errors.Join(model.ErrProviderState, model.ErrProviderStateMismatch)}
		}
		return capturedProviderState{}, message, nil
	}
}

func buildCapturedProviderState(snapshot TurnSnapshot, messageID session.MessageID, contract model.ProviderStateContract, items []model.ProviderStateItem, blockIDs []string) (capturedProviderState, error) {
	if len(items) == 0 {
		return capturedProviderState{}, nil
	}
	if err := model.ValidateProviderStateItems(items, contract.Limits); err != nil {
		return capturedProviderState{}, collapseProviderStateError(err)
	}
	result := capturedProviderState{payloads: make([]json.RawMessage, len(items))}
	storedBytes := 0
	for index, item := range items {
		payload, err := session.EncodeProviderStatePayload(session.ProviderStateEnvelope{
			CodecID: contract.CodecID, Version: contract.Version, ProviderID: string(snapshot.Model.Provider.ID),
			SourceModelID: string(snapshot.Model.Model.ID), CompatibilityKey: contract.CompatibilityKey,
			ItemIndex: index, BlockID: item.BlockID, Data: item.Data,
		})
		if err != nil {
			return capturedProviderState{}, runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
		storedBytes += len(payload)
		if len(payload) > contract.Limits.MaxEnvelopeBytes || storedBytes > contract.Limits.MaxStoredMessageBytes {
			return capturedProviderState{}, runtimeProviderStateError(model.ErrProviderStateTooLarge)
		}
		result.payloads[index] = payload
	}
	state := model.ProviderMessageState{
		MessageID: string(messageID), SourceSessionID: string(snapshot.SessionID), SourceRunID: string(snapshot.RunID),
		ProviderID: string(snapshot.Model.Provider.ID), SourceModelID: string(snapshot.Model.Model.ID),
		CodecID: contract.CodecID, Version: contract.Version, CompatibilityKey: contract.CompatibilityKey,
		BlockIDs: blockIDs, Items: items,
	}
	cloned, err := (model.Request{ProviderState: []model.ProviderMessageState{state}}).Clone()
	if err != nil {
		return capturedProviderState{}, runtimeProviderStateError(model.ErrProviderStateInvalid)
	}
	result.state = &cloned.ProviderState[0]
	return result, nil
}

func safeProviderStateContract(streamer contractStreamer) (contract model.ProviderStateContract, err error) {
	defer func() {
		if recover() != nil {
			contract = model.ProviderStateContract{}
			err = runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
	}()
	if streamer == nil {
		return model.ProviderStateContract{}, runtimeProviderStateError(model.ErrProviderStateInvalid)
	}
	contract = streamer.ProviderStateContract()
	if err := model.ValidateProviderStateContract(contract); err != nil {
		return model.ProviderStateContract{}, runtimeProviderStateError(model.ErrProviderStateInvalid)
	}
	return contract, nil
}

func safeCaptureAgenticProviderState(streamer model.AgenticProviderStateStreamer, message *einoschema.AgenticMessage, blockID func(int) string) (capture model.ProviderStateCapture, public *einoschema.AgenticMessage, err error) {
	defer func() {
		if recover() != nil {
			capture = model.ProviderStateCapture{}
			public = nil
			err = runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
	}()
	capture, public, err = streamer.CaptureProviderState(message, blockID)
	if err != nil {
		return model.ProviderStateCapture{}, nil, collapseProviderStateError(err)
	}
	return capture, public, nil
}

func safeCaptureClassicProviderState(streamer model.ProviderStateStreamer, message *einoschema.AgenticMessage) (capture model.ProviderStateCapture, err error) {
	defer func() {
		if recover() != nil {
			capture = model.ProviderStateCapture{}
			err = runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
	}()
	capture, err = streamer.CaptureProviderState(message)
	if err != nil {
		return model.ProviderStateCapture{}, collapseProviderStateError(err)
	}
	return capture, nil
}

func cloneRuntimeProviderState(states []model.ProviderMessageState) ([]model.ProviderMessageState, error) {
	cloned, err := (model.Request{ProviderState: states}).Clone()
	return cloned.ProviderState, err
}

func collapseProviderStateError(err error) error {
	switch {
	case errors.Is(err, model.ErrProviderStateTooLarge):
		return runtimeProviderStateError(model.ErrProviderStateTooLarge)
	case errors.Is(err, model.ErrProviderStateVersion):
		return runtimeProviderStateError(model.ErrProviderStateVersion)
	case errors.Is(err, model.ErrProviderStateMismatch):
		return runtimeProviderStateError(model.ErrProviderStateMismatch)
	default:
		return runtimeProviderStateError(model.ErrProviderStateInvalid)
	}
}

func runtimeProviderStateError(kind error) error {
	switch kind {
	case model.ErrProviderStateTooLarge:
		return model.Error{Code: "provider_state_too_large", Message: "provider state exceeds configured limits", Cause: errors.Join(model.ErrProviderState, kind)}
	case model.ErrProviderStateMismatch:
		return model.Error{Code: "provider_state_mismatch", Message: "provider state does not match the active model", Cause: errors.Join(model.ErrProviderState, kind)}
	case model.ErrProviderStateVersion:
		return model.Error{Code: "provider_state_version", Message: "provider state version is unsupported", Cause: errors.Join(model.ErrProviderState, kind)}
	default:
		return model.Error{Code: "provider_state_invalid", Message: "provider state is invalid", Cause: errors.Join(model.ErrProviderState, model.ErrProviderStateInvalid)}
	}
}
