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

func loadProviderHistory(ctx context.Context, store session.Store, sessionRecord session.Session, options history.Options, resolved model.Resolved) ([]*einoschema.Message, []model.ProviderMessageState, error) {
	batch, err := history.LoadBatch(ctx, store, sessionRecord.ID)
	if err != nil {
		return nil, nil, err
	}
	projection, err := history.ProjectWithSources(batch, options)
	if err != nil {
		return nil, nil, err
	}
	active := make(map[session.MessageID]bool, len(projection.SourceMessageIDs))
	assistantIndexes := make(map[session.MessageID][]int)
	for index, sourceID := range projection.SourceMessageIDs {
		active[sourceID] = true
		if projection.Messages[index] != nil && projection.Messages[index].Role == einoschema.Assistant {
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
	for index, part := range batch.Parts {
		if part.Kind != session.PartProviderState {
			continue
		}
		owner := part.MessageID
		if len(batch.PartOwnerMessageIDs) == len(batch.Parts) {
			owner = batch.PartOwnerMessageIDs[index]
		}
		if active[owner] {
			groups[owner] = append(groups[owner], ownedPart{part: part, owner: owner})
		}
	}
	if len(groups) == 0 {
		return projection.Messages, nil, nil
	}
	streamer, ok := resolved.Streamer.(model.ProviderStateStreamer)
	if !ok || streamer == nil {
		return nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
	}
	contract := streamer.ProviderStateContract()
	if err := model.ValidateProviderStateContract(contract); err != nil {
		return nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
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
			return nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		run, cached := runs[message.RunID]
		if !cached {
			run, err = store.GetRun(ctx, message.RunID)
			if err != nil {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
			}
			runs[message.RunID] = run
		}
		if run.ID != message.RunID || run.SessionID != sessionRecord.ID || run.ProviderID == "" || run.ModelID == "" ||
			run.ModelID != message.ModelID {
			return nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
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
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
			}
			seenPartIDs[part.ID] = true
			previousOrdinal = part.Ordinal
			storedBytes += len(part.Payload)
			if len(part.Payload) > contract.Limits.MaxEnvelopeBytes || storedBytes > contract.Limits.MaxStoredMessageBytes {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateTooLarge)
			}
			envelope, decodeErr := session.DecodeProviderStatePayload(part.Payload)
			if decodeErr != nil {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			if envelope.ItemIndex != itemIndex {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			if envelope.Version != contract.Version {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateVersion)
			}
			if envelope.ProviderID != run.ProviderID || envelope.SourceModelID != message.ModelID ||
				envelope.CodecID != contract.CodecID || envelope.CompatibilityKey != contract.CompatibilityKey ||
				envelope.ProviderID != string(resolved.Provider.ID) {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateMismatch)
			}
			if err := model.ValidateProviderStateIdentity(envelope.ProviderID, envelope.SourceModelID); err != nil {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
			}
			items = append(items, model.ProviderStateItem{Data: append(json.RawMessage(nil), envelope.Data...)})
		}
		if err := model.ValidateProviderStateItems(items, contract.Limits); err != nil {
			if errors.Is(err, model.ErrProviderStateTooLarge) {
				return nil, nil, runtimeProviderStateError(model.ErrProviderStateTooLarge)
			}
			return nil, nil, runtimeProviderStateError(model.ErrProviderStateInvalid)
		}
		states = append(states, model.ProviderMessageState{
			MessageIndex: indexes[0], MessageID: string(owner), SourceSessionID: string(sessionRecord.ID), SourceRunID: string(message.RunID),
			ProviderID: run.ProviderID, SourceModelID: message.ModelID, CodecID: contract.CodecID, Version: contract.Version,
			CompatibilityKey: contract.CompatibilityKey, Items: items,
		})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].MessageIndex < states[j].MessageIndex })
	return projection.Messages, states, nil
}

func captureAssistantProviderState(snapshot TurnSnapshot, messageID session.MessageID, message *einoschema.Message) (capturedProviderState, error) {
	streamer, stateful := snapshot.Model.Streamer.(model.ProviderStateStreamer)
	if !stateful || streamer == nil {
		if message != nil && len(message.Extra) != 0 {
			return capturedProviderState{}, model.Error{Code: "provider_state_unregistered", Message: "provider state codec is not registered", Cause: errors.Join(model.ErrProviderState, model.ErrProviderStateMismatch)}
		}
		return capturedProviderState{}, nil
	}
	capture, err := streamer.CaptureProviderState(message)
	if err != nil {
		return capturedProviderState{}, collapseProviderStateError(err)
	}
	if len(capture.Items) == 0 {
		return capturedProviderState{}, nil
	}
	contract := streamer.ProviderStateContract()
	result := capturedProviderState{payloads: make([]json.RawMessage, len(capture.Items))}
	storedBytes := 0
	for index, item := range capture.Items {
		payload, err := session.EncodeProviderStatePayload(session.ProviderStateEnvelope{
			CodecID: contract.CodecID, Version: contract.Version, ProviderID: string(snapshot.Model.Provider.ID),
			SourceModelID: string(snapshot.Model.Model.ID), CompatibilityKey: contract.CompatibilityKey,
			ItemIndex: index, Data: item.Data,
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
		Items: capture.Items,
	}
	cloned, err := (model.Request{ProviderState: []model.ProviderMessageState{state}}).Clone()
	if err != nil {
		return capturedProviderState{}, runtimeProviderStateError(model.ErrProviderStateInvalid)
	}
	result.state = &cloned.ProviderState[0]
	return result, nil
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
