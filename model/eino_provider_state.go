package model

import (
	"encoding/json"
	"errors"
	"unicode/utf8"

	einoschema "github.com/cloudwego/eino/schema"
)

// EinoJSONExtraStateConfig configures a strict one-key []json.RawMessage codec.
type EinoJSONExtraStateConfig struct {
	ExtraKey string
	Contract ProviderStateContract
}

type einoJSONExtraStateCodec struct {
	extraKey string
	contract ProviderStateContract
}

// NewEinoJSONExtraStateCodec constructs a strict byte-preserving Eino Extra codec.
func NewEinoJSONExtraStateCodec(config EinoJSONExtraStateConfig) (ProviderStateCodec, error) {
	if config.ExtraKey == "" || len(config.ExtraKey) > MaxProviderStateExtraKeyBytes || !utf8.ValidString(config.ExtraKey) {
		return nil, providerStateError(ErrProviderStateInvalid)
	}
	if err := ValidateProviderStateContract(config.Contract); err != nil {
		return nil, err
	}
	return &einoJSONExtraStateCodec{extraKey: config.ExtraKey, contract: cloneProviderStateContract(config.Contract)}, nil
}

func (c *einoJSONExtraStateCodec) Contract() ProviderStateContract {
	return cloneProviderStateContract(c.contract)
}

func (c *einoJSONExtraStateCodec) OwnedExtraKeys() []string {
	return []string{c.extraKey}
}

func (c *einoJSONExtraStateCodec) CaptureAssistant(message *einoschema.Message) (ProviderStateCapture, error) {
	if message == nil || message.Role != einoschema.Assistant {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	if len(message.Extra) == 0 {
		return ProviderStateCapture{}, nil
	}
	if len(message.Extra) != 1 {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	value, ok := message.Extra[c.extraKey]
	if !ok {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	rawItems, ok := value.([]json.RawMessage)
	if !ok || rawItems == nil {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	items := make([]ProviderStateItem, len(rawItems))
	for i := range rawItems {
		items[i].Data = append(json.RawMessage(nil), rawItems[i]...)
	}
	if err := ValidateProviderStateItems(items, c.contract.Limits); err != nil {
		return ProviderStateCapture{}, err
	}
	return ProviderStateCapture{Items: items, ClaimedKeys: []string{c.extraKey}}, nil
}

func (c *einoJSONExtraStateCodec) RestoreAssistant(message *einoschema.Message, items []ProviderStateItem) error {
	if message == nil || message.Role != einoschema.Assistant || len(message.Extra) != 0 {
		return providerStateError(ErrProviderStateInvalid)
	}
	if err := ValidateProviderStateItems(items, c.contract.Limits); err != nil {
		return err
	}
	rawItems := make([]json.RawMessage, len(items))
	for i := range items {
		rawItems[i] = append(json.RawMessage(nil), items[i].Data...)
	}
	message.Extra = map[string]any{c.extraKey: rawItems}
	return nil
}

func guardedCapture(codec ProviderStateCodec, owned map[string]struct{}, contract ProviderStateContract, message *einoschema.Message) (capture ProviderStateCapture, err error) {
	defer func() {
		if recover() != nil {
			capture = ProviderStateCapture{}
			err = providerStateError(ErrProviderStateInvalid)
		}
	}()
	if message == nil || message.Role != einoschema.Assistant {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	keys := make([]string, 0, len(message.Extra))
	for key := range message.Extra {
		if _, ok := owned[key]; !ok {
			return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
		}
		keys = append(keys, key)
	}
	capture, err = codec.CaptureAssistant(message)
	if err != nil {
		return ProviderStateCapture{}, providerStateErrorFrom(err)
	}
	claimed := make(map[string]struct{}, len(capture.ClaimedKeys))
	for _, key := range capture.ClaimedKeys {
		if _, duplicate := claimed[key]; duplicate {
			return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
		}
		if _, ok := owned[key]; !ok {
			return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
		}
		claimed[key] = struct{}{}
	}
	if len(claimed) != len(keys) {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	for _, key := range keys {
		if _, ok := claimed[key]; !ok {
			return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
		}
	}
	if len(keys) == 0 {
		if len(capture.Items) != 0 || len(capture.ClaimedKeys) != 0 {
			return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
		}
		return ProviderStateCapture{}, nil
	}
	if err := ValidateProviderStateItems(capture.Items, contract.Limits); err != nil {
		return ProviderStateCapture{}, err
	}
	for key := range claimed {
		delete(message.Extra, key)
	}
	if len(message.Extra) == 0 {
		message.Extra = nil
	}
	if _, err := cloneMessages([]*einoschema.Message{message}); err != nil {
		return ProviderStateCapture{}, providerStateError(ErrProviderStateInvalid)
	}
	capture.Items = cloneProviderStateItems(capture.Items)
	capture.ClaimedKeys = append([]string(nil), capture.ClaimedKeys...)
	return capture, nil
}

func validateOwnedKeys(keys []string) (map[string]struct{}, []string, error) {
	if len(keys) == 0 {
		return nil, nil, providerStateError(ErrProviderStateInvalid)
	}
	owned := make(map[string]struct{}, len(keys))
	copyKeys := make([]string, len(keys))
	for i, key := range keys {
		if key == "" || len(key) > MaxProviderStateExtraKeyBytes || !utf8.ValidString(key) {
			return nil, nil, providerStateError(ErrProviderStateInvalid)
		}
		if _, duplicate := owned[key]; duplicate {
			return nil, nil, providerStateError(ErrProviderStateInvalid)
		}
		owned[key] = struct{}{}
		copyKeys[i] = key
	}
	return owned, copyKeys, nil
}

func snapshotProviderStateCodec(codec ProviderStateCodec) (contract ProviderStateContract, owned map[string]struct{}, ownedKeys []string, err error) {
	defer func() {
		if recover() != nil {
			contract = ProviderStateContract{}
			owned, ownedKeys = nil, nil
			err = providerStateError(ErrProviderStateInvalid)
		}
	}()
	contract = codec.Contract()
	if err = ValidateProviderStateContract(contract); err != nil {
		return ProviderStateContract{}, nil, nil, err
	}
	owned, ownedKeys, err = validateOwnedKeys(codec.OwnedExtraKeys())
	return contract, owned, ownedKeys, err
}

func providerStateErrorFrom(err error) error {
	switch {
	case err == nil:
		return nil
	case errorsIs(err, ErrProviderStateTooLarge):
		return providerStateError(ErrProviderStateTooLarge)
	case errorsIs(err, ErrProviderStateMismatch):
		return providerStateError(ErrProviderStateMismatch)
	case errorsIs(err, ErrProviderStateVersion):
		return providerStateError(ErrProviderStateVersion)
	default:
		return providerStateError(ErrProviderStateInvalid)
	}
}

// errorsIs is kept local so codec failures are always collapsed to fixed text.
func errorsIs(err, target error) bool {
	return errors.Is(err, target)
}
