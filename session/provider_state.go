package session

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const (
	ProviderStateHardMaxItems              = 64
	ProviderStateHardMaxItemBytes          = 10 * 1024 * 1024
	ProviderStateHardMaxMessageBytes       = 16 * 1024 * 1024
	ProviderStateHardMaxEnvelopeBytes      = 13_985_112
	ProviderStateHardMaxStoredMessageBytes = 22_632_024
	ProviderStateMaxCodecIDBytes           = 128
	ProviderStateMaxCompatibilityKeyBytes  = 256
	ProviderStateMaxProviderIDBytes        = 128
	ProviderStateMaxModelIDBytes           = 256
)

// ErrProviderStateInvalid reports malformed or non-canonical durable state.
// Its text is deliberately content-free because store bytes are untrusted.
var ErrProviderStateInvalid = errors.New("durable provider state is invalid")

// ProviderStateEnvelope is the decoded byte-owning form of one durable state item.
type ProviderStateEnvelope struct {
	CodecID          string
	Version          int
	ProviderID       string
	SourceModelID    string
	CompatibilityKey string
	ItemIndex        int
	Data             json.RawMessage
}

type providerStatePayload struct {
	CodecID          string `json:"codec_id"`
	Version          int    `json:"version"`
	ProviderID       string `json:"provider_id"`
	SourceModelID    string `json:"source_model_id"`
	CompatibilityKey string `json:"compatibility_key"`
	ItemIndex        int    `json:"item_index"`
	DataBase64       string `json:"data_base64"`
}

// EncodeProviderStatePayload validates and canonically encodes one exact raw item.
func EncodeProviderStatePayload(envelope ProviderStateEnvelope) (json.RawMessage, error) {
	if err := validateProviderStateEnvelope(envelope); err != nil {
		return nil, err
	}
	payload := providerStatePayload{
		CodecID: envelope.CodecID, Version: envelope.Version,
		ProviderID: envelope.ProviderID, SourceModelID: envelope.SourceModelID,
		CompatibilityKey: envelope.CompatibilityKey, ItemIndex: envelope.ItemIndex,
		DataBase64: base64.StdEncoding.EncodeToString(envelope.Data),
	}
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > ProviderStateHardMaxEnvelopeBytes {
		return nil, ErrProviderStateInvalid
	}
	return raw, nil
}

// DecodeProviderStatePayload strictly decodes one canonical state envelope.
func DecodeProviderStatePayload(raw json.RawMessage) (ProviderStateEnvelope, error) {
	if len(raw) == 0 || len(raw) > ProviderStateHardMaxEnvelopeBytes {
		return ProviderStateEnvelope{}, ErrProviderStateInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var payload providerStatePayload
	if err := decoder.Decode(&payload); err != nil {
		return ProviderStateEnvelope{}, ErrProviderStateInvalid
	}
	if !errors.Is(decoder.Decode(&struct{}{}), io.EOF) {
		return ProviderStateEnvelope{}, ErrProviderStateInvalid
	}
	canonical, err := json.Marshal(payload)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ProviderStateEnvelope{}, ErrProviderStateInvalid
	}
	data, err := base64.StdEncoding.Strict().DecodeString(payload.DataBase64)
	if err != nil || base64.StdEncoding.EncodeToString(data) != payload.DataBase64 {
		return ProviderStateEnvelope{}, ErrProviderStateInvalid
	}
	envelope := ProviderStateEnvelope{
		CodecID: payload.CodecID, Version: payload.Version,
		ProviderID: payload.ProviderID, SourceModelID: payload.SourceModelID,
		CompatibilityKey: payload.CompatibilityKey, ItemIndex: payload.ItemIndex,
		Data: append(json.RawMessage(nil), data...),
	}
	if err := validateProviderStateEnvelope(envelope); err != nil {
		return ProviderStateEnvelope{}, err
	}
	return envelope, nil
}

func validateProviderStateEnvelope(envelope ProviderStateEnvelope) error {
	if !providerStateASCIIToken(envelope.CodecID, ProviderStateMaxCodecIDBytes) ||
		envelope.Version <= 0 ||
		!providerStateASCIIToken(envelope.ProviderID, ProviderStateMaxProviderIDBytes) ||
		!providerStateASCIIToken(envelope.SourceModelID, ProviderStateMaxModelIDBytes) ||
		envelope.CompatibilityKey == "" || !utf8.ValidString(envelope.CompatibilityKey) ||
		len(envelope.CompatibilityKey) > ProviderStateMaxCompatibilityKeyBytes ||
		envelope.ItemIndex < 0 || envelope.ItemIndex >= ProviderStateHardMaxItems ||
		len(envelope.Data) > ProviderStateHardMaxItemBytes || !providerStateJSONObject(envelope.Data) {
		return ErrProviderStateInvalid
	}
	return nil
}

func providerStateJSONObject(raw json.RawMessage) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var value any
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	if _, ok := value.(map[string]any); !ok || value == nil {
		return false
	}
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}

func providerStateASCIIToken(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for i := range len(value) {
		if !providerStateTokenByte(value[i]) {
			return false
		}
	}
	return true
}

func providerStateTokenByte(value byte) bool {
	if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' {
		return true
	}
	return bytes.ContainsRune([]byte("!#$%&'*+-.^_`|~:/@"), rune(value))
}
