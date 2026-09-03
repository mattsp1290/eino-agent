package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	einoschema "github.com/cloudwego/eino/schema"
)

const (
	HardProviderStateMaxItems              = 64
	HardProviderStateMaxItemBytes          = 10 * 1024 * 1024
	HardProviderStateMaxMessageBytes       = 16 * 1024 * 1024
	HardProviderStateMaxEnvelopeBytes      = 13_985_112
	HardProviderStateMaxStoredMessageBytes = 22_632_024
	MaxProviderStateCodecIDBytes           = 128
	MaxProviderStateCompatibilityKeyBytes  = 256
	MaxProviderStateExtraKeyBytes          = 256
	MaxProviderStateProviderIDBytes        = 128
	MaxProviderStateModelIDBytes           = 256
)

var (
	ErrProviderState         = errors.New("provider state error")
	ErrProviderStateInvalid  = errors.New("provider state invalid")
	ErrProviderStateTooLarge = errors.New("provider state too large")
	ErrProviderStateMismatch = errors.New("provider state mismatch")
	ErrProviderStateVersion  = errors.New("provider state version unsupported")
)

// ProviderStateLimits bounds one provider-private assistant message.
type ProviderStateLimits struct {
	MaxItems              int
	MaxItemBytes          int
	MaxMessageBytes       int
	MaxEnvelopeBytes      int
	MaxStoredMessageBytes int
}

// ProviderStateContract identifies one immutable provider-state wire format.
type ProviderStateContract struct {
	CodecID          string
	Version          int
	CompatibilityKey string
	Limits           ProviderStateLimits
}

// ProviderStateItem owns one opaque JSON object exactly as emitted by a provider.
type ProviderStateItem struct {
	Data json.RawMessage
}

// ProviderMessageState binds ordered provider items to one durable assistant message.
type ProviderMessageState struct {
	MessageIndex     int
	MessageID        string
	SourceSessionID  string
	SourceRunID      string
	ProviderID       string
	SourceModelID    string
	CodecID          string
	Version          int
	CompatibilityKey string
	Items            []ProviderStateItem
}

// ProviderStateCapture is a validated codec capture plus the complete set of
// top-level Extra keys consumed by that capture.
type ProviderStateCapture struct {
	Items       []ProviderStateItem
	ClaimedKeys []string
}

// ProviderStateCodec captures and restores one provider's private assistant state.
type ProviderStateCodec interface {
	Contract() ProviderStateContract
	OwnedExtraKeys() []string
	CaptureAssistant(*einoschema.Message) (ProviderStateCapture, error)
	RestoreAssistant(*einoschema.Message, []ProviderStateItem) error
}

// ProviderStateStreamer is a provider boundary that can safely capture and
// privately restore provider state.
type ProviderStateStreamer interface {
	Streamer
	ProviderStateContract() ProviderStateContract
	ProviderStateOwnedExtraKeys() []string
	CaptureProviderState(*einoschema.Message) (ProviderStateCapture, error)
}

// ValidateProviderStateContract validates a codec contract against the core ceilings.
func ValidateProviderStateContract(contract ProviderStateContract) error {
	if !validASCIIToken(contract.CodecID, MaxProviderStateCodecIDBytes) ||
		contract.Version <= 0 ||
		contract.CompatibilityKey == "" ||
		!utf8.ValidString(contract.CompatibilityKey) ||
		len(contract.CompatibilityKey) > MaxProviderStateCompatibilityKeyBytes {
		return providerStateError(ErrProviderStateInvalid)
	}
	limits := contract.Limits
	if limits.MaxItems <= 0 || limits.MaxItems > HardProviderStateMaxItems ||
		limits.MaxItemBytes <= 0 || limits.MaxItemBytes > HardProviderStateMaxItemBytes ||
		limits.MaxMessageBytes <= 0 || limits.MaxMessageBytes > HardProviderStateMaxMessageBytes ||
		limits.MaxEnvelopeBytes <= 0 || limits.MaxEnvelopeBytes > HardProviderStateMaxEnvelopeBytes ||
		limits.MaxStoredMessageBytes <= 0 || limits.MaxStoredMessageBytes > HardProviderStateMaxStoredMessageBytes {
		return providerStateError(ErrProviderStateInvalid)
	}
	return nil
}

// ValidateProviderStateIdentity validates provider/model identity fields shared
// by requests and durable envelopes.
func ValidateProviderStateIdentity(providerID, modelID string) error {
	if !validASCIIToken(providerID, MaxProviderStateProviderIDBytes) ||
		!validASCIIToken(modelID, MaxProviderStateModelIDBytes) {
		return providerStateError(ErrProviderStateInvalid)
	}
	return nil
}

// ValidateProviderStateItems validates and counts raw items without normalizing them.
func ValidateProviderStateItems(items []ProviderStateItem, limits ProviderStateLimits) error {
	if len(items) == 0 {
		return providerStateError(ErrProviderStateInvalid)
	}
	if len(items) > limits.MaxItems || len(items) > HardProviderStateMaxItems {
		return providerStateError(ErrProviderStateTooLarge)
	}
	total := 0
	for _, item := range items {
		if len(item.Data) > limits.MaxItemBytes || len(item.Data) > HardProviderStateMaxItemBytes {
			return providerStateError(ErrProviderStateTooLarge)
		}
		total += len(item.Data)
		if total > limits.MaxMessageBytes || total > HardProviderStateMaxMessageBytes {
			return providerStateError(ErrProviderStateTooLarge)
		}
		if !isJSONObject(item.Data) {
			return providerStateError(ErrProviderStateInvalid)
		}
	}
	return nil
}

func cloneProviderState(src []ProviderMessageState) []ProviderMessageState {
	if src == nil {
		return nil
	}
	dst := make([]ProviderMessageState, len(src))
	for i := range src {
		dst[i] = src[i]
		dst[i].Items = cloneProviderStateItems(src[i].Items)
	}
	return dst
}

func cloneProviderStateItems(src []ProviderStateItem) []ProviderStateItem {
	if src == nil {
		return nil
	}
	dst := make([]ProviderStateItem, len(src))
	for i := range src {
		dst[i].Data = append(json.RawMessage(nil), src[i].Data...)
	}
	return dst
}

func cloneProviderStateContract(contract ProviderStateContract) ProviderStateContract {
	return contract
}

func providerStateError(kind error) error {
	switch kind {
	case ErrProviderStateTooLarge:
		return Error{Code: "provider_state_too_large", Message: "provider state exceeds configured limits", Cause: errors.Join(ErrProviderState, kind)}
	case ErrProviderStateMismatch:
		return Error{Code: "provider_state_mismatch", Message: "provider state does not match the active model", Cause: errors.Join(ErrProviderState, kind)}
	case ErrProviderStateVersion:
		return Error{Code: "provider_state_version", Message: "provider state version is unsupported", Cause: errors.Join(ErrProviderState, kind)}
	default:
		return Error{Code: "provider_state_invalid", Message: "provider state is invalid", Cause: errors.Join(ErrProviderState, ErrProviderStateInvalid)}
	}
}

func isJSONObject(raw json.RawMessage) bool {
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

func validASCIIToken(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for i := 0; i < len(value); i++ {
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
