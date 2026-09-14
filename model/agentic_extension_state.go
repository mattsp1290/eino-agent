package model

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// AgenticResponseMetaExtensionState is the deliberately small contract a
// provider may implement when its otherwise-private response metadata needs
// durable agent state.  It is structural so model remains provider-neutral.
type AgenticResponseMetaExtensionState interface {
	EinoAgentResponseMetaState() (json.RawMessage, error)
}

const (
	agenticResponseMetaStateKind    = "eino-agent.response-meta-identity"
	agenticResponseMetaStateVersion = 1
)

const responseMetaStateMarkerPrefix = "eino-agent-response-meta-state:"

// responseMetaStateMarker uses string deliberately: Eino concatenates
// repeated stream metadata through its built-in string concat path. The
// payload is base64, so Capture can recover one repeated envelope without
// making an unregistered provider-specific Go type part of concatenation.
func responseMetaStateMarker(raw json.RawMessage) string {
	return responseMetaStateMarkerPrefix + base64.RawStdEncoding.EncodeToString(raw) + ";"
}

func responseMetaStateMarkerRaw(value string) (json.RawMessage, bool) {
	if len(value) < len(responseMetaStateMarkerPrefix) || value[:len(responseMetaStateMarkerPrefix)] != responseMetaStateMarkerPrefix {
		return nil, false
	}
	parts := strings.Split(value[len(responseMetaStateMarkerPrefix):], ";")
	if len(parts) < 2 || parts[len(parts)-1] != "" || parts[0] == "" {
		return nil, false
	}
	for _, part := range parts[1 : len(parts)-1] {
		if part != parts[0] {
			return nil, false
		}
	}
	raw, err := base64.RawStdEncoding.DecodeString(parts[0])
	return raw, err == nil
}

func responseMetaStateRawFromExtension(extension any) (json.RawMessage, bool) {
	switch marker := extension.(type) {
	case string:
		return responseMetaStateMarkerRaw(marker)
	case map[string]any:
		identity, ok := marker["identity"].(map[string]any)
		if !ok {
			return nil, false
		}
		provider, pOK := identity["provider"].(string)
		protocol, qOK := identity["protocol"].(string)
		if !pOK || !qOK {
			return nil, false
		}
		envelope := agenticResponseMetaStateEnvelope{Kind: agenticResponseMetaStateKind, Version: agenticResponseMetaStateVersion, Identity: agenticResponseMetaStateIdentity{Provider: provider, Protocol: protocol}}
		if v, ok := identity["requested_model"].(string); ok {
			envelope.Identity.RequestedModel = v
		}
		if v, ok := identity["returned_model"].(string); ok {
			envelope.Identity.ReturnedModel = v
		}
		if v, ok := identity["correlation_id"].(string); ok {
			envelope.Identity.CorrelationID = v
		}
		raw, err := json.Marshal(envelope)
		return raw, err == nil
	default:
		return nil, false
	}
}

type agenticResponseMetaStateEnvelope struct {
	Kind     string                           `json:"kind"`
	Version  int                              `json:"version"`
	Identity agenticResponseMetaStateIdentity `json:"identity"`
}

type agenticResponseMetaStateIdentity struct {
	Provider       string `json:"provider"`
	Protocol       string `json:"protocol"`
	RequestedModel string `json:"requested_model,omitempty"`
	ReturnedModel  string `json:"returned_model,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
}

func parseAgenticResponseMetaState(raw json.RawMessage) (agenticResponseMetaStateEnvelope, error) {
	var envelope agenticResponseMetaStateEnvelope
	if len(raw) == 0 || !json.Valid(raw) || !isSingleJSONObject(raw) {
		return envelope, fmt.Errorf("invalid response meta state")
	}
	var outer map[string]json.RawMessage
	if err := decodeUniqueObject(raw, &outer); err != nil {
		return envelope, err
	}
	if len(outer) != 3 || outer["kind"] == nil || outer["version"] == nil || outer["identity"] == nil {
		return envelope, fmt.Errorf("invalid response meta state envelope")
	}
	if err := json.Unmarshal(outer["kind"], &envelope.Kind); err != nil || envelope.Kind != agenticResponseMetaStateKind {
		return envelope, fmt.Errorf("invalid response meta state kind")
	}
	if err := json.Unmarshal(outer["version"], &envelope.Version); err != nil || envelope.Version != agenticResponseMetaStateVersion {
		return envelope, fmt.Errorf("invalid response meta state version")
	}
	var identity map[string]json.RawMessage
	if err := decodeUniqueObject(outer["identity"], &identity); err != nil {
		return envelope, err
	}
	if len(identity) < 2 || len(identity) > 5 || identity["provider"] == nil || identity["protocol"] == nil {
		return envelope, fmt.Errorf("invalid response meta state identity")
	}
	for key := range identity {
		switch key {
		case "provider", "protocol", "requested_model", "returned_model", "correlation_id":
		default:
			return envelope, fmt.Errorf("unknown response meta state identity field")
		}
	}
	if err := json.Unmarshal(outer["identity"], &envelope.Identity); err != nil || envelope.Identity.Provider == "" || envelope.Identity.Protocol == "" {
		return envelope, fmt.Errorf("invalid response meta state identity")
	}
	return envelope, nil
}

func isSingleJSONObject(raw []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	var value any
	if err := dec.Decode(&value); err != nil {
		return false
	}
	if _, ok := value.(map[string]any); !ok {
		return false
	}
	return dec.Decode(&struct{}{}) != nil
}

// decodeUniqueObject rejects duplicate keys before ordinary decoding erases
// them. It also makes trailing values invalid.
func decodeUniqueObject(raw []byte, destination *map[string]json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return fmt.Errorf("expected JSON object")
	}
	result := make(map[string]json.RawMessage)
	for dec.More() {
		token, err = dec.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return fmt.Errorf("invalid JSON object key")
		}
		if _, exists := result[key]; exists {
			return fmt.Errorf("duplicate JSON object key")
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
		result[key] = value
	}
	if token, err = dec.Token(); err != nil || token != json.Delim('}') {
		return fmt.Errorf("unterminated JSON object")
	}
	if dec.Decode(&struct{}{}) == nil {
		return fmt.Errorf("trailing JSON value")
	}
	*destination = result
	return nil
}
