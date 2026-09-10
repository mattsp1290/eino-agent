package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

const (
	maxAdmissionMessageBytes = 256 << 10
	maxAdmissionPayloadBytes = 1 << 20
)

type admissionSelection struct {
	ProviderID string `json:"provider_id"`
	ModelID    string `json:"model_id"`
	Variant    string `json:"variant"`
}
type admissionAgent struct {
	Name         string             `json:"name"`
	SystemPrompt string             `json:"system_prompt"`
	Mode         string             `json:"mode"`
	Model        admissionSelection `json:"model"`
	Options      map[string]string  `json:"options"`
}
type admissionPermission struct {
	Permission string `json:"permission"`
	Pattern    string `json:"pattern"`
	Action     string `json:"action"`
}
type admissionTools struct {
	Enabled     []string              `json:"enabled"`
	Disabled    []string              `json:"disabled"`
	Permissions []admissionPermission `json:"permissions"`
}
type admissionFingerprintPayload struct {
	Message         string             `json:"message"`
	Agent           admissionAgent     `json:"agent"`
	Model           admissionSelection `json:"model"`
	Tools           admissionTools     `json:"tools"`
	ConfigMetadata  map[string]string  `json:"config_metadata"`
	RequestMetadata map[string]string  `json:"request_metadata"`
}

func fingerprintSelection(value model.Selection) admissionSelection {
	return admissionSelection{ProviderID: string(value.ProviderID), ModelID: string(value.ModelID), Variant: value.Variant}
}

func emptyMap(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, entry := range value {
		result[key] = entry
	}
	return result
}

func frozenRequest(request Request) Request {
	request.Config = request.Config.Clone()
	request.Metadata = cloneStringMap(request.Metadata)
	return request
}

func emptySlice(value []string) []string { return append([]string{}, value...) }

func fingerprintAdmission(request Request) ([32]byte, error) {
	if err := session.ValidateAdmissionKey(request.AdmissionKey); err != nil {
		return [32]byte{}, err
	}
	if len(request.Message.Content) > maxAdmissionMessageBytes || !utf8.ValidString(request.Message.Content) {
		return [32]byte{}, session.ErrAdmissionInvalid
	}
	payload := admissionFingerprintPayload{
		Message:        request.Message.Content,
		Agent:          admissionAgent{Name: request.Config.Agent.Name, SystemPrompt: request.Config.Agent.SystemPrompt, Mode: request.Config.Agent.Mode, Model: fingerprintSelection(request.Config.Agent.Model), Options: emptyMap(request.Config.Agent.Options)},
		Model:          fingerprintSelection(request.Config.Model),
		Tools:          admissionTools{Enabled: emptySlice(request.Config.Tools.Enabled), Disabled: emptySlice(request.Config.Tools.Disabled), Permissions: []admissionPermission{}},
		ConfigMetadata: emptyMap(request.Config.Metadata), RequestMetadata: emptyMap(request.Metadata),
	}
	for _, rule := range request.Config.Tools.Permissions {
		payload.Tools.Permissions = append(payload.Tools.Permissions, admissionPermission{Permission: rule.Permission, Pattern: rule.Pattern, Action: rule.Action})
	}
	if !validAdmissionPayload(payload) {
		return [32]byte{}, session.ErrAdmissionInvalid
	}
	encoded, err := json.Marshal(payload)
	if err != nil || len(encoded) > maxAdmissionPayloadBytes {
		return [32]byte{}, session.ErrAdmissionInvalid
	}
	return sha256.Sum256(append([]byte("eino-agent-admission-v1\x00"), encoded...)), nil
}

func validAdmissionPayload(payload admissionFingerprintPayload) bool {
	values := []string{payload.Message, payload.Agent.Name, payload.Agent.SystemPrompt, payload.Agent.Mode, payload.Agent.Model.ProviderID, payload.Agent.Model.ModelID, payload.Agent.Model.Variant, payload.Model.ProviderID, payload.Model.ModelID, payload.Model.Variant}
	for _, item := range values {
		if !utf8.ValidString(item) {
			return false
		}
	}
	for _, values := range []map[string]string{payload.Agent.Options, payload.ConfigMetadata, payload.RequestMetadata} {
		for key, value := range values {
			if !utf8.ValidString(key) || !utf8.ValidString(value) {
				return false
			}
		}
	}
	for _, values := range [][]string{payload.Tools.Enabled, payload.Tools.Disabled} {
		for _, value := range values {
			if !utf8.ValidString(value) {
				return false
			}
		}
	}
	for _, rule := range payload.Tools.Permissions {
		if !utf8.ValidString(rule.Permission) || !utf8.ValidString(rule.Pattern) || !utf8.ValidString(rule.Action) {
			return false
		}
	}
	return true
}

func validateKeyedWorkspace(request Request) error {
	root := request.Config.Metadata["workspace_root"]
	if root == "" {
		return nil
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return fmt.Errorf("%w: workspace root", session.ErrAdmissionInvalid)
	}
	return nil
}
