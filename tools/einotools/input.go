package einotools

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/mattsp1290/eino-tools/catalog"

	"github.com/mattsp1290/eino-agent/internal/jsonobject"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func normalizeCatalogInput(id string, raw json.RawMessage) (json.RawMessage, error) {
	object, err := decodeJSONObject(raw)
	if err != nil {
		return nil, err
	}
	policy, err := inputPolicyFor(id)
	if err != nil {
		return nil, err
	}
	if policy.path != noPath {
		value, exists, err := stringField(object, "path")
		if err != nil {
			return nil, err
		}
		if !exists || value == "" {
			switch policy.path {
			case defaultPath:
				value, exists = ".", true
			case optionalPath:
				delete(object, "path")
				exists = false
			case requiredPath:
				return nil, fmt.Errorf("%w: path is required", agenttools.ErrMalformedInput)
			}
		}
		if exists {
			clean, err := cleanRelativePath(value)
			if err != nil {
				return nil, err
			}
			object["path"], _ = json.Marshal(clean)
		}
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func permissionPattern(id string, raw json.RawMessage) (string, error) {
	object, err := decodeJSONObject(raw)
	if err != nil {
		return "", err
	}
	policy, err := inputPolicyFor(id)
	if err != nil {
		return "", err
	}
	if policy.constant != "" {
		return policy.constant, nil
	}
	field := policy.field
	if policy.path == optionalPath {
		if _, exists := object["path"]; exists {
			field = "path"
		}
	}
	pattern, exists, err := stringField(object, field)
	if err != nil {
		return "", err
	}
	if !exists || pattern == "" {
		return "", fmt.Errorf("%w: %s is required", agenttools.ErrMalformedInput, field)
	}
	if len(pattern) > maxPermissionPatternBytes {
		return "", fmt.Errorf("%w: permission pattern exceeds %d bytes", agenttools.ErrMalformedInput, maxPermissionPatternBytes)
	}
	return pattern, nil
}

type pathPolicy uint8

const (
	noPath pathPolicy = iota
	requiredPath
	optionalPath
	defaultPath
)

type inputPolicy struct {
	field    string
	constant string
	path     pathPolicy
}

var catalogInputPolicies = map[string]inputPolicy{
	catalog.IDFileRead:     {field: "path", path: requiredPath},
	catalog.IDFileWrite:    {field: "path", path: requiredPath},
	catalog.IDFileEdit:     {field: "path", path: requiredPath},
	catalog.IDFileList:     {field: "path", path: defaultPath},
	catalog.IDGlob:         {field: "pattern", path: optionalPath},
	catalog.IDSearch:       {field: "pattern", path: optionalPath},
	catalog.IDApplyPatch:   {constant: "apply_patch"},
	catalog.IDShell:        {field: "cmd"},
	catalog.IDURLFetch:     {field: "url"},
	catalog.IDUserInteract: {constant: "user_interact"},
	catalog.IDTrackerWrite: {field: "id"},
}

func inputPolicyFor(id string) (inputPolicy, error) {
	policy, exists := catalogInputPolicies[id]
	if !exists {
		return inputPolicy{}, fmt.Errorf("%w: unsupported catalog ID %q", agenttools.ErrInvalidDefinition, id)
	}
	return policy, nil
}

func cleanRelativePath(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("%w: path must be workspace-relative", agenttools.ErrMalformedInput)
	}
	clean := path.Clean(value)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: path escapes workspace", agenttools.ErrMalformedInput)
	}
	return clean, nil
}

func stringField(object map[string]json.RawMessage, name string) (string, bool, error) {
	raw, exists := object[name]
	if !exists {
		return "", false, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", true, fmt.Errorf("%w: %s must be a string", agenttools.ErrMalformedInput, name)
	}
	return value, true, nil
}

func decodeJSONObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	object, err := jsonobject.Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", agenttools.ErrMalformedInput, err)
	}
	return object, nil
}

func standardMetadata(input map[string]string) (map[string]string, error) {
	metadata := cloneStringMap(input)
	if source, exists := metadata["source"]; exists && source != metadataSource {
		return nil, fmt.Errorf("%w: metadata source must be %q", agenttools.ErrInvalidDefinition, metadataSource)
	}
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["source"] = metadataSource
	return metadata, nil
}

func cloneRaw(raw json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), raw...) }

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
