// Package jsonobject validates and decodes JSON objects without accepting
// duplicate top-level keys.
package jsonobject

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Decode returns a JSON object's fields after enforcing an unambiguous
// top-level key set. Nested values remain raw and are validated by encoding/json.
func Decode(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, errors.New("invalid JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := first.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("JSON value must be a non-null object")
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("invalid JSON object key")
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate top-level key %q", key)
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("JSON value must be a non-null object")
	}
	return object, nil
}
