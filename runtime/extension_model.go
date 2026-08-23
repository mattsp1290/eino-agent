package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
)

type ModelStreamInput struct {
	Resolved model.Resolved
	Request  model.Request
}

func modelRequestContentHash(request model.Request) string {
	raw, _ := json.Marshal(struct {
		Messages any
		System   string
		Tools    any
	}{Messages: request.Messages, System: request.System, Tools: request.Tools})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func cloneModelStreamInput(value ModelStreamInput) ModelStreamInput {
	value.Request = value.Request.Clone()
	value.Resolved.Provider.Options = cloneStringMap(value.Resolved.Provider.Options)
	value.Resolved.Provider.Environment = cloneSlice(value.Resolved.Provider.Environment)
	value.Resolved.Model.Options = cloneStringMap(value.Resolved.Model.Options)
	if value.Resolved.Model.Capabilities != nil {
		capabilities := make(map[string]bool, len(value.Resolved.Model.Capabilities))
		for key, enabled := range value.Resolved.Model.Capabilities {
			capabilities[key] = enabled
		}
		value.Resolved.Model.Capabilities = capabilities
	}
	return value
}

func validateModelStreamInput(original, candidate ModelStreamInput) error {
	if original.Resolved.Client != nil || original.Resolved.Streamer != nil || original.Request.Observer != nil ||
		candidate.Resolved.Client != nil || candidate.Resolved.Streamer != nil || candidate.Request.Observer != nil ||
		!reflect.DeepEqual(original, candidate) {
		return extension.ErrProtectedMutation
	}
	return nil
}

func validateStreamReader(reader *einoschema.StreamReader[*einoschema.Message]) error {
	if reader == nil {
		return errors.New("nil provider stream")
	}
	return nil
}

func validateDelegatedStreamReader(delegated, returned *einoschema.StreamReader[*einoschema.Message]) error {
	if delegated != returned {
		return extension.ErrProtectedMutation
	}
	return nil
}

func extensionModelStreamInput(value ModelStreamInput) ModelStreamInput {
	value = cloneModelStreamInput(value)
	value.Resolved.Client = nil
	value.Resolved.Streamer = nil
	value.Request.Observer = nil
	return value
}

func cloneMessageDeep(message *einoschema.Message) *einoschema.Message {
	if message == nil {
		return nil
	}
	raw, err := json.Marshal(message)
	if err != nil {
		return nil
	}
	var clone einoschema.Message
	if json.Unmarshal(raw, &clone) != nil {
		return nil
	}
	return &clone
}

func cloneProtectedMessages(messages []*einoschema.Message) []*einoschema.Message {
	if messages == nil {
		return nil
	}
	cloned := make([]*einoschema.Message, len(messages))
	for index, message := range messages {
		cloned[index] = cloneMessageDeep(message)
	}
	return cloned
}
