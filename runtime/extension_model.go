package runtime

import (
	"errors"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

// ModelStreamInput is the data-only canonical request view exposed to model
// stream interceptors. Provider clients, observers, and request callables are
// deliberately absent.
type ModelStreamInput struct {
	ProviderID  string
	ModelID     string
	Audited     AuditedModelInput
	ContentHash string
}

func cloneModelStreamInput(value ModelStreamInput) (ModelStreamInput, error) {
	value.Audited.Messages = append([]AuditedMessage(nil), value.Audited.Messages...)
	for index := range value.Audited.Messages {
		value.Audited.Messages[index].Canonical = cloneJSON(value.Audited.Messages[index].Canonical)
	}
	value.Audited.Tools = append([]AuditedToolSchema(nil), value.Audited.Tools...)
	for index := range value.Audited.Tools {
		value.Audited.Tools[index].Schema = cloneJSON(value.Audited.Tools[index].Schema)
	}
	value.Audited.SafeCallConfig = cloneStringMap(value.Audited.SafeCallConfig)
	return value, nil
}

func validateStreamReader(reader *einoschema.StreamReader[model.StreamDelta]) error {
	if reader == nil {
		return errors.New("nil provider stream")
	}
	return nil
}

func cloneProtectedMessages(messages []*einoschema.Message) ([]*einoschema.Message, error) {
	request, err := (model.Request{Messages: messages}).Clone()
	if err != nil {
		return nil, err
	}
	return request.Messages, nil
}

func cloneMessageDeep(message *einoschema.Message) (*einoschema.Message, error) {
	messages, err := cloneProtectedMessages([]*einoschema.Message{message})
	if err != nil {
		return nil, err
	}
	return messages[0], nil
}
