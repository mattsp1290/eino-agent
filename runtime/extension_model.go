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
	value.Audited.DeferredTools = append([]AuditedToolSchema(nil), value.Audited.DeferredTools...)
	for index := range value.Audited.DeferredTools {
		value.Audited.DeferredTools[index].Schema = cloneJSON(value.Audited.DeferredTools[index].Schema)
	}
	if value.Audited.ToolSearchTool != nil {
		cloned := *value.Audited.ToolSearchTool
		cloned.Schema = cloneJSON(cloned.Schema)
		value.Audited.ToolSearchTool = &cloned
	}
	value.Audited.ToolChoice = cloneJSON(value.Audited.ToolChoice)
	value.Audited.Controls.Stop = cloneSlice(value.Audited.Controls.Stop)
	value.Audited.SafeCallConfig = cloneStringMap(value.Audited.SafeCallConfig)
	return value, nil
}

func validateStreamReader(reader *einoschema.StreamReader[model.StreamDelta]) error {
	if reader == nil {
		return errors.New("nil provider stream")
	}
	return nil
}

// cloneProtectedMessages deep-clones agentic messages via the same rules
// model.Request.Clone enforces (rejects StreamingMeta and Extra), so
// extension-visible context snapshots can never alias caller-owned or
// runtime-owned message state.
func cloneProtectedMessages(messages []*einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
	request, err := (model.Request{Messages: messages}).Clone()
	if err != nil {
		return nil, err
	}
	return request.Messages, nil
}

func cloneMessageDeep(message *einoschema.AgenticMessage) (*einoschema.AgenticMessage, error) {
	messages, err := cloneProtectedMessages([]*einoschema.AgenticMessage{message})
	if err != nil {
		return nil, err
	}
	return messages[0], nil
}
