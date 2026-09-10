package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

const defaultModelRequestMaxBytes = 4 << 20

type AuditedMessage struct {
	Canonical json.RawMessage `json:"canonical"`
}

type AuditedToolSchema struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
}

// AuditedControls carries the scalar generation controls of one audited
// model request. Every field is omitted from the canonical JSON when unset
// so requests that never touch a control hash identically to earlier
// requests, matching prior behavior for the fields that already existed.
type AuditedControls struct {
	Temperature *float32 `json:"temperature,omitempty"`
	TopP        *float32 `json:"top_p,omitempty"`
	MaxTokens   *int     `json:"max_tokens,omitempty"`
	Stop        []string `json:"stop,omitempty"`
}

type AuditedModelInput struct {
	Messages       []AuditedMessage    `json:"messages"`
	System         string              `json:"system"`
	Tools          []AuditedToolSchema `json:"tools"`
	DeferredTools  []AuditedToolSchema `json:"deferred_tools,omitempty"`
	ToolSearchTool *AuditedToolSchema  `json:"tool_search_tool,omitempty"`
	ToolChoice     json.RawMessage     `json:"tool_choice,omitempty"`
	Controls       AuditedControls     `json:"controls"`
	SafeCallConfig map[string]string   `json:"safe_call_config"`
}

// auditModelRequest takes canonical ownership of a request and derives the
// credential-free subset persisted by the request ledger. Every message in
// request.Messages is the request's public projection by construction (no
// provider-private material is ever attached to a model.Request message), so
// the canonical JSON below never carries signature/encrypted_index/sdk_blob/
// previous_response_id keys.
func auditModelRequest(request model.Request, safeOptionKeys []string, maxBytes int) (model.Request, AuditedModelInput, string, error) {
	canonicalRequest, err := request.Clone()
	if err != nil {
		return model.Request{}, AuditedModelInput{}, "", err
	}
	request = canonicalRequest
	if maxBytes <= 0 {
		maxBytes = defaultModelRequestMaxBytes
	}
	input := AuditedModelInput{System: request.System, SafeCallConfig: make(map[string]string)}
	for _, message := range request.Messages {
		if message == nil {
			return model.Request{}, AuditedModelInput{}, "", fmt.Errorf("nil model message")
		}
		raw, err := json.Marshal(message)
		if err != nil {
			return model.Request{}, AuditedModelInput{}, "", err
		}
		input.Messages = append(input.Messages, AuditedMessage{Canonical: raw})
	}
	input.Tools, err = auditToolSchemas(request.Controls.Tools)
	if err != nil {
		return model.Request{}, AuditedModelInput{}, "", err
	}
	input.DeferredTools, err = auditToolSchemas(request.Controls.DeferredTools)
	if err != nil {
		return model.Request{}, AuditedModelInput{}, "", err
	}
	if request.Controls.ToolSearchTool != nil {
		schemas, err := auditToolSchemas([]*einoschema.ToolInfo{request.Controls.ToolSearchTool})
		if err != nil {
			return model.Request{}, AuditedModelInput{}, "", err
		}
		input.ToolSearchTool = &schemas[0]
	}
	if request.Controls.ToolChoice != nil {
		raw, err := json.Marshal(request.Controls.ToolChoice)
		if err != nil {
			return model.Request{}, AuditedModelInput{}, "", err
		}
		input.ToolChoice = raw
	}
	input.Controls = AuditedControls{
		Temperature: request.Controls.Temperature,
		TopP:        request.Controls.TopP,
		MaxTokens:   request.Controls.MaxTokens,
		Stop:        request.Controls.Stop,
	}
	keys := append([]string(nil), safeOptionKeys...)
	sort.Strings(keys)
	for _, key := range keys {
		if value, ok := request.Options[key]; ok {
			input.SafeCallConfig[key] = value
		}
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		return model.Request{}, AuditedModelInput{}, "", err
	}
	if len(canonical) > maxBytes {
		return model.Request{}, AuditedModelInput{}, "", session.ErrModelRequestTooLarge
	}
	digest := sha256.Sum256(canonical)
	return request, input, hex.EncodeToString(digest[:]), nil
}

func auditToolSchemas(tools []*einoschema.ToolInfo) ([]AuditedToolSchema, error) {
	out := make([]AuditedToolSchema, 0, len(tools))
	for _, tool := range tools {
		if tool == nil {
			return nil, fmt.Errorf("nil tool schema")
		}
		if len(tool.Extra) != 0 {
			return nil, fmt.Errorf("tool schema Extra is not audit-safe")
		}
		schema := json.RawMessage("null")
		if tool.ParamsOneOf != nil {
			converted, err := tool.ToJSONSchema()
			if err != nil {
				return nil, err
			}
			var err2 error
			schema, err2 = json.Marshal(converted)
			if err2 != nil {
				return nil, err2
			}
		}
		out = append(out, AuditedToolSchema{Name: tool.Name, Description: tool.Desc, Schema: schema})
	}
	return out, nil
}

func (o *StreamingOrchestrator) prepareModelRequest(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, request model.Request, audited AuditedModelInput, contentHash string, messageID session.MessageID, attempt, step int) (session.ModelRequestRecord, error) {
	if execution == nil || execution.store == nil {
		return session.ModelRequestRecord{}, fmt.Errorf("%w: model request ledger requires an execution store", ErrInvalidOrchestrator)
	}
	messages, err := json.Marshal(audited.Messages)
	if err != nil {
		return session.ModelRequestRecord{}, fmt.Errorf("encode audited messages: %w", err)
	}
	tools, err := json.Marshal(audited.Tools)
	if err != nil {
		return session.ModelRequestRecord{}, fmt.Errorf("encode audited tools: %w", err)
	}
	safeConfig, err := json.Marshal(audited.SafeCallConfig)
	if err != nil {
		return session.ModelRequestRecord{}, fmt.Errorf("encode audited call config: %w", err)
	}
	now := o.now()
	planHash := ""
	if execution != nil && execution.plan != nil {
		planHash = execution.plan.sealed.Fingerprint()
	}
	record := session.ModelRequestRecord{
		ID:        session.ModelRequestID(fmt.Sprintf("%s:%s:%d:%d", snapshot.RunID, messageID, attempt, step)),
		SessionID: snapshot.SessionID, RunID: snapshot.RunID, AssistantMessageID: messageID,
		Attempt: attempt, Step: step, ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID),
		State: session.ModelRequestPrepared, Messages: messages, System: audited.System, Tools: tools,
		SafeCallConfig: safeConfig, ContentSHA256: contentHash, ExtensionPlanHash: planHash,
		CreatedAt: now, UpdatedAt: now,
	}
	created, err := execution.store.CreateModelRequest(ctx, record)
	if err != nil {
		return session.ModelRequestRecord{}, err
	}
	return created, nil
}

func updateModelRequest(ctx context.Context, store session.ExecutionStore, record *session.ModelRequestRecord, state session.ModelRequestState, err error, now time.Time) error {
	next := *record
	next.State = state
	next.UpdatedAt = now
	next.ErrorCode = classifyExtensionError(err).Code
	if updateErr := store.UpdateModelRequest(context.WithoutCancel(ctx), next); updateErr != nil {
		return updateErr
	}
	*record = next
	return nil
}
