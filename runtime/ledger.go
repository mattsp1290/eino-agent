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

type AuditedModelInput struct {
	Messages       []AuditedMessage    `json:"messages"`
	System         string              `json:"system"`
	Tools          []AuditedToolSchema `json:"tools"`
	SafeCallConfig map[string]string   `json:"safe_call_config"`
}

// AuditModelRequest derives the canonical, credential-free subset persisted by
// the request ledger.
func AuditModelRequest(request model.Request, safeOptionKeys []string, maxBytes int) (AuditedModelInput, string, error) {
	if maxBytes <= 0 {
		maxBytes = defaultModelRequestMaxBytes
	}
	input := AuditedModelInput{System: request.System, SafeCallConfig: make(map[string]string)}
	for _, message := range request.Messages {
		if message == nil {
			return AuditedModelInput{}, "", fmt.Errorf("nil model message")
		}
		if err := validateAuditSafeMessage(message); err != nil {
			return AuditedModelInput{}, "", err
		}
		raw, err := json.Marshal(message)
		if err != nil {
			return AuditedModelInput{}, "", err
		}
		input.Messages = append(input.Messages, AuditedMessage{Canonical: raw})
	}
	for _, tool := range request.Tools {
		if tool == nil {
			return AuditedModelInput{}, "", fmt.Errorf("nil tool schema")
		}
		if len(tool.Extra) != 0 {
			return AuditedModelInput{}, "", fmt.Errorf("tool schema Extra is not audit-safe")
		}
		schema := json.RawMessage("null")
		if tool.ParamsOneOf != nil {
			converted, err := tool.ToJSONSchema()
			if err != nil {
				return AuditedModelInput{}, "", err
			}
			schema, err = json.Marshal(converted)
			if err != nil {
				return AuditedModelInput{}, "", err
			}
		}
		input.Tools = append(input.Tools, AuditedToolSchema{Name: tool.Name, Description: tool.Desc, Schema: schema})
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
		return AuditedModelInput{}, "", err
	}
	if len(canonical) > maxBytes {
		return AuditedModelInput{}, "", session.ErrModelRequestTooLarge
	}
	digest := sha256.Sum256(canonical)
	return input, hex.EncodeToString(digest[:]), nil
}

func validateAuditSafeMessage(message *einoschema.Message) error {
	//nolint:staticcheck // The audit boundary rejects the deprecated field.
	if len(message.MultiContent) != 0 {
		return fmt.Errorf("model message contains unsupported deprecated MultiContent")
	}
	for _, part := range message.AssistantGenMultiContent {
		if part.StreamingMeta != nil {
			return fmt.Errorf("model message contains streaming metadata")
		}
	}
	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return rejectCanonicalExtra(raw)
}

func rejectCanonicalExtra(raw json.RawMessage) error {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	var visit func(any) error
	visit = func(current any) error {
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				if key == "extra" {
					return fmt.Errorf("model message contains unsupported extra metadata")
				}
				if err := visit(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(value)
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
		planHash = execution.plan.descriptor.Fingerprint
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
