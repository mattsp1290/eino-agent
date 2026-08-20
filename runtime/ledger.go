package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

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
// the optional request ledger.
func AuditModelRequest(request model.Request, safeOptionKeys []string, maxBytes int) (AuditedModelInput, string, error) {
	if maxBytes <= 0 {
		maxBytes = defaultModelRequestMaxBytes
	}
	input := AuditedModelInput{System: request.System, SafeCallConfig: make(map[string]string)}
	for _, message := range request.Messages {
		if message == nil {
			return AuditedModelInput{}, "", fmt.Errorf("nil model message")
		}
		if len(message.Extra) != 0 {
			return AuditedModelInput{}, "", fmt.Errorf("model message Extra is not audit-safe")
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

func (o *StreamingOrchestrator) prepareModelRequest(ctx context.Context, snapshot TurnSnapshot, request model.Request, messageID session.MessageID, attempt, step int) (*session.ModelRequestRecord, session.ModelRequestStore, error) {
	if !o.ModelRequestLedger {
		return nil, nil, nil
	}
	store, ok := o.Store.(session.ModelRequestStore)
	if !ok {
		return nil, nil, fmt.Errorf("%w: model request ledger requires session.ModelRequestStore", ErrInvalidOrchestrator)
	}
	audited, contentHash, err := AuditModelRequest(request, o.ModelRequestSafeOptions, o.ModelRequestMaxBytes)
	if err != nil {
		return nil, nil, err
	}
	messages, _ := json.Marshal(audited.Messages)
	tools, _ := json.Marshal(audited.Tools)
	safeConfig, _ := json.Marshal(audited.SafeCallConfig)
	now := o.now()
	planHash := ""
	if plan := runPlanFromContext(ctx); plan != nil {
		planHash = plan.Descriptor.Fingerprint
	}
	record := session.ModelRequestRecord{
		ID:        session.ModelRequestID(fmt.Sprintf("%s:%s:%d:%d", snapshot.RunID, messageID, attempt, step)),
		SessionID: snapshot.SessionID, RunID: snapshot.RunID, AssistantMessageID: messageID,
		Attempt: attempt, Step: step, ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID),
		State: session.ModelRequestPrepared, Messages: messages, System: audited.System, Tools: tools,
		SafeCallConfig: safeConfig, ContentSHA256: contentHash, ExtensionPlanHash: planHash,
		CreatedAt: now, UpdatedAt: now,
	}
	created, err := store.CreateModelRequest(ctx, record)
	if err != nil {
		return nil, nil, err
	}
	return &created, store, nil
}

func updateModelRequest(ctx context.Context, store session.ModelRequestStore, record *session.ModelRequestRecord, state session.ModelRequestState, err error, now time.Time) error {
	if store == nil || record == nil {
		return nil
	}
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
