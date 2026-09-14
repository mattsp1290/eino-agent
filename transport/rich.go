package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// Bounds for the rich AG-UI ingress handlers below. These are transport-layer
// shape limits, independent of session.ContentLimits (which bounds durable
// storage, not the wire request): a caller supplying content within these
// bounds may still be rejected downstream by the runtime's own configured
// ContentLimits.
const (
	maxRichRequestBytes  = 16 << 20
	maxInputContentParts = 64
	maxResumeTargets     = 256
	maxIDBytes           = 512
	maxIdempotencyBytes  = 256
)

// DecodeUserMessage decodes an application request body carrying a native
// AG-UI user message (see types.InputContent) into a runtime.UserMessage,
// mapping every representable input content kind onto its
// session.ContentBlock: text, image, audio, video, and document (as
// user_input_file). Block IDs are always left empty (runtime.UserMessage's
// own contract: Start mints every block's durable ID itself and rejects a
// caller-supplied one), so this never trusts a caller-supplied block
// identity. The request body is bounded to maxRichRequestBytes and at most
// maxInputContentParts content fragments; asset loading/rendering behind a
// URL or base64 payload is a host responsibility, not validated here beyond
// shape.
func DecodeUserMessage(r *http.Request) (runtime.UserMessage, error) {
	var payload struct {
		Content []types.InputContent `json:"content"`
	}
	body := io.LimitReader(r.Body, maxRichRequestBytes+1)
	raw, err := io.ReadAll(body)
	if err != nil {
		return runtime.UserMessage{}, err
	}
	if len(raw) > maxRichRequestBytes {
		return runtime.UserMessage{}, fmt.Errorf("request body exceeds %d bytes", maxRichRequestBytes)
	}
	// InputContent implements json.Unmarshaler for compatibility aliases and
	// consequently bypasses Decoder.DisallowUnknownFields for nested parts.
	// Validate the accepted wire shape first, then decode the SDK type used by
	// the mapper below.
	var strict strictRichPayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&strict); err != nil {
		return runtime.UserMessage{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("exactly one JSON object required")
		}
		return runtime.UserMessage{}, err
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return runtime.UserMessage{}, err
	}
	if len(payload.Content) == 0 {
		return runtime.UserMessage{}, fmt.Errorf("content required")
	}
	if len(payload.Content) > maxInputContentParts {
		return runtime.UserMessage{}, fmt.Errorf("content exceeds %d parts", maxInputContentParts)
	}
	blocks := make([]session.ContentBlock, 0, len(payload.Content))
	for i, part := range payload.Content {
		block, err := contentBlockFromInput(part)
		if err != nil {
			return runtime.UserMessage{}, fmt.Errorf("content[%d]: %w", i, err)
		}
		blocks = append(blocks, block)
	}
	return runtime.UserMessage{Blocks: blocks}, nil
}

type strictRichPayload struct {
	Content []strictInputContent `json:"content"`
}

// strictInputContent mirrors the supported SDK request surface solely for
// strict JSON validation. Metadata deliberately remains raw JSON because its
// schema is application-defined and is not persisted by this adapter.
type strictInputContent struct {
	Type     string                    `json:"type"`
	Text     string                    `json:"text,omitempty"`
	MimeType string                    `json:"mimeType,omitempty"`
	ID       string                    `json:"id,omitempty"`
	URL      string                    `json:"url,omitempty"`
	Data     string                    `json:"data,omitempty"`
	Filename string                    `json:"filename,omitempty"`
	Source   *strictInputContentSource `json:"source,omitempty"`
	Metadata json.RawMessage           `json:"metadata,omitempty"`
}

type strictInputContentSource struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	MimeType string `json:"mimeType,omitempty"`
}

func contentBlockFromInput(part types.InputContent) (session.ContentBlock, error) {
	switch part.Type {
	case types.InputContentTypeText:
		if part.Text == "" {
			return session.ContentBlock{}, fmt.Errorf("text content requires text")
		}
		return session.ContentBlock{Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: part.Text}}, nil
	case types.InputContentTypeImage, types.InputContentTypeAudio, types.InputContentTypeVideo, types.InputContentTypeDocument:
		media, err := mediaBlockFromSource(part.Source)
		if err != nil {
			return session.ContentBlock{}, err
		}
		if part.Type == types.InputContentTypeDocument {
			media.Name = part.Filename
		}
		return session.ContentBlock{Kind: userInputKindForType(part.Type), Media: media}, nil
	default:
		return session.ContentBlock{}, fmt.Errorf("unsupported input content type %q", part.Type)
	}
}

func userInputKindForType(inputType string) session.BlockKind {
	switch inputType {
	case types.InputContentTypeImage:
		return session.BlockKindUserInputImage
	case types.InputContentTypeAudio:
		return session.BlockKindUserInputAudio
	case types.InputContentTypeVideo:
		return session.BlockKindUserInputVideo
	default:
		return session.BlockKindUserInputFile
	}
}

// mediaBlockFromSource requires exactly one source value (URL or inline
// base64 data), matching the accepted eino-agui contract's "exactly one
// URL/base64 source" requirement for every media input kind.
func mediaBlockFromSource(source *types.InputContentSource) (*session.MediaBlock, error) {
	if source == nil || source.Value == "" {
		return nil, fmt.Errorf("media content requires exactly one source")
	}
	media := &session.MediaBlock{MIMEType: source.MimeType}
	switch source.Type {
	case types.InputContentSourceTypeURL:
		media.URL = source.Value
	case types.InputContentSourceTypeData:
		media.Base64Data = source.Value
	default:
		return nil, fmt.Errorf("unsupported media source type %q", source.Type)
	}
	return media, nil
}

// Enqueuer is the narrow runtime surface EnqueueHandler needs.
type Enqueuer interface {
	Enqueue(context.Context, session.ID, runtime.EnqueueRequest) (session.InboxItem, error)
}

// EnqueueHandler adapts an application-owned turn-enqueue route to
// runtime.Enqueue. auth runs before the request body is read or any session
// content is touched: EnqueueHandler never infers permission from the
// request shape (a valid, bounded idempotency key or run id is not itself
// authorization -- the host's auth callback decides that independently).
// The decoded runtime.UserMessage goes through the same DecodeUserMessage
// bounds and mapping used by rich message ingress generally.
func EnqueueHandler(auth AuthFunc, lookupRunID func(context.Context, *http.Request) (session.ID, session.RunID, error), enqueuer Enqueuer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		if auth != nil {
			next, err := auth(ctx, r)
			if err != nil {
				writeAuthError(w, err)
				return
			}
			ctx = next
			r = r.WithContext(ctx)
		}
		if lookupRunID == nil || enqueuer == nil {
			http.Error(w, "enqueue not configured", http.StatusInternalServerError)
			return
		}
		sessionID, runID, err := lookupRunID(ctx, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		idempotencyKey := r.Header.Get("Idempotency-Key")
		if idempotencyKey == "" || len(idempotencyKey) > maxIdempotencyBytes {
			http.Error(w, "a bounded Idempotency-Key header is required", http.StatusBadRequest)
			return
		}
		message, err := DecodeUserMessage(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		item, err := enqueuer.Enqueue(ctx, sessionID, runtime.EnqueueRequest{RunID: runID, IdempotencyKey: idempotencyKey, Message: message})
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(struct {
			InboxID string `json:"inbox_id"`
		}{InboxID: string(item.ID)})
	})
}

// Resumer is the narrow runtime surface ResumeTargetedHandler needs.
type Resumer interface {
	ResumeRun(context.Context, session.RunID, runtime.ResumeRequest) (runtime.Handle, error)
}

// resumeTargetPayload is the wire shape ResumeTargetedHandler decodes: a
// bounded set of current-generation interrupt target ids (see
// runtime.ResumeRequest.Targets's doc comment -- InterruptCtx.ID, the same
// ids a paused run's PauseInfo exposed) mapped to the host's typed decision
// for each. Possessing a target id is not itself authorization: auth runs
// before this body is even read.
type resumeTargetPayload struct {
	Targets map[string]json.RawMessage `json:"targets"`
}

// ResumeTargetedHandler adapts an application-owned targeted-resume route to
// runtime.ResumeRun. auth runs before lookupRunID or the request body is
// read, so a caller can never reach a paused run's state by possessing (or
// guessing) an interrupt target id alone -- the host's auth callback must
// independently authorize this caller for this run first. Targets are
// bounded in count and per-id byte length; idempotent replay of an
// already-applied resume is left to ResumeRun's own natural behavior (a run
// that already left Paused rejects a second claim), surfaced here as an
// ordinary conflict response rather than silently reapplied.
func ResumeTargetedHandler(auth AuthFunc, lookupRunID func(context.Context, *http.Request) (session.RunID, error), resumer Resumer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		if auth != nil {
			next, err := auth(ctx, r)
			if err != nil {
				writeAuthError(w, err)
				return
			}
			ctx = next
			r = r.WithContext(ctx)
		}
		if lookupRunID == nil || resumer == nil {
			http.Error(w, "targeted resume not configured", http.StatusInternalServerError)
			return
		}
		runID, err := lookupRunID(ctx, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		body := io.LimitReader(r.Body, maxRichRequestBytes+1)
		raw, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(raw) > maxRichRequestBytes {
			http.Error(w, fmt.Sprintf("request body exceeds %d bytes", maxRichRequestBytes), http.StatusRequestEntityTooLarge)
			return
		}
		var payload resumeTargetPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(payload.Targets) == 0 {
			http.Error(w, "at least one target is required", http.StatusBadRequest)
			return
		}
		if len(payload.Targets) > maxResumeTargets {
			http.Error(w, fmt.Sprintf("targets exceed %d", maxResumeTargets), http.StatusBadRequest)
			return
		}
		targets := make(map[string]any, len(payload.Targets))
		for id, decision := range payload.Targets {
			if id == "" || len(id) > maxIDBytes {
				http.Error(w, "every target id must be non-empty and bounded", http.StatusBadRequest)
				return
			}
			var decoded any
			if err := json.Unmarshal(decision, &decoded); err != nil {
				http.Error(w, fmt.Sprintf("target %s: %v", id, err), http.StatusBadRequest)
				return
			}
			targets[id] = decoded
		}
		handle, err := resumer.ResumeRun(ctx, runID, runtime.ResumeRequest{Targets: targets})
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Eino-Agent-Run-ID", string(handle.RunID()))
		w.WriteHeader(http.StatusAccepted)
	})
}
