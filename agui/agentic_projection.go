package agui

import (
	"context"
	"strconv"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agui/convert"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// rootAgentPathName is the display name used for a durable message's agent
// path segment when session.Message.Agent is empty -- true for every
// message today, since subagent nesting is not yet wired end to end (see
// runtime's adkEngine.agentPath doc comment). convert.ProjectAgenticMessage
// requires at least one agent-path segment for every identity.
const rootAgentPathName = "root"

// agenticIdentity builds the AgenticIdentityV1 an eino-agui projection call
// requires from a durable session.Message plus its resolved attempt id.
// TurnID comes from session.Message.TurnID (W7 durable identity), which
// every message this bridge can reach has been stamped with since it was
// introduced; ThreadID is always set equal to SessionID, matching eino-agui's
// own validateIdentity requirement.
func agenticIdentity(sessionID session.ID, message session.Message, attemptID string) convert.AgenticIdentityV1 {
	name := message.Agent
	if name == "" {
		name = rootAgentPathName
	}
	return convert.AgenticIdentityV1{
		SessionID: string(sessionID),
		ThreadID:  string(sessionID),
		RunID:     string(message.RunID),
		TurnID:    string(message.TurnID),
		MessageID: string(message.ID),
		AttemptID: attemptID,
		AgentPath: []convert.AgentPathSegment{{Name: name, RunID: string(message.RunID)}},
	}
}

// projectionLimits maps the host's durable content bounds onto eino-agui's
// ProjectionLimits: the byte/block bounds this bridge has a direct
// session.ContentLimits analogue for are carried over, every other bound
// (annotations, interrupt targets, JSON depth/entries, ...) keeps
// eino-agui's own default.
func projectionLimits(limits session.ContentLimits) convert.ProjectionLimits {
	out := convert.DefaultProjectionLimits()
	if limits.MaxMessageBytes > 0 {
		out.MaxMessageBytes = limits.MaxMessageBytes
	}
	if limits.MaxBlocks > 0 {
		out.MaxBlocks = limits.MaxBlocks
	}
	if limits.MaxBlockBytes > 0 {
		out.MaxBlockBytes = limits.MaxBlockBytes
	}
	return out
}

// attemptResolver resolves the durable "attempt identity" for a message: the
// InvocationID of the session.ModelRequestRecord that produced it (see that
// field's doc comment), memoized per run since a session's history can span
// many runs and many messages per run. A user message (never dispatched to a
// model) has no ModelRequestRecord; its own MessageID is a stable,
// never-retried "attempt" in that case, and so is an assistant message with
// no matching ledger row (e.g. a crash-reconciled carrier turn's
// content-free placeholder).
type attemptResolver struct {
	reader session.ModelRequestReader
	byRun  map[session.RunID]map[session.MessageID]string
}

func newAttemptResolver(reader session.ModelRequestReader) *attemptResolver {
	return &attemptResolver{reader: reader, byRun: map[session.RunID]map[session.MessageID]string{}}
}

func (r *attemptResolver) attemptID(ctx context.Context, message session.Message) (string, error) {
	if message.Role != session.RoleAssistant || r.reader == nil {
		return string(message.ID), nil
	}
	byMessage, ok := r.byRun[message.RunID]
	if !ok {
		var err error
		byMessage, err = r.loadRun(ctx, message.RunID)
		if err != nil {
			return "", err
		}
		r.byRun[message.RunID] = byMessage
	}
	if id, ok := byMessage[message.ID]; ok && id != "" {
		return id, nil
	}
	return string(message.ID), nil
}

func (r *attemptResolver) loadRun(ctx context.Context, runID session.RunID) (map[session.MessageID]string, error) {
	byMessage := map[session.MessageID]string{}
	cursor := session.ModelRequestCursor{Limit: 200}
	for {
		batch, err := r.reader.ListModelRequests(ctx, runID, cursor)
		if err != nil {
			return nil, err
		}
		for _, record := range batch.Records {
			if record.AssistantMessageID != "" && record.InvocationID != "" {
				byMessage[record.AssistantMessageID] = record.InvocationID
			}
		}
		if batch.Next.AfterID == "" {
			return byMessage, nil
		}
		cursor = batch.Next
	}
}

// committedMessageProjection pairs one durable message's full agentic
// projection with the CommitReceiptV1 that authorizes emitting it.
type committedMessageProjection struct {
	MessageID  session.MessageID
	Projection *convert.AgenticProjection
	Receipt    convert.CommitReceiptV1
}

// loadCommittedProjections projects every durable message in sessionID into
// eino-agui agentic projections plus the receipt needed to emit each one, in
// durable (created_at, id) order -- the same order history.Load/ListMessages
// return. Every projection in one call shares the session's current
// observation-watermark revision (see currentRevision): receipts are
// deduplicated by identity (session/run/turn/message/attempt), not by
// revision, so many messages safely sharing one revision string is not a
// collision risk (see eino-agui's agenticReceiptKey).
func loadCommittedProjections(ctx context.Context, store session.Store, sessionID session.ID, contentLimits session.ContentLimits) ([]committedMessageProjection, error) {
	batch, err := history.LoadBatch(ctx, store, sessionID)
	if err != nil {
		return nil, err
	}
	if len(batch.Messages) == 0 {
		return nil, nil
	}
	projected, err := history.ProjectAgentic(batch, history.Options{ContentLimits: contentLimits, IncludeReasoning: true})
	if err != nil {
		return nil, err
	}
	if len(projected.Messages) == 0 {
		return nil, nil
	}
	byID := make(map[session.MessageID]session.Message, len(batch.Messages))
	for _, m := range batch.Messages {
		byID[m.ID] = m
	}
	revision := currentRevision(ctx, store, sessionID)
	attempts := newAttemptResolver(store)
	limits := projectionLimits(contentLimits)
	out := make([]committedMessageProjection, 0, len(projected.Messages))
	for i, agentic := range projected.Messages {
		source, ok := byID[projected.SourceMessageIDs[i]]
		if !ok {
			continue
		}
		attemptID, err := attempts.attemptID(ctx, source)
		if err != nil {
			return nil, err
		}
		identity := agenticIdentity(sessionID, source, attemptID)
		blocks := blockContexts(source.ID, agentic, projected.BlockIDs[i])
		full, err := convert.ToAgenticProjection(agentic, convert.AgenticProjectionContext{Identity: identity, Blocks: blocks, Limits: limits})
		if err != nil {
			return nil, err
		}
		receipt := convert.CommitReceiptV1{Revision: revision, Domain: "projection", Identity: full.Public.Identity, Digest: full.Public.Digest}
		out = append(out, committedMessageProjection{MessageID: source.ID, Projection: full, Receipt: receipt})
	}
	return out, nil
}

// blockContexts derives a convert.AgenticBlockContext per content block.
// projection.BlockIDs is nil for a message projected through the legacy
// (compaction/free-text-reasoning) pipeline, which carries no durable block
// identity at all; a deterministic synthetic id (message id + block index)
// stands in so ProjectAgenticMessage's "every block id is required and
// unique" invariant still holds for that family.
func blockContexts(messageID session.MessageID, agentic *einoschema.AgenticMessage, blockIDs []string) []convert.AgenticBlockContext {
	blocks := make([]convert.AgenticBlockContext, len(agentic.ContentBlocks))
	for i := range agentic.ContentBlocks {
		id := ""
		if i < len(blockIDs) {
			id = blockIDs[i]
		}
		if id == "" {
			id = string(messageID) + ":" + strconv.Itoa(i)
		}
		blocks[i] = convert.AgenticBlockContext{BlockID: id}
	}
	return blocks
}

func currentRevision(ctx context.Context, store session.Store, sessionID session.ID) string {
	if reader, ok := store.(session.ObservationReader); ok {
		if watermark, err := reader.ReadObservationRevision(ctx, sessionID); err == nil {
			return strconv.FormatInt(watermark.Revision, 10)
		}
	}
	return "0"
}
