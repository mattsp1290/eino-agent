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
// path segment when session.Message.AgentPath is empty -- true for every
// message today, since subagent nesting is not yet wired end to end (see
// runtime's adkEngine.agentPath doc comment: every current caller stamps
// AgentPath ""). convert.ProjectAgenticMessage requires at least one
// agent-path segment for every identity. Note this is session.Message's
// AgentPath field, not its separate Agent field (the configured agent's
// display name, e.g. "root" or a host's own agent name) -- Agent is set for
// assistant messages but left empty for user messages, so keying off it
// would project a different agent path for a user message than for the
// assistant message in the same turn. AgentPath mirrors EventRecord/
// ModelRequestRecord.AgentPath and is the field that will actually carry a
// real (sub)agent path once nesting is wired, so every message in a turn
// projects the same agent path both today (all "root") and once nesting
// lands (one shared path per turn).
const rootAgentPathName = "root"

// agenticIdentity builds the AgenticIdentityV1 an eino-agui projection call
// requires from a durable session.Message plus its resolved attempt id.
// ThreadID is always set equal to SessionID, matching eino-agui's own
// validateIdentity requirement.
//
// TurnID normally comes straight from session.Message.TurnID (W7 durable
// identity). It falls back to a deterministic synthetic id ("msg:" + the
// message's own ID) when TurnID is empty -- true for every message written
// before W7 stamped TurnID at all, every compaction boundary message that
// predates session/compaction stamping one, and
// runtime.settleInterruptedTool's deliberately-empty crash-reconciliation
// stamp (see that function's doc comment). Without this fallback,
// convert.validateIdentity rejects the empty TurnID outright, which used to
// fail loadCommittedProjections' whole batch and brick replay for the
// entire session over a single such message (W7 review finding A1/C1) --
// mirrors the existing synthetic-block-id precedent in blockContexts below.
func agenticIdentity(sessionID session.ID, message session.Message, attemptID string) convert.AgenticIdentityV1 {
	name := message.AgentPath
	if name == "" {
		name = rootAgentPathName
	}
	turnID := string(message.TurnID)
	if turnID == "" {
		turnID = syntheticTurnID(message.ID)
	}
	return convert.AgenticIdentityV1{
		SessionID: string(sessionID),
		ThreadID:  string(sessionID),
		RunID:     string(message.RunID),
		TurnID:    turnID,
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
// observation-watermark revision (see currentRevision): eino-agui's
// agenticReceiptKey folds the receipt's Revision into its dedup key
// alongside the full identity (session/run/turn/message/attempt/...), so
// many messages safely sharing one revision string in a single call is not
// a collision risk (the rest of the identity still disambiguates them).
// That same revision-folding is why re-projecting the SAME message under a
// DIFFERENT revision (e.g. this function called again after the session's
// observation watermark has advanced) is NOT deduplicated -- it mints a new
// receipt key and emits again. That is exactly the shape of the
// message_committed-during-replay bug Bridge.projectedMessages guards
// against: replay() forwards every non-LiveOnly durable event, including
// session.MessageCommittedEventKind, to bridge.Emit (see replay.go); Bridge
// itself is what skips a notification naming a message it has already
// emitted through the committed-projection path, rather than relying on
// this function's receipt dedup to catch a redundant re-projection (see
// Bridge.projectedMessages and emitLiveMessageCommitted's doc comments in
// agui/bridge.go).
//
// includeReasoning gates whether reasoning content blocks are included in
// the projection at all (see history.Options.IncludeReasoning and
// agui.GateProviderReasoningStorage): callers must only pass true once the
// host has confirmed that gate is satisfied for this session.
func loadCommittedProjections(ctx context.Context, store session.Store, sessionID session.ID, contentLimits session.ContentLimits, includeReasoning bool) ([]committedMessageProjection, error) {
	batch, err := history.LoadBatch(ctx, store, sessionID)
	if err != nil {
		return nil, err
	}
	if len(batch.Messages) == 0 {
		return nil, nil
	}
	projected, err := history.ProjectAgentic(batch, history.Options{ContentLimits: contentLimits, IncludeReasoning: includeReasoning})
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

// syntheticTurnID mints a deterministic, per-message turn id for a durable
// message whose own TurnID is empty (see agenticIdentity's doc comment). It
// is derived only from the message's own ID, so replaying the same session
// twice (or reprojecting the same message on the live commit path) always
// produces the same synthetic id.
func syntheticTurnID(messageID session.MessageID) string {
	return "msg:" + string(messageID)
}

func currentRevision(ctx context.Context, store session.Store, sessionID session.ID) string {
	if reader, ok := store.(session.ObservationReader); ok {
		if watermark, err := reader.ReadObservationRevision(ctx, sessionID); err == nil {
			return strconv.FormatInt(watermark.Revision, 10)
		}
	}
	return "0"
}
