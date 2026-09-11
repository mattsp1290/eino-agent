package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// turnLoopIdleStopDelay is how long a fresh Start's loop waits, once idle,
// before stopping cleanly. It only needs to be long enough for the pushed
// item(s) to be dequeued and for a closely-following Enqueue call to land in
// the same loop; it never delays a turn that is already running.
const turnLoopIdleStopDelay = 15 * time.Millisecond

// adkTurnLoop is this package's one item/message type instantiation of
// adk.TurnLoop: items are durable inbox IDs only (never payloads or
// closures), so resume never needs to reconstruct anything beyond what the
// durable inbox already holds.
type adkTurnLoop = adk.TurnLoop[session.InboxID, *einoschema.AgenticMessage]

// PauseInfo describes a durably promoted pause a host can act on: resume it
// (via ResumeRun) or leave it paused indefinitely (a paused run reserves its
// session but holds no live lease).
type PauseInfo struct {
	RunID     session.RunID
	StopCause string
	// InterruptContexts targets a specific paused leaf. Their IDs are valid
	// only for this exact checkpoint generation (see the W1 finding in
	// docs/architecture/eino-feature-support.md): resuming with a stale ID
	// is a silent no-op re-pause, never a misdirected dispatch.
	InterruptContexts []*adk.InterruptCtx
}

// turnLoopCoordinator closes over one live TurnLoop's durable identity and
// implements the four TurnLoopConfig callbacks. One coordinator is built per
// Start/ResumeRun call and lives exactly as long as the loop it drives.
type turnLoopCoordinator struct {
	host           *StreamingOrchestrator
	execution      *runExecution
	plan           *RunPlan
	sessionID      session.ID
	runID          session.RunID
	config         config.Snapshot
	resolved       model.Resolved
	historyOptions history.Options
	epochID        session.EpochID

	mu                sync.Mutex
	ordinal           int64
	engine            *adkEngine
	resumeTargets     map[string]any
	firstTurnEngine   *adkEngine
	firstTurnConsumed bool
}

// firstTurnSentinelID is pushed exactly once by Start to hand TurnLoop the
// already-admitted first turn (admitted atomically with the run itself, see
// admission.go's admitDurable) instead of re-admitting it through the
// normal GenInput -> AdmitTurn path.
const firstTurnSentinelID session.InboxID = "\x00first-turn"

func (c *turnLoopCoordinator) nextOrdinal() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ordinal++
	return c.ordinal
}

func (c *turnLoopCoordinator) currentEngine() *adkEngine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.engine
}

func (c *turnLoopCoordinator) setEngine(e *adkEngine) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.engine = e
}

func (c *turnLoopCoordinator) setResumeTargets(targets map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeTargets = targets
}

// loadInboxItems fetches the durable content for a set of inbox IDs. It
// scans ListInbox (unbounded by state) since the inbox contract does not
// expose a single-ID lookup; inbox lists are session-scoped and bounded by
// normal conversational volume.
func (c *turnLoopCoordinator) loadInboxItems(ctx context.Context, ids []session.InboxID) ([]session.InboxItem, error) {
	all, err := c.host.store.ListInbox(ctx, c.sessionID, nil)
	if err != nil {
		return nil, err
	}
	byID := make(map[session.InboxID]session.InboxItem, len(all))
	for _, item := range all {
		byID[item.ID] = item
	}
	out := make([]session.InboxItem, 0, len(ids))
	for _, id := range ids {
		item, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("runtime: inbox item %s not found for session %s", id, c.sessionID)
		}
		out = append(out, item)
	}
	return out, nil
}

// admitTurn atomically admits the next-ordinal turn consuming itemIDs (which
// may be empty, for a degenerate turn used only to carry a between-turn
// pause -- see promoteQueuedContinuation), reconstructs the turn's full
// model input (committed prior history, freshly reloaded, plus this turn's
// newly admitted messages -- never a captured transcript), and prepares the
// turn's TurnSnapshot through the same extension pipeline every turn uses.
func (c *turnLoopCoordinator) admitTurn(ctx context.Context, itemIDs []session.InboxID) (*adkEngine, error) {
	items, err := c.loadInboxItems(ctx, itemIDs)
	if err != nil {
		return nil, err
	}
	at := c.host.now()
	var userMessages []session.Message
	var userParts []session.Part
	var userMessageIDs []session.MessageID
	var newAgentic []*einoschema.AgenticMessage
	for _, item := range items {
		msgID := c.host.ids.NewMessageID()
		content := session.Content{Role: session.RoleUser, Blocks: item.Blocks}
		agenticMsg, err := session.ContentToAgenticMessage(content)
		if err != nil {
			return nil, err
		}
		newAgentic = append(newAgentic, agenticMsg)
		parts, err := session.EncodeContentParts(content, func() session.PartID { return c.host.ids.NewPartID() }, msgID, c.sessionID, c.runID, at, c.host.contentLimits)
		if err != nil {
			return nil, err
		}
		userMessages = append(userMessages, session.Message{ID: msgID, SessionID: c.sessionID, RunID: c.runID, Role: session.RoleUser, CreatedAt: at, UpdatedAt: at})
		userParts = append(userParts, parts...)
		userMessageIDs = append(userMessageIDs, msgID)
		at = at.Add(time.Nanosecond)
	}
	assistantID := c.host.ids.NewMessageID()
	assistantAt := at
	turnID := c.host.ids.NewTurnID()
	turn := session.Turn{
		ID: turnID, RunID: c.runID, SessionID: c.sessionID, Ordinal: c.nextOrdinal(), State: session.TurnAdmitted,
		UserMessageIDs: userMessageIDs, AssistantMessageID: assistantID, EpochID: c.epochID, CreatedAt: c.host.now(),
	}
	event := session.EventRecord{
		ID: c.host.ids.NewEventID(), SessionID: c.sessionID, RunID: c.runID, MessageID: assistantID,
		EpochID: c.epochID, TurnID: turnID, Kind: session.TurnStartedEventKind, CreatedAt: c.host.now(),
	}
	var parentID session.MessageID
	if len(userMessageIDs) != 0 {
		parentID = userMessageIDs[len(userMessageIDs)-1]
	}
	assistantMessage := session.Message{
		ID: assistantID, SessionID: c.sessionID, RunID: c.runID, ParentID: parentID, Role: session.RoleAssistant,
		Agent: c.config.Agent.Name, ModelID: string(c.resolved.Model.ID), CreatedAt: assistantAt, UpdatedAt: assistantAt,
	}
	result, err := c.execution.store.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: turn, UserMessages: userMessages, UserParts: userParts, AssistantPlaceholder: assistantMessage, Event: event, InboxIDs: itemIDs,
	})
	if err != nil {
		return nil, err
	}
	c.execution.publishPersisted(ctx, result.Event)
	priorMessages, priorProviderState, err := loadProviderHistory(ctx, c.host.store, session.Session{ID: c.sessionID}, c.historyOptions, c.resolved)
	if err != nil {
		return nil, err
	}
	allMessages := append(append([]*einoschema.AgenticMessage(nil), priorMessages...), newAgentic...)
	base, err := FreezeTurnSnapshot(c.runID, c.sessionID, c.epochID, c.config, c.resolved, allMessages, c.config.Agent.SystemPrompt, c.host.now())
	if err != nil {
		return nil, err
	}
	base.providerState = priorProviderState
	snapshot, err := c.host.prepareSnapshot(ctx, c.execution, base)
	if err != nil {
		return nil, err
	}
	c.execution.seedDiscovered(discoveredToolsFromMessages(snapshot.Messages))
	engine := &adkEngine{host: c.host, execution: c.execution, plan: c.plan, snapshot: snapshot, turn: result.Turn, assistantMessageID: assistantID}
	c.setEngine(engine)
	return engine, nil
}

func (c *turnLoopCoordinator) genInput(ctx context.Context, _ *adkTurnLoop, items []session.InboxID) (*adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage], error) {
	if len(items) == 0 {
		return nil, errors.New("runtime: TurnLoop GenInput called with no buffered items")
	}
	if !c.firstTurnConsumed && c.firstTurnEngine != nil && items[0] == firstTurnSentinelID {
		c.firstTurnConsumed = true
		engine := c.firstTurnEngine
		c.setEngine(engine)
		return &adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage]{
			Input:     &adk.TypedAgentInput[*einoschema.AgenticMessage]{Messages: engine.snapshot.Messages},
			Consumed:  items[:1],
			Remaining: items[1:],
		}, nil
	}
	engine, err := c.admitTurn(ctx, items)
	if err != nil {
		return nil, err
	}
	return &adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage]{
		Input:    &adk.TypedAgentInput[*einoschema.AgenticMessage]{Messages: engine.snapshot.Messages},
		Consumed: items,
	}, nil
}

// genResume maps a host's requested resume targets (see ResumeRun) straight
// through to adk.ResumeParams. Targets are current-generation interrupt IDs
// (see PauseInfo); this package does not yet resolve a stable InterruptCtx
// address across multiple pause/resume generations (see the W5 section of
// docs/architecture/eino-feature-support.md).
func (c *turnLoopCoordinator) genResume(ctx context.Context, _ *adkTurnLoop, interrupted, unhandled, newItems []session.InboxID) (*adk.GenResumeResult[session.InboxID, *einoschema.AgenticMessage], error) {
	engine, err := c.resumeEngine(ctx)
	if err != nil {
		return nil, err
	}
	c.setEngine(engine)
	c.mu.Lock()
	targets := c.resumeTargets
	c.mu.Unlock()
	remaining := make([]session.InboxID, 0, len(unhandled)+len(newItems))
	remaining = append(remaining, unhandled...)
	remaining = append(remaining, newItems...)
	return &adk.GenResumeResult[session.InboxID, *einoschema.AgenticMessage]{
		ResumeParams: &adk.ResumeParams{Targets: targets},
		Consumed:     interrupted,
		Remaining:    remaining,
	}, nil
}

// resumeEngine rebuilds the adkEngine for the run's most recently
// interrupted turn: ADK's own checkpoint restoration owns the resumed
// conversation state (TurnLoop's ResumeWithParams call uses the promoted
// runner bytes, not GenResumeResult), so this only needs to reconstruct the
// bounded per-turn context (Tools/Model/ToolSearch) the model/tool adapters
// require, plus the durable turn identity CompleteTurn later needs.
func (c *turnLoopCoordinator) resumeEngine(ctx context.Context) (*adkEngine, error) {
	turns, err := c.host.store.ListTurns(ctx, c.runID)
	if err != nil {
		return nil, err
	}
	var target session.Turn
	for _, t := range turns {
		if t.State == session.TurnInterrupted && t.Ordinal >= target.Ordinal {
			target = t
		}
	}
	if target.ID == "" {
		return nil, fmt.Errorf("%w: no interrupted turn found to resume for run %s", ErrInvalidOrchestrator, c.runID)
	}
	priorMessages, priorProviderState, err := loadProviderHistory(ctx, c.host.store, session.Session{ID: c.sessionID}, c.historyOptions, c.resolved)
	if err != nil {
		return nil, err
	}
	base, err := FreezeTurnSnapshot(c.runID, c.sessionID, c.epochID, c.config, c.resolved, priorMessages, c.config.Agent.SystemPrompt, c.host.now())
	if err != nil {
		return nil, err
	}
	base.providerState = priorProviderState
	snapshot, err := c.host.prepareSnapshot(ctx, c.execution, base)
	if err != nil {
		return nil, err
	}
	c.execution.seedDiscovered(discoveredToolsFromMessages(snapshot.Messages))
	if target.Ordinal > c.ordinal {
		c.mu.Lock()
		c.ordinal = target.Ordinal
		c.mu.Unlock()
	}
	return &adkEngine{host: c.host, execution: c.execution, plan: c.plan, snapshot: snapshot, turn: target, assistantMessageID: target.AssistantMessageID}, nil
}

func (c *turnLoopCoordinator) prepareAgent(ctx context.Context, _ *adkTurnLoop, _ []session.InboxID) (adk.TypedAgent[*einoschema.AgenticMessage], error) {
	engine := c.currentEngine()
	if engine == nil {
		return nil, errors.New("runtime: TurnLoop PrepareAgent called with no admitted turn")
	}
	return engine.buildAgent(ctx, &adkApprovalBinding{})
}

// onAgentEvents drains one turn's events. Durability is already committed by
// the model/tool adapters as they run (see adkModel.commit and adkTool); this
// callback's only durable responsibility is to atomically settle the turn on
// normal completion. CancelError/InterruptError are not fatal callback
// errors: the framework (Stop) or a business interrupt handles them, and the
// exit protocol (runtime/turn_loop.go's finishTurnLoop) settles the run.
func (c *turnLoopCoordinator) onAgentEvents(ctx context.Context, _ *adk.TurnContext[session.InboxID, *einoschema.AgenticMessage], events *adk.AsyncIterator[*adk.TypedAgentEvent[*einoschema.AgenticMessage]]) error {
	engine := c.currentEngine()
	interrupted := false
	var callErr error
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			var cancelErr *adk.CancelError
			if errors.As(event.Err, &cancelErr) {
				interrupted = true
				continue
			}
			callErr = errors.Join(callErr, event.Err)
			continue
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			interrupted = true
		}
	}
	if callErr != nil {
		return callErr
	}
	if interrupted || engine == nil {
		return nil
	}
	return c.completeTurn(ctx, engine)
}

func (c *turnLoopCoordinator) completeTurn(ctx context.Context, engine *adkEngine) error {
	event := session.EventRecord{
		ID: c.host.ids.NewEventID(), SessionID: c.sessionID, RunID: c.runID, MessageID: engine.assistantMessageID,
		EpochID: engine.turn.EpochID, TurnID: engine.turn.ID, Kind: session.TurnCompletedEventKind, CreatedAt: c.host.now(),
	}
	result, err := c.execution.store.CompleteTurn(ctx, session.CompleteTurnRequest{
		TurnID: engine.turn.ID, ResponseMessageIDs: engine.responseMessageIDsSnapshot(), Usage: runtimeUsage(engine.usageSnapshot()), Event: event,
	})
	if err != nil {
		return err
	}
	c.execution.publishPersisted(ctx, result.Event)
	return nil
}

// planFingerprint derives the checkpoint envelope fingerprint from the run's
// sealed plan and the concrete AgentFactory type it was built with: a
// resume must never proceed against a different plan or a different
// construction shape than the one the checkpoint was staged under.
func planFingerprint(plan *RunPlan) string {
	factory := plan.AgentFactory()
	return fmt.Sprintf("%s|%T", plan.sealed.Fingerprint(), factory)
}

func newAdkCheckpointStore(host *StreamingOrchestrator, execution *runExecution, plan *RunPlan, runID session.RunID) *adkCheckpointStore {
	return &adkCheckpointStore{host: host, execution: execution, runID: runID, fingerprint: planFingerprint(plan)}
}

// buildTurnLoop constructs the TurnLoopConfig this run's coordinator drives.
func (o *StreamingOrchestrator) buildTurnLoop(coordinator *turnLoopCoordinator, checkpoints *adkCheckpointStore, runID session.RunID) *adkTurnLoop {
	cfg := adk.TurnLoopConfig[session.InboxID, *einoschema.AgenticMessage]{
		GenInput: coordinator.genInput, GenResume: coordinator.genResume, PrepareAgent: coordinator.prepareAgent, OnAgentEvents: coordinator.onAgentEvents,
		Store: checkpoints, CheckpointID: string(runID),
	}
	return adk.NewTurnLoop(cfg)
}

// liveLoop is this process's registry entry for a run's TurnLoop, letting
// Enqueue/Stop/Interrupt reach a loop this process owns without another
// durable round trip.
type liveLoop struct {
	loop        *adkTurnLoop
	coordinator *turnLoopCoordinator
	execution   *runExecution
}

func (o *StreamingOrchestrator) registerLoop(runID session.RunID, entry *liveLoop) {
	o.loopsMu.Lock()
	defer o.loopsMu.Unlock()
	if o.loops == nil {
		o.loops = make(map[session.RunID]*liveLoop)
	}
	o.loops[runID] = entry
}

func (o *StreamingOrchestrator) unregisterLoop(runID session.RunID) {
	o.loopsMu.Lock()
	defer o.loopsMu.Unlock()
	delete(o.loops, runID)
}

func (o *StreamingOrchestrator) liveLoopFor(runID session.RunID) *liveLoop {
	o.loopsMu.Lock()
	defer o.loopsMu.Unlock()
	return o.loops[runID]
}

// runTurnLoop drives one TurnLoop lifetime (fresh Start or ResumeRun) to
// exit and applies the exit protocol. done receives exactly one Result and
// pauseCh receives PauseInfo iff that Result is a durable pause; both
// channels are always closed exactly once before this function returns.
// prepareTurnLoop constructs and registers this run's TurnLoop synchronously
// (before Start/ResumeRun returns a Handle): Handle.Interrupt and
// (*StreamingOrchestrator).Stop/Enqueue look the loop up by run ID
// immediately, and must never race a goroutine that has not yet registered
// it.
func (o *StreamingOrchestrator) prepareTurnLoop(coordinator *turnLoopCoordinator, checkpoints *adkCheckpointStore) *liveLoop {
	loop := o.buildTurnLoop(coordinator, checkpoints, coordinator.runID)
	entry := &liveLoop{loop: loop, coordinator: coordinator, execution: coordinator.execution}
	o.registerLoop(coordinator.runID, entry)
	return entry
}

func (o *StreamingOrchestrator) runTurnLoop(ctx context.Context, entry *liveLoop, checkpoints *adkCheckpointStore, pushIDs []session.InboxID, done chan<- Result, pauseCh chan<- PauseInfo) Result {
	coordinator := entry.coordinator
	loop := entry.loop
	defer o.unregisterLoop(coordinator.runID)
	loop.Run(ctx)
	for _, id := range pushIDs {
		loop.Push(id)
	}
	// UntilIdleFor never touches a running or queued turn -- unlike
	// WithGraceful/WithGracefulTimeout, which are CANCEL modes that
	// interrupt the current turn at its next safe point -- so every pushed
	// item still runs to normal completion. The loop then stops as soon as
	// it goes idle (no pending items) for turnLoopIdleStopDelay, giving
	// Start's one-shot contract (this Start call's own work runs to
	// completion, then the loop exits cleanly) without racing a
	// closely-following Enqueue that lands before the idle timer fires.
	loop.Stop(adk.UntilIdleFor(turnLoopIdleStopDelay))
	state := loop.Wait()
	result := o.finishTurnLoop(ctx, coordinator, checkpoints, state)
	// release() must complete before done/pauseCh are observed by a
	// receiver: it stops this run's plan (its extension notification
	// worker goroutine among other things) and session-observer
	// registration, and a receiver proceeding to inspect durable/observed
	// state concurrently with that teardown is a real, reproducible race
	// (as opposed to the classic engine's executeLifecycle, whose defer
	// order already put release() before the done send).
	coordinator.execution.release()
	if result.Status == session.RunPaused {
		info := PauseInfo{RunID: coordinator.runID, StopCause: state.StopCause}
		var interruptErr *adk.InterruptError
		var cancelErr *adk.CancelError
		if errors.As(state.ExitReason, &interruptErr) {
			info.InterruptContexts = interruptErr.InterruptContexts
		} else if errors.As(state.ExitReason, &cancelErr) {
			info.InterruptContexts = cancelErr.InterruptContexts
		}
		pauseCh <- info
	}
	close(pauseCh)
	done <- result
	close(done)
	return result
}

// finishTurnLoop implements the TurnLoop exit protocol: interrupted (or
// canceled) with a successfully staged checkpoint promotes durable pause
// state; a between-turn stop with queued input promotes a queued-
// continuation pause; a clean idle exit terminally settles the run; a failed
// checkpoint Set never promotes and leaves the run for conservative lease-
// expiry recovery.
func (o *StreamingOrchestrator) finishTurnLoop(ctx context.Context, c *turnLoopCoordinator, checkpoints *adkCheckpointStore, state *adk.TurnLoopExitState[session.InboxID, *einoschema.AgenticMessage]) Result {
	// Every exit path stops this process's lease heartbeat before
	// finishTurnLoop returns (stopLease is idempotent via sync.Once, so the
	// few paths below that also call it explicitly, e.g. before a
	// conservative early return, are harmless): the caller sends the
	// returned Result on the run's done channel immediately afterward, and
	// a still-running heartbeat goroutine would keep calling
	// RenewRunLease/reading execution state concurrently with whatever the
	// receiver does next (observed as a real, reproducible data race under
	// -race on the settled-completion path).
	defer func() { _ = c.execution.stopLease() }()
	settleCtx := context.WithoutCancel(ctx)
	var interruptErr *adk.InterruptError
	var cancelErr *adk.CancelError
	switch {
	// A skip-checkpoint stop (Handle.Interrupt; see turnLoopHandle.Interrupt)
	// is an explicit terminal interruption/abandon, not a resumable pause:
	// no checkpoint was even attempted, so the run terminally settles
	// interrupted (and any staged-but-unpromoted revision is retired).
	case state.ExitReason != nil && !state.CheckpointAttempted && (errors.As(state.ExitReason, &interruptErr) || errors.As(state.ExitReason, &cancelErr)):
		var messageID session.MessageID
		var snapshot TurnSnapshot
		if engine := c.currentEngine(); engine != nil {
			messageID = engine.assistantMessageID
			snapshot = engine.snapshot
		}
		o.observeError(settleCtx, snapshot, messageID, "provider_stream", state.ExitReason)
		settlement := session.RunSettlement{Status: session.RunInterrupted, FinishedAt: o.now(), Error: state.ExitReason.Error()}
		committed, err := c.execution.store.SettleRun(settleCtx, session.SettleRunRequest{
			Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: messageID},
		})
		if err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunFailed, Error: errors.Join(state.ExitReason, err)}
		}
		c.execution.publishPersisted(settleCtx, committed.Event)
		result := Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, MessageID: messageID, Error: state.ExitReason}
		c.publishRunSettledNotice(settleCtx, result)
		return result
	// A checkpoint Set failure is conservative: no promotion, and the run
	// stays running (lease left to expire) so recovery cannot race a
	// partially-written revision.
	case state.ExitReason != nil && state.CheckpointAttempted && state.CheckpointErr != nil && (errors.As(state.ExitReason, &interruptErr) || errors.As(state.ExitReason, &cancelErr)):
		_ = c.execution.stopLease()
		return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: errors.Join(state.ExitReason, state.CheckpointErr)}
	case state.ExitReason != nil && (errors.As(state.ExitReason, &interruptErr) || errors.As(state.ExitReason, &cancelErr)):
		engine := c.currentEngine()
		if engine == nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: state.ExitReason}
		}
		var late []session.InboxID
		if state.TakeLateItems != nil {
			late = state.TakeLateItems()
		}
		inboxIDs := append(interruptedItemIDs(state.InterruptedItems), interruptedItemIDs(state.UnhandledItems)...)
		inboxIDs = append(inboxIDs, interruptedItemIDs(late)...)
		event := session.EventRecord{
			ID: o.ids.NewEventID(), SessionID: c.sessionID, RunID: c.runID, EpochID: c.epochID, TurnID: engine.turn.ID,
			Kind: session.RunPausedEventKind, CreatedAt: o.now(),
		}
		_, err := c.execution.store.PromotePause(settleCtx, session.PromotePauseRequest{
			Revision: checkpoints.lastStaged, TurnID: engine.turn.ID, InboxIDs: inboxIDs, Event: event,
		})
		if err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: errors.Join(state.ExitReason, err)}
		}
		return Result{RunID: c.runID, Status: session.RunPaused, Interrupted: true, Error: state.ExitReason}
	case state.ExitReason == nil && len(state.UnhandledItems) != 0 && state.CheckpointAttempted && state.CheckpointErr == nil:
		if err := o.promoteQueuedContinuation(settleCtx, c, checkpoints); err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: err}
		}
		return Result{RunID: c.runID, Status: session.RunPaused}
	case state.ExitReason != nil:
		_ = c.execution.stopLease()
		var messageID session.MessageID
		var snapshot TurnSnapshot
		if engine := c.currentEngine(); engine != nil {
			messageID = engine.assistantMessageID
			snapshot = engine.snapshot
		}
		o.observeError(settleCtx, snapshot, messageID, "provider_stream", state.ExitReason)
		status := session.RunFailed
		// A provider (or context) reporting cancellation is a domain
		// interruption, not an operational failure, matching the classic
		// engine's statusForError convention -- regardless of whether the
		// cancellation reached ADK as a *CancelError/*InterruptError or as
		// an ordinary wrapped context.Canceled error from a physical call.
		if errors.Is(state.ExitReason, context.Canceled) {
			status = session.RunInterrupted
		}
		settlement := session.RunSettlement{Status: status, FinishedAt: o.now(), Error: state.ExitReason.Error()}
		committed, err := c.execution.store.SettleRun(settleCtx, session.SettleRunRequest{
			Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: messageID},
		})
		if err != nil {
			return Result{RunID: c.runID, Status: session.RunFailed, Error: errors.Join(state.ExitReason, err)}
		}
		c.execution.publishPersisted(settleCtx, committed.Event)
		result := Result{RunID: c.runID, Status: status, Interrupted: status == session.RunInterrupted, MessageID: messageID, Error: state.ExitReason}
		c.publishRunSettledNotice(settleCtx, result)
		return result
	default:
		var messageID session.MessageID
		var usage model.Usage
		if engine := c.currentEngine(); engine != nil {
			usage = engine.usageSnapshot()
			if ids := engine.responseMessageIDsSnapshot(); len(ids) != 0 {
				messageID = ids[len(ids)-1]
			} else {
				messageID = engine.assistantMessageID
			}
		}
		settlement := session.RunSettlement{Status: session.RunCompleted, FinishedAt: o.now()}
		committed, err := c.execution.store.SettleRun(settleCtx, session.SettleRunRequest{
			Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: messageID},
		})
		if err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunFailed, Error: err}
		}
		c.execution.publishPersisted(settleCtx, committed.Event)
		if checkpoints.lastStaged > 0 {
			_ = o.store.RetireRunCheckpoints(settleCtx, c.runID, checkpoints.lastStaged)
		}
		result := Result{RunID: c.runID, Status: session.RunCompleted, MessageID: messageID, Usage: runtimeUsage(usage)}
		c.publishRunSettledNotice(settleCtx, result)
		return result
	}
}

// promoteQueuedContinuation records a between-turn stop with queued durable
// input as a nonterminal pause with no runner state: the queued inbox items
// stay 'queued' (untouched -- a resume plans them through GenInput, never
// GenResume) and a degenerate, content-free turn is admitted purely to carry
// PromotePause's required turn identity, then immediately interrupted.
func (o *StreamingOrchestrator) promoteQueuedContinuation(ctx context.Context, c *turnLoopCoordinator, checkpoints *adkCheckpointStore) error {
	engine, err := c.admitTurn(ctx, nil)
	if err != nil {
		return err
	}
	event := session.EventRecord{
		ID: o.ids.NewEventID(), SessionID: c.sessionID, RunID: c.runID, EpochID: c.epochID, TurnID: engine.turn.ID,
		Kind: session.RunPausedEventKind, CreatedAt: o.now(),
	}
	_, err = c.execution.store.PromotePause(ctx, session.PromotePauseRequest{
		Revision: checkpoints.lastStaged, TurnID: engine.turn.ID, Event: event,
	})
	return err
}

func interruptedItemIDs(items []session.InboxID) []session.InboxID {
	return append([]session.InboxID(nil), items...)
}

// publishRunSettledNotice notifies RunSettledPoint for a terminal Result,
// mirroring the legacy engine's executeLifecycle. It is a best-effort
// metadata projection: a coordinator does not track the run's admission
// time or exact turn metadata the way the classic lifecycle did, so
// Duration is left zero when unknown and Metadata comes from the most
// recently prepared turn snapshot, if any.
func (c *turnLoopCoordinator) publishRunSettledNotice(ctx context.Context, result Result) {
	var metadata BoundedTurnMetadata
	if engine := c.currentEngine(); engine != nil {
		metadata = boundedTurnMetadata(engine.snapshot)
	}
	extension.Notify(c.execution.dispatch(), context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{
		SessionID: c.sessionID, Result: result, Metadata: metadata, Error: classifyExtensionError(result.Error),
	})
}

// turnLoopHandle is the Handle implementation for a run driven by a
// TurnLoop (both a fresh Start and a ResumeRun use it).
type turnLoopHandle struct {
	runID  session.RunID
	host   *StreamingOrchestrator
	done   chan Result
	pause  chan PauseInfo
	cancel context.CancelFunc
}

func (h *turnLoopHandle) RunID() session.RunID         { return h.runID }
func (h *turnLoopHandle) Done() <-chan Result          { return h.done }
func (h *turnLoopHandle) AwaitPause() <-chan PauseInfo { return h.pause }

func (h *turnLoopHandle) Status(ctx context.Context) (session.Run, error) {
	return h.host.store.GetRun(ctx, h.runID)
}

// Interrupt maps to an immediate, skip-checkpoint stop of this process's
// live loop for the run: an explicit terminal interruption (Handle.Interrupt
// has never promised a resumable pause), settled RunInterrupted by the exit
// protocol rather than promoted as a durable pause. Hosts that want a
// resumable pause instead should use Stop (graceful or immediate, both
// checkpointed) and ResumeRun. A run with no live loop in this process (for
// example, already paused, or owned by a different process) reports
// ErrInvalidOrchestrator.
func (h *turnLoopHandle) Interrupt(_ context.Context, reason string) error {
	// Cancel the run's own context unconditionally and immediately: this
	// must win even when it races ahead of the driving goroutine calling
	// loop.Run (TurnLoop's Stop is a no-op signal until Run observes it --
	// "a subsequent Run will exit immediately" only helps if Run has not
	// yet dispatched a blocking provider call). The loop.Stop call below is
	// still issued for its own protocol effects (skip-checkpoint exit
	// classification, StopCause) whenever a live loop is registered.
	if h.cancel != nil {
		h.cancel()
	}
	entry := h.host.liveLoopFor(h.runID)
	if entry == nil {
		if h.cancel != nil {
			return nil
		}
		return fmt.Errorf("%w: run %s has no live loop in this process", ErrInvalidOrchestrator, h.runID)
	}
	entry.loop.Stop(adk.WithImmediate(), adk.WithSkipCheckpoint(), adk.WithStopCause(reason))
	return nil
}

// StopPolicy configures (*StreamingOrchestrator).Stop.
type StopPolicy struct {
	// Graceful lets the current turn (and any queued input) complete before
	// stopping. Immediate cancels recursively right away. Exactly one
	// should be set; Immediate wins if both are.
	Graceful  bool
	Immediate bool
	Timeout   time.Duration
	Cause     string
}

// Stop signals the process-local live loop for runID to stop, per policy.
// It reports ErrInvalidOrchestrator if this process has no live loop for
// runID (for example, it already exited, or is owned by another process).
func (o *StreamingOrchestrator) Stop(_ context.Context, runID session.RunID, policy StopPolicy) error {
	entry := o.liveLoopFor(runID)
	if entry == nil {
		return fmt.Errorf("%w: run %s has no live loop in this process", ErrInvalidOrchestrator, runID)
	}
	opts := []adk.StopOption{}
	if policy.Cause != "" {
		opts = append(opts, adk.WithStopCause(policy.Cause))
	}
	switch {
	case policy.Immediate:
		opts = append(opts, adk.WithImmediate())
	case policy.Graceful:
		opts = append(opts, adk.WithGraceful())
		if policy.Timeout > 0 {
			opts = append(opts, adk.WithGracefulTimeout(policy.Timeout))
		}
	}
	entry.loop.Stop(opts...)
	return nil
}

// EnqueueRequest submits one durable, idempotent user input for a session's
// live or paused run.
type EnqueueRequest struct {
	RunID          session.RunID
	IdempotencyKey string
	Message        UserMessage
}

// Enqueue persists request as a durable inbox item first (idempotent on
// IdempotencyKey), then pushes its ID into this process's live loop for
// RunID, if one is registered. A durably persisted item that cannot be
// pushed (no live loop in this process) stays queued for a later Start,
// Enqueue against a process that does own the loop, or ResumeRun to pick up.
func (o *StreamingOrchestrator) Enqueue(ctx context.Context, sessionID session.ID, request EnqueueRequest) (session.InboxItem, error) {
	if err := o.validateConfigured(); err != nil {
		return session.InboxItem{}, err
	}
	if request.IdempotencyKey == "" {
		return session.InboxItem{}, fmt.Errorf("%w: idempotency key required", ErrInvalidOrchestrator)
	}
	blocks, err := assignContentBlockIDs(request.Message.Blocks, o.ids)
	if err != nil {
		return session.InboxItem{}, err
	}
	now := o.now()
	item := session.InboxItem{
		ID: o.ids.NewInboxID(), SessionID: sessionID, IdempotencyKey: request.IdempotencyKey,
		Blocks: blocks, State: session.InboxQueued, CreatedAt: now, UpdatedAt: now,
	}
	persisted, err := o.store.EnqueueInbox(ctx, item, o.contentLimits)
	if err != nil {
		return session.InboxItem{}, err
	}
	if entry := o.liveLoopFor(request.RunID); entry != nil {
		entry.loop.Push(persisted.ID)
	}
	return persisted, nil
}

// ResumeRequest resumes a durably paused run.
type ResumeRequest struct {
	// Targets maps a current-generation InterruptCtx.ID (see PauseInfo) to
	// its host-supplied resume data.
	Targets map[string]any
}

// ResumeRun claims a paused run's fence (CAS on status=paused), validates
// its promoted checkpoint envelope, rebuilds the run's engine, and drives a
// fresh TurnLoop seeded from the promoted checkpoint bytes.
func (o *StreamingOrchestrator) ResumeRun(ctx context.Context, runID session.RunID, request ResumeRequest) (Handle, error) {
	if err := o.validateConfigured(); err != nil {
		return nil, err
	}
	run, err := o.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if !run.Paused() {
		return nil, fmt.Errorf("%w: run %s is not paused", ErrInvalidOrchestrator, runID)
	}
	claimToken := string(o.ids.NewEventID())
	claimed, err := o.store.ClaimRun(ctx, session.RunClaim{RunID: runID, OwnerID: o.ownerID(), ClaimToken: claimToken, LeaseDuration: o.lease()})
	if err != nil {
		return nil, err
	}
	plan, err := o.acquireResumePlan(ctx, claimed.SessionID, claimed.ExtensionPlan.Clone())
	if err != nil {
		return nil, err
	}
	ownershipTransferred := false
	defer func() {
		if !ownershipTransferred {
			plan.release()
		}
	}()
	checkpoint, ok, err := o.store.ReadPromotedCheckpoint(ctx, runID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: run %s has no promoted checkpoint", ErrInvalidOrchestrator, runID)
	}
	fingerprint := planFingerprint(plan)
	if checkpoint.AgentFingerprint != fingerprint || checkpoint.EinoVersion != EinoPinnedVersion || checkpoint.CodecVersion != adkCheckpointCodecVersion {
		return nil, ErrCheckpointFingerprintMismatch
	}
	execution := newRunExecution(o, plan, claimed)
	selection := model.Selection{ProviderID: model.ProviderID(claimed.ProviderID), ModelID: model.ID(claimed.ModelID)}
	resolved, err := o.model.Resolve(ctx, selection, model.Runtime{
		Directory: claimed.Config["workspace_root"],
	})
	if err != nil {
		return nil, err
	}
	cfg := config.Snapshot{Agent: config.Agent{Name: claimed.Agent, Model: selection}, Metadata: map[string]string{
		"workspace_id": claimed.Config["workspace_id"], "workspace_root": claimed.Config["workspace_root"],
	}}
	turns, err := o.store.ListTurns(ctx, runID)
	if err != nil {
		return nil, err
	}
	var maxOrdinal int64
	for _, t := range turns {
		if t.Ordinal > maxOrdinal {
			maxOrdinal = t.Ordinal
		}
	}
	coordinator := &turnLoopCoordinator{
		host: o, execution: execution, plan: plan, sessionID: claimed.SessionID, runID: claimed.ID,
		config: cfg, resolved: resolved, historyOptions: o.history, epochID: claimed.ContextEpoch, ordinal: maxOrdinal,
	}
	coordinator.setResumeTargets(request.Targets)
	checkpoints := newAdkCheckpointStore(o, execution, plan, runID)
	handle := &turnLoopHandle{runID: runID, host: o, done: make(chan Result, 1), pause: make(chan PauseInfo, 1)}
	entry := o.prepareTurnLoop(coordinator, checkpoints)
	ownershipTransferred = true
	go func() {
		started, err := execution.store.StartRun(ctx, o.now())
		if err != nil {
			o.unregisterLoop(runID)
			handle.done <- Result{RunID: runID, Status: session.RunFailed, Error: err}
			close(handle.done)
			close(handle.pause)
			execution.release()
			return
		}
		_ = started
		runCtx := execution.startLease(ctx, o.lease())
		defer execution.release()
		o.runTurnLoop(runCtx, entry, checkpoints, nil, handle.done, handle.pause)
	}()
	return handle, nil
}
