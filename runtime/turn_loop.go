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
	// checkpoints is this run's checkpoint store adapter, set once right
	// after construction (see Start/ResumeRun's own construction sites).
	// setEngine keeps its currentTurnID in sync with whichever turn is
	// currently being driven, so every checkpoint revision it stages
	// records the turn it actually belongs to (round-five reconciliation
	// item 2/TR-I1).
	checkpoints *adkCheckpointStore

	mu                sync.Mutex
	ordinal           int64
	engine            *adkEngine
	resumeTargets     map[string]any
	resumeLifecycle   *resumeLifecycleFact
	firstTurnEngine   *adkEngine
	firstTurnConsumed bool
	// admittedItems is every durable inbox ID this coordinator has already
	// handed to admitTurn, across every GenInput call for the life of this
	// loop. genInput's claimItems is the single funnel every item passes
	// through before admission, so this is the in-process backstop against
	// double delivery: ADK's own checkpoint restores a buffered-but-
	// unconsumed item's ID as UnhandledItems, which ResumeRun's
	// drainQueuedInbox also pushes (both correctly, given the item-5/ED-3
	// fix leaves such an item durably `queued` through a pause); an
	// idempotent Enqueue retry against a live loop can likewise re-push an
	// ID already buffered or already admitted. Guarded by mu (reusing the
	// coordinator's existing lock rather than a dedicated one).
	admittedItems map[session.InboxID]bool

	// runUsageMu guards runUsage, the accumulated usage of every turn this
	// coordinator's completeTurn has durably settled -- kept distinct from
	// any single adkEngine.usageSnapshot() so a multi-turn run's terminal
	// settlement (finishTurnLoop) reports every turn's usage exactly once,
	// never just the last turn's (see completeTurn/finishedRunUsage).
	runUsageMu sync.Mutex
	runUsage   model.Usage
}

func (c *turnLoopCoordinator) addRunUsage(u model.Usage) {
	c.runUsageMu.Lock()
	defer c.runUsageMu.Unlock()
	c.runUsage = addUsage(c.runUsage, u)
}

func (c *turnLoopCoordinator) runUsageSnapshot() model.Usage {
	c.runUsageMu.Lock()
	defer c.runUsageMu.Unlock()
	return c.runUsage
}

// finishedRunUsage returns every already-completed turn's usage
// (runUsageSnapshot) plus, when engine is non-nil and its turn never reached
// completeTurn (an interrupted or failed exit -- onAgentEvents only calls
// completeTurn on normal completion), that turn's own not-yet-folded usage.
// A clean RunCompleted exit must NOT pass its engine here: completeTurn
// already folded it in via onAgentEvents, and adding it again would double-
// count the last turn.
func (c *turnLoopCoordinator) finishedRunUsage(engine *adkEngine) model.Usage {
	total := c.runUsageSnapshot()
	if engine != nil {
		total = addUsage(total, engine.usageSnapshot())
	}
	return total
}

// firstTurnSentinelID is pushed exactly once by Start to hand TurnLoop the
// already-admitted first turn (admitted atomically with the run itself, see
// admission.go's admitDurable) instead of re-admitting it through the
// normal GenInput -> AdmitTurn path.
const firstTurnSentinelID session.InboxID = "\x00first-turn"

// reconciledTurnSentinelID is pushed exactly once by ResumeRun (see
// pushReconciledSentinel and currentTurn) to hand TurnLoop a run's most
// recently crash-reconciled interrupted turn (round-four reconciliation item 1/
// CR-C1) instead of admitting a fresh turn over new inbox items: it is only
// ever pushed when the promoted checkpoint's own payload has no real ADK
// runner state (decodeLoopCheckpointHasRunnerState), so it is guaranteed to
// reach GenInput, never GenResume -- see resumeReconciledTurn's doc comment.
// It is guaranteed to be somewhere in the batch GenInput sees, NOT
// guaranteed to be items[0]: upstream's tryLoadCheckpoint builds that batch
// as cp.UnhandledItems ++ newItems, and this package only ever prepends the
// sentinel to the newItems half it controls (round-five reconciliation item
// 1/TR-C1) -- genInput scans the whole batch for it.
const reconciledTurnSentinelID session.InboxID = "\x00reconciled-turn"

// indexOf returns the index of the first occurrence of target in ids, or -1
// if target is not present.
func indexOf(ids []session.InboxID, target session.InboxID) int {
	for i, id := range ids {
		if id == target {
			return i
		}
	}
	return -1
}

// currentTurn is the single selector shared by crash reconciliation
// (interrupt.go's reconcileCrashedRun), RepauseRun/PromoteRevision staging,
// and ResumeRun's checkpoint-TurnID validation below (round-six
// reconciliation items 1-3): a run's "current" turn is the newest turn
// (highest ordinal) whose state is not TurnCompleted or TurnFailed --
// TurnAdmitted, TurnRunning, or TurnInterrupted all qualify, covering both a
// genuine in-flight dispatch a crash left dangling and an already-durably-
// paused leaf (a genuine ADK tool-interrupt pause, or a content-free
// queued-continuation carrier -- see promoteQueuedContinuation). Every
// checkpoint envelope this package ever stages records currentTurn's ID at
// staging time (adkCheckpointStore.currentTurnID, kept in sync by setEngine
// and every explicit setCurrentTurnID call), so a promoted checkpoint's
// TurnID and this selector's output agree by construction on any run
// reconciliation has finished with -- ResumeRun's own consistency check
// below is a defensive assertion that can never fire after reconciliation,
// not the primary mechanism.
func currentTurn(turns []session.Turn) session.Turn {
	var current session.Turn
	for _, t := range turns {
		if t.State == session.TurnCompleted || t.State == session.TurnFailed {
			continue
		}
		if t.Ordinal >= current.Ordinal {
			current = t
		}
	}
	return current
}

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
	if e != nil {
		e.resumeLifecycle = c.resumeLifecycle
	}
	// Keep the checkpoint store's currentTurnID in sync with whichever
	// turn's engine is now live: every Set call upstream ADK makes from
	// here on -- a periodic tool-boundary checkpoint or a genuine
	// tool-interrupt pause -- stages an envelope recording THIS turn
	// (round-five reconciliation item 2/TR-I1).
	if c.checkpoints != nil && e != nil {
		c.checkpoints.setCurrentTurnID(e.turn.ID)
	}
}

func (c *turnLoopCoordinator) setResumeTargets(targets map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeTargets = targets
}

// claimItems partitions ids into (a) the sentinel, which is never a durable
// inbox row and must never reach admitTurn/PromotePause (see
// firstTurnSentinelID's doc comment), and (b) IDs this coordinator has
// already admitted in a prior GenInput call this loop's lifetime -- dropped
// silently rather than admitted again (see admittedItems's doc comment).
// Every remaining ID is recorded admitted and returned. Dedup here alone is
// not sufficient for the cross-GenInput-call race (two separate calls each
// see an empty local batch), which is why this claims against the
// coordinator-wide admittedItems set, not just within one call's items
// slice; admitTurn's own loadInboxItems state filter is the further,
// durable-state backstop for a delivery this in-process map cannot see
// (e.g. across a process restart).
func (c *turnLoopCoordinator) claimItems(ids []session.InboxID) []session.InboxID {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.admittedItems == nil {
		c.admittedItems = make(map[session.InboxID]bool, len(ids))
	}
	out := make([]session.InboxID, 0, len(ids))
	for _, id := range ids {
		if id == firstTurnSentinelID || id == reconciledTurnSentinelID || c.admittedItems[id] {
			continue
		}
		c.admittedItems[id] = true
		out = append(out, id)
	}
	return out
}

// loadInboxItems fetches the durable content for a set of inbox IDs. It
// scans ListInbox (unbounded by state) since the inbox contract does not
// expose a single-ID lookup; inbox lists are session-scoped and bounded by
// normal conversational volume.
//
// An ID whose durable state is not InboxQueued is silently dropped rather
// than admitted a second time or treated as an error: this is the
// durable-state backstop for a duplicate delivery genInput's
// admittedItems dedup cannot see (a process restart between two deliveries
// of the same checkpoint-restored/re-enqueued ID -- admittedItems is
// in-memory only). An ID with no row at all is still a genuine error: that
// is a real data-integrity problem, not a duplicate-delivery race.
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
		if item.State != session.InboxQueued {
			continue
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
	// turnID is minted before this turn's user messages so every message
	// this admission durably commits (user and assistant placeholder alike)
	// can be stamped with the same durable turn identity (see
	// session.Message.TurnID).
	turnID := c.host.ids.NewTurnID()
	// consumedIDs is derived from items (already filtered to InboxQueued
	// rows by loadInboxItems), never from the raw itemIDs parameter: this
	// keeps userMessages/userParts/InboxIDs consistent by construction, so
	// an ID loadInboxItems silently dropped (already consumed by an earlier
	// delivery) is never passed to AdmitTurn's InboxIDs either.
	consumedIDs := make([]session.InboxID, 0, len(items))
	for _, item := range items {
		msgID := c.host.ids.NewMessageID()
		content := session.Content{Role: session.RoleUser, Blocks: item.Blocks}
		parts, err := session.EncodeContentParts(content, func() session.PartID { return c.host.ids.NewPartID() }, msgID, c.sessionID, c.runID, at, c.host.contentLimits)
		if err != nil {
			return nil, err
		}
		userMessages = append(userMessages, session.Message{ID: msgID, SessionID: c.sessionID, RunID: c.runID, Role: session.RoleUser, TurnID: turnID, CreatedAt: at, UpdatedAt: at})
		userParts = append(userParts, parts...)
		userMessageIDs = append(userMessageIDs, msgID)
		consumedIDs = append(consumedIDs, item.ID)
		at = at.Add(time.Nanosecond)
	}
	assistantID := c.host.ids.NewMessageID()
	assistantAt := at
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
		Agent: c.config.Agent.Name, ModelID: string(c.resolved.Model.ID), TurnID: turnID, CreatedAt: assistantAt, UpdatedAt: assistantAt,
	}
	result, err := c.execution.store.AdmitTurn(ctx, session.AdmitTurnRequest{
		Turn: turn, UserMessages: userMessages, UserParts: userParts, AssistantPlaceholder: assistantMessage, Event: event, InboxIDs: consumedIDs,
	})
	if err != nil {
		return nil, err
	}
	c.execution.publishPersisted(ctx, result.Event)
	// AdmitTurn has already durably committed this turn's user message(s),
	// so reloading now already includes them -- no need to reconstruct and
	// append them again from the inbox items. dropUnfinalizedAssistantPlaceholders
	// removes the still-empty assistant placeholder row AdmitTurn also just
	// created (real, but content-free until adkModel.commit finalizes it),
	// the same way adkModel.durableProjection does for every later dispatch.
	// Resolve fresh for THIS turn admission, from c.historyOptions' pristine
	// (never-mutated) host template: a summarization epoch committed by an
	// earlier turn on this same run must narrow this new turn's admission,
	// while c.historyOptions itself stays unresolved so the NEXT turn after
	// this one re-resolves independently too -- see
	// resolveTurnHistoryOptions's doc comment.
	turnHistoryOptions, err := resolveTurnHistoryOptions(ctx, c.host.store, c.sessionID, c.historyOptions)
	if err != nil {
		return nil, err
	}
	priorMessages, priorSourceIDs, priorProviderState, err := loadProviderHistory(ctx, c.host.store, session.Session{ID: c.sessionID}, turnHistoryOptions, c.resolved)
	if err != nil {
		return nil, err
	}
	allMessages, allSourceIDs, priorProviderState := dropUnfinalizedAssistantPlaceholders(priorMessages, priorSourceIDs, priorProviderState)
	base, err := FreezeTurnSnapshot(c.runID, c.sessionID, c.epochID, turnID, c.config, c.resolved, allMessages, c.config.Agent.SystemPrompt, c.host.now())
	if err != nil {
		return nil, err
	}
	base.MessageSourceIDs = allSourceIDs
	base.providerState = priorProviderState
	snapshot, err := c.host.prepareSnapshot(ctx, c.execution, base)
	if err != nil {
		return nil, err
	}
	c.execution.seedDiscovered(discoveredToolsFromMessages(snapshot.Messages))
	engine := &adkEngine{host: c.host, execution: c.execution, plan: c.plan, snapshot: snapshot, turn: result.Turn, assistantMessageID: assistantID, historyOptions: turnHistoryOptions, baseMessageCount: len(allMessages)}
	c.setEngine(engine)
	return engine, nil
}

func (c *turnLoopCoordinator) genInput(ctx context.Context, loop *adkTurnLoop, items []session.InboxID) (*adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage], error) {
	if len(items) == 0 {
		return nil, errors.New("runtime: TurnLoop GenInput called with no buffered items")
	}
	if !c.firstTurnConsumed && c.firstTurnEngine != nil && items[0] == firstTurnSentinelID {
		c.firstTurnConsumed = true
		engine := c.firstTurnEngine
		c.setEngine(engine)
		return &adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage]{
			// EnableStreaming routes every physical model dispatch through
			// adkModel.Stream, not Generate: only Stream wires a live onDelta
			// callback into dispatch()'s receive loop (session observer
			// AppendText + EventMessageDelta), which is what lets a watcher
			// see partial text while a provider chunk is still in flight, not
			// only after the whole physical call returns. The classic engine
			// always streamed every dispatch; this restores that invariant
			// for the typed engine instead of silently defaulting to
			// Generate's whole-call-at-once behavior (see
			// TestPublicSessionWatchConstructionExecutionAndReopen).
			Input:     &adk.TypedAgentInput[*einoschema.AgenticMessage]{Messages: engine.snapshot.Messages, EnableStreaming: true},
			Consumed:  items[:1],
			Remaining: items[1:],
		}, nil
	}
	// reconciledTurnSentinelID is only ever pushed by ResumeRun when it has
	// already confirmed (decodeLoopCheckpointHasRunnerState) that this
	// resume's promoted checkpoint has no real ADK runner state, so this
	// call is guaranteed to be the one GenInput call ADK makes this Run()
	// -- never raced by GenResume (round-four reconciliation item 1/CR-C1).
	// See resumeReconciledTurn's doc comment for the full redrive contract.
	//
	// The sentinel is NOT guaranteed to land at items[0]: upstream's
	// tryLoadCheckpoint builds this batch as cp.UnhandledItems ++ newItems
	// (eino@v0.9.19/adk/turn_loop.go), and ResumeRun only ever prepends the
	// sentinel to the newItems half it pushes (see pushReconciledSentinel).
	// A promoted checkpoint carrying its own UnhandledItems -- the shape
	// promoteQueuedContinuation produces for a graceful stop with queued
	// input -- puts those ids ahead of the sentinel, so this scans the
	// whole batch rather than checking only the first id (round-five
	// reconciliation item 1/TR-C1).
	if sentinelAt := indexOf(items, reconciledTurnSentinelID); sentinelAt >= 0 {
		engine, err := c.resumeReconciledTurn(ctx)
		if err != nil {
			return nil, err
		}
		rest := make([]session.InboxID, 0, len(items)-1)
		rest = append(rest, items[:sentinelAt]...)
		rest = append(rest, items[sentinelAt+1:]...)
		return &adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage]{
			Input:     &adk.TypedAgentInput[*einoschema.AgenticMessage]{Messages: engine.snapshot.Messages, EnableStreaming: true},
			Consumed:  []session.InboxID{reconciledTurnSentinelID},
			Remaining: rest,
		}, nil
	}
	// A leftover first-turn sentinel here has no durable inbox row (Start's
	// first turn is admitted directly inside the admission transaction --
	// see admission.go's admitDurable -- never through the inbox), so it
	// must never be looked up. It can reach this fallback branch on a
	// coordinator that never had (or already consumed) a firstTurnEngine --
	// a ResumeRun coordinator resuming a checkpoint taken before the very
	// first GenInput ever ran (a Stop landing before dispatch; see the W5
	// doc's deviation list). claimItems filters it out (along with any ID
	// this coordinator already admitted -- see its own doc comment: ADK's
	// checkpoint restoring a still-`queued` item's ID as UnhandledItems
	// while ResumeRun's drainQueuedInbox also pushes it, or an idempotent
	// Enqueue retry re-pushing an ID already buffered, would otherwise mint
	// two durable user messages for one inbox item, or -- when the copies
	// land in different GenInput calls -- fail the second admission outright
	// once the first has already consumed it). The turn a filtered sentinel
	// would have started is already durably visible via the committed
	// history admitDurable wrote, so a fresh admitTurn over the remaining
	// (possibly empty) items still answers it -- but the sentinel stays in
	// Consumed so TurnLoop's own item partition stays total.
	filtered := c.claimItems(items)
	// hadRealDuplicate is true only when this batch's emptiness came from
	// claimItems' admittedItems dedup dropping an id already admitted by an
	// earlier GenInput call in this coordinator's lifetime -- NOT from the
	// unconditional sentinel filter (a batch that is only the first-turn
	// sentinel legitimately empties filtered on a resumed coordinator's
	// very first GenInput call, e.g. a between-turn stop landing before
	// the run's first dispatch ever happened; that is the existing,
	// correct admitTurn(ctx, nil) degenerate-turn path below, exercised by
	// TestResumeRunAfterStopBeforeFirstDispatchAgainstSQLite, and must not
	// be confused with a genuine duplicate delivery).
	hadRealDuplicate := false
	if len(filtered) == 0 {
		for _, id := range items {
			if id != firstTurnSentinelID && id != reconciledTurnSentinelID {
				hadRealDuplicate = true
				break
			}
		}
	} else {
		// Durable-state backstop (round-four reconciliation item 2/CR-I1):
		// claimItems' in-process admittedItems dedup only sees this ONE
		// coordinator's lifetime, so a fresh coordinator across a process
		// restart (or a checkpoint-restored UnhandledItems batch replayed
		// into a brand-new coordinator) cannot recognize an id its own
		// earlier delivery already durably admitted -- filtered stays
		// non-empty here even though every id in it is stale. Left
		// unchecked, admitTurn below would still run: its own
		// loadInboxItems silently drops every non-`queued` id, but
		// admitTurn does not decline just because its resulting batch is
		// empty -- it happily mints a real, content-free turn and TurnLoop
		// dispatches the model again for content already durably
		// committed by whichever delivery actually admitted it first (the
		// exact dispatch #4 CR-I1 reproduced). Check BEFORE calling
		// admitTurn, not after: if NONE of filtered is still durably
		// queued, this is the same "nothing new to do" case as the
		// in-process check above.
		genuine, err := c.loadInboxItems(ctx, filtered)
		if err != nil {
			return nil, err
		}
		if len(genuine) == 0 {
			hadRealDuplicate = true
		}
	}
	if hadRealDuplicate {
		// Every non-sentinel id in this batch was already admitted earlier
		// in this coordinator's lifetime (claimItems's own dedup): a
		// duplicate delivery that still split across two GenInput calls
		// despite the push-before-Run ordering above (e.g. an idempotent
		// Enqueue retry re-pushing an id already buffered/admitted). Do
		// NOT call admitTurn: with no new content it would mint a
		// content-free turn and TurnLoop would still dispatch the model a
		// second time for content the earlier call already durably
		// committed (SR-2/RD-1 "belt and braces", round-three
		// reconciliation item 2).
		//
		// planTurn requires a non-nil Input from every GenInput call, so
		// this cannot simply decline to run a turn -- but the run loop
		// re-checks stopCtrl.isCommitted() immediately after this call
		// returns and, when true, discards whatever this result says and
		// pushes the ORIGINAL raw items back to the front of the buffer
		// instead of ever calling PrepareAgent (see eino's
		// adk/turn_loop.go run()). Stop the loop synchronously here so
		// that happens: no dispatch occurs, and the pushed-back ids
		// surface as a between-turn pause with queued input
		// (finishTurnLoop's UnhandledItems branch), not a spurious
		// dispatch. A later resume's drainQueuedInbox simply will not
		// find these ids again once they are not InboxQueued anymore
		// (already consumed by whichever call actually admitted them).
		//
		// A fresh coordinator can legitimately have no engine at all here
		// (e.g. a resume whose only batch is an id a durable-state check
		// -- not this coordinator -- already admitted, with no prior
		// GenInput call in this coordinator's own lifetime): that is the
		// same "nothing new to do" outcome as the case above with a live
		// engine, not an error. Failing the run outright would turn a
		// stale/duplicate delivery into a hard RunFailed with no answer at
		// all (round-five reconciliation item 1/TR-C1); fall back to an
		// empty input instead, which the run loop discards the same way
		// once isCommitted() is true after Stop below.
		input := &adk.TypedAgentInput[*einoschema.AgenticMessage]{EnableStreaming: true}
		if engine := c.currentEngine(); engine != nil {
			input.Messages = engine.snapshot.Messages
		}
		loop.Stop(adk.WithStopCause("duplicate-delivery-noop"))
		return &adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage]{
			Input:    input,
			Consumed: items,
		}, nil
	}
	engine, err := c.admitTurn(ctx, filtered)
	if err != nil {
		return nil, err
	}
	return &adk.GenInputResult[session.InboxID, *einoschema.AgenticMessage]{
		Input:    &adk.TypedAgentInput[*einoschema.AgenticMessage]{Messages: engine.snapshot.Messages, EnableStreaming: true},
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
	turnHistoryOptions, err := resolveTurnHistoryOptions(ctx, c.host.store, c.sessionID, c.historyOptions)
	if err != nil {
		return nil, err
	}
	priorMessages, priorSourceIDs, priorProviderState, err := loadProviderHistory(ctx, c.host.store, session.Session{ID: c.sessionID}, turnHistoryOptions, c.resolved)
	if err != nil {
		return nil, err
	}
	// The interrupted turn's own assistant placeholder (or a later
	// tool-turn continuation message minted before the interruption) may
	// still be an unfinalized, content-free row; keep this in sync with
	// adkModel.durableProjection's later filtering so baseMessageCount below
	// stays consistent with what a fresh reload will show.
	priorMessages, priorSourceIDs, priorProviderState = dropUnfinalizedAssistantPlaceholders(priorMessages, priorSourceIDs, priorProviderState)
	base, err := FreezeTurnSnapshot(c.runID, c.sessionID, c.epochID, target.ID, c.config, c.resolved, priorMessages, c.config.Agent.SystemPrompt, c.host.now())
	if err != nil {
		return nil, err
	}
	base.MessageSourceIDs = priorSourceIDs
	base.providerState = priorProviderState
	snapshot, err := c.host.prepareSnapshot(ctx, c.execution, base)
	if err != nil {
		return nil, err
	}
	// Resume rebuilds the advertised deferred-tool set from the session's
	// full durable history, paged until exhausted -- not just this turn's
	// reconstructed snapshot messages -- matching the legacy tool-only
	// resume path (interrupt.go's resumeRun) and the W4 fix-pass finding
	// that a single unpaged page silently dropped discoveries beyond it.
	discovered, err := discoveredToolsFromHistoryPaged(ctx, c.host.store, c.sessionID)
	if err != nil {
		return nil, err
	}
	c.execution.seedDiscovered(discovered)
	if target.Ordinal > c.ordinal {
		c.mu.Lock()
		c.ordinal = target.Ordinal
		c.mu.Unlock()
	}
	// placeholderUsed is derived from whether the target turn's assistant
	// placeholder already durably carries content, not assumed
	// unconditionally true: in the common case a TurnInterrupted turn was
	// already dispatched at least once before it paused (a tool interrupt
	// or approval pause only ever fires from inside a dispatch this turn
	// already committed), so its AssistantMessageID placeholder already
	// carries durable content, and a fresh adkEngine's zero-value
	// placeholderUsed=false would let the resumed continuation's first
	// physical dispatch re-claim that same already-content-bearing message
	// via claimPlaceholder, corrupting it with a second, mismatched content
	// kind (e.g. assistant_gen_text appended onto a message that already
	// carries function_tool_call) instead of minting the fresh message that
	// dispatch requires. But finishTurnLoop's between-turn Stop landing
	// before the turn's first physical dispatch (see promoteQueuedContinuation's
	// sibling case, TL P0#4) also produces a TurnInterrupted turn whose
	// placeholder never claimed any content -- unconditionally treating
	// that as "used" leaves a harmless orphan row (dropUnfinalizedAssistantPlaceholders
	// already absorbs it from every later reload) but is not, in fact, true
	// "by construction". Check the durable record directly instead of
	// assuming either way.
	placeholderUsed, err := messageHasParts(ctx, c.host.store, c.sessionID, target.AssistantMessageID)
	if err != nil {
		return nil, err
	}
	return &adkEngine{host: c.host, execution: c.execution, plan: c.plan, snapshot: snapshot, turn: target, assistantMessageID: target.AssistantMessageID, historyOptions: turnHistoryOptions, baseMessageCount: len(priorMessages), placeholderUsed: placeholderUsed}, nil
}

// resumeReconciledTurn rebuilds the adkEngine for the run's most recently
// crash-reconciled interrupted turn -- via the SAME resumeEngine logic
// genResume already uses for a genuine ADK-level tool-interrupt resume,
// since both cases share the identical "rebuild bounded per-turn context
// and reload committed history" shape -- and durably resumes it
// (ResumeInterruptedTurn: interrupted -> running, its InboxInterrupted rows
// -> InboxConsumed), so the SAME TurnID and SAME already-committed user
// message rows drive this turn's redriven dispatch (round-four
// reconciliation item 1/CR-C1). It is only ever called from genInput's
// reconciledTurnSentinelID branch, which ResumeRun only pushes after
// confirming (decodeLoopCheckpointHasRunnerState) this resume's promoted
// checkpoint has no real ADK runner state -- so resumeEngine's
// TurnInterrupted target here is never one genuinely mid an ADK-level
// tool-interrupt pause (that shape always resumes via GenResume instead,
// which genInput -- and so this function -- is never called during; see
// eino's tryLoadCheckpoint).
func (c *turnLoopCoordinator) resumeReconciledTurn(ctx context.Context) (*adkEngine, error) {
	engine, err := c.resumeEngine(ctx)
	if err != nil {
		return nil, err
	}
	result, err := c.execution.store.ResumeInterruptedTurn(ctx, session.ResumeInterruptedTurnRequest{TurnID: engine.turn.ID, ResumedAt: c.host.now()})
	if err != nil {
		return nil, err
	}
	engine.turn = result.Turn
	c.setEngine(engine)
	return engine, nil
}

// messageHasParts reports whether messageID already has any durable Part
// rows in sessionID's history -- used by resumeEngine to derive
// placeholderUsed from the resumed turn's actual durable state (see its
// call site's doc comment) instead of assuming it either way.
//
// This is deliberately NOT the same predicate as
// dropUnfinalizedAssistantPlaceholders' "unfinalized" check (adk_model.go):
// that one counts only decoded ContentBlocks, while this one counts ANY
// session.Part row, including a placeholder that carries only
// PartProviderState/PartResponseMeta parts with no content block at all. A
// message in that narrow shape is "used" here (true) but still
// "unfinalized" there -- which is the safe direction to err in (it mints a
// fresh message rather than risk a mismatched-content-kind append onto one
// dropUnfinalizedAssistantPlaceholders would otherwise still be willing to
// treat as absorbable), so the two are not meant to be unified into one
// predicate (round-three reconciliation item 9, RD-S2).
func messageHasParts(ctx context.Context, store session.Store, sessionID session.ID, messageID session.MessageID) (bool, error) {
	cursor := session.ReplayCursor{Limit: 1000}
	for {
		batch, err := store.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			return false, err
		}
		for _, part := range batch.Parts {
			if part.MessageID == messageID {
				return true, nil
			}
		}
		if batch.Next == (session.ReplayCursor{}) {
			return false, nil
		}
		cursor = batch.Next
	}
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
	// Check interrupted/engine==nil BEFORE either durability-enforcement
	// check below: a legitimate cancellation or business interrupt must
	// never be misreported as a construction failure. BeforeAgent always
	// fires at the very start of a compliant agent's execution -- before
	// any model dispatch -- so an interrupted turn can, in principle, exit
	// before the guard would have run; this ordering guarantees that case
	// is treated as an ordinary pause, not an ErrInvalidOrchestrator.
	if interrupted || engine == nil {
		return nil
	}
	// The mandatory durable guard (adkEngine.buildAgent's durableGuard) must
	// actually have run: BeforeAgent fires at the very start of any
	// compliant agent's execution, before any model dispatch, so by the
	// time the events iterator has fully drained the guard must have marked
	// itself ran. An AgentFactory that ignores AgentBuildContext.Guard
	// entirely never wires it in at all, so our own adapters -- and this
	// check -- never observe anything from that agent's real execution;
	// this is the only point that can still catch it, since a fully
	// noncompliant agent's dispatch never reaches adkModel/adkTool at all.
	// Fail the turn as a construction error rather than silently accepting
	// undurable output. This does NOT catch a factory that installs the
	// guard but substitutes its own model (durableGuard.BeforeAgent only
	// inspects the agent's tool list, never its model) -- see the
	// dispatches check below for that case.
	if !engine.guardRan() {
		return fmt.Errorf("%w: agent factory %T did not install the durable guard before dispatch", ErrInvalidOrchestrator, engine.plan.AgentFactory())
	}
	// HA-S6 in the W6 round-two review: guardRan alone does not prove a
	// factory wired DurableBaseline/SettlementSeal into its agent's real
	// handler chain -- it only proves Guard was wired (both fields are
	// independent AgentBuildContext values an AgentFactory could drop
	// while still installing Guard and dispatching through build.Model).
	// A dispatch that happened without either of these actually running
	// means state.Messages was never seeded from durable history
	// (DurableBaseline) and/or a settled result was never verified before
	// the physical call (SettlementSeal) -- fail the turn rather than
	// accept output produced under either gap.
	if !engine.baselineRan() {
		return fmt.Errorf("%w: agent factory %T did not install the durable baseline handler before dispatch", ErrInvalidOrchestrator, engine.plan.AgentFactory())
	}
	if !engine.sealRan() {
		return fmt.Errorf("%w: agent factory %T did not install the settlement seal before dispatch", ErrInvalidOrchestrator, engine.plan.AgentFactory())
	}
	// A normally-completed turn (not interrupted, guard ran) that recorded
	// zero durable model dispatches means the agent's real execution never
	// routed a physical call through adkModel.begin at all -- the rogue-
	// model case the guard check above cannot see, since durableGuard only
	// inspects the agent's tool list. Nothing durable happened for that
	// provider call (no ledger row, no assistant content committed), so
	// this must fail the turn rather than let it settle RunCompleted with a
	// content-free placeholder. Like the guard check, this is necessarily
	// post-hoc: it runs after the events iterator has fully drained, so the
	// rogue model's own (unledgered) provider call has already happened by
	// the time this fires -- see the W5 doc's AgentBuildContext bullet for
	// the exact scope of this enforcement.
	if engine.dispatchCount() == 0 {
		return fmt.Errorf("%w: agent factory %T produced a turn with no durable model dispatch", ErrInvalidOrchestrator, engine.plan.AgentFactory())
	}
	return c.completeTurn(ctx, engine)
}

func (c *turnLoopCoordinator) completeTurn(ctx context.Context, engine *adkEngine) error {
	usage := engine.usageSnapshot()
	event := session.EventRecord{
		ID: c.host.ids.NewEventID(), SessionID: c.sessionID, RunID: c.runID, MessageID: engine.assistantMessageID,
		EpochID: engine.turn.EpochID, TurnID: engine.turn.ID, Kind: session.TurnCompletedEventKind, CreatedAt: c.host.now(),
	}
	result, err := c.execution.store.CompleteTurn(ctx, session.CompleteTurnRequest{
		TurnID: engine.turn.ID, ResponseMessageIDs: engine.responseMessageIDsSnapshot(), Usage: runtimeUsage(usage), Event: event,
	})
	if err != nil {
		return err
	}
	// This turn's usage is now durably settled on its own turn row (above);
	// fold it into the run-level total exactly once so a multi-turn run's
	// eventual terminal settlement (finishTurnLoop) reports every turn's
	// usage, not just the last one's (see finishedRunUsage's doc comment).
	c.addRunUsage(usage)
	c.execution.publishPersisted(ctx, result.Event)
	// Round-six reconciliation items 1-2: completeTurn no longer retires its
	// own promoted checkpoint here. A promoted revision recorded for this
	// turn is now stale BY FACT once this turn durably completes (currentTurn
	// no longer selects it), not by an explicit mid-run delete -- retirement
	// happens only at terminal settlement (settleCleanRunCompletion,
	// abandonPausedRun) or when crash reconciliation finds nothing left
	// pending to resume (interrupt.go's reconcileCrashedRun). This closes the
	// C1/C2 class of bug where a mid-run delete raced a crash and left the
	// run permanently unresumable: see currentTurn's doc comment.
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

// pushToLoop pushes id into runID's live loop while HOLDING the registry
// lock, so the push can never be reordered past unregisterLoop (which takes
// the same lock and runs strictly before finishTurnLoop seals the late
// buffer -- see runTurnLoop's doc comment). A plain liveLoopFor-then-Push is
// not enough: the calling goroutine can be preempted between the lookup and
// the Push, and the Push then panics on a loop whose late buffer has
// already been sealed by TakeLateItems (round-three reconciliation I1/
// action item 1). Push with no options is non-blocking (buffer.TrySend /
// appendLate), so holding the lock across it cannot deadlock.
func (o *StreamingOrchestrator) pushToLoop(runID session.RunID, id session.InboxID) {
	o.loopsMu.Lock()
	defer o.loopsMu.Unlock()
	if entry := o.loops[runID]; entry != nil {
		entry.loop.Push(id)
	}
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

func (o *StreamingOrchestrator) runTurnLoop(ctx context.Context, entry *liveLoop, checkpoints *adkCheckpointStore, pushIDs []session.InboxID, done chan<- Result, pauseCh chan<- PauseInfo, beforeDone func(Result)) Result {
	coordinator := entry.coordinator
	loop := entry.loop
	// Pushed BEFORE Run, exactly like Start's own sentinel push
	// (orchestrator.go): upstream buffers a pre-Run Push and
	// tryLoadCheckpoint's buffer.TakeAll() merges it with the checkpoint's
	// own UnhandledItems into ONE GenInput batch that claimItems can dedupe.
	// Pushing after Run (the round-two shape) races that TakeAll: a losing
	// push lands in a SECOND GenInput call whose items claimItems has
	// already admitted, and genInput would otherwise still admit a
	// content-free continuation turn that dispatches the model a second
	// time for content already durably committed by the first call (SR-2/
	// RD-1, round-three reconciliation item 2).
	for _, id := range pushIDs {
		loop.Push(id)
	}
	loop.Run(ctx)
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
	// No Push may reach this loop again once finishTurnLoop below seals
	// upstream's late buffer (via state.TakeLateItems): a later Push on a
	// sealed loop panics ("TurnLoop: Push called after TakeLateItems", see
	// eino's adk/turn_loop.go). Unregister here, immediately after Wait
	// returns and strictly before that seal, so a racing Enqueue's
	// liveLoopFor lookup simply finds no live loop for this run and leaves
	// its item durably queued (see Enqueue's doc comment) instead of
	// reaching a sealed loop's Push. This function has no return path
	// between Wait and here, so a plain call -- not a defer -- is correct:
	// every exit below already runs after this line.
	o.unregisterLoop(coordinator.runID)
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
	// beforeDone must complete before done is observed by a receiver, for
	// the same reason release() above does (see its comment): a fresh run's
	// observability session/run spans (finishObservedRun) close here so a
	// receiver reading Done() and immediately snapshotting the observer
	// never races their still-open End() calls.
	if beforeDone != nil {
		beforeDone(result)
	}
	done <- result
	close(done)
	return result
}

// settleRunRetrying calls store.SettleRun, retrying briefly on
// session.ErrConflict. SettleRun refuses to settle a run with any
// non-terminal tool call (store/internal/sqlstore's SettleRun), and ADK's
// WithImmediate stop tears an agent turn down "without waiting for any safe
// point" (see turnLoopHandle.Interrupt, which issues it): an interrupted
// tool call's own settlement -- durably written synchronously inside
// adkTool.InvokableRun's own call stack, in ADK's tool-node goroutine, not
// ours -- can still be completing that write for a brief window after
// TurnLoop.Run has already returned to this goroutine. The run must not
// finalize while that settlement is still in flight, so retry a bounded
// number of times rather than surfacing a spurious conflict; a genuine
// conflict (any other cause) still fails immediately.
func settleRunRetrying(ctx context.Context, store session.ExecutionStore, request session.SettleRunRequest) (session.RunSettlementResult, error) {
	const maxAttempts = 40
	const retryDelay = 5 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		committed, err := store.SettleRun(ctx, request)
		if err == nil {
			return committed, nil
		}
		// ErrRunHasQueuedInput is a structural conflict (a durably queued
		// inbox item), not the in-flight tool settlement this retry loop
		// exists for -- only a FUTURE run's drain can ever clear it, so
		// retrying here only burns the full budget before the caller's own
		// conflict branch takes the drain-and-divert path it was always
		// going to take. Return it immediately (round-three reconciliation
		// item 9, SR-S1/RD-S4); it still satisfies errors.Is(err,
		// session.ErrConflict) for the caller's own check.
		if errors.Is(err, session.ErrRunHasQueuedInput) {
			return session.RunSettlementResult{}, err
		}
		if !errors.Is(err, session.ErrConflict) {
			return session.RunSettlementResult{}, err
		}
		lastErr = err
		if attempt < maxAttempts-1 {
			time.Sleep(retryDelay)
		}
	}
	return session.RunSettlementResult{}, lastErr
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
		engine := c.currentEngine()
		if engine != nil {
			messageID = engine.assistantMessageID
			snapshot = engine.snapshot
		}
		usage := c.finishedRunUsage(engine)
		o.observeError(settleCtx, snapshot, messageID, "provider_stream", state.ExitReason)
		settlement := session.RunSettlement{Status: session.RunInterrupted, FinishedAt: o.now(), Error: state.ExitReason.Error()}
		committed, err := settleRunRetrying(settleCtx, c.execution.store, session.SettleRunRequest{
			Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: messageID, Usage: runtimeUsage(usage)},
		})
		if err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunFailed, Error: errors.Join(state.ExitReason, err)}
		}
		c.execution.publishPersisted(settleCtx, committed.Event)
		result := Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, MessageID: messageID, Error: state.ExitReason, Usage: runtimeUsage(usage)}
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
		// TakeLateItems must still be drained exactly once to seal the late
		// buffer, but its items (and state.UnhandledItems) were never
		// consumed by this interrupted turn -- they are still durably
		// `queued` -- so only the turn's own actually-consumed items
		// (state.InterruptedItems) are interrupted here. Marking a
		// never-consumed item interrupted would make a resumed
		// GenInput->AdmitTurn->consumeInboxForTurn (which accepts only
		// `queued`) fail the resumed run and lose the input.
		if state.TakeLateItems != nil {
			_ = state.TakeLateItems()
		}
		inboxIDs := interruptedItemIDs(state.InterruptedItems)
		event := session.EventRecord{
			ID: o.ids.NewEventID(), SessionID: c.sessionID, RunID: c.runID, EpochID: c.epochID, TurnID: engine.turn.ID,
			Kind: session.RunPausedEventKind, CreatedAt: o.now(),
		}
		var interruptErr *adk.InterruptError
		var cancelErr *adk.CancelError
		var targets []*adk.InterruptCtx
		if errors.As(state.ExitReason, &interruptErr) {
			targets = interruptErr.InterruptContexts
		} else if errors.As(state.ExitReason, &cancelErr) {
			targets = cancelErr.InterruptContexts
		}
		payload, payloadErr := pauseLifecyclePayload(event, checkpoints.lastStaged, engine.agentPath, targets)
		if payloadErr != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: errors.Join(state.ExitReason, payloadErr)}
		}
		event.Payload = payload
		_, err := c.execution.store.PromotePause(settleCtx, session.PromotePauseRequest{
			Revision: checkpoints.lastStaged, TurnID: engine.turn.ID, InboxIDs: inboxIDs, Event: event,
		})
		if err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: errors.Join(state.ExitReason, err)}
		}
		return Result{RunID: c.runID, Status: session.RunPaused, Interrupted: true, Error: state.ExitReason}
	case state.ExitReason == nil && len(state.UnhandledItems) != 0 && state.CheckpointAttempted && state.CheckpointErr == nil:
		// Round-four reconciliation item 2 (CR-I1): state.UnhandledItems is
		// the raw batch genInput's duplicate-delivery guard (or upstream's
		// own PushFront) pushed back to the buffer -- it may already be
		// durably consumed by whichever delivery actually admitted it
		// first (a fresh coordinator's checkpoint-restored UnhandledItems
		// cannot see a DIFFERENT coordinator's in-process admittedItems
		// dedup). Filter against durable inbox state before deciding to
		// pause: pausing on ids that are not genuinely `queued` anymore
		// would strand the run behind a pause nobody needs to resume; if
		// none of them is real, settle this run normally instead.
		genuine, err := o.queuedAmong(settleCtx, c.sessionID, state.UnhandledItems)
		if err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: err}
		}
		if len(genuine) == 0 {
			var messageID session.MessageID
			if engine := c.currentEngine(); engine != nil {
				if ids := engine.responseMessageIDsSnapshot(); len(ids) != 0 {
					messageID = ids[len(ids)-1]
				} else {
					messageID = engine.assistantMessageID
				}
			}
			return o.settleCleanRunCompletion(settleCtx, c, checkpoints, messageID)
		}
		if err := o.promoteQueuedContinuation(settleCtx, c, checkpoints); err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: err}
		}
		return Result{RunID: c.runID, Status: session.RunPaused}
	case state.ExitReason != nil:
		_ = c.execution.stopLease()
		var messageID session.MessageID
		var snapshot TurnSnapshot
		engine := c.currentEngine()
		if engine != nil {
			messageID = engine.assistantMessageID
			snapshot = engine.snapshot
		}
		usage := c.finishedRunUsage(engine)
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
		committed, err := settleRunRetrying(settleCtx, c.execution.store, session.SettleRunRequest{
			Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: messageID, Usage: runtimeUsage(usage)},
		})
		if err != nil {
			return Result{RunID: c.runID, Status: session.RunFailed, Error: errors.Join(state.ExitReason, err)}
		}
		c.execution.publishPersisted(settleCtx, committed.Event)
		result := Result{RunID: c.runID, Status: status, Interrupted: status == session.RunInterrupted, MessageID: messageID, Error: state.ExitReason, Usage: runtimeUsage(usage)}
		c.publishRunSettledNotice(settleCtx, result)
		return result
	default:
		var messageID session.MessageID
		engine := c.currentEngine()
		if engine != nil {
			if ids := engine.responseMessageIDsSnapshot(); len(ids) != 0 {
				messageID = ids[len(ids)-1]
			} else {
				messageID = engine.assistantMessageID
			}
		}
		// ADK reported state.ExitReason == nil (an ordinary clean exit,
		// matching none of the interrupt/pause/fail cases above), but that
		// alone is not proof no interruption was requested: Interrupt
		// cancels this run's own ctx unconditionally and immediately (see
		// turnLoopHandle.Interrupt), and if that cancellation lands after
		// runFreshTurnLoop's own upfront ctx.Err() check but still before
		// (or during) loop.Run ever dispatches anything, TurnLoop.Run can
		// exit with nothing "in flight" to report as interrupted -- the
		// same race runFreshTurnLoop's early check narrows but cannot fully
		// close, since work still happens between that check and here.
		// Cross-check ctx directly rather than trusting ExitReason alone.
		if err := ctx.Err(); err != nil {
			_ = c.execution.stopLease()
			// Always observe the cancellation, even when no engine was ever
			// set (Interrupt won the race before genInput ran): every other
			// interrupt branch above does the same with a zero TurnSnapshot,
			// and skipping this one when engine is nil is what made
			// TestStreamingOrchestratorRecordsInterrupt flaky under -race.
			var snapshot TurnSnapshot
			if engine != nil {
				snapshot = engine.snapshot
			}
			o.observeError(context.WithoutCancel(ctx), snapshot, messageID, "provider_stream", err)
			usage := c.finishedRunUsage(engine)
			settlement := session.RunSettlement{Status: statusForError(err), FinishedAt: o.now(), Error: err.Error()}
			committed, settleErr := settleRunRetrying(settleCtx, c.execution.store, session.SettleRunRequest{
				Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: messageID, Usage: runtimeUsage(usage)},
			})
			if settleErr != nil {
				return Result{RunID: c.runID, Status: session.RunFailed, Error: errors.Join(err, settleErr)}
			}
			c.execution.publishPersisted(settleCtx, committed.Event)
			result := Result{RunID: c.runID, Status: statusForError(err), Interrupted: errors.Is(err, context.Canceled), MessageID: messageID, Error: err, Usage: runtimeUsage(usage)}
			c.publishRunSettledNotice(settleCtx, result)
			return result
		}
		// An Enqueue accepted while this loop was live but raced its idle
		// shutdown lands as a "late" item: drain it now, before ever
		// settling RunCompleted, so an acknowledged item is never silently
		// stranded (see Enqueue's doc comment). A non-empty late buffer
		// means real, still-queued work exists; divert to the same
		// queued-continuation pause a between-turn stop with pending input
		// takes, instead of completing the run out from under it. Filtered
		// against durable inbox state (round-four reconciliation item 2/
		// CR-I1) for the same reason the UnhandledItems branch above is:
		// defensive, since a late item is ordinarily always freshly
		// `queued`, but never assume that from this distance.
		var late []session.InboxID
		if state.TakeLateItems != nil {
			late = state.TakeLateItems()
		}
		genuineLate, err := o.queuedAmong(settleCtx, c.sessionID, late)
		if err != nil {
			_ = c.execution.stopLease()
			return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: err}
		}
		if len(genuineLate) != 0 {
			if err := o.promoteQueuedContinuation(settleCtx, c, checkpoints); err != nil {
				_ = c.execution.stopLease()
				return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: err}
			}
			return Result{RunID: c.runID, Status: session.RunPaused}
		}
		return o.settleCleanRunCompletion(settleCtx, c, checkpoints, messageID)
	}
}

// queuedAmong reports which of ids are still durably InboxQueued for
// sessionID, filtering out the first-turn and reconciled-turn sentinels
// (round-four reconciliation item 2/CR-I1). Used by finishTurnLoop to
// distinguish a genuinely still-queued item from one a duplicate delivery's
// pushed-back or checkpoint-restored id batch carries but that some earlier
// delivery already durably admitted -- pausing on the latter would strand
// the run behind a resume nobody needs to issue.
func (o *StreamingOrchestrator) queuedAmong(ctx context.Context, sessionID session.ID, ids []session.InboxID) ([]session.InboxID, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	queued, err := o.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxQueued})
	if err != nil {
		return nil, err
	}
	queuedSet := make(map[session.InboxID]bool, len(queued))
	for _, item := range queued {
		queuedSet[item.ID] = true
	}
	out := make([]session.InboxID, 0, len(ids))
	for _, id := range ids {
		if id == firstTurnSentinelID || id == reconciledTurnSentinelID {
			continue
		}
		if queuedSet[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

// settleCleanRunCompletion settles the run RunCompleted when finishTurnLoop
// has determined no genuine queued or in-flight work remains (round-four
// reconciliation item 2/CR-I1): shared by the default clean-exit branch and
// the UnhandledItems branch's durable-state-filtered empty case, so both
// settle identically instead of one of them pausing on stale ids nobody
// will ever resume.
func (o *StreamingOrchestrator) settleCleanRunCompletion(settleCtx context.Context, c *turnLoopCoordinator, checkpoints *adkCheckpointStore, messageID session.MessageID) Result {
	usage := c.runUsageSnapshot()
	settlement := session.RunSettlement{Status: session.RunCompleted, FinishedAt: o.now()}
	committed, err := settleRunRetrying(settleCtx, c.execution.store, session.SettleRunRequest{
		Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: messageID, Usage: runtimeUsage(usage)},
	})
	if err != nil {
		// A conflict here can mean a durable inbox item committed (an
		// Enqueue racing this exact settlement -- see EnqueueInboxForRun
		// and the store-level queued-inbox check SettleRun itself now
		// applies before ever finalizing RunCompleted) in the narrow window
		// between this function's caller's own check and this CAS. Check
		// once more before declaring failure: a nonempty queue means real
		// work arrived, and this diverts to the same queued-continuation
		// pause a nonempty late/unhandled buffer takes, so an acknowledged
		// item is never silently stranded behind a RunFailed settlement --
		// the residual half of the terminal-settlement race (see Enqueue's
		// doc comment). Any other cause of the conflict still falls through
		// to ordinary failure handling below.
		if errors.Is(err, session.ErrConflict) {
			if queued, drainErr := o.drainQueuedInbox(settleCtx, c.sessionID); drainErr == nil && len(queued) != 0 {
				if pauseErr := o.promoteQueuedContinuation(settleCtx, c, checkpoints); pauseErr != nil {
					_ = c.execution.stopLease()
					return Result{RunID: c.runID, Status: session.RunInterrupted, Interrupted: true, Error: pauseErr}
				}
				return Result{RunID: c.runID, Status: session.RunPaused}
			}
		}
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
	// Always stage a FRESH Kind=loop "between turns, no runner state"
	// revision for this carrier turn, never trust whatever upstream's own
	// Set call staged (round-five reconciliation item 2/TR-I1): upstream's
	// own isIdle/shouldSaveCheckpoint computation may already have called
	// Set with real UnhandledItems before finishTurnLoop's branch ever
	// runs, but that call necessarily happened BEFORE this carrier turn
	// was admitted, so its envelope cannot carry the carrier's TurnID --
	// promoting it as-is would later fail ResumeRun's TurnID-consistency
	// check (see ResumeRun) against a perfectly legitimate pause. The
	// queued items such a stale envelope would have carried in
	// UnhandledItems are redelivered anyway, independently, by ResumeRun's
	// own drainQueuedInbox (they remain durably `queued` throughout); this
	// adapter's own empty shape carries none, by design (see
	// stageLoopCheckpoint).
	checkpoints.setCurrentTurnID(engine.turn.ID)
	if err := checkpoints.stageLoopCheckpoint(ctx); err != nil {
		return err
	}
	event := session.EventRecord{
		ID: o.ids.NewEventID(), SessionID: c.sessionID, RunID: c.runID, EpochID: c.epochID, TurnID: engine.turn.ID,
		Kind: session.RunPausedEventKind, CreatedAt: o.now(),
	}
	payload, err := pauseLifecyclePayload(event, checkpoints.lastStaged, engine.agentPath, nil)
	if err != nil {
		return err
	}
	event.Payload = payload
	_, err = c.execution.store.PromotePause(ctx, session.PromotePauseRequest{
		Revision: checkpoints.lastStaged, TurnID: engine.turn.ID, Event: event,
	})
	return err
}

// interruptedItemIDs copies the durable inbox IDs out of a TurnLoop item
// slice, filtering out the first-turn sentinel. The sentinel is NOT a
// durable inbox row (Start's first turn is admitted directly inside the
// admission transaction, never through the inbox -- see admission.go's
// admitDurable), so it must never reach PromotePause/AdmitTurn: the real
// stores turn an unknown inbox ID into ErrConflict, which would fail every
// pause of a run's first turn.
func interruptedItemIDs(items []session.InboxID) []session.InboxID {
	out := make([]session.InboxID, 0, len(items))
	for _, id := range items {
		if id == firstTurnSentinelID || id == reconciledTurnSentinelID {
			continue
		}
		out = append(out, id)
	}
	return out
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
func (h *turnLoopHandle) Interrupt(ctx context.Context, reason string) error {
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
	// Recorded synchronously (best-effort: a lookup failure never fails the
	// interrupt itself) so an interrupt that races ahead of the driving
	// goroutine's own startup -- see runFreshTurnLoop's ctx.Err() check --
	// still surfaces an "interrupt requested" observation.
	if run, err := h.host.store.GetRun(context.WithoutCancel(ctx), h.runID); err == nil {
		h.host.observeInterrupt(context.WithoutCancel(ctx), run, "", reason)
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
	// Abandon settles a durably PAUSED run (see session.RunPaused) that has
	// no live loop in this process -- for example, one left paused by a
	// prior process that never resumed it, or one this process itself
	// paused earlier -- as an explicit terminal interruption
	// (session.RunInterrupted), retiring its checkpoints (the plan's
	// "Stop-with-abandon", docs/architecture/eino-feature-support.md's W5
	// section): the operator escape for a paused run nobody intends to
	// resume. It applies ONLY to the no-live-loop, durably-paused case that
	// would otherwise report ErrInvalidOrchestrator with no way forward.
	// When a live loop IS registered for runID in this process, Stop
	// reports ErrInvalidOrchestrator rather than silently downgrading to an
	// ordinary stop -- that case is already reachable via Graceful/
	// Immediate, or Handle.Interrupt for a skip-checkpoint terminal stop.
	// See (*StreamingOrchestrator).abandonPausedRun.
	Abandon bool
}

// Stop signals the process-local live loop for runID to stop, per policy.
// It reports ErrInvalidOrchestrator if this process has no live loop for
// runID (for example, it already exited, or is owned by another process) --
// UNLESS policy.Abandon is set and the run is durably paused, in which case
// it performs an explicit terminal interruption instead (see StopPolicy's
// Abandon doc comment).
func (o *StreamingOrchestrator) Stop(ctx context.Context, runID session.RunID, policy StopPolicy) error {
	entry := o.liveLoopFor(runID)
	if entry == nil {
		if policy.Abandon {
			return o.abandonPausedRun(ctx, runID, policy.Cause)
		}
		return fmt.Errorf("%w: run %s has no live loop in this process", ErrInvalidOrchestrator, runID)
	}
	if policy.Abandon {
		// Abandon is the escape for a durably PAUSED run this process is
		// NOT driving (see StopPolicy.Abandon). A live loop is reachable
		// via Graceful/Immediate (or Handle.Interrupt for a skip-checkpoint
		// terminal stop); silently downgrading Abandon to a bare Stop()
		// here would return nil while leaving the run nonterminal -- and a
		// caller that believed it had abandoned the run would then fail
		// ErrSessionBusy on its next Start. Refuse instead, matching
		// docs/consumer-guide.md.
		return fmt.Errorf("%w: run %s has a live loop in this process; stop it first (Graceful/Immediate), then abandon the resulting paused run", ErrInvalidOrchestrator, runID)
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

// abandonPausedRun implements StopPolicy.Abandon (the plan's "Stop-with-
// abandon"): it settles a durably paused run terminally as
// session.RunInterrupted and retires its checkpoints, giving a host an
// explicit way to close out a paused run nobody intends to resume -- round-
// six reconciliation item 4, the operator escape for the defensive
// ResumeRun/reconcileCrashedRun refusals elsewhere in this package. It
// reclaims the run exactly like ResumeRun/reclaimAndReconcile do (paused
// runs claim immediately by CAS, no lease-expiry wait -- see
// store/internal/sqlstore/runs.go's ClaimRun), retires any promoted
// checkpoint while the fence is still live (RetireCheckpoints requires a
// running fence), then settles terminally so a fresh Start on the session
// succeeds afterward.
func (o *StreamingOrchestrator) abandonPausedRun(ctx context.Context, runID session.RunID, cause string) error {
	run, err := o.store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if !run.Paused() {
		return fmt.Errorf("%w: run %s is %s, not paused; Stop-with-abandon applies only to a durably paused run", ErrInvalidOrchestrator, runID, run.Status)
	}
	plan, err := o.acquireResumePlan(ctx, run.SessionID, run.ExtensionPlan.Clone())
	if err != nil {
		return err
	}
	ownershipTransferred := false
	defer func() {
		if !ownershipTransferred {
			plan.release()
		}
	}()
	claimed, err := o.store.ClaimRun(ctx, session.RunClaim{
		RunID: runID, OwnerID: o.ownerID(), ClaimToken: string(o.ids.NewEventID()), LeaseDuration: o.lease(),
	})
	if err != nil {
		if errors.Is(err, session.ErrConflict) || errors.Is(err, session.ErrSessionBusy) {
			return session.ErrSessionBusy
		}
		return err
	}
	execution := newRunExecution(o, plan, claimed)
	ownershipTransferred = true
	defer execution.release()
	// A paused leaf can be a genuine tool-interrupt pause whose durable
	// ToolCall row never settled (ADK's typed interrupt fires before the
	// executor ever runs -- see adk_execution.go's persistToolClaim call
	// sites): SettleRun refuses to settle while any tool_calls row is
	// pending/running, exactly like reconcileCrashedRun's own crash-recovery
	// path (interrupt.go) must also account for. Terminalize first.
	calls, err := o.store.ListUnfinishedToolCalls(ctx, runID)
	if err != nil {
		return err
	}
	if len(calls) != 0 {
		if err := execution.terminalizeUnfinishedTools(ctx, o.resumeSnapshot(claimed), calls); err != nil {
			return err
		}
	}
	// Retire while the fence is still live (RetireCheckpoints requires a
	// running fence); RetireRunCheckpoints (the terminal-run counterpart) is
	// not usable here because the run has not settled yet.
	if checkpoint, ok, err := o.store.ReadPromotedCheckpoint(ctx, runID); err != nil {
		return err
	} else if ok {
		if err := execution.store.RetireCheckpoints(ctx, checkpoint.Revision); err != nil {
			return err
		}
	}
	settlement := session.RunSettlement{Status: session.RunInterrupted, FinishedAt: o.now(), Error: cause}
	committed, err := execution.store.SettleRun(ctx, session.SettleRunRequest{
		Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID()},
	})
	if err != nil {
		return err
	}
	execution.publishPersisted(ctx, committed.Event)
	result := Result{RunID: runID, Status: session.RunInterrupted, Interrupted: true, Error: errorString(cause)}
	extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{
		SessionID: claimed.SessionID, Result: result, Error: classifyExtensionError(result.Error),
	})
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
// pushed (no live loop in this process) stays queued for a later Start or
// ResumeRun to drain (see drainQueuedInbox): both push every durably queued
// item for the session into a freshly registered loop before running it.
// RunID must belong to sessionID and must not already be terminal --
// Enqueue never silently acknowledges input nothing will ever consume.
func (o *StreamingOrchestrator) Enqueue(ctx context.Context, sessionID session.ID, request EnqueueRequest) (session.InboxItem, error) {
	if err := o.validateConfigured(); err != nil {
		return session.InboxItem{}, err
	}
	if request.IdempotencyKey == "" {
		return session.InboxItem{}, fmt.Errorf("%w: idempotency key required", ErrInvalidOrchestrator)
	}
	if request.RunID == "" {
		return session.InboxItem{}, fmt.Errorf("%w: run id required", ErrInvalidOrchestrator)
	}
	run, err := o.store.GetRun(ctx, request.RunID)
	if err != nil {
		return session.InboxItem{}, err
	}
	if run.SessionID != sessionID {
		return session.InboxItem{}, fmt.Errorf("%w: run %s is not in session %s", ErrInvalidOrchestrator, request.RunID, sessionID)
	}
	if run.Terminal() {
		return session.InboxItem{}, fmt.Errorf("%w: run %s already settled", ErrInvalidOrchestrator, request.RunID)
	}
	if err := rejectCallerContentBlockIDs(request.Message.Blocks); err != nil {
		return session.InboxItem{}, err
	}
	blocks, err := assignContentBlockIDs(request.Message.Blocks, o.ids)
	if err != nil {
		return session.InboxItem{}, err
	}
	// Enqueue is a second public submission entry point alongside Start and
	// must apply the same fence: session.Content.Validate alone permits
	// BlockKindFunctionToolResult/BlockKindToolSearchResult under RoleUser
	// (they are valid durable content on a user-role message when this
	// runtime's own tool settlement path writes them), so a caller could
	// otherwise hand-author a tool_search_result block and seed
	// execution.discovered with no claim, no guard evaluation, and no
	// durable tool-call record behind it (see rejectNonCallerBlocks).
	if err := rejectNonCallerBlocks(blocks); err != nil {
		return session.InboxItem{}, err
	}
	if len(blocks) == 0 || !hasNonEmptyTextOrMediaBlock(blocks) {
		return session.InboxItem{}, fmt.Errorf("%w: message requires non-empty text or media content", ErrInvalidOrchestrator)
	}
	if err := (session.Content{Role: session.RoleUser, Blocks: blocks}).Validate(o.contentLimits); err != nil {
		return session.InboxItem{}, fmt.Errorf("%w: %w", ErrInvalidOrchestrator, err)
	}
	now := o.now()
	item := session.InboxItem{
		ID: o.ids.NewInboxID(), SessionID: sessionID, IdempotencyKey: request.IdempotencyKey,
		Blocks: blocks, State: session.InboxQueued, CreatedAt: now, UpdatedAt: now,
	}
	// EnqueueInboxForRun re-checks request.RunID's terminal status
	// atomically with the write, under the same session-row lock a
	// concurrent terminal SettleRun takes: the plain GetRun check above is
	// only a fast, friendly-error short circuit for the common case (a
	// caller enqueuing against a run it already knows is closed) and is not
	// itself race-proof -- this call is (see EnqueueInboxForRun's doc
	// comment and the terminal-settlement race rule in the W5 plan).
	persisted, created, err := o.store.EnqueueInboxForRun(ctx, request.RunID, item, o.contentLimits)
	if err != nil {
		if errors.Is(err, session.ErrRunClosed) {
			return session.InboxItem{}, fmt.Errorf("%w: run %s already settled", ErrInvalidOrchestrator, request.RunID)
		}
		return session.InboxItem{}, err
	}
	// Push only a durably NEW row: a repeated call under the same
	// IdempotencyKey (a genuine client retry) returns the original,
	// already-buffered-or-consumed item, and re-pushing that ID into a live
	// loop would deliver it a second time (see claimItems's doc comment and
	// TestDuplicateEnqueueIsIdempotentOnKey).
	if created {
		o.pushToLoop(request.RunID, persisted.ID)
	}
	return persisted, nil
}

// drainQueuedInbox lists sessionID's durably queued inbox items in admission
// order and returns their IDs, for a fresh Start or ResumeRun to push into
// its freshly registered loop -- the only path that ever consumes an
// Enqueue accepted while this process held no live loop for the run (see
// Enqueue's doc comment). A listing failure here is reported to the caller
// but never invented as data loss: the items themselves stay durably
// `queued` and remain available to the next successful drain.
//
// This is deliberately session-scoped, not run-scoped, even though Enqueue
// itself validates and is accounted against a specific run: an item
// accepted by an Enqueue call that found no live loop for that (now-
// terminal) run must still be picked up by a LATER run of the same session
// -- that is the only way the "stays queued for a later Start" contract can
// hold, since nothing else ever revisits it. Both call sites (Start,
// ResumeRun) rely on this; do not narrow it to the caller's own run ID.
func (o *StreamingOrchestrator) drainQueuedInbox(ctx context.Context, sessionID session.ID) ([]session.InboxID, error) {
	items, err := o.store.ListInbox(ctx, sessionID, []session.InboxState{session.InboxQueued})
	if err != nil {
		return nil, err
	}
	ids := make([]session.InboxID, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}
	return ids, nil
}

// ResumeRequest resumes a durably paused run.
type ResumeRequest struct {
	// Targets maps a current-generation InterruptCtx.ID (see PauseInfo) to
	// its host-supplied resume data.
	Targets map[string]any
}

// ResumeRun validates a paused run's promoted checkpoint envelope, model
// resolution, and turn history -- everything derivable from the durable
// GetRun record -- BEFORE ever claiming the run's fence (CAS on
// status=paused -> running): a rejected resume must leave the run paused,
// not strand it running with no driver and no heartbeat (see ErrCheckpoint*
// and the fingerprint/version/model-resolve checks below). Only once every
// such check has passed does it claim the fence, rebuild the run's engine,
// and drive a fresh TurnLoop seeded from the promoted checkpoint bytes. One
// fenced write still happens after the claim (StartRun, in the returned
// handle's driving goroutine): its failure is deliberately not compensated
// with a re-pause and instead left for lease-expiry recovery, matching
// finishTurnLoop's checkpoint-Set-failure posture -- see that goroutine's
// own doc comment for why.
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
	// Everything below, up to ClaimRun, is validated off the already-read
	// run record (or read-only, unfenced store calls) and never mutates
	// durable run state: a rejected resume leaves the run exactly as paused
	// as GetRun found it.
	plan, err := o.acquireResumePlan(ctx, run.SessionID, run.ExtensionPlan.Clone())
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
	resumeLifecycle, err := loadPauseLifecycle(ctx, o.store, run.SessionID, runID, checkpoint.Revision, request.Targets)
	if err != nil {
		return nil, err
	}
	fingerprint := planFingerprint(plan)
	if checkpoint.AgentFingerprint != fingerprint || checkpoint.EinoVersion != EinoPinnedVersion || checkpoint.CodecVersion != adkCheckpointCodecVersion {
		return nil, ErrCheckpointFingerprintMismatch
	}
	envelope, err := decodeCheckpointEnvelope(checkpoint.Bytes)
	if err != nil {
		return nil, err
	}
	hasRunnerState, err := decodeLoopCheckpointHasRunnerState(envelope.Payload)
	if err != nil {
		return nil, err
	}
	selection := model.Selection{ProviderID: model.ProviderID(run.ProviderID), ModelID: model.ID(run.ModelID)}
	durable := decodeResumeRunConfig(run.Config)
	resolved, err := o.model.Resolve(ctx, selection, model.Runtime{
		Directory: durable.WorkspaceRoot,
		Options:   cloneStringMap(durable.AgentOptions),
	})
	if err != nil {
		return nil, err
	}
	cfg := config.Snapshot{Agent: config.Agent{
		Name: run.Agent, Model: selection, SystemPrompt: durable.SystemPrompt, Options: durable.AgentOptions, Mode: durable.AgentMode,
	}, Tools: config.ToolConfig{Enabled: durable.ToolsEnabled, Disabled: durable.ToolsDisabled}, Metadata: map[string]string{
		"workspace_id": durable.WorkspaceID, "workspace_root": durable.WorkspaceRoot,
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
	// Round-six reconciliation items 1 and 3: currentTurn is the SAME
	// selector crash reconciliation uses to decide what turn a repaused
	// run's checkpoint belongs to (interrupt.go's reconcileCrashedRun), so a
	// promoted checkpoint's recorded TurnID and this call's output can never
	// disagree on a run reconciliation has finished with -- this check is
	// therefore a defensive assertion, not the primary mechanism. Refuse
	// closed here, before ever claiming the fence, on the rare/synthetic
	// state it CAN still catch (a checkpoint whose turn has since completed,
	// or belongs to no turn of this run at all): the run is left exactly as
	// paused as GetRun found it above, with a clear error, rather than
	// corrupting turn identity.
	turn := currentTurn(turns)
	if envelope.TurnID != turn.ID {
		return nil, fmt.Errorf("%w: run %s has a promoted checkpoint recorded for turn %s, but its current turn is %s",
			ErrInvalidOrchestrator, runID, envelope.TurnID, turn.ID)
	}
	// A crash-reconciled interrupted turn with real, already-committed
	// content (round-four reconciliation item 1/CR-C1) must be redriven
	// under its OWN TurnID, never re-admitted as a fresh turn -- but that
	// redrive is only ever safe to deliver through GenInput
	// (reconciledTurnSentinelID; see resumeReconciledTurn's doc comment),
	// which eino's own tryLoadCheckpoint reaches only when this resume's
	// promoted checkpoint payload has no real ADK runner state. A
	// degenerate, content-free queued-continuation marker turn (see
	// promoteQueuedContinuation, and reconcileCrashedRun's own carrier for
	// the "nothing left pending" case) has no UserMessageIDs and needs no
	// redrive at all.
	pushReconciledSentinel := !hasRunnerState && turn.State == session.TurnInterrupted && len(turn.UserMessageIDs) != 0
	// A durably queued inbox item accepted by Enqueue while no live loop
	// existed for this run must be drained into the loop this call is about
	// to start -- otherwise it stays queued forever (see Enqueue's doc
	// comment). Read-only and unfenced: a failure here fails ResumeRun
	// closed, before ever claiming, rather than risk silently dropping it.
	queuedIDs, err := o.drainQueuedInbox(ctx, run.SessionID)
	if err != nil {
		return nil, err
	}
	if pushReconciledSentinel {
		queuedIDs = append([]session.InboxID{reconciledTurnSentinelID}, queuedIDs...)
	}
	// Read the durable message floor here, unfenced and before ClaimRun,
	// alongside drainQueuedInbox: this must NOT be deferred to
	// nextDurableMessageTime's lazy-init path, whose first call would
	// otherwise run this same non-transactional read (latestAdmissionMessageTime)
	// from inside the approval decision's WithinTx-wrapped transaction (see
	// adk_approval.go's prepare), a busy/deadlock hazard on SQLite and a
	// snapshot-skew hazard in general.
	latestMessageAt, err := latestAdmissionMessageTime(ctx, o.store, run.SessionID)
	if err != nil {
		return nil, err
	}
	// Round-two W6 review item 3: re-read every skill this session has
	// ever durably recorded activating (SkillActivatedEventKind) from the
	// CURRENT workspace state and compare its content digest against what
	// was recorded at activation time. A changed or missing SKILL.md
	// between pause and resume fails this resume closed, with a clear
	// ErrSkillChangedSinceActivation error, before the fence below is ever
	// claimed -- the run is left exactly as paused as GetRun found it,
	// never resumed under silently divergent skill content.
	if err := verifySkillActivationsUnchanged(ctx, o.store, run.SessionID, run.ID, durable.WorkspaceRoot); err != nil {
		return nil, err
	}

	// Only now take the fence: every check above that could reject this
	// resume has already run against the still-paused run.
	claimToken := string(o.ids.NewEventID())
	claimed, err := o.store.ClaimRun(ctx, session.RunClaim{RunID: runID, OwnerID: o.ownerID(), ClaimToken: claimToken, LeaseDuration: o.lease()})
	if err != nil {
		return nil, err
	}
	execution := newRunExecution(o, plan, claimed)
	// Seed the durable message floor at the LATER of o.now() and the real
	// maximum already-committed message time (read above, before the
	// claim). o.now() alone is not a safe floor: message timestamps are
	// allocated forward in nanosecond steps from a turn's own now()
	// (admitTurn, nextDurableMessageTime), so a session's committed
	// messages routinely carry times after the now() they were derived
	// from -- seeding at o.now() alone can hand the resumed execution
	// timestamps that collide with, or precede, messages already
	// committed (round-three reconciliation item 3, SR-3: reproduced as
	// an approval response sorting before the assistant message that
	// requested it). Taking the max keeps the "no non-transactional read
	// inside WithinTx" property this seed exists for, with no regression
	// in ordering.
	floor := o.now()
	if latestMessageAt.After(floor) {
		floor = latestMessageAt
	}
	execution.seedDurableMessageFloor(floor)
	coordinator := &turnLoopCoordinator{
		host: o, execution: execution, plan: plan, sessionID: claimed.SessionID, runID: claimed.ID,
		config: cfg, resolved: resolved, historyOptions: o.history, epochID: claimed.ContextEpoch, ordinal: maxOrdinal, resumeLifecycle: resumeLifecycle,
	}
	coordinator.setResumeTargets(request.Targets)
	checkpoints := newAdkCheckpointStore(o, execution, plan, runID)
	coordinator.checkpoints = checkpoints
	// Seed currentTurnID synchronously, before this resume's TurnLoop is
	// even constructed (round-six reconciliation item 8/CP-S1): setEngine
	// otherwise only keeps it in sync starting from the first GenInput/
	// GenResume call, leaving a window (a Set racing ahead of that first
	// call, e.g. a Stop landing before it) where stage() would see an empty
	// currentTurnID and hard-fail, turning a resumable pause into a
	// non-pause. currentTurn(turns) is exactly the turn this call already
	// validated the promoted envelope against above.
	checkpoints.setCurrentTurnID(turn.ID)
	// The resumed run gets its own cancellable context, detached from the
	// caller's (possibly request-scoped) ctx: Handle.Interrupt must be able
	// to cancel it, and a resumed run must not die when an HTTP handler's
	// context returns -- matching Start (orchestrator.go's context.WithCancel).
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	handle := &turnLoopHandle{runID: runID, host: o, done: make(chan Result, 1), pause: make(chan PauseInfo, 1), cancel: cancel}
	entry := o.prepareTurnLoop(coordinator, checkpoints)
	ownershipTransferred = true
	go func() {
		// A StartRun failure here is the one surviving instance of the
		// shape ED-1/item-1's claim-after-validate ordering was about:
		// every check that could reject this resume has already run above,
		// before ClaimRun, but this fenced write can still fail after the
		// claim. Round-three reconciliation item 4a (SR-I4): leaving the
		// run durably `running` with no driver here is NOT recoverable by
		// lease expiry -- ClaimRun has already CAS'd the status to
		// `running`, and lease expiry changes only lease_until, never
		// status, so activeRun keeps counting this run and every later
		// Start on the session fails ErrSessionBusy indefinitely; the only
		// other in-tree path (legacy Resume) has no notion of ADK
		// checkpoints and would settle the run interrupted, discarding the
		// promoted checkpoint entirely. Compensate instead: revert the
		// claim back to paused (RepauseRun), keeping the checkpoint this
		// resume attempt never touched intact, so a later ResumeRun call
		// can simply try again.
		started, err := execution.store.StartRun(runCtx, o.now())
		if err != nil {
			o.resumeStartFailureRepause(runCtx, execution, runID, claimed.SessionID, claimed.ContextEpoch, checkpoint.Revision, handle, err)
			return
		}
		_ = started
		leaseCtx := execution.startLease(runCtx, o.lease())
		defer execution.release()
		o.runTurnLoop(leaseCtx, entry, checkpoints, queuedIDs, handle.done, handle.pause, nil)
	}()
	return handle, nil
}

// resumeStartFailureRepause compensates a post-claim StartRun failure on
// ResumeRun's path (see the goroutine above's doc comment): it reverts the
// just-taken claim back to paused via RepauseRun, so the run stays
// resumable instead of wedged `running` with no driver. If the compensating
// write itself fails, the run is reported RunFailed and left for lease-
// expiry recovery -- the same conservative posture finishTurnLoop's
// checkpoint-Set-failure branch already takes when it cannot safely
// compensate either.
func (o *StreamingOrchestrator) resumeStartFailureRepause(ctx context.Context, execution *runExecution, runID session.RunID, sessionID session.ID, epochID session.EpochID, checkpointRevision int64, handle *turnLoopHandle, cause error) {
	o.unregisterLoop(runID)
	repauseCtx := context.WithoutCancel(ctx)
	event := session.EventRecord{
		ID: o.ids.NewEventID(), SessionID: sessionID, RunID: runID, EpochID: epochID,
		Kind: session.RunPausedEventKind, CreatedAt: o.now(),
	}
	if payload, err := pauseLifecyclePayload(event, checkpointRevision, "root", nil); err == nil {
		event.Payload = payload
	}
	status := session.RunPaused
	resultErr := cause
	if _, repauseErr := execution.store.RepauseRun(repauseCtx, session.RepauseRunRequest{Event: event}); repauseErr != nil {
		status = session.RunFailed
		resultErr = errors.Join(cause, repauseErr)
	}
	handle.done <- Result{RunID: runID, Status: status, Error: resultErr}
	close(handle.done)
	// Handle's contract (types.go): AwaitPause reports the durably promoted
	// pause this run reaches, or is closed without a value if the run
	// instead settles terminally -- never closed with no value while Done()
	// reports RunPaused. runReconcileCrashedRun (interrupt.go) already gets
	// this right; this compensation path must too (round-four reconciliation
	// item 5/CR-I4's ambiguity, round-seven fix-pass-6 item 4/MG-I5).
	if status == session.RunPaused {
		handle.pause <- PauseInfo{RunID: runID, StopCause: "resume-start-failure-repause"}
	}
	close(handle.pause)
	execution.release()
}
