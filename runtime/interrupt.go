package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// Resume reclaims resumable durable work for runID. The current implementation
// resumes the durable tool-call boundary: pending calls are atomically claimed
// before execution, terminal calls are skipped, and active non-owned leases are
// rejected.
func (o *StreamingOrchestrator) Resume(ctx context.Context, runID session.RunID) (Handle, error) {
	if err := o.validateConfigured(); err != nil {
		return nil, err
	}
	if runID == "" {
		return nil, fmt.Errorf("%w: run id required", ErrInvalidOrchestrator)
	}
	run, err := o.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.Terminal() {
		return terminalRunHandle(run), nil
	}
	// A run durably paused by the ADK TurnLoop exit protocol (see
	// runtime/turn_loop.go) carries a promoted checkpoint and no live
	// lease: it must resume through ResumeRun's checkpoint-aware path, not
	// the legacy tool-only claim/resume below, which has no notion of ADK
	// checkpoints or turns. An untargeted resume (no Targets) keeps any
	// still-paused leaf paused rather than dispatching anything, exactly
	// like ResumeWithParams with an empty target set.
	if run.Paused() {
		return o.ResumeRun(ctx, runID, ResumeRequest{})
	}
	// A running (not paused) run may still be ADK-TurnLoop-driven and have
	// been abandoned by a crashed process: every fresh Start uses the ADK
	// engine now, so a run with any admitted turn, or any promoted
	// checkpoint from an earlier pause/resume cycle, was built by it. The
	// legacy tool-only resume below has no notion of either -- it
	// terminalizes unfinished tool calls but never reconciles a dangling
	// turn's consumed inbox items, and unconditionally settles the run
	// interrupted, discarding any promoted checkpoint entirely (round-three
	// reconciliation item 4b/c, SR-4/RD-S5). Route those runs through the
	// ADK-aware crash reconciliation path instead; only a run with no turns
	// and no promoted checkpoint at all (never driven by the ADK engine)
	// falls through to the legacy path.
	if adkDriven, err := o.wasADKDriven(ctx, run); err != nil {
		return nil, err
	} else if adkDriven {
		return o.reclaimAndReconcile(ctx, run)
	}
	plan, err := o.acquireResumePlan(ctx, run.SessionID, run.ExtensionPlan.Clone())
	if err != nil {
		return nil, err
	}
	run, err = o.store.ClaimRun(ctx, session.RunClaim{
		RunID: run.ID, OwnerID: o.ownerID(), ClaimToken: string(o.ids.NewEventID()), LeaseDuration: o.lease(),
	})
	if err != nil {
		plan.release()
		if errors.Is(err, session.ErrConflict) || errors.Is(err, session.ErrSessionBusy) {
			return nil, session.ErrSessionBusy
		}
		return nil, err
	}
	ownershipTransferred := false
	defer func() {
		if !ownershipTransferred {
			plan.release()
		}
	}()
	execution := newRunExecution(o, plan, run)
	runCtx, cancel := context.WithCancel(ctx)
	handle := &streamingHandle{
		runID:       runID,
		host:        o,
		cancel:      cancel,
		done:        make(chan Result, 1),
		onInterrupt: func(reason string) { o.observeInterrupt(context.WithoutCancel(ctx), run, "", reason) },
	}
	ownershipTransferred = true
	go o.executeResume(runCtx, execution, run, handle.done)
	return handle, nil
}

func terminalRunHandle(run session.Run) Handle {
	done := make(chan Result, 1)
	done <- Result{RunID: run.ID, Status: run.Status, Interrupted: run.Status == session.RunInterrupted, Error: errorString(run.Error)}
	close(done)
	return &streamingHandle{runID: run.ID, cancel: func() {}, done: done}
}

func (o *StreamingOrchestrator) executeResume(ctx context.Context, execution *runExecution, run session.Run, done chan<- Result) {
	resumeStartedAt := o.now()
	lifecycle := &runLifecycle{
		run:         run,
		result:      Result{RunID: run.ID},
		metadata:    boundedTurnMetadata(o.resumeSnapshot(run)),
		startedAt:   resumeStartedAt,
		panicPrefix: "resume run panic",
	}
	o.executeLifecycle(ctx, execution, lifecycle, done, func(ctx context.Context) {
		o.observeResume(ctx, run, "resume")
		run.StartedAt = resumeStartedAt
		lifecycle.observed = o.startObservedRun(ctx, run, "", run.StartedAt)
		lifecycle.result = o.resumeRun(ctx, execution, run)
	})
}

func (o *StreamingOrchestrator) resumeRun(ctx context.Context, execution *runExecution, run session.Run) Result {
	started, err := execution.store.StartRun(ctx, run.StartedAt)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	run = started
	o.sessionObserver.Hint(run.SessionID)
	calls, err := o.store.ListUnfinishedToolCalls(ctx, run.ID)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	if len(calls) == 0 {
		return Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true}
	}
	// Rebuild the per-execution tool-search advertised set from durable
	// history before resuming any outstanding call, so a future turn that
	// continues past this resume (not yet built past the tool-call
	// boundary) sees every tool the model had already discovered. Page the
	// full session history (not just the first 1000 messages -- the
	// discoveries that matter most are the most recent ones), decode with
	// session.MaxContentLimits() (content may have been admitted under a
	// writer's raised ContentLimits), and propagate a store failure instead
	// of silently seeding an empty set.
	snapshot := o.resumeSnapshot(run)
	withCleanup := func(result Result) Result {
		if cleanupErr := execution.terminalizeUnfinishedTools(context.WithoutCancel(ctx), snapshot, calls); cleanupErr != nil {
			result.Status = session.RunFailed
			result.Error = errors.Join(result.Error, cleanupErr)
		}
		return result
	}
	discovered, err := discoveredToolsFromHistoryPaged(ctx, o.store, run.SessionID)
	if err != nil {
		// The outstanding calls proved above must still be terminalized,
		// otherwise settling the run leaves them pending forever.
		return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: err})
	}
	execution.seedDiscovered(discovered)
	tools, snapshot, toolContext, err := o.resumeTools(ctx, execution, run, snapshot)
	if err != nil {
		return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: err})
	}
	searchName := ""
	if ts := execution.plan.ToolSearch(); ts != nil {
		searchName = ts.Name
	}
	for _, call := range calls {
		if session.TerminalToolCall(call.Status) {
			continue
		}
		if searchName != "" && call.Name == searchName {
			// A tool-search call is persisted as an ordinary session.ToolCall
			// (see prepareToolCalls/isToolSearchCall) but is never a plan
			// tool, so it is absent from `tools` by design. Resuming it must
			// never fail the run with "tool_search unavailable": search is
			// side-effect free, so a pending call can simply be
			// (re-)executed, and a running call settles as interrupted like
			// any other outstanding call.
			if call.Status == session.ToolCallRunning {
				if _, err := execution.settleInterruptedRunningTool(ctx, run, Tool{Name: call.Name}, call); err != nil {
					return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: err})
				}
				continue
			}
			searchCall := ToolCall{
				ID: call.ID, SessionID: call.SessionID, RunID: call.RunID, MessageID: call.MessageID,
				ResultMessageID: call.ResultMessageID, ResultPartID: call.ResultPartID,
				Name: call.Name, RequestedName: call.Name, Pattern: call.Pattern,
				Input: cloneJSON(call.Input), Context: toolContext.Clone(),
			}
			if _, err := execution.executeToolSearchCall(ctx, snapshot, searchCall, call); err != nil {
				return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: err})
			}
			continue
		}
		canonicalInput, canonicalErr := canonicalToolObject(call.Input)
		if canonicalErr != nil || string(canonicalInput) != string(call.Input) || call.Pattern == "" || len(call.Pattern) > 4096 {
			return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: fmt.Errorf("invalid persisted tool call %q", call.ID)})
		}
		tool, ok := tools[call.Name]
		if !ok || tool.Executor == nil {
			return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: fmt.Errorf("tool %q unavailable", call.Name)})
		}
		if call.Status == session.ToolCallRunning {
			if _, err := execution.settleInterruptedRunningTool(ctx, run, tool, call); err != nil {
				return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: err})
			}
			continue
		}
		startedAt := o.now()
		claimToken := call.ClaimToken
		if claimToken == "" || call.ClaimedBy != o.ownerID() {
			claimToken = string(o.ids.NewEventID())
		}
		claimResult, err := execution.persistToolClaim(ctx, session.ClaimToolCallRequest{
			ID: call.ID, ClaimedBy: o.ownerID(), ClaimToken: claimToken, StartedAt: startedAt,
			LeaseDuration: o.lease(), Event: toolTransitionEnvelope(o, snapshot, startedAt),
		})
		if err != nil {
			if errors.Is(err, session.ErrConflict) {
				return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: session.ErrSessionBusy})
			}
			return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: err})
		}
		claimed := claimResult.Call
		extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), ToolStartedPoint, ToolStartedNotice{SessionID: run.SessionID, RunID: run.ID, ToolCallID: claimed.ID, ToolName: claimed.Name, Time: claimed.StartedAt})
		toolCall := ToolCall{
			ID:              claimed.ID,
			SessionID:       claimed.SessionID,
			RunID:           claimed.RunID,
			MessageID:       claimed.MessageID,
			ResultMessageID: claimed.ResultMessageID,
			ResultPartID:    claimed.ResultPartID,
			Name:            claimed.Name,
			Scope:           tool.Scope,
			Pattern:         claimed.Pattern,
			Input:           cloneJSON(claimed.Input),
			Context:         toolContext.Clone(),
		}
		settledTool, err := execution.executeAndSettleClaimedTool(ctx, snapshot, tool, toolCall, claimed, nil)
		if err != nil {
			return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: err})
		}
		if errors.Is(settledTool.Outcome.RawError, errToolExecutionPanic) {
			return withCleanup(Result{RunID: run.ID, Status: session.RunFailed, Error: settledTool.Outcome.RawError})
		}
		if errors.Is(settledTool.Outcome.RawError, context.Canceled) {
			return withCleanup(Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true, Error: settledTool.Outcome.RawError})
		}
	}
	return Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true}
}

// resumeTools resolves the plan's tools for a resumed run and returns snapshot
// updated with the resolved Tools and the plan's ToolSearch configuration
// (see RunPlan.ToolSearch), so a resumed tool-search call has the same
// resolved deferred-tool registry available to searchDeferredTools that a
// live turn would.
func (o *StreamingOrchestrator) resumeTools(ctx context.Context, execution *runExecution, run session.Run, snapshot TurnSnapshot) (map[string]Tool, TurnSnapshot, ToolContext, error) {
	if len(execution.plan.tools.capabilities) == 0 {
		return nil, snapshot, ToolContext{}, fmt.Errorf("%w: tool registry required", ErrInvalidOrchestrator)
	}
	resolved, err := execution.plan.ResolveTools(ctx, NewToolScopeContext(snapshot))
	if err != nil {
		return nil, snapshot, ToolContext{}, err
	}
	snapshot.Tools = cloneSlice(resolved)
	snapshot.ToolSearch = execution.plan.ToolSearch()
	tools := make(map[string]Tool, len(resolved))
	for _, tool := range resolved {
		if _, exists := tools[tool.Name]; exists {
			return nil, snapshot, ToolContext{}, fmt.Errorf("duplicate effective tool %q", tool.Name)
		}
		tools[tool.Name] = tool
	}
	return tools, snapshot, toolContext(snapshot, resolved), nil
}

func (o *StreamingOrchestrator) resumeSnapshot(run session.Run) TurnSnapshot {
	return TurnSnapshot{
		RunID:     run.ID,
		SessionID: run.SessionID,
		EpochID:   run.ContextEpoch,
		Config: config.Snapshot{
			Agent: config.Agent{Name: run.Agent},
			Model: model.Selection{ProviderID: model.ProviderID(run.ProviderID), ModelID: model.ID(run.ModelID)},
			Metadata: map[string]string{
				"workspace_id":   run.Config["workspace_id"],
				"workspace_root": run.Config["workspace_root"],
			},
		},
		Model: model.Resolved{
			Provider: model.Provider{ID: model.ProviderID(run.ProviderID)},
			Model:    model.Descriptor{ID: model.ID(run.ModelID), ProviderID: model.ProviderID(run.ProviderID)},
		},
		CreatedAt: o.now(),
	}
}

func errorString(value string) error {
	if value == "" {
		return nil
	}
	return errors.New(value)
}

// wasADKDriven reports whether run was ever driven by the ADK TurnLoop
// engine: it has at least one durable turn (every ADK admission, including
// a fresh Start's first turn, goes through AdmitTurn), or a promoted
// checkpoint from an earlier pause/resume cycle. A run with neither was
// never touched by prepareTurnLoop and is safe for the legacy tool-only
// resume path below.
func (o *StreamingOrchestrator) wasADKDriven(ctx context.Context, run session.Run) (bool, error) {
	turns, err := o.store.ListTurns(ctx, run.ID)
	if err != nil {
		return false, err
	}
	if len(turns) != 0 {
		return true, nil
	}
	_, found, err := o.store.ReadPromotedCheckpoint(ctx, run.ID)
	if err != nil {
		return false, err
	}
	return found, nil
}

// reclaimAndReconcile is the ADK-aware counterpart of the legacy tool-only
// resume above (round-three reconciliation item 4b/c): it reclaims a
// running (or pending) run abandoned by a crashed process -- via the same
// generic ClaimRun lease-expiry reclaim the legacy path already uses, so a
// genuinely still-alive owner still wins ErrSessionBusy -- then
// conservatively reconciles it: unfinished tool calls are terminalized
// (never re-executed; matches terminalizeUnfinishedTools' existing
// unsafe-running-tool-never-reruns contract), and any turn left admitted/
// running by the crash is settled interrupted (ReconcileInterruptedTurn),
// its already-consumed inbox items carried forward as InboxInterrupted --
// NEVER requeued to InboxQueued, since they were already durably committed
// into that turn's own user messages by the AdmitTurn that admitted it; a
// later ResumeRun redrives the SAME turn by TurnID (ResumeInterruptedTurn),
// never re-admits its content as a fresh turn. The run is then either
// repaused (staging and promoting a fresh Kind=loop checkpoint recorded for
// the just-reconciled turn -- round-five reconciliation item 2/TR-I1 -- if
// it has ever had a promoted checkpoint, so a later ResumeRun reads a
// consistent state) or settled interrupted (if it never did, so there is
// nothing to resume -- the reconciled turn's content is answered only via
// its own already-committed history, on some later run over this session).
func (o *StreamingOrchestrator) reclaimAndReconcile(ctx context.Context, run session.Run) (Handle, error) {
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
	claimed, err := o.store.ClaimRun(ctx, session.RunClaim{
		RunID: run.ID, OwnerID: o.ownerID(), ClaimToken: string(o.ids.NewEventID()), LeaseDuration: o.lease(),
	})
	if err != nil {
		if errors.Is(err, session.ErrConflict) || errors.Is(err, session.ErrSessionBusy) {
			return nil, session.ErrSessionBusy
		}
		return nil, err
	}
	execution := newRunExecution(o, plan, claimed)
	ownershipTransferred = true
	// runCtx is cancelled only by an explicit Interrupt on the returned
	// handle; reconciliation itself always runs detached from the caller's
	// ctx (context.WithoutCancel below), matching every other run-driving
	// goroutine in this package.
	_, cancel := context.WithCancel(ctx)
	// turnLoopHandle (not streamingHandle) is used so a RunPaused outcome
	// -- reachable from this path via repause -- honours the documented
	// Handle contract: its AwaitPause is a real channel, closed without a
	// value on every terminal outcome and carrying a PauseInfo on a repause
	// (round-four reconciliation item 5/CR-I4; see runtime/types.go's
	// Handle doc comment).
	handle := &turnLoopHandle{runID: claimed.ID, host: o, done: make(chan Result, 1), pause: make(chan PauseInfo, 1), cancel: cancel}
	go o.runReconcileCrashedRun(context.WithoutCancel(ctx), execution, claimed, handle)
	return handle, nil
}

// runReconcileCrashedRun drives reconcileCrashedRun under the same
// protections every other run-driving goroutine gets (round-four
// reconciliation item 3/CR-I2): panic recovery (a panic anywhere in the
// reconciliation -- the store, extension dispatch, or
// terminalizeUnfinishedTools -- settles the run RunFailed instead of taking
// down the host process, matching executeLifecycle's own recover()), an
// observed-run span (startObservedRun/finishObservedRun), a RunSettledNotice
// on genuinely terminal settlement (never on the nonterminal repause branch,
// matching finishTurnLoop's own convention), and a lease heartbeat for the
// duration of the reconciliation's writes (ListUnfinishedToolCalls plus
// terminalizeUnfinishedTools can settle an unbounded number of tool calls).
//
// reconcileCrashedRun is not driven through the shared executeLifecycle
// boundary: that helper's own deferred settleRun unconditionally calls
// SettleRun again, which would double-settle a run this function already
// settled via RepauseRun or SettleRun itself (RepauseRun leaves the fence
// unable to write further, so a second SettleRun would fail ErrConflict and
// overwrite an already-correct RunPaused result with RunFailed).
func (o *StreamingOrchestrator) runReconcileCrashedRun(ctx context.Context, execution *runExecution, run session.Run, handle *turnLoopHandle) {
	observed := o.startObservedRun(ctx, run, "", o.now())
	leaseCtx := execution.startLease(ctx, o.lease())
	result := func() (result Result) {
		defer func() {
			_ = execution.stopLease()
			if recovered := recover(); recovered != nil {
				result = Result{RunID: run.ID, Status: session.RunFailed, Error: fmt.Errorf("crash reconciliation panic: %v", recovered)}
			}
		}()
		return o.reconcileCrashedRun(leaseCtx, execution, run)
	}()
	o.finishObservedRun(observed, result, o.now())
	if result.Status == session.RunPaused {
		handle.pause <- PauseInfo{RunID: run.ID, StopCause: "crash-reconciled"}
	} else {
		metadata := boundedTurnMetadata(o.resumeSnapshot(run))
		extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{
			SessionID: run.SessionID, Result: result, Metadata: metadata, Error: classifyExtensionError(result.Error),
		})
	}
	close(handle.pause)
	execution.release()
	handle.done <- result
	close(handle.done)
}

// reconcileCrashedRun performs reclaimAndReconcile's actual reconciliation
// work under the just-taken claim; see that function's doc comment for the
// full sequence and rationale.
func (o *StreamingOrchestrator) reconcileCrashedRun(ctx context.Context, execution *runExecution, run session.Run) Result {
	o.observeResume(ctx, run, "reconcile")
	calls, err := o.store.ListUnfinishedToolCalls(ctx, run.ID)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	if len(calls) != 0 {
		if err := execution.terminalizeUnfinishedTools(ctx, o.resumeSnapshot(run), calls); err != nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
	}
	turns, err := o.store.ListTurns(ctx, run.ID)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	var dangling session.Turn
	for _, t := range turns {
		if (t.State == session.TurnAdmitted || t.State == session.TurnRunning) && t.Ordinal >= dangling.Ordinal {
			dangling = t
		}
	}
	if dangling.ID != "" {
		event := session.EventRecord{
			ID: o.ids.NewEventID(), SessionID: run.SessionID, RunID: run.ID, EpochID: run.ContextEpoch,
			TurnID: dangling.ID, Kind: session.RunPausedEventKind, CreatedAt: o.now(),
		}
		if _, err := execution.store.ReconcileInterruptedTurn(ctx, session.ReconcileInterruptedTurnRequest{TurnID: dangling.ID, Event: event}); err != nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
	}
	_, hasCheckpoint, err := o.store.ReadPromotedCheckpoint(ctx, run.ID)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	if hasCheckpoint {
		event := session.EventRecord{
			ID: o.ids.NewEventID(), SessionID: run.SessionID, RunID: run.ID, EpochID: run.ContextEpoch,
			Kind: session.RunPausedEventKind, CreatedAt: o.now(),
		}
		request := session.RepauseRunRequest{Event: event}
		if dangling.ID != "" {
			// Round-five reconciliation item 2 (TR-I1): the previously
			// promoted revision may belong to an older, already-superseded
			// turn -- or to no turn ResumeRun's own TurnID-consistency
			// check would now accept at all. Stage a fresh, correctly
			// turn-identified Kind=loop checkpoint for the JUST-reconciled
			// turn and promote it instead, so a later ResumeRun reads a
			// consistent state. dangling is already durably TurnInterrupted
			// by this point (ReconcileInterruptedTurn above), so
			// PromotePause's own turn-interrupt precondition (admitted/
			// running) no longer holds -- RepauseRun's PromoteRevision is
			// what promotes it here instead.
			checkpoints := newAdkCheckpointStore(o, execution, execution.plan, run.ID)
			checkpoints.setCurrentTurnID(dangling.ID)
			if err := checkpoints.stageLoopCheckpoint(ctx); err != nil {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
			}
			request.PromoteRevision = checkpoints.lastStaged
		}
		if _, err := execution.store.RepauseRun(ctx, request); err != nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		return Result{RunID: run.ID, Status: session.RunPaused}
	}
	// No promoted checkpoint ever existed for this run (it crashed on its
	// very first turn, before ever pausing): there is nothing a later
	// ResumeRun could resume, so settle it interrupted. Any turn reconciled
	// above stays durably `interrupted`, its consumed inbox items carried
	// forward as `interrupted` -- never requeued to `queued` -- so its
	// content is answered only via already-committed history, on some
	// later run over this session (drainQueuedInbox will never see it
	// again: it is not `queued`).
	settlement := session.RunSettlement{Status: session.RunInterrupted, FinishedAt: o.now()}
	committed, err := execution.store.SettleRun(ctx, session.SettleRunRequest{
		Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID()},
	})
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	execution.publishPersisted(ctx, committed.Event)
	return Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true}
}
