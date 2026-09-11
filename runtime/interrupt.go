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
