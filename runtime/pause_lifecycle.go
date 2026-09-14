package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/cloudwego/eino/adk"

	"github.com/mattsp1290/eino-agent/internal/jsonvalue"
	"github.com/mattsp1290/eino-agent/session"
)

// resumeLifecycleFact carries the immutable paused boundary through a resumed
// coordinator until its first physical successor dispatch has a durable
// invocation ID. It is deliberately one-shot: a retry is represented by its
// own AttemptReplaced event, never another resume fact.
type resumeLifecycleFact struct {
	paused    session.PauseLifecycleV1
	targetIDs []string
	mode      string
	emitted   bool
	mu        sync.Mutex
}

func loadPauseLifecycle(ctx context.Context, store session.Store, sessionID session.ID, runID session.RunID, checkpointRevision int64, targets map[string]any) (*resumeLifecycleFact, error) {
	cursor := session.EventCursor{Limit: 100}
	var matched *session.PauseLifecycleV1
	for {
		batch, err := store.ListEvents(ctx, sessionID, cursor)
		if err != nil {
			return nil, err
		}
		for _, event := range batch.Events {
			if event.RunID != runID || event.Kind != session.RunPausedEventKind {
				continue
			}
			// A post-claim StartRun failure is compensated by a payload-less
			// audit record correlated to the unresolved lifecycle pause. It did
			// not promote a new checkpoint or create a new pause generation, so
			// retain that exact currently matched lifecycle. Correlation only
			// preserves a match when it names the match's durable event revision;
			// every other payload-less pause remains authoritative and clears it.
			if jsonvalue.IsAbsent(event.Payload) {
				if matched != nil && event.Correlation != "" && event.Correlation == matched.EventRevision {
					continue
				}
				matched = nil
				continue
			}
			// A later repause is the authoritative generation even when it is
			// malformed. Never resurrect an older fact sharing a promoted
			// checkpoint revision.
			matched = nil
			var value session.PauseLifecycleV1
			if json.Unmarshal(event.Payload, &value) != nil || session.ValidatePauseLifecycle(session.RunPausedEventKind, value) != nil {
				continue // historical/non-lifecycle pause records remain resumable
			}
			if value.CheckpointRevision == checkpointRevision {
				copy := value
				matched = &copy
			}
		}
		if batch.Next.AfterEventID == "" {
			break
		}
		cursor = batch.Next
	}
	if matched == nil {
		return nil, nil
	}
	ids := make([]string, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	mode := session.ResumeModeTargeted
	if len(ids) == 0 {
		mode = session.ResumeModeFull
	} else {
		known := make(map[string]bool, len(matched.Targets))
		for _, target := range matched.Targets {
			known[target.ID] = true
		}
		allTargets := len(known) == len(ids)
		for _, id := range ids {
			allTargets = allTargets && known[id]
		}
		for _, id := range ids {
			if !known[id] {
				return nil, fmt.Errorf("%w: resume target %q is not in current pause generation", ErrInvalidOrchestrator, id)
			}
		}
		if allTargets {
			mode = session.ResumeModeFull
		}
	}
	return &resumeLifecycleFact{paused: *matched, targetIDs: ids, mode: mode}, nil
}

func (f *resumeLifecycleFact) emit(ctx context.Context, engine *adkEngine, messageID session.MessageID, invocationID string) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.emitted {
		return nil
	}
	eventID := engine.host.ids.NewEventID()
	value := f.paused
	value.EventRevision = string(eventID)
	value.ResumedPauseID = f.paused.PauseID
	value.ResumedTargetIDs = append([]string(nil), f.targetIDs...)
	value.ResumeMode = f.mode
	value.NewTurnID = engine.turn.ID
	value.NewAttemptID = invocationID
	value.ResumePhase = session.ResumePhaseFact
	if err := session.ValidatePauseLifecycle(session.RunResumedEventKind, value); err != nil {
		return fmt.Errorf("build resumed lifecycle: %w", err)
	}
	committed, err := engine.execution.store.AppendEvent(ctx, session.EventRecord{
		ID: eventID, SessionID: engine.snapshot.SessionID, RunID: engine.snapshot.RunID, EpochID: engine.snapshot.EpochID,
		MessageID: messageID, TurnID: engine.turn.ID, AgentPath: engine.agentPath, Kind: session.RunResumedEventKind,
		Correlation: f.paused.EventRevision, Payload: mustJSON(value), CreatedAt: engine.host.now(),
	})
	if err != nil {
		return err
	}
	engine.execution.publishPersisted(ctx, committed)
	f.emitted = true
	return nil
}

// pauseLifecyclePayload builds the immutable, redacted payload stored in the
// same transaction as the pause event. It snapshots only target identity and
// address, never adk.InterruptCtx.Info or a host's resume decision.
func pauseLifecyclePayload(event session.EventRecord, revision int64, agentPath string, targets []*adk.InterruptCtx) (json.RawMessage, error) {
	if agentPath == "" {
		agentPath = "root"
	}
	value := session.PauseLifecycleV1{
		Version: session.PauseLifecycleVersion, PauseID: string(event.ID), CheckpointRevision: revision,
		Generation: revision, AgentPath: agentPath, MessageID: session.MessageID("pause:" + string(event.RunID)),
		AttemptID: "pause:" + string(event.RunID), EventRevision: string(event.ID),
	}
	for _, target := range targets {
		if target == nil {
			return nil, fmt.Errorf("nil pause interrupt target")
		}
		value.Targets = append(value.Targets, session.PauseInterruptTarget{ID: target.ID, Address: target.Address.String()})
	}
	if err := session.ValidatePauseLifecycle(session.RunPausedEventKind, value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
