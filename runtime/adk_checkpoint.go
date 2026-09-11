package runtime

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cloudwego/eino/adk"

	"github.com/mattsp1290/eino-agent/session"
)

// EinoPinnedVersion is the exact pinned Eino module version this runtime's
// checkpoint envelopes are versioned against (see docs/architecture/eino-feature-support.md
// "Pin"). A checkpoint promoted under a different Eino version must never be
// resumed by this binary.
const EinoPinnedVersion = "v0.9.19"

// adkCheckpointCodecVersion is bumped whenever this package changes what it
// gob-encodes into a checkpoint's envelope in an incompatible way.
const adkCheckpointCodecVersion = 1

var (
	// ErrCheckpointVersionMismatch reports a checkpoint envelope stamped with
	// an Eino version or codec version this binary does not support.
	ErrCheckpointVersionMismatch = errors.New("adk checkpoint version mismatch")
	// ErrCheckpointFingerprintMismatch reports a checkpoint envelope whose
	// plan fingerprint does not match the run's current frozen plan.
	ErrCheckpointFingerprintMismatch = errors.New("adk checkpoint plan fingerprint mismatch")
	// ErrCheckpointMalformed reports a checkpoint envelope that failed
	// structural validation before any attempt to gob-decode its payload.
	ErrCheckpointMalformed = errors.New("adk checkpoint envelope malformed")
)

// adkCheckpointStore implements adk.CheckPointStore and adk.CheckPointDeleter
// as an execution-bound adapter over the durable session.Checkpoint model
// (session/checkpoint.go, store/internal/sqlstore/checkpoints.go). Set stages
// a new revision under the current run fence (bounded, enveloped with the
// run's plan fingerprint, agent factory descriptor, the pinned Eino version,
// and this package's codec version); it never promotes a revision itself --
// promotion is the TurnLoop exit protocol's job (runtime/turn_loop.go), after
// the iterator has fully drained with no error. Get reads only PROMOTED
// bytes: an unpromoted (merely staged) revision is never resumable. Delete
// records retirement intent; the runtime applies it after terminal or
// continuation settlement (see RetireCheckpoints/RetireRunCheckpoints), since
// upstream may ignore a Delete error and this adapter must never let a stale
// revision survive a successful retirement request.
type adkCheckpointStore struct {
	host      *StreamingOrchestrator
	execution *runExecution
	runID     session.RunID
	// fingerprint identifies this run's frozen plan + agent factory
	// descriptor; it is stamped into every staged envelope and checked on
	// Get.
	fingerprint string
	maxBytes    int
	// lastStaged is the highest revision this adapter instance has staged
	// (or, once known, retired up to). It lets Delete retire without a
	// redundant read when Set already established the boundary in this
	// process.
	lastStaged int64
	// currentTurnID is stamped into every envelope this adapter stages
	// (round-five reconciliation item 2/TR-I1): the coordinator keeps it in
	// sync with whichever turn's adkEngine is currently set (see
	// turnLoopCoordinator.setEngine), so a Set call upstream ADK makes
	// mid-dispatch -- whether a periodic tool-boundary checkpoint or a
	// genuine tool-interrupt pause -- always records the turn it actually
	// belongs to. A promoted checkpoint's own TurnID is what lets ResumeRun
	// refuse to replay one whose turn has since completed, instead of
	// silently redriving the wrong turn's identity (see ResumeRun and
	// resumeEngine).
	currentTurnID session.TurnID
}

// setCurrentTurnID records which turn's checkpoint state this adapter's
// NEXT stage call belongs to. See currentTurnID's doc comment.
func (s *adkCheckpointStore) setCurrentTurnID(id session.TurnID) {
	s.currentTurnID = id
}

var (
	_ adk.CheckPointStore   = (*adkCheckpointStore)(nil)
	_ adk.CheckPointDeleter = (*adkCheckpointStore)(nil)
)

// adkCheckpointEnvelope is the versioned private wrapper this adapter stores
// as session.Checkpoint.Bytes. The upstream ADK gob payload itself is opaque
// and only decoded by ADK after this envelope's own fields validate.
type adkCheckpointEnvelope struct {
	EinoVersion  string
	CodecVersion int
	Fingerprint  string
	// TurnID is the durable turn this checkpoint revision belongs to
	// (round-five reconciliation item 2/TR-I1) -- see adkCheckpointStore's
	// currentTurnID doc comment for how it is populated. ResumeRun
	// validates it against the run's newest non-completed turn before ever
	// claiming the run's fence, and completeTurn retires a promoted
	// checkpoint whose TurnID matches the turn that just completed.
	TurnID  session.TurnID
	Payload []byte
}

func (s *adkCheckpointStore) nextRevision(ctx context.Context) (int64, error) {
	checkpoint, ok, err := s.host.store.ReadPromotedCheckpoint(ctx, s.runID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 1, nil
	}
	return checkpoint.Revision + 1, nil
}

// Get implements adk.CheckPointStore. It reads only the latest PROMOTED
// revision, never a merely staged one, and validates the envelope's Eino
// version, codec version, and plan fingerprint before returning the opaque
// upstream payload.
func (s *adkCheckpointStore) Get(ctx context.Context, checkPointID string) ([]byte, bool, error) {
	checkpoint, ok, err := s.host.store.ReadPromotedCheckpoint(ctx, s.runID)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	envelope, err := decodeCheckpointEnvelope(checkpoint.Bytes)
	if err != nil {
		return nil, false, err
	}
	if envelope.EinoVersion != EinoPinnedVersion || envelope.CodecVersion != adkCheckpointCodecVersion {
		return nil, false, fmt.Errorf("%w: envelope eino=%q codec=%d, binary eino=%q codec=%d",
			ErrCheckpointVersionMismatch, envelope.EinoVersion, envelope.CodecVersion, EinoPinnedVersion, adkCheckpointCodecVersion)
	}
	if envelope.Fingerprint != s.fingerprint {
		return nil, false, ErrCheckpointFingerprintMismatch
	}
	return envelope.Payload, true, nil
}

// Set implements adk.CheckPointStore. It stages a new unpromoted revision
// under the current run fence; the caller (runtime/turn_loop.go) promotes it
// only after the run's iterator has fully drained with no error.
//
// nextRevision only knows about the latest PROMOTED revision: a fresh
// adapter instance (this run's first Set after a resume, or after this
// process restarted) that lands on a revision a *different*, crashed
// staging attempt already occupied with different payload bytes gets
// ErrConflict from StageCheckpoint (createCheckpoint's SameRecord check).
// Rather than block every later Set for this run on that stale row, probe
// forward past it: this is safe exactly because the probing only ever
// happens on a fresh instance's first attempt (lastStaged == 0 on entry);
// once this instance has staged anything, s.lastStaged is trusted and a
// conflict is reported immediately as genuine.
func (s *adkCheckpointStore) Set(ctx context.Context, checkPointID string, payload []byte) error {
	return s.stage(ctx, checkPointID, session.CheckpointKindRunner, payload)
}

// stageLoopCheckpoint stages this adapter's own Kind=loop "between turns, no
// runner state" checkpoint revision: an ADK-shaped envelope
// (HasRunnerState=false, no unhandled/canceled items) that tryLoadCheckpoint
// treats exactly like an ordinary between-turns checkpoint on any later
// resume -- items simply flow through GenInput next time, never GenResume
// (see eino's adk/turn_loop.go tryLoadCheckpoint). Used by
// promoteQueuedContinuation when upstream's own cleanup found the loop idle
// and so never called Set itself (see that function's doc comment), so
// PromotePause always has a real staged revision to promote.
func (s *adkCheckpointStore) stageLoopCheckpoint(ctx context.Context) error {
	payload, err := marshalEmptyLoopCheckpoint()
	if err != nil {
		return err
	}
	return s.stage(ctx, string(s.runID), session.CheckpointKindLoop, payload)
}

// stage is Set/stageLoopCheckpoint's shared staging logic: it stages a new
// unpromoted revision under the current run fence, probing forward past a
// stale row from a crashed prior attempt exactly as Set's own doc comment
// describes.
func (s *adkCheckpointStore) stage(ctx context.Context, checkPointID string, kind session.CheckpointKind, payload []byte) error {
	if s.currentTurnID == "" {
		return fmt.Errorf("adk checkpoint store: staging revision for run %s with no current turn ID set", s.runID)
	}
	revision, err := s.stagedRevision(ctx)
	if err != nil {
		return err
	}
	trustedSequence := s.lastStaged > 0
	envelope := adkCheckpointEnvelope{EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, Fingerprint: s.fingerprint, TurnID: s.currentTurnID, Payload: payload}
	raw, err := encodeCheckpointEnvelope(envelope)
	if err != nil {
		return err
	}
	const maxProbeAttempts = 8
	for attempt := 0; ; attempt++ {
		_, stageErr := s.execution.store.StageCheckpoint(ctx, session.StageCheckpointRequest{
			Checkpoint: session.Checkpoint{
				RunID: s.runID, Revision: revision, Kind: kind, AgentFingerprint: s.fingerprint,
				EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, CheckpointID: checkPointID,
				Bytes: raw, CreatedAt: s.host.now(),
			},
			MaxBytes: s.maxBytes,
		})
		if stageErr == nil {
			s.lastStaged = revision
			return nil
		}
		if trustedSequence || !errors.Is(stageErr, session.ErrConflict) || attempt >= maxProbeAttempts-1 {
			return stageErr
		}
		revision++
	}
}

// turnLoopCheckpointShape mirrors the unexported field names/types of
// eino's adk.turnLoopCheckpoint[session.InboxID] (adk/turn_loop.go): gob
// matches by field name and type, not by concrete struct identity, so
// encoding this shape produces bytes eino's own unmarshalTurnLoopCheckpoint
// decodes correctly, and decoding upstream's own bytes with this shape
// recovers them correctly (see decodeLoopCheckpointHasRunnerState, which
// does exactly that in production on ResumeRun's path). Kept in exact sync
// with upstream's turnLoopCheckpoint; TestTurnLoopCheckpointShapeDecodesThroughRealTurnLoop
// (runtime/w5_round3_test.go) drives an encoded non-zero value of this
// shape through a real adk.TurnLoop's CheckPointStore/GenInput, so a
// one-field rename here fails that test loudly (it decodes to zero through
// eino's own unmarshalTurnLoopCheckpoint) instead of silently misreading a
// promoted checkpoint's UnhandledItems.
type turnLoopCheckpointShape struct {
	RunnerCheckpoint []byte
	HasRunnerState   bool
	UnhandledItems   []session.InboxID
	CanceledItems    []session.InboxID
}

// marshalEmptyLoopCheckpoint gob-encodes a turnLoopCheckpointShape with
// HasRunnerState=false and no items: eino's tryLoadCheckpoint treats this
// exactly like an ordinary "between turns, nothing pending" checkpoint.
func marshalEmptyLoopCheckpoint() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(turnLoopCheckpointShape{}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeLoopCheckpointHasRunnerState gob-decodes an opaque upstream
// turn-loop checkpoint payload (the Payload field of an already-validated
// adkCheckpointEnvelope) just far enough to read HasRunnerState -- so a
// caller can determine, BEFORE ever calling adk.TurnLoop.Run, whether
// eino's own tryLoadCheckpoint will resume this run via GenResume (true) or
// route straight through GenInput (false); see eino's adk/turn_loop.go. Used
// by ResumeRun to decide whether pushing reconciledTurnSentinelID (which is
// only ever safe to deliver through GenInput -- see resumeReconciledTurn's
// doc comment) is safe for this resume. A decode failure is reported, not
// silently treated as false: guessing wrong here would let the sentinel
// race a genuine ADK-level resume instead of being pushed only when it is
// guaranteed to land in GenInput.
func decodeLoopCheckpointHasRunnerState(payload []byte) (bool, error) {
	var shape turnLoopCheckpointShape
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&shape); err != nil {
		return false, err
	}
	return shape.HasRunnerState, nil
}

// Delete implements adk.CheckPointDeleter. It retires every staged revision
// up to and including the latest one this adapter staged immediately, under
// the still-live run fence (RetireCheckpoints) -- not deferred until after
// the exit protocol finishes acting on the checkpoint. This is safe to call
// eagerly (upstream may ignore its returned error) precisely because
// retirement only ever deletes revisions this same fenced execution staged;
// nothing else can promote or depend on them in the meantime.
func (s *adkCheckpointStore) Delete(ctx context.Context, checkPointID string) error {
	if s.lastStaged <= 0 {
		checkpoint, ok, err := s.host.store.ReadPromotedCheckpoint(ctx, s.runID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		s.lastStaged = checkpoint.Revision
	}
	return s.execution.store.RetireCheckpoints(ctx, s.lastStaged)
}

func (s *adkCheckpointStore) stagedRevision(ctx context.Context) (int64, error) {
	if s.lastStaged > 0 {
		return s.lastStaged + 1, nil
	}
	return s.nextRevision(ctx)
}

func decodeCheckpointEnvelope(raw []byte) (adkCheckpointEnvelope, error) {
	var envelope adkCheckpointEnvelope
	if len(raw) == 0 {
		return adkCheckpointEnvelope{}, ErrCheckpointMalformed
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return adkCheckpointEnvelope{}, fmt.Errorf("%w: %v", ErrCheckpointMalformed, err)
	}
	if envelope.EinoVersion == "" || envelope.CodecVersion <= 0 || envelope.Fingerprint == "" || envelope.TurnID == "" || len(envelope.Payload) == 0 {
		return adkCheckpointEnvelope{}, ErrCheckpointMalformed
	}
	return envelope, nil
}

func encodeCheckpointEnvelope(envelope adkCheckpointEnvelope) ([]byte, error) {
	return json.Marshal(envelope)
}
