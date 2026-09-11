package runtime

import (
	"bytes"
	"context"
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
}

var (
	_ adk.CheckPointStore   = (*adkCheckpointStore)(nil)
	_ adk.CheckPointDeleter = (*adkCheckpointStore)(nil)
)

// adkCheckpointEnvelope is the versioned private wrapper this adapter stores
// as session.Checkpoint.Bytes. The upstream ADK gob payload itself is opaque
// and only decoded by ADK after this envelope's own fields validate.
type adkCheckpointEnvelope struct {
	EinoVersion string
	CodecVersion int
	Fingerprint  string
	Payload      []byte
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
func (s *adkCheckpointStore) Set(ctx context.Context, checkPointID string, payload []byte) error {
	revision, err := s.stagedRevision(ctx)
	if err != nil {
		return err
	}
	envelope := adkCheckpointEnvelope{EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, Fingerprint: s.fingerprint, Payload: payload}
	raw, err := encodeCheckpointEnvelope(envelope)
	if err != nil {
		return err
	}
	_, err = s.execution.store.StageCheckpoint(ctx, session.StageCheckpointRequest{
		Checkpoint: session.Checkpoint{
			RunID: s.runID, Revision: revision, Kind: session.CheckpointKindRunner, AgentFingerprint: s.fingerprint,
			EinoVersion: EinoPinnedVersion, CodecVersion: adkCheckpointCodecVersion, CheckpointID: checkPointID,
			Bytes: raw, CreatedAt: s.host.now(),
		},
		MaxBytes: s.maxBytes,
	})
	if err != nil {
		return err
	}
	s.lastStaged = revision
	return nil
}

// Delete implements adk.CheckPointDeleter. It records retirement intent for
// every staged revision up to and including the latest one this adapter
// staged; the runtime applies it (RetireCheckpoints under the still-live
// fence, or RetireRunCheckpoints after terminal settlement) once the exit
// protocol has finished acting on the checkpoint, since upstream may ignore
// a Delete error.
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
	if envelope.EinoVersion == "" || envelope.CodecVersion <= 0 || envelope.Fingerprint == "" || len(envelope.Payload) == 0 {
		return adkCheckpointEnvelope{}, ErrCheckpointMalformed
	}
	return envelope, nil
}

func encodeCheckpointEnvelope(envelope adkCheckpointEnvelope) ([]byte, error) {
	return json.Marshal(envelope)
}
