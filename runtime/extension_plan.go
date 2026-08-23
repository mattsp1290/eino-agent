package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

var ErrExtensionPlanMismatch = errors.New("extension plan mismatch")

type RunPlanRequest struct {
	SessionID session.ID
	Config    config.Snapshot
}

type RunPlanProvider interface {
	AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error)
	AcquireResumePlan(context.Context, session.ExtensionPlanDescriptor) (*RunPlan, error)
}

type RunPlan struct {
	Dispatch   *extension.Plan
	Tools      ToolRegistry
	Prompts    []MountedPrompt
	Guards     []MountedToolGuard
	Descriptor session.ExtensionPlanDescriptor
	// RequiresToolSettlement is set when the frozen plan contains a mounted
	// tool. Strict plans reject stores without atomic settlement before durable
	// admission or resume mutation.
	RequiresToolSettlement bool
	Release                func()
	once                   sync.Once
}

func (p *RunPlan) release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.Dispatch != nil {
			p.Dispatch.Release()
		}
		if p.Release != nil {
			p.Release()
		}
	})
}

func (o *StreamingOrchestrator) acquireRunPlan(ctx context.Context, request RunPlanRequest) (*RunPlan, error) {
	if o.Plans == nil {
		if o.hasLegacyExtensions() {
			return nil, fmt.Errorf("%w: anonymous extension fields require a run plan provider", ErrInvalidOrchestrator)
		}
		return &RunPlan{Descriptor: emptyExtensionPlanDescriptor()}, nil
	}
	plan, err := o.Plans.AcquireRunPlan(ctx, RunPlanRequest{SessionID: request.SessionID, Config: request.Config.Clone()})
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, fmt.Errorf("%w: provider returned nil plan", ErrExtensionPlanMismatch)
	}
	if plan.Descriptor.SchemaVersion == 0 {
		plan.Descriptor.SchemaVersion = session.ExtensionPlanSchemaVersion
	}
	if plan.Descriptor.Mode == "" {
		plan.Descriptor.Mode = session.PlanStrict
	}
	if plan.Descriptor.Mode != session.PlanStrict {
		plan.release()
		return nil, fmt.Errorf("%w: provider returned non-strict plan", ErrExtensionPlanMismatch)
	}
	if !descriptorOrderingVerifiable(plan.Descriptor) {
		plan.release()
		return nil, fmt.Errorf("%w: descriptor schema does not record prompt/guard order", ErrExtensionPlanMismatch)
	}
	providedFingerprint := plan.Descriptor.Fingerprint
	plan.Descriptor.Fingerprint = ""
	fingerprint, fingerprintErr := session.FingerprintExtensionPlan(plan.Descriptor)
	if fingerprintErr != nil {
		plan.release()
		return nil, fingerprintErr
	}
	if providedFingerprint != "" && providedFingerprint != fingerprint {
		plan.release()
		return nil, fmt.Errorf("%w: invalid fresh descriptor fingerprint", ErrExtensionPlanMismatch)
	}
	if o.hasLegacyExtensions() {
		plan.Descriptor.Mode = session.PlanPartialLegacy
	}
	fingerprint, fingerprintErr = session.FingerprintExtensionPlan(plan.Descriptor)
	if fingerprintErr != nil {
		plan.release()
		return nil, fingerprintErr
	}
	plan.Descriptor.Fingerprint = fingerprint
	if descriptorRequiresToolSettlement(plan.Descriptor) {
		if _, ok := o.Store.(session.ToolSettlementStore); !ok {
			plan.release()
			return nil, fmt.Errorf("%w: strict tool plan requires ToolSettlementStore", ErrInvalidOrchestrator)
		}
	}
	return plan, nil
}

func (o *StreamingOrchestrator) acquireResumePlan(ctx context.Context, descriptor session.ExtensionPlanDescriptor) (*RunPlan, error) {
	if descriptor.SchemaVersion == 0 || descriptor.Mode == "" || descriptor.Mode == session.PlanLegacy || descriptor.Fingerprint == "" {
		return nil, ErrExtensionPlanMismatch
	}
	persistedFingerprint, fingerprintErr := session.FingerprintExtensionPlan(descriptor)
	if fingerprintErr != nil || descriptor.Fingerprint != persistedFingerprint {
		return nil, ErrExtensionPlanMismatch
	}
	if descriptor.Mode == session.PlanStrict && o.hasLegacyExtensions() || descriptor.Mode == session.PlanPartialLegacy && !o.hasLegacyExtensions() {
		return nil, ErrExtensionPlanMismatch
	}
	if descriptor.Mode != session.PlanStrict && descriptor.Mode != session.PlanPartialLegacy {
		return nil, ErrExtensionPlanMismatch
	}
	if !descriptorOrderingVerifiable(descriptor) {
		return nil, fmt.Errorf("%w: descriptor schema does not record prompt/guard order", ErrExtensionPlanMismatch)
	}
	if o.Plans == nil {
		empty := emptyExtensionPlanDescriptor()
		if descriptor.Fingerprint == empty.Fingerprint && len(descriptor.Entries) == 0 && descriptor.SchemaVersion == empty.SchemaVersion {
			return &RunPlan{Descriptor: descriptor.Clone()}, nil
		}
		return nil, fmt.Errorf("%w: run requires a plan provider", ErrExtensionPlanMismatch)
	}
	plan, err := o.Plans.AcquireResumePlan(ctx, descriptor.Clone())
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, ErrExtensionPlanMismatch
	}
	if plan.Descriptor.Mode != descriptor.Mode {
		plan.release()
		return nil, ErrExtensionPlanMismatch
	}
	providedFingerprint := plan.Descriptor.Fingerprint
	fingerprint, fingerprintErr := session.FingerprintExtensionPlan(plan.Descriptor)
	if fingerprintErr != nil {
		plan.release()
		return nil, fingerprintErr
	}
	if providedFingerprint != "" && providedFingerprint != fingerprint {
		plan.release()
		return nil, fmt.Errorf("%w: invalid resume descriptor fingerprint", ErrExtensionPlanMismatch)
	}
	plan.Descriptor.Fingerprint = fingerprint
	if fingerprint != descriptor.Fingerprint {
		plan.release()
		return nil, ErrExtensionPlanMismatch
	}
	if descriptorRequiresToolSettlement(descriptor) {
		if _, ok := o.Store.(session.ToolSettlementStore); !ok {
			plan.release()
			return nil, fmt.Errorf("%w: strict tool plan requires ToolSettlementStore", ErrInvalidOrchestrator)
		}
	}
	return plan, nil
}

func (o *StreamingOrchestrator) hasLegacyExtensions() bool {
	return o.Tools != nil || len(o.Context) != 0 || len(o.Hooks) != 0 || len(o.Middleware) != 0
}

func emptyExtensionPlanDescriptor() session.ExtensionPlanDescriptor {
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
	return descriptor
}

func descriptorHasTools(descriptor session.ExtensionPlanDescriptor) bool {
	for _, entry := range descriptor.Entries {
		if entry.Kind == session.ExtensionTool && entry.Required {
			return true
		}
	}
	return false
}

func descriptorRequiresToolSettlement(descriptor session.ExtensionPlanDescriptor) bool {
	return descriptor.Mode == session.PlanStrict && descriptorHasTools(descriptor)
}

func descriptorOrderingVerifiable(descriptor session.ExtensionPlanDescriptor) bool {
	if descriptor.Mode == session.PlanLegacy || descriptor.SchemaVersion >= session.ExtensionPlanSchemaVersion {
		return true
	}
	for _, entry := range descriptor.Entries {
		if entry.Kind == session.ExtensionPrompt || entry.Kind == session.ExtensionGuard {
			return false
		}
	}
	return true
}
