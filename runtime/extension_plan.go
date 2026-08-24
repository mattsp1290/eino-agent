package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

type PlanTool struct {
	Identity session.ExtensionPlanEntry
	Resolve  func(context.Context, ToolScopeContext) (Tool, error)
}

// PlanPrompt binds one prompt implementation to its persisted identity.
type PlanPrompt struct {
	Identity session.ExtensionPlanEntry
	Prompt   MountedPrompt
}

// PlanGuard binds one tool guard implementation to its persisted identity.
type PlanGuard struct {
	Identity session.ExtensionPlanEntry
	Guard    MountedToolGuard
}

// PlanRestriction binds one tool restriction policy to its persisted identity.
type PlanRestriction struct {
	Identity session.ExtensionPlanEntry
	Allowed  []string
	Denied   []string
}

// RunPlanSpec is unfingerprinted behavior evidence. Each executable capability
// carries its identity in the same record; callers cannot provide a descriptor.
type RunPlanSpec struct {
	Dispatch     *extension.Plan
	Tools        []PlanTool
	Prompts      []PlanPrompt
	Guards       []PlanGuard
	Restrictions []PlanRestriction
	Release      func()
}

// RunPlan is the immutable executable state for one run.
type RunPlan struct {
	dispatch     *extension.Plan
	tools        sealedPlanTools
	prompts      []MountedPrompt
	guards       []MountedToolGuard
	descriptor   session.ExtensionPlanDescriptor
	releaseExtra func()
	once         sync.Once
}

// NewRunPlan validates identity-bound behavior and seals its descriptor.
func NewRunPlan(spec RunPlanSpec) (*RunPlan, error) {
	plan := &RunPlan{dispatch: spec.Dispatch, releaseExtra: spec.Release}
	fail := func(err error) (*RunPlan, error) {
		plan.release()
		return nil, err
	}
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion}
	if spec.Dispatch != nil {
		byInstance := make(map[string]int)
		for _, diagnostic := range spec.Dispatch.Diagnostics() {
			if diagnostic.InstanceID == "" || diagnostic.ID == "" || diagnostic.Contract.ID == "" || diagnostic.Contract.Version == "" {
				return fail(fmt.Errorf("%w: invalid dispatch diagnostic", ErrExtensionPlanMismatch))
			}
			index, ok := byInstance[diagnostic.InstanceID]
			if !ok {
				index = len(descriptor.Entries)
				byInstance[diagnostic.InstanceID] = index
				descriptor.Entries = append(descriptor.Entries, session.ExtensionPlanEntry{
					InstanceID: diagnostic.InstanceID,
					Kind:       session.ExtensionHandlers,
					Artifact: session.ArtifactIdentity{
						Name: diagnostic.Artifact.Name, Version: diagnostic.Artifact.Version,
						Hash: diagnostic.Artifact.Hash, ConfigHash: diagnostic.Artifact.ConfigHash,
						SourceKind: string(diagnostic.Artifact.SourceKind),
					},
					Required: true,
					Scope:    session.ExtensionScope{Kind: string(diagnostic.Scope.Kind), Key: diagnostic.Scope.Key},
				})
			}
			descriptor.Entries[index].Registrations = append(descriptor.Entries[index].Registrations, session.RegistrationIdentity{
				ID: diagnostic.ID, Contract: diagnostic.Contract.ID, Version: diagnostic.Contract.Version,
				Order: diagnostic.Order, Scope: session.ExtensionScope{Kind: string(diagnostic.Scope.Kind), Key: diagnostic.Scope.Key},
			})
		}
	}

	tools := make([]PlanTool, len(spec.Tools))
	for index, capability := range spec.Tools {
		if err := validatePlanIdentity(capability.Identity, session.ExtensionTool); err != nil || capability.Resolve == nil {
			if err == nil {
				err = errors.New("tool resolver required")
			}
			return fail(fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err))
		}
		tools[index] = capability
		descriptor.Entries = append(descriptor.Entries, capability.Identity.Clone())
	}
	prompts := make([]MountedPrompt, len(spec.Prompts))
	for index, capability := range spec.Prompts {
		if err := validatePlanIdentity(capability.Identity, session.ExtensionPrompt); err != nil || capability.Prompt.Provider == nil || capability.Prompt.Name == "" {
			if err == nil {
				err = errors.New("prompt behavior required")
			}
			return fail(fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err))
		}
		prompts[index] = capability.Prompt
		descriptor.Entries = append(descriptor.Entries, capability.Identity.Clone())
	}
	guards := make([]MountedToolGuard, len(spec.Guards))
	for index, capability := range spec.Guards {
		if err := validatePlanIdentity(capability.Identity, session.ExtensionGuard); err != nil || capability.Guard.Guard == nil || capability.Guard.ID == "" {
			if err == nil {
				err = errors.New("guard behavior required")
			}
			return fail(fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err))
		}
		guards[index] = capability.Guard
		descriptor.Entries = append(descriptor.Entries, capability.Identity.Clone())
	}
	restrictions := make([]PlanRestriction, len(spec.Restrictions))
	for index, capability := range spec.Restrictions {
		if err := validatePlanIdentity(capability.Identity, session.ExtensionRestriction); err != nil {
			return fail(fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err))
		}
		capability.Allowed = append([]string(nil), capability.Allowed...)
		capability.Denied = append([]string(nil), capability.Denied...)
		restrictions[index] = capability
		descriptor.Entries = append(descriptor.Entries, capability.Identity.Clone())
	}
	fingerprint, err := session.FingerprintExtensionPlan(descriptor)
	if err != nil {
		return fail(err)
	}
	descriptor.Fingerprint = fingerprint
	plan.tools = sealedPlanTools{capabilities: tools, restrictions: restrictions}
	plan.prompts = prompts
	plan.guards = guards
	plan.descriptor = descriptor
	return plan, nil
}

func validatePlanIdentity(identity session.ExtensionPlanEntry, kind session.ExtensionKind) error {
	if identity.Kind != kind || identity.InstanceID == "" || identity.CapabilityID == "" || !identity.Required {
		return errors.New("invalid capability identity")
	}
	if identity.Artifact.Name == "" || identity.Artifact.Version == "" || identity.Artifact.Hash == "" || identity.Artifact.SourceKind == "" {
		return errors.New("invalid artifact identity")
	}
	if identity.Scope.Kind != string(extension.ScopeGlobal) && identity.Scope.Kind != string(extension.ScopeSession) {
		return errors.New("invalid capability scope")
	}
	if identity.Scope.Kind == string(extension.ScopeGlobal) && identity.Scope.Key != "" || identity.Scope.Kind == string(extension.ScopeSession) && identity.Scope.Key == "" {
		return errors.New("invalid capability scope key")
	}
	return nil
}

type sealedPlanTools struct {
	capabilities []PlanTool
	restrictions []PlanRestriction
}

func (s sealedPlanTools) ResolveTools(ctx context.Context, scope ToolScopeContext) ([]Tool, error) {
	result := make([]Tool, 0, len(s.capabilities))
	seen := make(map[string]bool, len(s.capabilities))
	for _, capability := range s.capabilities {
		tool, err := capability.Resolve(ctx, scope.Clone())
		if err != nil {
			return nil, err
		}
		expected := strings.SplitN(capability.Identity.CapabilityID, "/", 2)[0]
		if tool.Name == "" || tool.Name != expected || seen[tool.Name] {
			return nil, fmt.Errorf("%w: sealed tool resolver returned %q for %q", ErrExtensionPlanMismatch, tool.Name, capability.Identity.CapabilityID)
		}
		seen[tool.Name] = true
		if planToolAllowed(tool.Name, s.restrictions) {
			cloned, cloneErr := cloneToolChecked(tool)
			if cloneErr != nil {
				return nil, fmt.Errorf("freeze resolved tool %q: %w", tool.Name, cloneErr)
			}
			result = append(result, cloned)
		}
	}
	return result, nil
}

func planToolAllowed(name string, restrictions []PlanRestriction) bool {
	for _, restriction := range restrictions {
		for _, denied := range restriction.Denied {
			if denied == name {
				return false
			}
		}
		if len(restriction.Allowed) != 0 {
			allowed := false
			for _, candidate := range restriction.Allowed {
				allowed = allowed || candidate == name
			}
			if !allowed {
				return false
			}
		}
	}
	return true
}

func (p *RunPlan) Descriptor() session.ExtensionPlanDescriptor {
	if p == nil {
		return session.ExtensionPlanDescriptor{}
	}
	return p.descriptor.Clone()
}

// ResolveTools materializes the immutable tool capabilities sealed into the
// plan from bounded scope data. It never consults a live registry.
func (p *RunPlan) ResolveTools(ctx context.Context, scope ToolScopeContext) ([]Tool, error) {
	if p == nil || len(p.tools.capabilities) == 0 {
		return nil, nil
	}
	return p.tools.ResolveTools(ctx, scope)
}

// Prompts returns a defensive copy of the sealed prompt capability list.
func (p *RunPlan) Prompts() []MountedPrompt {
	if p == nil {
		return nil
	}
	return append([]MountedPrompt(nil), p.prompts...)
}

// Guards returns a defensive copy of the sealed guard capability list.
func (p *RunPlan) Guards() []MountedToolGuard {
	if p == nil {
		return nil
	}
	return append([]MountedToolGuard(nil), p.guards...)
}

func (p *RunPlan) Release() { p.release() }

func (p *RunPlan) release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.dispatch != nil {
			p.dispatch.Release()
		}
		if p.releaseExtra != nil {
			p.releaseExtra()
		}
	})
}

func (o *StreamingOrchestrator) acquireRunPlan(ctx context.Context, request RunPlanRequest) (*RunPlan, error) {
	if o.plans == nil {
		return NewRunPlan(RunPlanSpec{})
	}
	plan, err := o.plans.AcquireRunPlan(ctx, RunPlanRequest{SessionID: request.SessionID, Config: request.Config.Clone()})
	if err != nil {
		return nil, err
	}
	if plan == nil || plan.descriptor.SchemaVersion != session.ExtensionPlanSchemaVersion || plan.descriptor.Fingerprint == "" {
		if plan != nil {
			plan.release()
		}
		return nil, fmt.Errorf("%w: provider returned invalid plan", ErrExtensionPlanMismatch)
	}
	return plan, nil
}

func (o *StreamingOrchestrator) acquireResumePlan(ctx context.Context, descriptor session.ExtensionPlanDescriptor) (*RunPlan, error) {
	if descriptor.SchemaVersion != session.ExtensionPlanSchemaVersion || descriptor.Fingerprint == "" {
		return nil, ErrExtensionPlanMismatch
	}
	fingerprint, err := session.FingerprintExtensionPlan(descriptor)
	if err != nil || fingerprint != descriptor.Fingerprint {
		return nil, ErrExtensionPlanMismatch
	}
	if o.plans == nil {
		empty, emptyErr := NewRunPlan(RunPlanSpec{})
		if emptyErr == nil && descriptor.Fingerprint == empty.descriptor.Fingerprint && len(descriptor.Entries) == 0 {
			return empty, nil
		}
		return nil, fmt.Errorf("%w: run requires a plan provider", ErrExtensionPlanMismatch)
	}
	plan, err := o.plans.AcquireResumePlan(ctx, descriptor.Clone())
	if err != nil {
		return nil, err
	}
	if plan == nil || plan.descriptor.Fingerprint != descriptor.Fingerprint {
		if plan != nil {
			plan.release()
		}
		return nil, ErrExtensionPlanMismatch
	}
	return plan, nil
}

func emptyExtensionPlanDescriptor() session.ExtensionPlanDescriptor {
	plan, err := NewRunPlan(RunPlanSpec{})
	if err != nil {
		panic(err)
	}
	return plan.Descriptor()
}
