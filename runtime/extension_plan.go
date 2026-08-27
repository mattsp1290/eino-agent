package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	Identity session.ToolPlanIdentity
	Resolve  func(context.Context, ToolScopeContext) (Tool, error)
}

// PlanPrompt binds one prompt implementation to its persisted identity.
type PlanPrompt struct {
	Identity session.PromptPlanIdentity
	Prompt   MountedPrompt
}

// PlanGuard binds one tool guard implementation to its persisted identity.
type PlanGuard struct {
	Identity session.GuardPlanIdentity
	Guard    MountedToolGuard
}

// PlanRestriction binds one tool restriction policy to its persisted identity.
type PlanRestriction struct {
	Identity session.RestrictionPlanIdentity
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
			if extension.ValidateComponent(extension.Component{InstanceID: diagnostic.InstanceID, Artifact: diagnostic.Artifact}) != nil || extension.ValidateIdentifier(diagnostic.ID) != nil || extension.ValidateContract(diagnostic.Contract) != nil || extension.ValidateScope(diagnostic.Scope) != nil {
				return fail(fmt.Errorf("%w: invalid dispatch diagnostic", ErrExtensionPlanMismatch))
			}
			index, ok := byInstance[diagnostic.InstanceID]
			if !ok {
				index = len(descriptor.Handlers)
				byInstance[diagnostic.InstanceID] = index
				descriptor.Handlers = append(descriptor.Handlers, session.HandlerPlanIdentity{
					InstanceID: diagnostic.InstanceID,
					Artifact:   diagnostic.Artifact,
				})
			}
			descriptor.Handlers[index].Registrations = append(descriptor.Handlers[index].Registrations, session.RegistrationIdentity{
				ID: diagnostic.ID, Contract: diagnostic.Contract.ID, Version: diagnostic.Contract.Version,
				Order: diagnostic.Order, Scope: diagnostic.Scope, Kind: session.HandlerKind(diagnostic.Kind),
			})
		}
	}

	tools := make([]PlanTool, len(spec.Tools))
	for index, capability := range spec.Tools {
		if capability.Resolve == nil {
			return fail(fmt.Errorf("%w: tool resolver required", ErrExtensionPlanMismatch))
		}
		tools[index] = capability
		descriptor.Tools = append(descriptor.Tools, capability.Identity)
	}
	prompts := make([]MountedPrompt, len(spec.Prompts))
	for index, capability := range spec.Prompts {
		if capability.Prompt.Provider == nil || capability.Prompt.Name == "" {
			return fail(fmt.Errorf("%w: prompt behavior required", ErrExtensionPlanMismatch))
		}
		if capability.Identity.Name != capability.Prompt.Name || capability.Identity.Order != capability.Prompt.Order || capability.Identity.InstanceID != capability.Prompt.InstanceID {
			return fail(fmt.Errorf("%w: prompt identity does not match behavior", ErrExtensionPlanMismatch))
		}
		prompts[index] = capability.Prompt
		descriptor.Prompts = append(descriptor.Prompts, capability.Identity)
	}
	guards := make([]MountedToolGuard, len(spec.Guards))
	for index, capability := range spec.Guards {
		if capability.Guard.Guard == nil || capability.Guard.ID == "" {
			return fail(fmt.Errorf("%w: guard behavior required", ErrExtensionPlanMismatch))
		}
		if capability.Identity.RegistrationID != capability.Guard.ID || capability.Identity.Order != capability.Guard.Order || capability.Identity.InstanceID != capability.Guard.InstanceID {
			return fail(fmt.Errorf("%w: guard identity does not match behavior", ErrExtensionPlanMismatch))
		}
		guards[index] = capability.Guard
		descriptor.Guards = append(descriptor.Guards, capability.Identity)
	}
	restrictions := make([]PlanRestriction, len(spec.Restrictions))
	for index, capability := range spec.Restrictions {
		rules, err := CanonicalizeRestrictionRules(capability.Allowed, capability.Denied)
		if err != nil {
			return fail(fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err))
		}
		if capability.Identity.RulesHash != rules.Hash {
			return fail(fmt.Errorf("%w: restriction identity does not match behavior", ErrExtensionPlanMismatch))
		}
		capability.Allowed = rules.Allowed
		capability.Denied = rules.Denied
		restrictions[index] = capability
		descriptor.Restrictions = append(descriptor.Restrictions, capability.Identity)
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

// RestrictionRules is the canonical identity and executable representation of
// one tool-restriction policy.
type RestrictionRules struct {
	Allowed []string
	Denied  []string
	Hash    string
}

// CanonicalizeRestrictionRules validates and canonicalizes one restriction set.
func CanonicalizeRestrictionRules(allowed, denied []string) (RestrictionRules, error) {
	canonicalize := func(values []string) ([]string, error) {
		seen := make(map[string]bool, len(values))
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return nil, errors.New("restriction tool name required")
			}
			seen[value] = true
		}
		if len(seen) == 0 {
			return nil, nil
		}
		result := make([]string, 0, len(seen))
		for value := range seen {
			result = append(result, value)
		}
		sort.Strings(result)
		return result, nil
	}
	allowed, err := canonicalize(allowed)
	if err != nil {
		return RestrictionRules{}, err
	}
	denied, err = canonicalize(denied)
	if err != nil {
		return RestrictionRules{}, err
	}
	if len(allowed) == 0 && len(denied) == 0 {
		return RestrictionRules{}, errors.New("restriction rules required")
	}
	deniedSet := make(map[string]bool, len(denied))
	for _, name := range denied {
		deniedSet[name] = true
	}
	for _, name := range allowed {
		if deniedSet[name] {
			return RestrictionRules{}, fmt.Errorf("restriction tool %q is both allowed and denied", name)
		}
	}
	raw, err := json.Marshal(struct{ Allowed, Denied []string }{allowed, denied})
	if err != nil {
		return RestrictionRules{}, err
	}
	digest := sha256.Sum256(raw)
	return RestrictionRules{Allowed: allowed, Denied: denied, Hash: hex.EncodeToString(digest[:])}, nil
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
		expected := capability.Identity.Name
		if tool.Name == "" || tool.Name != expected || seen[tool.Name] {
			return nil, fmt.Errorf("%w: sealed tool resolver returned %q for %q", ErrExtensionPlanMismatch, tool.Name, expected)
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
