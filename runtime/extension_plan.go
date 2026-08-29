package runtime

import (
	"cmp"
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

type ResumePlanRequest struct {
	SessionID  session.ID
	Descriptor session.ExtensionPlanDescriptor
}

type RunPlanProvider interface {
	AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error)
	AcquireResumePlan(context.Context, ResumePlanRequest) (*RunPlan, error)
}

type PlanTool struct {
	Identity session.ToolPlanIdentity
	Resolve  func(context.Context, ToolScopeContext) (Tool, error)
}

// PlanPrompt binds one prompt implementation to its persisted identity.
type PlanPrompt struct {
	Identity session.PromptPlanIdentity
	Provider PromptProvider
}

// PlanGuard binds one tool guard implementation to its persisted identity.
type PlanGuard struct {
	Identity session.GuardPlanIdentity
	Guard    ToolGuard
}

// PlanRestriction binds one tool restriction policy to its persisted identity.
type PlanRestriction struct {
	Identity session.RestrictionPlanIdentity
	Allowed  []string
	Denied   []string
}

type PlanComponent struct {
	Component    extension.Component
	Handlers     []extension.HandlerIdentity
	Tools        []PlanTool
	Prompts      []PlanPrompt
	Guards       []PlanGuard
	Restrictions []PlanRestriction
}

// RunPlanSpec is unfingerprinted, component-owned behavior evidence.
type RunPlanSpec struct {
	Dispatch   *extension.Plan
	Components []PlanComponent
}

// RunPlan is the immutable executable state for one run.
type RunPlan struct {
	dispatch   *extension.Plan
	tools      sealedPlanTools
	prompts    []MountedPrompt
	guards     []MountedToolGuard
	descriptor session.ExtensionPlanDescriptor
	once       sync.Once
}

// NewRunPlan validates identity-bound behavior and seals its descriptor.
func NewRunPlan(spec RunPlanSpec) (*RunPlan, error) {
	plan := &RunPlan{dispatch: spec.Dispatch}
	fail := func(err error) (*RunPlan, error) {
		plan.release()
		return nil, err
	}
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion}
	components := append([]PlanComponent(nil), spec.Components...)
	sort.Slice(components, func(i, j int) bool { return components[i].Component.InstanceID < components[j].Component.InstanceID })
	authoritativeHandlers := []extension.ComponentHandlers(nil)
	if spec.Dispatch != nil {
		authoritativeHandlers = spec.Dispatch.HandlerComponents()
	}
	matchedHandlers := make([]bool, len(authoritativeHandlers))
	type ownedTool struct {
		owner string
		value PlanTool
	}
	type ownedRestriction struct {
		owner string
		value PlanRestriction
	}
	var tools []ownedTool
	var prompts []MountedPrompt
	var guards []MountedToolGuard
	var restrictions []ownedRestriction
	for componentIndex, owned := range components {
		if err := extension.ValidateComponent(owned.Component); err != nil || componentIndex > 0 && components[componentIndex-1].Component.InstanceID == owned.Component.InstanceID {
			return fail(fmt.Errorf("%w: invalid or duplicate component owner", ErrExtensionPlanMismatch))
		}
		if len(owned.Handlers)+len(owned.Tools)+len(owned.Prompts)+len(owned.Guards)+len(owned.Restrictions) == 0 {
			return fail(fmt.Errorf("%w: empty component owner", ErrExtensionPlanMismatch))
		}
		var expected []extension.HandlerIdentity
		for index, candidate := range authoritativeHandlers {
			if candidate.Component.InstanceID == owned.Component.InstanceID {
				if candidate.Component != owned.Component {
					return fail(fmt.Errorf("%w: conflicting handler owner", ErrExtensionPlanMismatch))
				}
				expected = candidate.Handlers
				matchedHandlers[index] = true
				break
			}
		}
		if !sameHandlerIdentities(expected, owned.Handlers) {
			return fail(fmt.Errorf("%w: dispatch handler identities differ", ErrExtensionPlanMismatch))
		}
		durable := session.ComponentPlan{InstanceID: owned.Component.InstanceID, Artifact: owned.Component.Artifact}
		for _, handler := range owned.Handlers {
			durable.Handlers = append(durable.Handlers, session.RegistrationIdentity{ID: handler.ID, Contract: handler.Contract.ID, Version: handler.Contract.Version, Order: handler.Order, Scope: handler.Scope, Kind: handler.Kind})
		}
		for _, capability := range owned.Tools {
			if capability.Resolve == nil {
				return fail(fmt.Errorf("%w: tool resolver required", ErrExtensionPlanMismatch))
			}
			tools = append(tools, ownedTool{owner: owned.Component.InstanceID, value: capability})
			durable.Tools = append(durable.Tools, capability.Identity)
		}
		for _, capability := range owned.Prompts {
			if capability.Provider == nil || capability.Identity.Name == "" {
				return fail(fmt.Errorf("%w: prompt behavior required", ErrExtensionPlanMismatch))
			}
			prompts = append(prompts, MountedPrompt{Name: capability.Identity.Name, ID: capability.Identity.RegistrationID, Scope: capability.Identity.Scope, Order: capability.Identity.Order, InstanceID: owned.Component.InstanceID, Provider: capability.Provider})
			durable.Prompts = append(durable.Prompts, capability.Identity)
		}
		for _, capability := range owned.Guards {
			if capability.Guard == nil || capability.Identity.RegistrationID == "" {
				return fail(fmt.Errorf("%w: guard behavior required", ErrExtensionPlanMismatch))
			}
			guards = append(guards, MountedToolGuard{ID: capability.Identity.RegistrationID, Order: capability.Identity.Order, InstanceID: owned.Component.InstanceID, Scope: capability.Identity.Scope, Guard: capability.Guard})
			durable.Guards = append(durable.Guards, capability.Identity)
		}
		for _, capability := range owned.Restrictions {
			rules, err := CanonicalizeRestrictionRules(capability.Allowed, capability.Denied)
			if err != nil {
				return fail(fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err))
			}
			if capability.Identity.RulesHash != rules.Hash {
				return fail(fmt.Errorf("%w: restriction identity does not match behavior", ErrExtensionPlanMismatch))
			}
			capability.Allowed = rules.Allowed
			capability.Denied = rules.Denied
			restrictions = append(restrictions, ownedRestriction{owner: owned.Component.InstanceID, value: capability})
			durable.Restrictions = append(durable.Restrictions, capability.Identity)
		}
		descriptor.Components = append(descriptor.Components, durable)
	}
	for index, matched := range matchedHandlers {
		if !matched && len(authoritativeHandlers[index].Handlers) > 0 {
			return fail(fmt.Errorf("%w: dispatch handler owner omitted", ErrExtensionPlanMismatch))
		}
	}
	sort.Slice(tools, func(i, j int) bool {
		return comparePlanTool(tools[i].owner, tools[i].value.Identity, tools[j].owner, tools[j].value.Identity) < 0
	})
	sort.Slice(prompts, func(i, j int) bool { return compareMountedPrompt(prompts[i], prompts[j]) < 0 })
	sort.Slice(guards, func(i, j int) bool { return compareMountedGuard(guards[i], guards[j]) < 0 })
	sort.Slice(restrictions, func(i, j int) bool {
		return comparePlanRestriction(restrictions[i].owner, restrictions[i].value.Identity, restrictions[j].owner, restrictions[j].value.Identity) < 0
	})
	sealedTools := make([]PlanTool, len(tools))
	sealedPrompts := make([]MountedPrompt, len(prompts))
	sealedGuards := make([]MountedToolGuard, len(guards))
	sealedRestrictions := make([]PlanRestriction, len(restrictions))
	for index := range tools {
		sealedTools[index] = tools[index].value
	}
	copy(sealedPrompts, prompts)
	copy(sealedGuards, guards)
	for index := range restrictions {
		sealedRestrictions[index] = restrictions[index].value
	}
	fingerprint, err := session.FingerprintExtensionPlan(descriptor)
	if err != nil {
		return fail(err)
	}
	descriptor.Fingerprint = fingerprint
	plan.tools = sealedPlanTools{capabilities: sealedTools, restrictions: sealedRestrictions}
	plan.prompts = sealedPrompts
	plan.guards = sealedGuards
	plan.descriptor = descriptor
	return plan, nil
}

func sameHandlerIdentities(left, right []extension.HandlerIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func comparePlanTool(leftOwner string, left session.ToolPlanIdentity, rightOwner string, right session.ToolPlanIdentity) int {
	for _, result := range []int{
		cmp.Compare(left.Order, right.Order), cmp.Compare(left.Name, right.Name), cmp.Compare(leftOwner, rightOwner),
		cmp.Compare(left.RegistrationID, right.RegistrationID), compareExecutionScope(left.Scope, right.Scope),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareMountedPrompt(left, right MountedPrompt) int {
	for _, result := range []int{
		cmp.Compare(left.Order, right.Order), cmp.Compare(left.Name, right.Name), cmp.Compare(left.InstanceID, right.InstanceID),
		cmp.Compare(left.ID, right.ID), compareExecutionScope(left.Scope, right.Scope),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareMountedGuard(left, right MountedToolGuard) int {
	for _, result := range []int{
		cmp.Compare(left.Order, right.Order), cmp.Compare(left.InstanceID, right.InstanceID), cmp.Compare(left.ID, right.ID),
		compareExecutionScope(left.Scope, right.Scope),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func comparePlanRestriction(leftOwner string, left session.RestrictionPlanIdentity, rightOwner string, right session.RestrictionPlanIdentity) int {
	for _, result := range []int{
		cmp.Compare(leftOwner, rightOwner), cmp.Compare(left.RegistrationID, right.RegistrationID), compareExecutionScope(left.Scope, right.Scope),
		cmp.Compare(left.RulesHash, right.RulesHash),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareExecutionScope(left, right extension.Scope) int {
	if result := cmp.Compare(left.Kind, right.Kind); result != 0 {
		return result
	}
	return cmp.Compare(left.Key, right.Key)
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

func (o *StreamingOrchestrator) acquireResumePlan(ctx context.Context, request ResumePlanRequest) (*RunPlan, error) {
	if request.SessionID == "" {
		return nil, ErrExtensionPlanMismatch
	}
	descriptor := request.Descriptor
	if descriptor.SchemaVersion != session.ExtensionPlanSchemaVersion || descriptor.Fingerprint == "" {
		return nil, ErrExtensionPlanMismatch
	}
	fingerprint, err := session.FingerprintExtensionPlan(descriptor)
	if err != nil || fingerprint != descriptor.Fingerprint {
		return nil, ErrExtensionPlanMismatch
	}
	plan, err := o.plans.AcquireResumePlan(ctx, ResumePlanRequest{SessionID: request.SessionID, Descriptor: descriptor.Clone()})
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
