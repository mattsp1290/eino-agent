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
	SessionID session.ID
	Plan      session.SealedExtensionPlan
}

type RunPlanProvider interface {
	AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error)
	AcquireResumePlan(context.Context, ResumePlanRequest) (*RunPlan, error)
}

type PlanTool struct {
	Name, RegistrationID     string
	Scope                    extension.Scope
	SchemaHash, ExecutorHash string
	Order                    int
	Resolve                  func(context.Context, ToolScopeContext) (Tool, error)
}

// PlanPrompt binds one prompt implementation to its registration.
type PlanPrompt struct {
	Name, RegistrationID string
	Scope                extension.Scope
	Order                int
	Provider             PromptProvider
}

// PlanGuard binds one tool guard implementation to its registration.
type PlanGuard struct {
	RegistrationID string
	Scope          extension.Scope
	Order          int
	Guard          ToolGuard
}

// PlanRestriction binds one tool restriction policy to its registration.
type PlanRestriction struct {
	RegistrationID string
	Scope          extension.Scope
	Allowed        []string
	Denied         []string
}

type PlanComponent struct {
	Component    extension.Component
	Tools        []PlanTool
	Prompts      []PlanPrompt
	Guards       []PlanGuard
	Restrictions []PlanRestriction
}

// RunPlanSpec is unfingerprinted, component-owned behavior evidence.
type RunPlanSpec struct {
	SessionID  session.ID
	Dispatch   *extension.Plan
	Components []PlanComponent
}

// RunPlan is the immutable executable state for one run.
type RunPlan struct {
	dispatch  *extension.Plan
	sessionID session.ID
	tools     sealedPlanTools
	prompts   []MountedPrompt
	guards    []MountedToolGuard
	sealed    session.SealedExtensionPlan
	once      sync.Once
}

// NewRunPlan derives durable identity from registered behavior and seals it.
func NewRunPlan(spec RunPlanSpec) (*RunPlan, error) {
	plan := &RunPlan{dispatch: spec.Dispatch, sessionID: spec.SessionID}
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
	durableComponents := make(map[string]session.ComponentPlan, len(authoritativeHandlers)+len(components))
	componentOwners := make(map[string]extension.Component, len(authoritativeHandlers)+len(components))
	for _, owned := range authoritativeHandlers {
		if err := extension.ValidateComponent(owned.Component); err != nil {
			return fail(fmt.Errorf("%w: invalid handler owner", ErrExtensionPlanMismatch))
		}
		if _, exists := componentOwners[owned.Component.InstanceID]; exists {
			return fail(fmt.Errorf("%w: duplicate handler owner", ErrExtensionPlanMismatch))
		}
		durable := session.ComponentPlan{InstanceID: owned.Component.InstanceID, Artifact: owned.Component.Artifact}
		for _, handler := range owned.Handlers {
			if err := validatePlanScope(spec.SessionID, handler.Scope); err != nil {
				return fail(err)
			}
			durable.Handlers = append(durable.Handlers, session.RegistrationIdentity{ID: handler.ID, Contract: handler.Contract.ID, Version: handler.Contract.Version, Order: handler.Order, Scope: handler.Scope, Kind: handler.Kind})
		}
		componentOwners[owned.Component.InstanceID] = owned.Component
		durableComponents[owned.Component.InstanceID] = durable
	}
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
		if len(owned.Tools)+len(owned.Prompts)+len(owned.Guards)+len(owned.Restrictions) == 0 {
			return fail(fmt.Errorf("%w: empty component owner", ErrExtensionPlanMismatch))
		}
		if existing, ok := componentOwners[owned.Component.InstanceID]; ok && existing != owned.Component {
			return fail(fmt.Errorf("%w: conflicting component owner", ErrExtensionPlanMismatch))
		}
		durable, ok := durableComponents[owned.Component.InstanceID]
		if !ok {
			durable = session.ComponentPlan{InstanceID: owned.Component.InstanceID, Artifact: owned.Component.Artifact}
			componentOwners[owned.Component.InstanceID] = owned.Component
		}
		for _, capability := range owned.Tools {
			if extension.ValidateIdentifier(capability.Name) != nil || extension.ValidateIdentifier(capability.RegistrationID) != nil || capability.SchemaHash == "" || capability.ExecutorHash == "" || capability.Resolve == nil {
				return fail(fmt.Errorf("%w: tool resolver required", ErrExtensionPlanMismatch))
			}
			if err := validatePlanScope(spec.SessionID, capability.Scope); err != nil {
				return fail(err)
			}
			tools = append(tools, ownedTool{owner: owned.Component.InstanceID, value: capability})
			durable.Tools = append(durable.Tools, toolPlanIdentity(capability))
		}
		for _, capability := range owned.Prompts {
			if extension.ValidateIdentifier(capability.Name) != nil || extension.ValidateIdentifier(capability.RegistrationID) != nil || capability.Name == systemPromptSectionName || capability.Provider == nil {
				return fail(fmt.Errorf("%w: prompt behavior required", ErrExtensionPlanMismatch))
			}
			if err := validatePlanScope(spec.SessionID, capability.Scope); err != nil {
				return fail(err)
			}
			prompts = append(prompts, MountedPrompt{Name: capability.Name, ID: capability.RegistrationID, Scope: capability.Scope, Order: capability.Order, InstanceID: owned.Component.InstanceID, Provider: capability.Provider})
			durable.Prompts = append(durable.Prompts, session.PromptPlanIdentity{Name: capability.Name, RegistrationID: capability.RegistrationID, Scope: capability.Scope, Order: capability.Order})
		}
		for _, capability := range owned.Guards {
			if extension.ValidateIdentifier(capability.RegistrationID) != nil || capability.Guard == nil {
				return fail(fmt.Errorf("%w: guard behavior required", ErrExtensionPlanMismatch))
			}
			if err := validatePlanScope(spec.SessionID, capability.Scope); err != nil {
				return fail(err)
			}
			guards = append(guards, MountedToolGuard{ID: capability.RegistrationID, Order: capability.Order, InstanceID: owned.Component.InstanceID, Scope: capability.Scope, Guard: capability.Guard})
			durable.Guards = append(durable.Guards, session.GuardPlanIdentity{RegistrationID: capability.RegistrationID, Scope: capability.Scope, Order: capability.Order})
		}
		for _, capability := range owned.Restrictions {
			if extension.ValidateIdentifier(capability.RegistrationID) != nil {
				return fail(fmt.Errorf("%w: invalid restriction registration", ErrExtensionPlanMismatch))
			}
			if err := validatePlanScope(spec.SessionID, capability.Scope); err != nil {
				return fail(err)
			}
			rules, err := CanonicalizeRestrictionRules(capability.Allowed, capability.Denied)
			if err != nil {
				return fail(fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err))
			}
			capability.Allowed = rules.Allowed
			capability.Denied = rules.Denied
			restrictions = append(restrictions, ownedRestriction{owner: owned.Component.InstanceID, value: capability})
			durable.Restrictions = append(durable.Restrictions, session.RestrictionPlanIdentity{RegistrationID: capability.RegistrationID, Scope: capability.Scope, RulesHash: rules.Hash})
		}
		durableComponents[owned.Component.InstanceID] = durable
	}
	componentIDs := make([]string, 0, len(durableComponents))
	for instanceID := range durableComponents {
		componentIDs = append(componentIDs, instanceID)
	}
	sort.Strings(componentIDs)
	for _, instanceID := range componentIDs {
		descriptor.Components = append(descriptor.Components, durableComponents[instanceID])
	}
	sort.Slice(tools, func(i, j int) bool {
		return comparePlanTool(tools[i].owner, toolPlanIdentity(tools[i].value), tools[j].owner, toolPlanIdentity(tools[j].value)) < 0
	})
	sort.Slice(prompts, func(i, j int) bool { return compareMountedPrompt(prompts[i], prompts[j]) < 0 })
	sort.Slice(guards, func(i, j int) bool { return compareMountedGuard(guards[i], guards[j]) < 0 })
	sort.Slice(restrictions, func(i, j int) bool {
		return comparePlanRestriction(restrictions[i].owner, restrictions[i].value, restrictions[j].owner, restrictions[j].value) < 0
	})
	toolNames := make(map[string]bool, len(tools))
	for _, tool := range tools {
		if toolNames[tool.value.Name] {
			return fail(fmt.Errorf("%w: duplicate tool name %q", ErrExtensionPlanMismatch, tool.value.Name))
		}
		toolNames[tool.value.Name] = true
	}
	promptNames := make(map[string]bool, len(prompts))
	for _, prompt := range prompts {
		if promptNames[prompt.Name] {
			return fail(fmt.Errorf("%w: duplicate prompt name %q", ErrExtensionPlanMismatch, prompt.Name))
		}
		promptNames[prompt.Name] = true
	}
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
	sealed, err := session.SealExtensionPlan(descriptor)
	if err != nil {
		return fail(err)
	}
	plan.tools = sealedPlanTools{capabilities: sealedTools, restrictions: sealedRestrictions}
	plan.prompts = sealedPrompts
	plan.guards = sealedGuards
	plan.sealed = sealed
	return plan, nil
}

func validatePlanScope(sessionID session.ID, scope extension.Scope) error {
	if err := extension.ValidateScope(scope); err != nil {
		return fmt.Errorf("%w: invalid capability scope", ErrExtensionPlanMismatch)
	}
	if scope.Kind == extension.ScopeSession && (sessionID == "" || scope.Key != string(sessionID)) {
		return fmt.Errorf("%w: session scope does not match plan session", ErrExtensionPlanMismatch)
	}
	return nil
}

func toolPlanIdentity(capability PlanTool) session.ToolPlanIdentity {
	return session.ToolPlanIdentity{
		Name: capability.Name, RegistrationID: capability.RegistrationID, Scope: capability.Scope,
		SchemaHash: capability.SchemaHash, ExecutorHash: capability.ExecutorHash, Order: capability.Order,
	}
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

func comparePlanRestriction(leftOwner string, left PlanRestriction, rightOwner string, right PlanRestriction) int {
	for _, result := range []int{
		cmp.Compare(leftOwner, rightOwner), cmp.Compare(left.RegistrationID, right.RegistrationID), compareExecutionScope(left.Scope, right.Scope),
		cmp.Compare(strings.Join(left.Allowed, "\x00"), strings.Join(right.Allowed, "\x00")), cmp.Compare(strings.Join(left.Denied, "\x00"), strings.Join(right.Denied, "\x00")),
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
		expected := capability.Name
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
	return p.sealed.Descriptor()
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

// FlushNotifications waits for notifications accepted by this plan. Runtime
// execution never calls it; it provides an explicit bounded synchronization
// point for shutdown and tests.
func (p *RunPlan) FlushNotifications(ctx context.Context) error {
	if p == nil || p.dispatch == nil {
		return nil
	}
	return p.dispatch.Flush(ctx)
}

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
	if plan == nil || plan.sealed.Fingerprint() == "" || plan.sessionID != "" && plan.sessionID != request.SessionID {
		if plan != nil {
			plan.release()
		}
		return nil, fmt.Errorf("%w: provider returned invalid plan", ErrExtensionPlanMismatch)
	}
	return plan, nil
}

func (o *StreamingOrchestrator) acquireResumePlan(ctx context.Context, sessionID session.ID, descriptor session.ExtensionPlanDescriptor) (*RunPlan, error) {
	verified, err := session.VerifyExtensionPlanForSession(sessionID, descriptor)
	if err != nil {
		return nil, ErrExtensionPlanMismatch
	}
	plan, err := o.plans.AcquireResumePlan(ctx, ResumePlanRequest{SessionID: sessionID, Plan: verified})
	if err != nil {
		return nil, err
	}
	if plan == nil || !plan.sealed.Matches(verified) {
		if plan != nil {
			plan.release()
		}
		return nil, ErrExtensionPlanMismatch
	}
	return plan, nil
}
