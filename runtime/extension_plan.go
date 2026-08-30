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
		if !errors.Is(err, ErrExtensionPlanMismatch) {
			err = fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err)
		}
		return nil, err
	}
	compiled, err := compileRunPlan(spec)
	if err != nil {
		return fail(err)
	}
	sealed, err := session.SealExtensionPlanForSession(spec.SessionID, compiled.descriptor)
	if err != nil {
		return fail(err)
	}
	plan.tools = sealedPlanTools{capabilities: compiled.tools, restrictions: compiled.restrictions}
	plan.prompts = compiled.prompts
	plan.guards = compiled.guards
	plan.sealed = sealed
	return plan, nil
}

type ownedPlanTool struct {
	owner string
	value PlanTool
}

type ownedPlanRestriction struct {
	owner string
	value PlanRestriction
}

type handlerFragment struct {
	component extension.Component
	durable   session.ComponentPlan
	merged    bool
}

type compiledRunPlan struct {
	descriptor   session.ExtensionPlanDescriptor
	ownedTools   []ownedPlanTool
	prompts      []MountedPrompt
	guards       []MountedToolGuard
	ownedRules   []ownedPlanRestriction
	tools        []PlanTool
	restrictions []PlanRestriction
}

func compileRunPlan(spec RunPlanSpec) (compiledRunPlan, error) {
	compiled := compiledRunPlan{}
	handlers := compileHandlerFragments(spec.Dispatch)
	for _, owned := range spec.Components {
		durable, err := compiled.compileCapabilities(owned)
		if err != nil {
			return compiledRunPlan{}, err
		}
		compiled.mergeCapabilityFragment(owned.Component, durable, handlers)
	}
	for _, fragment := range handlers {
		if !fragment.merged {
			compiled.descriptor.Components = append(compiled.descriptor.Components, fragment.durable)
		}
	}
	if err := compiled.finalize(); err != nil {
		return compiledRunPlan{}, err
	}
	return compiled, nil
}

func compileHandlerFragments(dispatch *extension.Plan) []*handlerFragment {
	if dispatch == nil {
		return nil
	}
	ownedHandlers := dispatch.HandlerComponents()
	fragments := make([]*handlerFragment, 0, len(ownedHandlers))
	for _, owned := range ownedHandlers {
		durable := session.ComponentPlan{InstanceID: owned.Component.InstanceID, Artifact: owned.Component.Artifact}
		for _, handler := range owned.Handlers {
			durable.Handlers = append(durable.Handlers, session.RegistrationIdentity{ID: handler.ID, Contract: handler.Contract.ID, Version: handler.Contract.Version, Order: handler.Order, Scope: handler.Scope, Kind: handler.Kind})
		}
		fragments = append(fragments, &handlerFragment{component: owned.Component, durable: durable})
	}
	return fragments
}

func (c *compiledRunPlan) compileCapabilities(owned PlanComponent) (session.ComponentPlan, error) {
	durable := session.ComponentPlan{InstanceID: owned.Component.InstanceID, Artifact: owned.Component.Artifact}
	if err := c.compileTools(owned, &durable); err != nil {
		return session.ComponentPlan{}, err
	}
	if err := c.compilePrompts(owned, &durable); err != nil {
		return session.ComponentPlan{}, err
	}
	if err := c.compileGuards(owned, &durable); err != nil {
		return session.ComponentPlan{}, err
	}
	if err := c.compileRestrictions(owned, &durable); err != nil {
		return session.ComponentPlan{}, err
	}
	return durable, nil
}

func (c *compiledRunPlan) compileTools(owned PlanComponent, durable *session.ComponentPlan) error {
	for _, capability := range owned.Tools {
		if capability.Resolve == nil {
			return fmt.Errorf("%w: tool resolver required", ErrExtensionPlanMismatch)
		}
		c.ownedTools = append(c.ownedTools, ownedPlanTool{owner: owned.Component.InstanceID, value: capability})
		durable.Tools = append(durable.Tools, toolPlanIdentity(capability))
	}
	return nil
}

func (c *compiledRunPlan) compilePrompts(owned PlanComponent, durable *session.ComponentPlan) error {
	for _, capability := range owned.Prompts {
		if capability.Name == systemPromptSectionName || capability.Provider == nil {
			return fmt.Errorf("%w: prompt behavior required", ErrExtensionPlanMismatch)
		}
		c.prompts = append(c.prompts, MountedPrompt{Name: capability.Name, ID: capability.RegistrationID, Scope: capability.Scope, Order: capability.Order, InstanceID: owned.Component.InstanceID, Provider: capability.Provider})
		durable.Prompts = append(durable.Prompts, session.PromptPlanIdentity{Name: capability.Name, RegistrationID: capability.RegistrationID, Scope: capability.Scope, Order: capability.Order})
	}
	return nil
}

func (c *compiledRunPlan) compileGuards(owned PlanComponent, durable *session.ComponentPlan) error {
	for _, capability := range owned.Guards {
		if capability.Guard == nil {
			return fmt.Errorf("%w: guard behavior required", ErrExtensionPlanMismatch)
		}
		c.guards = append(c.guards, MountedToolGuard{ID: capability.RegistrationID, Order: capability.Order, InstanceID: owned.Component.InstanceID, Scope: capability.Scope, Guard: capability.Guard})
		durable.Guards = append(durable.Guards, session.GuardPlanIdentity{RegistrationID: capability.RegistrationID, Scope: capability.Scope, Order: capability.Order})
	}
	return nil
}

func (c *compiledRunPlan) compileRestrictions(owned PlanComponent, durable *session.ComponentPlan) error {
	for _, capability := range owned.Restrictions {
		rules, err := CanonicalizeRestrictionRules(capability.Allowed, capability.Denied)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrExtensionPlanMismatch, err)
		}
		capability.Allowed, capability.Denied = rules.Allowed, rules.Denied
		c.ownedRules = append(c.ownedRules, ownedPlanRestriction{owner: owned.Component.InstanceID, value: capability})
		durable.Restrictions = append(durable.Restrictions, session.RestrictionPlanIdentity{RegistrationID: capability.RegistrationID, Scope: capability.Scope, RulesHash: rules.Hash})
	}
	return nil
}

func (c *compiledRunPlan) mergeCapabilityFragment(component extension.Component, durable session.ComponentPlan, handlers []*handlerFragment) {
	behaviorCount := len(durable.Tools) + len(durable.Prompts) + len(durable.Guards) + len(durable.Restrictions)
	if behaviorCount != 0 {
		for _, fragment := range handlers {
			if !fragment.merged && fragment.component == component {
				fragment.merged = true
				durable.Handlers = fragment.durable.Handlers
				break
			}
		}
	}
	c.descriptor.Components = append(c.descriptor.Components, durable)
}

func (c *compiledRunPlan) finalize() error {
	sort.Slice(c.ownedTools, func(i, j int) bool {
		return comparePlanTool(c.ownedTools[i].owner, toolPlanIdentity(c.ownedTools[i].value), c.ownedTools[j].owner, toolPlanIdentity(c.ownedTools[j].value)) < 0
	})
	sort.Slice(c.prompts, func(i, j int) bool { return compareMountedPrompt(c.prompts[i], c.prompts[j]) < 0 })
	sort.Slice(c.guards, func(i, j int) bool { return compareMountedGuard(c.guards[i], c.guards[j]) < 0 })
	sort.Slice(c.ownedRules, func(i, j int) bool {
		return comparePlanRestriction(c.ownedRules[i].owner, c.ownedRules[i].value, c.ownedRules[j].owner, c.ownedRules[j].value) < 0
	})
	if err := uniqueCapabilityNames(c.ownedTools, c.prompts); err != nil {
		return err
	}
	c.tools = make([]PlanTool, len(c.ownedTools))
	c.restrictions = make([]PlanRestriction, len(c.ownedRules))
	for index := range c.ownedTools {
		c.tools[index] = c.ownedTools[index].value
	}
	for index := range c.ownedRules {
		c.restrictions[index] = c.ownedRules[index].value
	}
	return nil
}

func uniqueCapabilityNames(tools []ownedPlanTool, prompts []MountedPrompt) error {
	seenTools := make(map[string]bool, len(tools))
	for _, tool := range tools {
		if seenTools[tool.value.Name] {
			return fmt.Errorf("%w: duplicate tool name %q", ErrExtensionPlanMismatch, tool.value.Name)
		}
		seenTools[tool.value.Name] = true
	}
	seenPrompts := make(map[string]bool, len(prompts))
	for _, prompt := range prompts {
		if seenPrompts[prompt.Name] {
			return fmt.Errorf("%w: duplicate prompt name %q", ErrExtensionPlanMismatch, prompt.Name)
		}
		seenPrompts[prompt.Name] = true
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
