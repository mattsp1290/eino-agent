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
	// Aliases are additional model-visible names that resolve to this tool.
	Aliases []string
	// ArgumentAliases maps a canonical parameter name to alternate argument
	// names a model may use for it.
	ArgumentAliases map[string][]string
	// Deferred marks the tool as advertised only through tool search.
	Deferred bool
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

// ToolSearchConfig configures the runtime-implemented tool-search tool for a
// plan (see runtime/tool_search.go). At most one may be active across an
// assembled plan; composition.Registry.acquire rejects more than one before
// ever calling NewRunPlan.
type ToolSearchConfig struct {
	// Name is the model-visible tool name. Defaults to "tool_search" when
	// empty (applied by composition.Registrar.ToolSearch).
	Name string
	// Description is the model-visible tool description.
	Description string
}

// RunPlanSpec is unfingerprinted, component-owned behavior evidence.
type RunPlanSpec struct {
	SessionID  session.ID
	Dispatch   *extension.Plan
	Components []PlanComponent
	// ToolSearch configures the runtime tool-search tool for this plan, or
	// nil when tool search is not enabled. Its Description carries no
	// execution authority by itself (every discovered tool it can ever
	// surface is still validated against the frozen tool registry at claim
	// time) and is not sealed. Its Name is different: NewRunPlan seals the
	// configured name into ExtensionPlanDescriptor.ToolSearch so that a
	// resume whose tool-search registration was renamed or removed between
	// the original run and the resume is rejected by the standard
	// plan-mismatch error instead of failing deep inside resume with
	// "tool_search unavailable" (composition-search-reviewer I3).
	ToolSearch *ToolSearchConfig
	// Agent is the typed ADK agent factory this run's engine builds through.
	// Like ToolSearch, it is deliberately NOT part of the sealed durable
	// ExtensionPlanDescriptor/fingerprint: it is host construction code
	// identity, not durable capability evidence, and every tool/model it can
	// ever use is still validated against the frozen plan and mandatory
	// adapters at build time (see adkEngine.buildAgent). Defaults to
	// DefaultChatModelAgentFactory when nil.
	Agent AgentFactory
}

// RunPlan is the immutable executable state for one run.
type RunPlan struct {
	dispatch   *extension.Plan
	sessionID  session.ID
	tools      sealedPlanTools
	prompts    []MountedPrompt
	guards     []MountedToolGuard
	sealed     session.SealedExtensionPlan
	toolSearch *ToolSearchConfig
	agent      AgentFactory
	once       sync.Once
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
	toolSearch, err := normalizeToolSearchConfig(spec.ToolSearch)
	if err != nil {
		return fail(err)
	}
	if toolSearch != nil {
		if err := validateToolSearchNameCollision(toolSearch.Name, compiled); err != nil {
			return fail(err)
		}
		// The configured tool-search name is sealed into the durable
		// fingerprint here, before sealing below, so that
		// composition.Registry.AcquireResumePlan's fingerprint comparison
		// (which re-derives ToolSearch from the live registry) rejects a
		// resume whose tool-search registration was renamed or removed
		// between the original run and the resume, instead of that drift
		// surfacing deep inside resume as "tool_search unavailable"
		// (composition-search-reviewer I3). It is sealed at its
		// pre-restriction name deliberately: whether a restriction denies
		// it is already independently captured by that restriction's own
		// RestrictionPlanIdentity in the same descriptor.
		compiled.descriptor.ToolSearch = toolSearch.Name
	}
	sealed, err := session.SealExtensionPlanForSession(spec.SessionID, compiled.descriptor)
	if err != nil {
		return fail(err)
	}
	if toolSearch != nil {
		// Only an explicit Denied entry disables tool search for this plan;
		// an Allowed list applies to ordinary tools and never disables the
		// search tool merely by omitting its name (a restriction authored
		// purely about tools, e.g. Allowed: ["echo"], must not silently
		// strand every deferred tool as advertised-but-uncallable -- see
		// ProviderRequest's doc comment). The collision check above
		// guarantees the search name is disjoint from every tool name and
		// alias, so this cannot accidentally disable an unrelated tool.
		if planToolDenied(toolSearch.Name, compiled.restrictions, nil) {
			toolSearch = nil
		}
	}
	plan.tools = sealedPlanTools{capabilities: compiled.tools, restrictions: compiled.restrictions, aliasIndex: compiled.aliasIndex}
	plan.prompts = compiled.prompts
	plan.guards = compiled.guards
	plan.sealed = sealed
	plan.toolSearch = toolSearch
	plan.agent = spec.Agent
	if plan.agent == nil {
		plan.agent = DefaultChatModelAgentFactory{}
	}
	return plan, nil
}

// validateToolSearchNameCollision rejects a tool-search name that collides
// with any owned tool's canonical name or alias. Without this check, a
// colliding name would silently shadow the tool: prepareToolCalls tests
// isToolSearchCall before resolving the call against the tool registry, so
// every model call on that name diverts into the runtime search path and
// the registered tool becomes permanently unreachable with no error
// anywhere (tool-identity-reviewer I2 / composition-search-reviewer I1).
func validateToolSearchNameCollision(name string, compiled compiledRunPlan) error {
	if _, taken := compiled.aliasIndex[name]; taken {
		return fmt.Errorf("%w: tool search name %q collides with a tool alias", ErrExtensionPlanMismatch, name)
	}
	for _, owned := range compiled.ownedTools {
		if owned.value.Name == name {
			return fmt.Errorf("%w: tool search name %q collides with a tool name", ErrExtensionPlanMismatch, name)
		}
	}
	return nil
}

func normalizeToolSearchConfig(cfg *ToolSearchConfig) (*ToolSearchConfig, error) {
	if cfg == nil {
		return nil, nil
	}
	next := *cfg
	if next.Name == "" {
		next.Name = "tool_search"
	}
	if err := extension.ValidateIdentifier(next.Name); err != nil {
		return nil, fmt.Errorf("%w: invalid tool search name %q", ErrExtensionPlanMismatch, next.Name)
	}
	return &next, nil
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
	aliasIndex   map[string]string
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
	aliasIndex, err := buildToolAliasIndex(c.ownedTools)
	if err != nil {
		return err
	}
	c.aliasIndex = aliasIndex
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
		Aliases: append([]string(nil), capability.Aliases...), Deferred: capability.Deferred,
	}
}

// buildToolAliasIndex rejects any alias that collides with another tool's
// canonical name or with another tool's alias across the whole plan, and
// returns the compile-time alias -> canonical name index used by
// sealedPlanTools and RunPlan.ResolveToolName.
func buildToolAliasIndex(tools []ownedPlanTool) (map[string]string, error) {
	canonicalNames := make(map[string]bool, len(tools))
	for _, owned := range tools {
		canonicalNames[owned.value.Name] = true
	}
	index := make(map[string]string)
	for _, owned := range tools {
		for _, alias := range owned.value.Aliases {
			if canonicalNames[alias] {
				return nil, fmt.Errorf("%w: tool alias %q collides with a tool name", ErrExtensionPlanMismatch, alias)
			}
			if existing, dup := index[alias]; dup {
				return nil, fmt.Errorf("%w: tool alias %q is registered by both %q and %q", ErrExtensionPlanMismatch, alias, existing, owned.value.Name)
			}
			index[alias] = owned.value.Name
		}
	}
	if len(index) == 0 {
		return nil, nil
	}
	return index, nil
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
	// aliasIndex maps a compile-time-validated alias to its canonical tool
	// name; it never grants access to a tool absent from capabilities.
	aliasIndex map[string]string
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
		if planToolAllowed(tool.Name, s.restrictions, s.aliasIndex) {
			cloned, cloneErr := cloneToolChecked(tool)
			if cloneErr != nil {
				return nil, fmt.Errorf("freeze resolved tool %q: %w", tool.Name, cloneErr)
			}
			result = append(result, cloned)
		}
	}
	return result, nil
}

// planToolAllowed reports whether name (a tool's canonical name, or the
// tool-search tool's configured name) survives every restriction. Each
// restriction entry is resolved through aliasIndex before comparison, so a
// restriction authored against a tool's alias (e.g. Denied: ["say"] for a
// tool "echo" with alias "say") denies the tool under both names instead of
// silently doing nothing (composition-search-reviewer I2). aliasIndex may be
// nil (the tool-search name is never itself an alias target, so it needs no
// resolution). An entry naming no tool and no alias in the plan is not an
// error -- restriction sets may be authored for a wider registry than this
// plan's -- it is simply inert here.
func planToolAllowed(name string, restrictions []PlanRestriction, aliasIndex map[string]string) bool {
	canonicalize := func(entry string) string {
		if canonical, ok := aliasIndex[entry]; ok {
			return canonical
		}
		return entry
	}
	for _, restriction := range restrictions {
		for _, denied := range restriction.Denied {
			if canonicalize(denied) == name {
				return false
			}
		}
		if len(restriction.Allowed) != 0 {
			allowed := false
			for _, candidate := range restriction.Allowed {
				allowed = allowed || canonicalize(candidate) == name
			}
			if !allowed {
				return false
			}
		}
	}
	return true
}

// planToolDenied reports whether name is named by an explicit Denied entry
// in any restriction, resolved through aliasIndex exactly like
// planToolAllowed. Unlike planToolAllowed, it ignores every restriction's
// Allowed list: it answers only "was this name explicitly denied", not "does
// this name survive every restriction". It exists specifically for the
// tool-search name, where an Allowed list authored purely about ordinary
// tools must not implicitly disable search merely by omitting the search
// name (see NewRunPlan).
func planToolDenied(name string, restrictions []PlanRestriction, aliasIndex map[string]string) bool {
	canonicalize := func(entry string) string {
		if canonical, ok := aliasIndex[entry]; ok {
			return canonical
		}
		return entry
	}
	for _, restriction := range restrictions {
		for _, denied := range restriction.Denied {
			if canonicalize(denied) == name {
				return true
			}
		}
	}
	return false
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

// AgentFactory returns the plan's typed ADK agent factory (never nil once
// NewRunPlan has returned successfully).
func (p *RunPlan) AgentFactory() AgentFactory {
	if p == nil || p.agent == nil {
		return DefaultChatModelAgentFactory{}
	}
	return p.agent
}

// ToolSearch returns the plan's normalized tool-search configuration, or nil
// when tool search is not enabled for this plan.
func (p *RunPlan) ToolSearch() *ToolSearchConfig {
	if p == nil || p.toolSearch == nil {
		return nil
	}
	cfg := *p.toolSearch
	return &cfg
}

// ResolveToolName returns the canonical tool name for name, which may
// already be a canonical name or a compile-time-registered alias. It never
// consults a live registry and never resolves an alias to a tool absent from
// the sealed plan.
func (p *RunPlan) ResolveToolName(name string) (string, bool) {
	if p == nil {
		return "", false
	}
	if canonical, ok := p.tools.aliasIndex[name]; ok {
		return canonical, true
	}
	for _, capability := range p.tools.capabilities {
		if capability.Name == name {
			return name, true
		}
	}
	return "", false
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
