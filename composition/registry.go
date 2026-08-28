package composition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

type Installer interface {
	Install(context.Context, *Registrar) error
}
type InstallerFunc func(context.Context, *Registrar) error

func (f InstallerFunc) Install(ctx context.Context, registrar *Registrar) error {
	return f(ctx, registrar)
}

type ToolRegistration struct {
	ID                 string
	Order              int
	Scope              extension.Scope
	SourceSchemaHash   string
	SourceExecutorHash string
	Definition         tools.Definition
}

type PromptRegistration struct {
	ID       string
	Name     string
	Order    int
	Scope    extension.Scope
	Provider runtime.PromptProvider
}

type GuardRegistration struct {
	ID    string
	Order int
	Scope extension.Scope
	Guard runtime.ToolGuard
}

type RestrictionRegistration struct {
	ID      string
	Scope   extension.Scope
	Allowed []string
	Denied  []string
}

type Registrar struct {
	extensions   extension.Registrar
	component    extension.Component
	tools        []ToolRegistration
	prompts      []PromptRegistration
	guards       []GuardRegistration
	restrictions []RestrictionRegistration
}

func (r *Registrar) Prompt(registration PromptRegistration) error {
	if err := extension.ValidateIdentifier(registration.ID); err != nil {
		return err
	}
	if err := extension.ValidateIdentifier(registration.Name); err != nil {
		return err
	}
	if err := extension.ValidateScope(registration.Scope); err != nil {
		return err
	}
	if registration.Provider == nil {
		return fmt.Errorf("%w: invalid prompt registration", extension.ErrInvalidRegistration)
	}
	for _, existing := range r.prompts {
		if existing.Scope == registration.Scope && existing.Name == registration.Name {
			return fmt.Errorf("%w: prompt %s", extension.ErrDuplicateRegistration, registration.Name)
		}
	}
	if err := r.extensions.Lease(registration.Scope); err != nil {
		return err
	}
	r.prompts = append(r.prompts, registration)
	return nil
}

func (r *Registrar) Guard(registration GuardRegistration) error {
	if err := extension.ValidateIdentifier(registration.ID); err != nil {
		return err
	}
	if err := extension.ValidateScope(registration.Scope); err != nil {
		return err
	}
	if registration.Guard == nil {
		return fmt.Errorf("%w: invalid guard registration", extension.ErrInvalidRegistration)
	}
	for _, existing := range r.guards {
		if existing.ID == registration.ID && existing.Scope == registration.Scope {
			return fmt.Errorf("%w: guard %s", extension.ErrDuplicateRegistration, registration.ID)
		}
	}
	if err := r.extensions.Lease(registration.Scope); err != nil {
		return err
	}
	r.guards = append(r.guards, registration)
	return nil
}

func (r *Registrar) RestrictTools(registration RestrictionRegistration) error {
	if err := extension.ValidateIdentifier(registration.ID); err != nil {
		return err
	}
	if err := extension.ValidateScope(registration.Scope); err != nil {
		return err
	}
	rules, err := runtime.CanonicalizeRestrictionRules(registration.Allowed, registration.Denied)
	if err != nil {
		return fmt.Errorf("%w: %v", extension.ErrInvalidRegistration, err)
	}
	for _, existing := range r.restrictions {
		if existing.ID == registration.ID && existing.Scope == registration.Scope {
			return fmt.Errorf("%w: restriction %s", extension.ErrDuplicateRegistration, registration.ID)
		}
	}
	registration.Allowed = rules.Allowed
	registration.Denied = rules.Denied
	if err := r.extensions.Lease(registration.Scope); err != nil {
		return err
	}
	r.restrictions = append(r.restrictions, registration)
	return nil
}

func (r *Registrar) Extensions() extension.Registrar       { return r.extensions }
func (r *Registrar) Defer(cleanup extension.Cleanup) error { return r.extensions.Defer(cleanup) }

func (r *Registrar) Tool(registration ToolRegistration) error {
	if err := extension.ValidateIdentifier(registration.ID); err != nil {
		return err
	}
	if err := extension.ValidateIdentifier(registration.Definition.Name); err != nil {
		return err
	}
	if err := extension.ValidateScope(registration.Scope); err != nil {
		return err
	}
	if err := tools.ValidateDefinition(registration.Definition); err != nil {
		return err
	}
	if err := validateToolSourceIdentity(registration.SourceSchemaHash, registration.SourceExecutorHash); err != nil {
		return err
	}
	for _, existing := range r.tools {
		if existing.Scope == registration.Scope && existing.Definition.Name == registration.Definition.Name {
			return fmt.Errorf("%w: %s", tools.ErrDuplicateRegistration, registration.Definition.Name)
		}
	}
	frozen, err := registration.Definition.Clone()
	if err != nil {
		return fmt.Errorf("freeze composed tool %q: %w", registration.Definition.Name, err)
	}
	registration.Definition = frozen
	executorHash, err := composedToolExecutorHash(registration.SourceExecutorHash, r.component.Artifact.Hash)
	if err != nil {
		return err
	}
	registration.Definition.Provenance = tools.Provenance{
		InstanceID: r.component.InstanceID, ArtifactName: r.component.Artifact.Name,
		ArtifactVersion: r.component.Artifact.Version, ArtifactHash: r.component.Artifact.Hash,
		ConfigHash: r.component.Artifact.ConfigHash, ExecutorHash: executorHash,
	}
	if err := r.extensions.Lease(registration.Scope); err != nil {
		return err
	}
	r.tools = append(r.tools, registration)
	return nil
}

type Registry struct {
	mu         sync.Mutex
	extensions *extension.Registry
	components map[string]*mountedComponent
}

type mountedComponent struct {
	component    extension.Component
	extension    *extension.Mount
	tools        []ToolRegistration
	prompts      []PromptRegistration
	guards       []GuardRegistration
	restrictions []RestrictionRegistration
}

func NewRegistry(reporter extension.Reporter) *Registry {
	return &Registry{extensions: extension.NewRegistry(reporter), components: make(map[string]*mountedComponent)}
}

type Mount struct {
	registry  *Registry
	extension *extension.Mount
	instance  string
	once      sync.Once
}

func (r *Registry) Mount(ctx context.Context, component extension.Component, installer Installer) (*Mount, error) {
	if r == nil || installer == nil {
		return nil, fmt.Errorf("%w: registry and installer required", extension.ErrInvalidComponent)
	}
	var staged *Registrar
	prepared, err := r.extensions.PrepareMount(ctx, component, extension.InstallerFunc(func(ctx context.Context, extensions extension.Registrar) error {
		registrar := &Registrar{extensions: extensions, component: component}
		if err := installer.Install(ctx, registrar); err != nil {
			return err
		}
		staged = registrar
		return nil
	}))
	if err != nil {
		return nil, err
	}
	rollback := func(primary error) error {
		return errors.Join(primary, prepared.Rollback(context.WithoutCancel(ctx)))
	}
	r.mu.Lock()
	if _, exists := r.components[component.InstanceID]; exists {
		r.mu.Unlock()
		return nil, rollback(fmt.Errorf("%w: component instance %s", extension.ErrDuplicateInstance, component.InstanceID))
	}
	if err := r.validateTools(staged.tools); err != nil {
		r.mu.Unlock()
		return nil, rollback(err)
	}
	if err := r.validatePrompts(staged.prompts); err != nil {
		r.mu.Unlock()
		return nil, rollback(err)
	}
	extensionMount, err := r.extensions.CommitMount(prepared)
	if err != nil {
		r.mu.Unlock()
		return nil, rollback(err)
	}
	r.publishMountedComponentLocked(ctx, component, extensionMount, staged)
	return &Mount{registry: r, extension: extensionMount, instance: component.InstanceID}, nil
}

func (r *Registry) publishMountedComponentLocked(ctx context.Context, component extension.Component, extensionMount *extension.Mount, staged *Registrar) {
	defer func() {
		if recovered := recover(); recovered != nil {
			delete(r.components, component.InstanceID)
			extensionMount.Deactivate()
			r.mu.Unlock()
			_ = extensionMount.Close(context.WithoutCancel(ctx))
			panic(recovered)
		}
	}()
	for index, registration := range staged.tools {
		registration.Definition = mountToolDefinition(extensionMount, registration.Definition)
		staged.tools[index] = registration
	}
	for index, registration := range staged.prompts {
		registration.Provider = mountedPromptProvider{mount: extensionMount, next: registration.Provider}
		staged.prompts[index] = registration
	}
	for index, registration := range staged.guards {
		registration.Guard = mountedToolGuard{mount: extensionMount, next: registration.Guard}
		staged.guards[index] = registration
	}
	r.components[component.InstanceID] = &mountedComponent{
		component: component, extension: extensionMount,
		tools: staged.tools, prompts: staged.prompts, guards: staged.guards, restrictions: staged.restrictions,
	}
	r.mu.Unlock()
}

type mountedPromptProvider struct {
	mount *extension.Mount
	next  runtime.PromptProvider
}

func (p mountedPromptProvider) ProvidePrompt(ctx context.Context, prompt runtime.PromptContext) (string, error) {
	return p.next.ProvidePrompt(p.mount.CallbackContext(ctx), prompt)
}

type mountedToolGuard struct {
	mount *extension.Mount
	next  runtime.ToolGuard
}

func (g mountedToolGuard) GuardTool(ctx context.Context, request runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
	return g.next.GuardTool(g.mount.CallbackContext(ctx), request)
}

func mountToolDefinition(mount *extension.Mount, definition tools.Definition) tools.Definition {
	next := definition
	if callback := next.Normalize; callback != nil {
		next.Normalize = func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			return callback(mount.CallbackContext(ctx), input)
		}
	}
	if callback := next.Pattern; callback != nil {
		next.Pattern = func(ctx context.Context, input json.RawMessage) (string, error) {
			return callback(mount.CallbackContext(ctx), input)
		}
	}
	if callback := next.Execute; callback != nil {
		next.Execute = func(ctx context.Context, execution tools.Execution) (json.RawMessage, error) {
			return callback(mount.CallbackContext(ctx), execution)
		}
	}
	return next
}

func (r *Registry) validateTools(staged []ToolRegistration) error {
	for index, candidate := range staged {
		for _, other := range staged[index+1:] {
			if other.Definition.Name == candidate.Definition.Name && toolScopesOverlap(other.Scope, candidate.Scope) {
				return fmt.Errorf("%w: %s", tools.ErrDuplicateRegistration, candidate.Definition.Name)
			}
		}
		for _, mounted := range r.components {
			for _, existing := range mounted.tools {
				if existing.Definition.Name == candidate.Definition.Name && toolScopesOverlap(existing.Scope, candidate.Scope) {
					return fmt.Errorf("%w: %s", tools.ErrDuplicateRegistration, candidate.Definition.Name)
				}
			}
		}
	}
	return nil
}

func toolScopesOverlap(left, right extension.Scope) bool {
	if left.Kind == extension.ScopeGlobal || right.Kind == extension.ScopeGlobal {
		return true
	}
	return left.Kind == extension.ScopeSession && right.Kind == extension.ScopeSession && left.Key == right.Key
}

func (r *Registry) validatePrompts(staged []PromptRegistration) error {
	for _, candidate := range staged {
		for _, mounted := range r.components {
			for _, existing := range mounted.prompts {
				if existing.Scope == candidate.Scope && existing.Name == candidate.Name {
					return fmt.Errorf("%w: prompt %s", extension.ErrDuplicateRegistration, candidate.Name)
				}
			}
		}
	}
	return nil
}

func (m *Mount) Deactivate() {
	if m == nil || m.registry == nil {
		return
	}
	m.once.Do(func() {
		m.registry.mu.Lock()
		mounted := m.registry.components[m.instance]
		delete(m.registry.components, m.instance)
		if mounted != nil {
			mounted.extension.Deactivate()
		} else {
			m.extension.Deactivate()
		}
		m.registry.mu.Unlock()
	})
}

func (m *Mount) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if err := m.extension.CheckClose(ctx); err != nil {
		return err
	}
	m.Deactivate()
	return m.extension.Close(ctx)
}

func (r *Registry) AcquireRunPlan(ctx context.Context, request runtime.RunPlanRequest) (*runtime.RunPlan, error) {
	return r.acquire(ctx, request.SessionID, nil, requestedToolSelector(request.Config.Tools))
}

func (r *Registry) AcquireResumePlan(ctx context.Context, persisted session.ExtensionPlanDescriptor) (*runtime.RunPlan, error) {
	if err := session.ValidateExtensionPlan(persisted); err != nil || persisted.Fingerprint == "" {
		return nil, runtime.ErrExtensionPlanMismatch
	}
	fingerprint, err := session.FingerprintExtensionPlan(persisted)
	if err != nil || fingerprint != persisted.Fingerprint {
		return nil, runtime.ErrExtensionPlanMismatch
	}
	instances := make(map[string]bool)
	toolIdentities := make(map[planToolIdentity]bool)
	var sessionID session.ID
	recoverSessionID := func(scope extension.Scope) error {
		if scope.Kind != extension.ScopeSession || scope.Key == "" {
			return nil
		}
		if sessionID != "" && sessionID != session.ID(scope.Key) {
			return runtime.ErrExtensionPlanMismatch
		}
		sessionID = session.ID(scope.Key)
		return nil
	}
	for _, identity := range persisted.Handlers {
		instances[identity.InstanceID] = true
		for _, registration := range identity.Registrations {
			if err := recoverSessionID(registration.Scope); err != nil {
				return nil, err
			}
		}
	}
	for _, identity := range persisted.Tools {
		instances[identity.InstanceID] = true
		toolIdentities[planToolIdentity{InstanceID: identity.InstanceID, Scope: identity.Scope, RegistrationID: identity.RegistrationID, ToolName: identity.Name}] = true
		if err := recoverSessionID(identity.Scope); err != nil {
			return nil, err
		}
	}
	for _, identity := range persisted.Prompts {
		instances[identity.InstanceID] = true
		if err := recoverSessionID(identity.Scope); err != nil {
			return nil, err
		}
	}
	for _, identity := range persisted.Guards {
		instances[identity.InstanceID] = true
		if err := recoverSessionID(identity.Scope); err != nil {
			return nil, err
		}
	}
	for _, identity := range persisted.Restrictions {
		instances[identity.InstanceID] = true
		if err := recoverSessionID(identity.Scope); err != nil {
			return nil, err
		}
	}
	plan, err := r.acquire(ctx, sessionID, instances, persistedToolSelector(toolIdentities))
	if err != nil {
		return nil, err
	}
	if plan.Descriptor().Fingerprint != persisted.Fingerprint {
		plan.Release()
		return nil, runtime.ErrExtensionPlanMismatch
	}
	return plan, nil
}

func (r *Registry) acquire(ctx context.Context, sessionID session.ID, instances map[string]bool, selectTool planToolSelector) (*runtime.RunPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	target := extension.GlobalScope()
	if sessionID != "" {
		target = extension.SessionScope(string(sessionID))
	}
	var dispatch *extension.Plan
	var err error
	if instances == nil {
		dispatch, err = r.extensions.Snapshot(target)
	} else {
		ids := make([]string, 0, len(instances))
		for id := range instances {
			ids = append(ids, id)
		}
		dispatch, err = r.extensions.SnapshotInstances(target, ids)
	}
	if err != nil {
		return nil, err
	}
	selected := selectTools(r.components, target, instances, selectTool)
	prompts := selectPrompts(r.components, target, instances)
	guards := selectGuards(r.components, target, instances)
	restrictions := selectRestrictions(r.components, target, instances)
	planTools := make([]runtime.PlanTool, len(selected))
	for index, entry := range selected {
		schemaHash, hashErr := composedToolSchemaHash(entry.ToolRegistration)
		if hashErr != nil {
			dispatch.Release()
			return nil, hashErr
		}
		definition, cloneErr := entry.Definition.Clone()
		if cloneErr != nil {
			dispatch.Release()
			return nil, cloneErr
		}
		planTools[index] = runtime.PlanTool{
			Identity: session.ToolPlanIdentity{
				InstanceID: entry.component.InstanceID, Artifact: entry.component.Artifact, Name: entry.Definition.Name,
				RegistrationID: entry.ID, Scope: entry.Scope, SchemaHash: schemaHash, ExecutorHash: entry.Definition.Provenance.ExecutorHash,
			},
			Resolve: func(ctx context.Context, scope runtime.ToolScopeContext) (runtime.Tool, error) {
				return tools.Materialize(ctx, definition, scope)
			},
		}
	}
	planPrompts := make([]runtime.PlanPrompt, len(prompts))
	for index, prompt := range prompts {
		planPrompts[index] = runtime.PlanPrompt{
			Identity: session.PromptPlanIdentity{InstanceID: prompt.component.InstanceID, Artifact: prompt.component.Artifact, Name: prompt.Name, RegistrationID: prompt.ID, Scope: prompt.Scope, Order: prompt.Order},
			Prompt:   runtime.MountedPrompt{Name: prompt.Name, Order: prompt.Order, InstanceID: prompt.component.InstanceID, Provider: prompt.Provider},
		}
	}
	planGuards := make([]runtime.PlanGuard, len(guards))
	for index, guard := range guards {
		planGuards[index] = runtime.PlanGuard{
			Identity: session.GuardPlanIdentity{InstanceID: guard.component.InstanceID, Artifact: guard.component.Artifact, RegistrationID: guard.ID, Scope: guard.Scope, Order: guard.Order},
			Guard:    runtime.MountedToolGuard{ID: guard.ID, Order: guard.Order, InstanceID: guard.component.InstanceID, Guard: guard.Guard},
		}
	}
	planRestrictions := make([]runtime.PlanRestriction, len(restrictions))
	for index, restriction := range restrictions {
		rules, rulesErr := runtime.CanonicalizeRestrictionRules(restriction.Allowed, restriction.Denied)
		if rulesErr != nil {
			dispatch.Release()
			return nil, rulesErr
		}
		planRestrictions[index] = runtime.PlanRestriction{
			Identity: session.RestrictionPlanIdentity{InstanceID: restriction.component.InstanceID, Artifact: restriction.component.Artifact, RegistrationID: restriction.ID, Scope: restriction.Scope, RulesHash: rules.Hash},
			Allowed:  rules.Allowed, Denied: rules.Denied,
		}
	}
	return runtime.NewRunPlan(runtime.RunPlanSpec{Dispatch: dispatch, Tools: planTools, Prompts: planPrompts, Guards: planGuards, Restrictions: planRestrictions})
}

type selectedTool struct {
	ToolRegistration
	component extension.Component
}

type selectedPrompt struct {
	PromptRegistration
	component extension.Component
}

type selectedGuard struct {
	GuardRegistration
	component extension.Component
}

type selectedRestriction struct {
	RestrictionRegistration
	component extension.Component
}

type planToolSelector func(selectedTool) bool

type planToolIdentity struct {
	InstanceID     string
	Scope          extension.Scope
	RegistrationID string
	ToolName       string
}

func requestedToolSelector(toolConfig config.ToolConfig) planToolSelector {
	enabled := make(map[string]bool, len(toolConfig.Enabled))
	for _, name := range toolConfig.Enabled {
		enabled[name] = true
	}
	disabled := make(map[string]bool, len(toolConfig.Disabled))
	for _, name := range toolConfig.Disabled {
		disabled[name] = true
	}
	return func(entry selectedTool) bool {
		if disabled[entry.Definition.Name] {
			return false
		}
		return toolConfig.Enabled == nil || enabled[entry.Definition.Name]
	}
}

func persistedToolSelector(identities map[planToolIdentity]bool) planToolSelector {
	return func(entry selectedTool) bool {
		return identities[planToolIdentity{
			InstanceID: entry.component.InstanceID, Scope: entry.Scope, RegistrationID: entry.ID, ToolName: entry.Definition.Name,
		}]
	}
}

func selectTools(components map[string]*mountedComponent, target extension.Scope, instances map[string]bool, selectTool planToolSelector) []selectedTool {
	var result []selectedTool
	for _, mounted := range components {
		for _, registration := range mounted.tools {
			entry := selectedTool{ToolRegistration: registration, component: mounted.component}
			if instances != nil && !instances[entry.component.InstanceID] || selectTool != nil && !selectTool(entry) {
				continue
			}
			if entry.Scope.Kind == extension.ScopeGlobal || entry.Scope.Kind == extension.ScopeSession && target.Kind == extension.ScopeSession && entry.Scope.Key == target.Key {
				result = append(result, entry)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Order != result[j].Order {
			return result[i].Order < result[j].Order
		}
		if result[i].Definition.Name != result[j].Definition.Name {
			return result[i].Definition.Name < result[j].Definition.Name
		}
		if result[i].component.InstanceID != result[j].component.InstanceID {
			return result[i].component.InstanceID < result[j].component.InstanceID
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func selectPrompts(components map[string]*mountedComponent, target extension.Scope, instances map[string]bool) []selectedPrompt {
	selected := make(map[string]selectedPrompt)
	for _, mounted := range components {
		for _, registration := range mounted.prompts {
			entry := selectedPrompt{PromptRegistration: registration, component: mounted.component}
			if !capabilityApplies(entry.component.InstanceID, entry.Scope, target, instances) {
				continue
			}
			current, exists := selected[entry.Name]
			if !exists || current.Scope.Kind == extension.ScopeGlobal && entry.Scope.Kind == extension.ScopeSession {
				selected[entry.Name] = entry
			}
		}
	}
	result := make([]selectedPrompt, 0, len(selected))
	for _, entry := range selected {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Order != result[j].Order {
			return result[i].Order < result[j].Order
		}
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].component.InstanceID < result[j].component.InstanceID
	})
	return result
}

func selectGuards(components map[string]*mountedComponent, target extension.Scope, instances map[string]bool) []selectedGuard {
	var result []selectedGuard
	for _, mounted := range components {
		for _, registration := range mounted.guards {
			entry := selectedGuard{GuardRegistration: registration, component: mounted.component}
			if capabilityApplies(entry.component.InstanceID, entry.Scope, target, instances) {
				result = append(result, entry)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Order != result[j].Order {
			return result[i].Order < result[j].Order
		}
		if result[i].component.InstanceID != result[j].component.InstanceID {
			return result[i].component.InstanceID < result[j].component.InstanceID
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func selectRestrictions(components map[string]*mountedComponent, target extension.Scope, instances map[string]bool) []selectedRestriction {
	var result []selectedRestriction
	for _, mounted := range components {
		for _, registration := range mounted.restrictions {
			entry := selectedRestriction{RestrictionRegistration: registration, component: mounted.component}
			if capabilityApplies(entry.component.InstanceID, entry.Scope, target, instances) {
				result = append(result, entry)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].component.InstanceID != result[j].component.InstanceID {
			return result[i].component.InstanceID < result[j].component.InstanceID
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func capabilityApplies(instanceID string, scope, target extension.Scope, instances map[string]bool) bool {
	if instances != nil && !instances[instanceID] {
		return false
	}
	return scope.Kind == extension.ScopeGlobal || scope.Kind == extension.ScopeSession && target.Kind == extension.ScopeSession && scope.Key == target.Key
}

func toolSchemaHash(definition tools.Definition) (string, error) {
	parameters, err := definition.Parameters.ToJSONSchema()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(struct {
		Name        string
		Description string
		Parameters  any
		Permissions []string
		RetrySafe   bool
		Retention   runtime.RetentionPolicy
		Metadata    map[string]string
	}{
		Name: definition.Name, Description: definition.Description, Parameters: parameters,
		Permissions: definition.Permissions, RetrySafe: definition.RetrySafe,
		Retention: definition.Retention, Metadata: definition.Metadata,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

type Diagnostics struct {
	Components []extension.ComponentDiagnostic
	Tools      []ToolDiagnostic
}

type ToolDiagnostic struct {
	InstanceID string
	ID         string
	Name       string
	Order      int
	Scope      extension.Scope
}

func (r *Registry) Diagnostics() Diagnostics {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := Diagnostics{Components: r.extensions.Diagnostics()}
	for _, mounted := range r.components {
		for _, entry := range mounted.tools {
			result.Tools = append(result.Tools, ToolDiagnostic{InstanceID: mounted.component.InstanceID, ID: entry.ID, Name: entry.Definition.Name, Order: entry.Order, Scope: entry.Scope})
		}
	}
	sort.Slice(result.Tools, func(i, j int) bool {
		if result.Tools[i].InstanceID != result.Tools[j].InstanceID {
			return result.Tools[i].InstanceID < result.Tools[j].InstanceID
		}
		return result.Tools[i].ID < result.Tools[j].ID
	})
	return result
}

var _ runtime.RunPlanProvider = (*Registry)(nil)
