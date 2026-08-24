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
	ID         string
	InstanceID string
	Order      int
	Scope      extension.Scope
	Definition tools.Definition
}

type PromptRegistration struct {
	ID         string
	InstanceID string
	Name       string
	Order      int
	Scope      extension.Scope
	Provider   runtime.PromptProvider
}

type GuardRegistration struct {
	ID         string
	InstanceID string
	Order      int
	Scope      extension.Scope
	Guard      runtime.ToolGuard
}

type RestrictionRegistration struct {
	ID         string
	InstanceID string
	Scope      extension.Scope
	Allowed    []string
	Denied     []string
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
	if err := validateCapabilityIdentity(r.component, registration.InstanceID, registration.ID, registration.Scope); err != nil || registration.Name == "" || registration.Provider == nil {
		if err != nil {
			return err
		}
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
	if err := validateCapabilityIdentity(r.component, registration.InstanceID, registration.ID, registration.Scope); err != nil || registration.Guard == nil {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: invalid guard registration", extension.ErrInvalidRegistration)
	}
	if err := r.extensions.Lease(registration.Scope); err != nil {
		return err
	}
	r.guards = append(r.guards, registration)
	return nil
}

func (r *Registrar) RestrictTools(registration RestrictionRegistration) error {
	if err := validateCapabilityIdentity(r.component, registration.InstanceID, registration.ID, registration.Scope); err != nil {
		return err
	}
	registration.Allowed = append([]string(nil), registration.Allowed...)
	registration.Denied = append([]string(nil), registration.Denied...)
	if err := r.extensions.Lease(registration.Scope); err != nil {
		return err
	}
	r.restrictions = append(r.restrictions, registration)
	return nil
}

func validateCapabilityIdentity(component extension.Component, instanceID, id string, scope extension.Scope) error {
	if instanceID != component.InstanceID || id == "" {
		return fmt.Errorf("%w: capability identity does not match mount", extension.ErrInvalidRegistration)
	}
	if scope.Kind != extension.ScopeGlobal && scope.Kind != extension.ScopeSession || scope.Kind == extension.ScopeGlobal && scope.Key != "" || scope.Kind == extension.ScopeSession && scope.Key == "" {
		return fmt.Errorf("%w: invalid capability scope", extension.ErrInvalidRegistration)
	}
	return nil
}

func (r *Registrar) Extensions() extension.Registrar       { return r.extensions }
func (r *Registrar) Defer(cleanup extension.Cleanup) error { return r.extensions.Defer(cleanup) }

func (r *Registrar) Tool(registration ToolRegistration) error {
	if registration.InstanceID != r.component.InstanceID || registration.ID == "" || registration.Definition.Name == "" {
		return fmt.Errorf("%w: invalid composed tool identity", extension.ErrInvalidRegistration)
	}
	if registration.Scope.Kind != extension.ScopeGlobal && registration.Scope.Kind != extension.ScopeSession {
		return fmt.Errorf("%w: invalid composed tool scope", extension.ErrInvalidRegistration)
	}
	if registration.Scope.Kind == extension.ScopeGlobal && registration.Scope.Key != "" || registration.Scope.Kind == extension.ScopeSession && registration.Scope.Key == "" {
		return fmt.Errorf("%w: invalid composed tool scope key", extension.ErrInvalidRegistration)
	}
	if err := tools.ValidateDefinition(registration.Definition); err != nil {
		return err
	}
	for _, existing := range r.tools {
		if existing.Scope == registration.Scope && existing.Definition.Name == registration.Definition.Name {
			return fmt.Errorf("%w: %s", tools.ErrDuplicateRegistration, registration.Definition.Name)
		}
	}
	registration.Definition = registration.Definition.Clone()
	registration.Definition.Provenance = tools.Provenance{
		InstanceID: r.component.InstanceID, ArtifactName: r.component.Artifact.Name,
		ArtifactVersion: r.component.Artifact.Version, ArtifactHash: r.component.Artifact.Hash,
		ConfigHash: r.component.Artifact.ConfigHash, ExecutorHash: r.component.Artifact.Hash,
	}
	if err := r.extensions.Lease(registration.Scope); err != nil {
		return err
	}
	r.tools = append(r.tools, registration)
	return nil
}

type Registry struct {
	mu           sync.Mutex
	extensions   *extension.Registry
	tools        []mountedTool
	prompts      []mountedPrompt
	guards       []mountedGuard
	restrictions []mountedRestriction
}

type mountedTool struct {
	ToolRegistration
	component extension.Component
}

type mountedPrompt struct {
	PromptRegistration
	component extension.Component
}

type mountedGuard struct {
	GuardRegistration
	component extension.Component
}

type mountedRestriction struct {
	RestrictionRegistration
	component extension.Component
}

func NewRegistry(reporter extension.Reporter) *Registry {
	return &Registry{extensions: extension.NewRegistry(reporter)}
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
	for _, registration := range staged.tools {
		registration.Definition = mountToolDefinition(extensionMount, registration.Definition)
		r.tools = append(r.tools, mountedTool{ToolRegistration: registration, component: component})
	}
	for _, registration := range staged.prompts {
		registration.Provider = mountedPromptProvider{mount: extensionMount, next: registration.Provider}
		r.prompts = append(r.prompts, mountedPrompt{PromptRegistration: registration, component: component})
	}
	for _, registration := range staged.guards {
		registration.Guard = mountedToolGuard{mount: extensionMount, next: registration.Guard}
		r.guards = append(r.guards, mountedGuard{GuardRegistration: registration, component: component})
	}
	for _, registration := range staged.restrictions {
		r.restrictions = append(r.restrictions, mountedRestriction{RestrictionRegistration: registration, component: component})
	}
	r.mu.Unlock()
	return &Mount{registry: r, extension: extensionMount, instance: component.InstanceID}, nil
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
	next := definition.Clone()
	if callback := next.Decode; callback != nil {
		next.Decode = func(ctx context.Context, raw json.RawMessage) (any, error) {
			return callback(mount.CallbackContext(ctx), raw)
		}
	}
	if callback := next.Normalize; callback != nil {
		next.Normalize = func(ctx context.Context, input any) (json.RawMessage, error) {
			return callback(mount.CallbackContext(ctx), input)
		}
	}
	if callback := next.Encode; callback != nil {
		next.Encode = func(ctx context.Context, value any) (json.RawMessage, error) {
			return callback(mount.CallbackContext(ctx), value)
		}
	}
	if callback := next.Execute; callback != nil {
		next.Execute = func(ctx context.Context, execution tools.Execution) (any, error) {
			return callback(mount.CallbackContext(ctx), execution)
		}
	}
	return next
}

func (r *Registry) validateTools(staged []ToolRegistration) error {
	for _, candidate := range staged {
		for _, existing := range r.tools {
			if existing.Scope == candidate.Scope && existing.Definition.Name == candidate.Definition.Name {
				return fmt.Errorf("%w: %s", tools.ErrDuplicateRegistration, candidate.Definition.Name)
			}
		}
	}
	return nil
}

func (r *Registry) validatePrompts(staged []PromptRegistration) error {
	for _, candidate := range staged {
		for _, existing := range r.prompts {
			if existing.Scope == candidate.Scope && existing.Name == candidate.Name {
				return fmt.Errorf("%w: prompt %s", extension.ErrDuplicateRegistration, candidate.Name)
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
		filtered := m.registry.tools[:0]
		for _, entry := range m.registry.tools {
			if entry.component.InstanceID != m.instance {
				filtered = append(filtered, entry)
			}
		}
		m.registry.tools = filtered
		prompts := m.registry.prompts[:0]
		for _, entry := range m.registry.prompts {
			if entry.component.InstanceID != m.instance {
				prompts = append(prompts, entry)
			}
		}
		m.registry.prompts = prompts
		guards := m.registry.guards[:0]
		for _, entry := range m.registry.guards {
			if entry.component.InstanceID != m.instance {
				guards = append(guards, entry)
			}
		}
		m.registry.guards = guards
		restrictions := m.registry.restrictions[:0]
		for _, entry := range m.registry.restrictions {
			if entry.component.InstanceID != m.instance {
				restrictions = append(restrictions, entry)
			}
		}
		m.registry.restrictions = restrictions
		m.extension.Deactivate()
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
	instances := make(map[string]bool)
	toolIdentities := make(map[planToolIdentity]bool)
	var sessionID session.ID
	recoverSessionID := func(scope session.ExtensionScope) error {
		if scope.Kind != string(extension.ScopeSession) || scope.Key == "" {
			return nil
		}
		if sessionID != "" && sessionID != session.ID(scope.Key) {
			return runtime.ErrExtensionPlanMismatch
		}
		sessionID = session.ID(scope.Key)
		return nil
	}
	for _, entry := range persisted.Entries {
		if entry.Required {
			instances[entry.InstanceID] = true
		}
		if entry.Required && entry.Kind == session.ExtensionTool {
			toolIdentities[planToolIdentity{InstanceID: entry.InstanceID, Scope: entry.Scope, CapabilityID: entry.CapabilityID}] = true
		}
		if err := recoverSessionID(entry.Scope); err != nil {
			return nil, err
		}
		for _, registration := range entry.Registrations {
			if err := recoverSessionID(registration.Scope); err != nil {
				return nil, err
			}
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
	selected := selectTools(r.tools, target, instances, selectTool)
	prompts := selectPrompts(r.prompts, target, instances)
	guards := selectGuards(r.guards, target, instances)
	restrictions := selectRestrictions(r.restrictions, target, instances)
	planTools := make([]runtime.PlanTool, len(selected))
	for index, entry := range selected {
		schemaHash, hashErr := toolSchemaHash(entry.Definition)
		if hashErr != nil {
			dispatch.Release()
			return nil, hashErr
		}
		frozen := tools.NewSnapshot([]tools.SnapshotEntry{{Registration: tools.Registration{Name: entry.Definition.Name, Generation: 1}, Definition: entry.Definition.Clone()}})
		planTools[index] = runtime.PlanTool{
			Identity: session.ExtensionPlanEntry{
				InstanceID: entry.InstanceID, Kind: session.ExtensionTool, Artifact: artifactIdentity(entry.component.Artifact), Required: true,
				Scope: scopeIdentity(entry.Scope), CapabilityID: entry.Definition.Name + "/" + entry.ID,
				SchemaHash: schemaHash, ExecutorHash: entry.Definition.Provenance.ExecutorHash,
			},
			Resolve: func(ctx context.Context, snapshot runtime.TurnSnapshot) (runtime.Tool, error) {
				resolved, resolveErr := frozen.ResolveTools(ctx, snapshot)
				if resolveErr != nil {
					return runtime.Tool{}, resolveErr
				}
				if len(resolved) != 1 {
					return runtime.Tool{}, fmt.Errorf("sealed tool %q did not materialize", entry.Definition.Name)
				}
				return resolved[0], nil
			},
		}
	}
	planPrompts := make([]runtime.PlanPrompt, len(prompts))
	for index, prompt := range prompts {
		planPrompts[index] = runtime.PlanPrompt{
			Identity: session.ExtensionPlanEntry{InstanceID: prompt.InstanceID, Kind: session.ExtensionPrompt, Artifact: artifactIdentity(prompt.component.Artifact), Required: true, Scope: scopeIdentity(prompt.Scope), CapabilityID: prompt.Name + "/" + prompt.ID, Order: prompt.Order},
			Prompt:   runtime.MountedPrompt{Name: prompt.Name, Order: prompt.Order, InstanceID: prompt.InstanceID, Provider: prompt.Provider},
		}
	}
	planGuards := make([]runtime.PlanGuard, len(guards))
	for index, guard := range guards {
		planGuards[index] = runtime.PlanGuard{
			Identity: session.ExtensionPlanEntry{InstanceID: guard.InstanceID, Kind: session.ExtensionGuard, Artifact: artifactIdentity(guard.component.Artifact), Required: true, Scope: scopeIdentity(guard.Scope), CapabilityID: guard.ID, Order: guard.Order},
			Guard:    runtime.MountedToolGuard{ID: guard.ID, Order: guard.Order, InstanceID: guard.InstanceID, Guard: guard.Guard},
		}
	}
	planRestrictions := make([]runtime.PlanRestriction, len(restrictions))
	for index, restriction := range restrictions {
		raw, marshalErr := json.Marshal(struct{ Allowed, Denied []string }{restriction.Allowed, restriction.Denied})
		if marshalErr != nil {
			dispatch.Release()
			return nil, marshalErr
		}
		digest := sha256.Sum256(raw)
		planRestrictions[index] = runtime.PlanRestriction{
			Identity: session.ExtensionPlanEntry{InstanceID: restriction.InstanceID, Kind: session.ExtensionRestriction, Artifact: artifactIdentity(restriction.component.Artifact), Required: true, Scope: scopeIdentity(restriction.Scope), CapabilityID: restriction.ID, SchemaHash: hex.EncodeToString(digest[:])},
			Allowed:  append([]string(nil), restriction.Allowed...), Denied: append([]string(nil), restriction.Denied...),
		}
	}
	return runtime.NewRunPlan(runtime.RunPlanSpec{Dispatch: dispatch, Tools: planTools, Prompts: planPrompts, Guards: planGuards, Restrictions: planRestrictions})
}

type planToolSelector func(mountedTool) bool

type planToolIdentity struct {
	InstanceID   string
	Scope        session.ExtensionScope
	CapabilityID string
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
	return func(entry mountedTool) bool {
		if disabled[entry.Definition.Name] {
			return false
		}
		return toolConfig.Enabled == nil || enabled[entry.Definition.Name]
	}
}

func persistedToolSelector(identities map[planToolIdentity]bool) planToolSelector {
	return func(entry mountedTool) bool {
		return identities[planToolIdentity{
			InstanceID:   entry.InstanceID,
			Scope:        scopeIdentity(entry.Scope),
			CapabilityID: entry.Definition.Name + "/" + entry.ID,
		}]
	}
}

func selectTools(entries []mountedTool, target extension.Scope, instances map[string]bool, selectTool planToolSelector) []mountedTool {
	global := make(map[string]mountedTool)
	sessionLayer := make(map[string]mountedTool)
	for _, entry := range entries {
		if instances != nil && !instances[entry.component.InstanceID] || selectTool != nil && !selectTool(entry) {
			continue
		}
		switch {
		case entry.Scope.Kind == extension.ScopeGlobal:
			global[entry.Definition.Name] = entry
		case entry.Scope.Kind == extension.ScopeSession && target.Kind == extension.ScopeSession && entry.Scope.Key == target.Key:
			sessionLayer[entry.Definition.Name] = entry
		}
	}
	for name, entry := range sessionLayer {
		global[name] = entry
	}
	result := make([]mountedTool, 0, len(global))
	for _, entry := range global {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Order != result[j].Order {
			return result[i].Order < result[j].Order
		}
		if result[i].Definition.Name != result[j].Definition.Name {
			return result[i].Definition.Name < result[j].Definition.Name
		}
		if result[i].InstanceID != result[j].InstanceID {
			return result[i].InstanceID < result[j].InstanceID
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func selectPrompts(entries []mountedPrompt, target extension.Scope, instances map[string]bool) []mountedPrompt {
	selected := make(map[string]mountedPrompt)
	for _, entry := range entries {
		if !capabilityApplies(entry.component.InstanceID, entry.Scope, target, instances) {
			continue
		}
		current, exists := selected[entry.Name]
		if !exists || current.Scope.Kind == extension.ScopeGlobal && entry.Scope.Kind == extension.ScopeSession {
			selected[entry.Name] = entry
		}
	}
	result := make([]mountedPrompt, 0, len(selected))
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
		return result[i].InstanceID < result[j].InstanceID
	})
	return result
}

func selectGuards(entries []mountedGuard, target extension.Scope, instances map[string]bool) []mountedGuard {
	var result []mountedGuard
	for _, entry := range entries {
		if capabilityApplies(entry.component.InstanceID, entry.Scope, target, instances) {
			result = append(result, entry)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Order != result[j].Order {
			return result[i].Order < result[j].Order
		}
		if result[i].InstanceID != result[j].InstanceID {
			return result[i].InstanceID < result[j].InstanceID
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func selectRestrictions(entries []mountedRestriction, target extension.Scope, instances map[string]bool) []mountedRestriction {
	var result []mountedRestriction
	for _, entry := range entries {
		if capabilityApplies(entry.component.InstanceID, entry.Scope, target, instances) {
			result = append(result, entry)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].InstanceID != result[j].InstanceID {
			return result[i].InstanceID < result[j].InstanceID
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

func artifactIdentity(artifact extension.Artifact) session.ArtifactIdentity {
	return session.ArtifactIdentity{Name: artifact.Name, Version: artifact.Version, Hash: artifact.Hash, ConfigHash: artifact.ConfigHash, SourceKind: string(artifact.SourceKind)}
}

func scopeIdentity(scope extension.Scope) session.ExtensionScope {
	return session.ExtensionScope{Kind: string(scope.Kind), Key: scope.Key}
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
		Concurrency runtime.ToolConcurrency
		Retention   runtime.RetentionPolicy
		Metadata    map[string]string
	}{
		Name: definition.Name, Description: definition.Description, Parameters: parameters,
		Permissions: definition.Permissions, RetrySafe: definition.RetrySafe, Concurrency: definition.Concurrency,
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
	for _, entry := range r.tools {
		result.Tools = append(result.Tools, ToolDiagnostic{InstanceID: entry.InstanceID, ID: entry.ID, Name: entry.Definition.Name, Order: entry.Order, Scope: entry.Scope})
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
