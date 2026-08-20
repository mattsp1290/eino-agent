package composition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

var leasePoint = extension.NewNotification(extension.Contract{ID: "eino-agent/composition/lease", Version: "1"}, extension.NotificationContained, func(value struct{}) struct{} { return value })

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
	r.guards = append(r.guards, registration)
	return nil
}

func (r *Registrar) RestrictTools(registration RestrictionRegistration) error {
	if err := validateCapabilityIdentity(r.component, registration.InstanceID, registration.ID, registration.Scope); err != nil {
		return err
	}
	registration.Allowed = append([]string(nil), registration.Allowed...)
	registration.Denied = append([]string(nil), registration.Denied...)
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
	r.mu.Lock()
	defer r.mu.Unlock()
	var staged *Registrar
	extensionMount, err := r.extensions.Mount(ctx, component, extension.InstallerFunc(func(ctx context.Context, extensions extension.Registrar) error {
		registrar := &Registrar{extensions: extensions, component: component}
		if err := installer.Install(ctx, registrar); err != nil {
			return err
		}
		if err := r.validateTools(registrar.tools); err != nil {
			return err
		}
		if err := r.validatePrompts(registrar.prompts); err != nil {
			return err
		}
		staged = registrar
		scopes := map[extension.Scope]bool{}
		for _, registration := range registrar.tools {
			scopes[registration.Scope] = true
		}
		for _, registration := range registrar.prompts {
			scopes[registration.Scope] = true
		}
		for _, registration := range registrar.guards {
			scopes[registration.Scope] = true
		}
		for _, registration := range registrar.restrictions {
			scopes[registration.Scope] = true
		}
		if len(scopes) == 0 {
			scopes[extension.GlobalScope()] = true
		}
		index := 0
		for scope := range scopes {
			id := fmt.Sprintf("composition-lease-%06d", index)
			if err := extension.On(extensions, leasePoint, extension.Registration{ID: id, InstanceID: component.InstanceID, Order: -1 << 30, Scope: scope}, func(context.Context, struct{}) error { return nil }); err != nil {
				return err
			}
			index++
		}
		return nil
	}))
	if err != nil {
		return nil, err
	}
	for _, registration := range staged.tools {
		r.tools = append(r.tools, mountedTool{ToolRegistration: registration, component: component})
	}
	for _, registration := range staged.prompts {
		r.prompts = append(r.prompts, mountedPrompt{PromptRegistration: registration, component: component})
	}
	for _, registration := range staged.guards {
		r.guards = append(r.guards, mountedGuard{GuardRegistration: registration, component: component})
	}
	for _, registration := range staged.restrictions {
		r.restrictions = append(r.restrictions, mountedRestriction{RestrictionRegistration: registration, component: component})
	}
	return &Mount{registry: r, extension: extensionMount, instance: component.InstanceID}, nil
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
	m.Deactivate()
	return m.extension.Close(ctx)
}

func (r *Registry) AcquireRunPlan(ctx context.Context, request runtime.RunPlanRequest) (*runtime.RunPlan, error) {
	return r.acquire(ctx, request.SessionID, nil, session.PlanStrict)
}

func (r *Registry) AcquireResumePlan(ctx context.Context, persisted session.ExtensionPlanDescriptor) (*runtime.RunPlan, error) {
	instances := make(map[string]bool)
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
		if err := recoverSessionID(entry.Scope); err != nil {
			return nil, err
		}
		for _, registration := range entry.Registrations {
			if err := recoverSessionID(registration.Scope); err != nil {
				return nil, err
			}
		}
	}
	plan, err := r.acquire(ctx, sessionID, instances, persisted.Mode)
	if err != nil {
		return nil, err
	}
	if plan.Descriptor.Fingerprint != persisted.Fingerprint {
		plan.Dispatch.Release()
		return nil, runtime.ErrExtensionPlanMismatch
	}
	return plan, nil
}

func (r *Registry) acquire(ctx context.Context, sessionID session.ID, instances map[string]bool, mode session.PlanMode) (*runtime.RunPlan, error) {
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
	selected := selectTools(r.tools, target, instances)
	prompts := selectPrompts(r.prompts, target, instances)
	guards := selectGuards(r.guards, target, instances)
	restrictions := selectRestrictions(r.restrictions, target, instances)
	descriptor, err := buildDescriptor(dispatch, selected, prompts, guards, restrictions, mode)
	if err != nil {
		dispatch.Release()
		return nil, err
	}
	entries := make([]tools.SnapshotEntry, len(selected))
	for index, entry := range selected {
		entries[index] = tools.SnapshotEntry{Registration: tools.Registration{Name: entry.Definition.Name, Generation: uint64(index + 1)}, Definition: entry.Definition.Clone()}
	}
	frozen := tools.NewSnapshot(entries)
	mountedPrompts := make([]runtime.MountedPrompt, len(prompts))
	for index, prompt := range prompts {
		mountedPrompts[index] = runtime.MountedPrompt{Name: prompt.Name, Order: prompt.Order, InstanceID: prompt.InstanceID, Provider: prompt.Provider}
	}
	mountedGuards := make([]runtime.MountedToolGuard, len(guards))
	for index, guard := range guards {
		mountedGuards[index] = runtime.MountedToolGuard{ID: guard.ID, Order: guard.Order, InstanceID: guard.InstanceID, Guard: guard.Guard}
	}
	return &runtime.RunPlan{Dispatch: dispatch, Tools: frozenTools{snapshot: frozen, restrictions: restrictions}, Prompts: mountedPrompts, Guards: mountedGuards, Descriptor: descriptor, RequiresToolSettlement: len(selected) != 0}, nil
}

type frozenTools struct {
	snapshot     tools.Snapshot
	restrictions []mountedRestriction
}

func (f frozenTools) ResolveTools(ctx context.Context, snapshot runtime.TurnSnapshot) ([]runtime.Tool, error) {
	resolved, err := f.snapshot.ResolveTools(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	filtered := resolved[:0]
	for _, tool := range resolved {
		if toolAllowed(tool.Name, f.restrictions) {
			filtered = append(filtered, tool)
		}
	}
	return filtered, nil
}

func toolAllowed(name string, restrictions []mountedRestriction) bool {
	for _, restriction := range restrictions {
		for _, denied := range restriction.Denied {
			if denied == name {
				return false
			}
		}
		if len(restriction.Allowed) != 0 {
			found := false
			for _, allowed := range restriction.Allowed {
				found = found || allowed == name
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func selectTools(entries []mountedTool, target extension.Scope, instances map[string]bool) []mountedTool {
	global := make(map[string]mountedTool)
	sessionLayer := make(map[string]mountedTool)
	for _, entry := range entries {
		if instances != nil && !instances[entry.component.InstanceID] {
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

func buildDescriptor(dispatch *extension.Plan, selected []mountedTool, prompts []mountedPrompt, guards []mountedGuard, restrictions []mountedRestriction, mode session.PlanMode) (session.ExtensionPlanDescriptor, error) {
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: 1, Mode: mode}
	byInstance := map[string]int{}
	for _, diagnostic := range dispatch.Diagnostics() {
		if diagnostic.Contract == leasePoint.Contract() {
			continue
		}
		index, ok := byInstance[diagnostic.InstanceID]
		if !ok {
			index = len(descriptor.Entries)
			byInstance[diagnostic.InstanceID] = index
			descriptor.Entries = append(descriptor.Entries, session.ExtensionPlanEntry{InstanceID: diagnostic.InstanceID, Kind: session.ExtensionHandlers, Artifact: artifactIdentity(diagnostic.Artifact), Required: true, Scope: scopeIdentity(diagnostic.Scope)})
		}
		descriptor.Entries[index].Registrations = append(descriptor.Entries[index].Registrations, session.RegistrationIdentity{ID: diagnostic.ID, Contract: diagnostic.Contract.ID, Version: diagnostic.Contract.Version, Order: diagnostic.Order, Scope: scopeIdentity(diagnostic.Scope)})
	}
	for _, entry := range selected {
		descriptor.Entries = append(descriptor.Entries, session.ExtensionPlanEntry{
			InstanceID: entry.InstanceID, Kind: session.ExtensionTool, Artifact: artifactIdentity(entry.component.Artifact), Required: true,
			Scope: scopeIdentity(entry.Scope), CapabilityID: entry.Definition.Name + "/" + entry.ID,
			SchemaHash: toolSchemaHash(entry.Definition), ExecutorHash: entry.Definition.Provenance.ExecutorHash,
		})
	}
	for _, entry := range prompts {
		descriptor.Entries = append(descriptor.Entries, session.ExtensionPlanEntry{InstanceID: entry.InstanceID, Kind: session.ExtensionPrompt, Artifact: artifactIdentity(entry.component.Artifact), Required: true, Scope: scopeIdentity(entry.Scope), CapabilityID: entry.Name + "/" + entry.ID})
	}
	for _, entry := range guards {
		descriptor.Entries = append(descriptor.Entries, session.ExtensionPlanEntry{InstanceID: entry.InstanceID, Kind: session.ExtensionGuard, Artifact: artifactIdentity(entry.component.Artifact), Required: true, Scope: scopeIdentity(entry.Scope), CapabilityID: entry.ID})
	}
	for _, entry := range restrictions {
		raw, _ := json.Marshal(struct{ Allowed, Denied []string }{entry.Allowed, entry.Denied})
		digest := sha256.Sum256(raw)
		descriptor.Entries = append(descriptor.Entries, session.ExtensionPlanEntry{InstanceID: entry.InstanceID, Kind: session.ExtensionRestriction, Artifact: artifactIdentity(entry.component.Artifact), Required: true, Scope: scopeIdentity(entry.Scope), CapabilityID: entry.ID, SchemaHash: hex.EncodeToString(digest[:])})
	}
	fingerprint, err := session.FingerprintExtensionPlan(descriptor)
	if err != nil {
		return session.ExtensionPlanDescriptor{}, err
	}
	descriptor.Fingerprint = fingerprint
	return descriptor, nil
}

func artifactIdentity(artifact extension.Artifact) session.ArtifactIdentity {
	return session.ArtifactIdentity{Name: artifact.Name, Version: artifact.Version, Hash: artifact.Hash, ConfigHash: artifact.ConfigHash, SourceKind: string(artifact.SourceKind)}
}

func scopeIdentity(scope extension.Scope) session.ExtensionScope {
	return session.ExtensionScope{Kind: string(scope.Kind), Key: scope.Key}
}

func toolSchemaHash(definition tools.Definition) string {
	raw, _ := json.Marshal(struct {
		Name        string
		Description string
		Parameters  any
		Permissions []string
		RetrySafe   bool
	}{definition.Name, definition.Description, definition.Parameters, definition.Permissions, definition.RetrySafe})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
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
