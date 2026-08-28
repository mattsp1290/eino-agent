package composition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

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
	r.tools = append(r.tools, registration)
	return nil
}

type Registry struct {
	extensions *extension.Registry[componentPayload]
}

type componentPayload struct {
	tools        []ToolRegistration
	prompts      []PromptRegistration
	guards       []GuardRegistration
	restrictions []RestrictionRegistration
}

func NewRegistry(reporter extension.Reporter) *Registry {
	return &Registry{extensions: extension.NewRegistry[componentPayload](reporter)}
}

type Mount struct {
	extension *extension.Mount[componentPayload]
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
	payload := componentPayload{
		tools: append([]ToolRegistration(nil), staged.tools...), prompts: append([]PromptRegistration(nil), staged.prompts...),
		guards: append([]GuardRegistration(nil), staged.guards...), restrictions: append([]RestrictionRegistration(nil), staged.restrictions...),
	}
	extensionMount, err := r.extensions.CommitMount(prepared, payload, payloadScopes(payload), validateComponentPayload)
	if err != nil {
		return nil, rollback(err)
	}
	return &Mount{extension: extensionMount}, nil
}

func payloadScopes(payload componentPayload) []extension.Scope {
	result := make([]extension.Scope, 0, len(payload.tools)+len(payload.prompts)+len(payload.guards)+len(payload.restrictions))
	for _, registration := range payload.tools {
		result = append(result, registration.Scope)
	}
	for _, registration := range payload.prompts {
		result = append(result, registration.Scope)
	}
	for _, registration := range payload.guards {
		result = append(result, registration.Scope)
	}
	for _, registration := range payload.restrictions {
		result = append(result, registration.Scope)
	}
	return result
}

type mountedPromptProvider struct {
	callbackContext func(context.Context) context.Context
	next            runtime.PromptProvider
}

func (p mountedPromptProvider) ProvidePrompt(ctx context.Context, prompt runtime.PromptContext) (string, error) {
	return p.next.ProvidePrompt(p.callbackContext(ctx), prompt)
}

type mountedToolGuard struct {
	callbackContext func(context.Context) context.Context
	next            runtime.ToolGuard
}

func (g mountedToolGuard) GuardTool(ctx context.Context, request runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
	return g.next.GuardTool(g.callbackContext(ctx), request)
}

func mountToolDefinition(callbackContext func(context.Context) context.Context, definition tools.Definition) tools.Definition {
	next := definition
	if callback := next.Normalize; callback != nil {
		next.Normalize = func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			return callback(callbackContext(ctx), input)
		}
	}
	if callback := next.Pattern; callback != nil {
		next.Pattern = func(ctx context.Context, input json.RawMessage) (string, error) {
			return callback(callbackContext(ctx), input)
		}
	}
	if callback := next.Execute; callback != nil {
		next.Execute = func(ctx context.Context, execution tools.Execution) (json.RawMessage, error) {
			return callback(callbackContext(ctx), execution)
		}
	}
	if callback := next.Scope; callback != nil {
		next.Scope = func(ctx context.Context, scope runtime.ToolScopeContext) runtime.ToolScope {
			return callback(callbackContext(ctx), scope)
		}
	}
	return next
}

func validateComponentPayload(active []extension.CommitValue[componentPayload], candidate extension.CommitValue[componentPayload]) error {
	payload := candidate.Value()
	for index, tool := range payload.tools {
		for _, other := range payload.tools[index+1:] {
			if other.Definition.Name == tool.Definition.Name && toolScopesOverlap(other.Scope, tool.Scope) {
				return fmt.Errorf("%w: %s", tools.ErrDuplicateRegistration, tool.Definition.Name)
			}
		}
		for _, mounted := range active {
			for _, existing := range mounted.Value().tools {
				if existing.Definition.Name == tool.Definition.Name && toolScopesOverlap(existing.Scope, tool.Scope) {
					return fmt.Errorf("%w: %s", tools.ErrDuplicateRegistration, tool.Definition.Name)
				}
			}
		}
	}
	for _, prompt := range payload.prompts {
		for _, mounted := range active {
			for _, existing := range mounted.Value().prompts {
				if existing.Scope == prompt.Scope && existing.Name == prompt.Name {
					return fmt.Errorf("%w: prompt %s", extension.ErrDuplicateRegistration, prompt.Name)
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

func (m *Mount) Deactivate() {
	if m == nil || m.extension == nil {
		return
	}
	m.extension.Deactivate()
}

func (m *Mount) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}
	return m.extension.Close(ctx)
}

func (r *Registry) AcquireRunPlan(ctx context.Context, request runtime.RunPlanRequest) (*runtime.RunPlan, error) {
	return r.acquire(ctx, request.SessionID, nil, requestedToolSelector(request.Config.Tools))
}

func (r *Registry) AcquireResumePlan(ctx context.Context, request runtime.ResumePlanRequest) (*runtime.RunPlan, error) {
	if request.SessionID == "" {
		return nil, runtime.ErrExtensionPlanMismatch
	}
	persisted := request.Descriptor
	if err := session.ValidateExtensionPlan(persisted); err != nil || persisted.Fingerprint == "" {
		return nil, runtime.ErrExtensionPlanMismatch
	}
	fingerprint, err := session.FingerprintExtensionPlan(persisted)
	if err != nil || fingerprint != persisted.Fingerprint {
		return nil, runtime.ErrExtensionPlanMismatch
	}
	instances := make(map[string]bool)
	toolIdentities := make(map[planToolIdentity]bool)
	for _, component := range persisted.Components {
		instances[component.InstanceID] = true
		for _, registration := range component.Handlers {
			if err := validateResumeScope(request.SessionID, registration.Scope); err != nil {
				return nil, err
			}
		}
		for _, identity := range component.Tools {
			toolIdentities[planToolIdentity{InstanceID: component.InstanceID, Scope: identity.Scope, RegistrationID: identity.RegistrationID, ToolName: identity.Name}] = true
			if err := validateResumeScope(request.SessionID, identity.Scope); err != nil {
				return nil, err
			}
		}
		for _, identity := range component.Prompts {
			if err := validateResumeScope(request.SessionID, identity.Scope); err != nil {
				return nil, err
			}
		}
		for _, identity := range component.Guards {
			if err := validateResumeScope(request.SessionID, identity.Scope); err != nil {
				return nil, err
			}
		}
		for _, identity := range component.Restrictions {
			if err := validateResumeScope(request.SessionID, identity.Scope); err != nil {
				return nil, err
			}
		}
	}
	plan, err := r.acquire(ctx, request.SessionID, instances, persistedToolSelector(toolIdentities))
	if err != nil {
		return nil, err
	}
	if plan.Descriptor().Fingerprint != persisted.Fingerprint {
		plan.Release()
		return nil, runtime.ErrExtensionPlanMismatch
	}
	return plan, nil
}

func validateResumeScope(sessionID session.ID, scope extension.Scope) error {
	if scope.Kind == extension.ScopeSession && scope.Key != string(sessionID) {
		return runtime.ErrExtensionPlanMismatch
	}
	return nil
}

func (r *Registry) acquire(ctx context.Context, sessionID session.ID, instances map[string]bool, selectTool planToolSelector) (*runtime.RunPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	target := extension.GlobalScope()
	if sessionID != "" {
		target = extension.SessionScope(string(sessionID))
	}
	var snapshot *extension.Snapshot[componentPayload]
	var err error
	if instances == nil {
		snapshot, err = r.extensions.Snapshot(target)
	} else {
		ids := make([]string, 0, len(instances))
		for id := range instances {
			ids = append(ids, id)
		}
		snapshot, err = r.extensions.SnapshotInstances(target, ids)
	}
	if err != nil {
		return nil, err
	}
	components := snapshotComponents(snapshot.Values())
	selected := selectTools(components, target, instances, selectTool)
	prompts := selectPrompts(components, target, instances)
	guards := selectGuards(components, target, instances)
	restrictions := selectRestrictions(components, target, instances)
	planTools := make([]runtime.PlanTool, len(selected))
	for index, entry := range selected {
		schemaHash, hashErr := composedToolSchemaHash(entry.ToolRegistration)
		if hashErr != nil {
			snapshot.Release()
			return nil, hashErr
		}
		definition, cloneErr := mountToolDefinition(entry.callbackContext, entry.Definition).Clone()
		if cloneErr != nil {
			snapshot.Release()
			return nil, cloneErr
		}
		planTools[index] = runtime.PlanTool{
			Component: entry.component,
			Identity: session.ToolPlanIdentity{
				Name: entry.Definition.Name, RegistrationID: entry.ID, Scope: entry.Scope,
				SchemaHash: schemaHash, ExecutorHash: entry.Definition.Provenance.ExecutorHash,
			},
			Resolve: func(ctx context.Context, scope runtime.ToolScopeContext) (runtime.Tool, error) {
				return tools.Materialize(ctx, definition, scope)
			},
		}
	}
	planPrompts := make([]runtime.PlanPrompt, len(prompts))
	for index, prompt := range prompts {
		planPrompts[index] = runtime.PlanPrompt{
			Component: prompt.component,
			Identity:  session.PromptPlanIdentity{Name: prompt.Name, RegistrationID: prompt.ID, Scope: prompt.Scope, Order: prompt.Order},
			Prompt: runtime.MountedPrompt{Name: prompt.Name, Order: prompt.Order, InstanceID: prompt.component.InstanceID, Provider: mountedPromptProvider{
				callbackContext: prompt.callbackContext, next: prompt.Provider,
			}},
		}
	}
	planGuards := make([]runtime.PlanGuard, len(guards))
	for index, guard := range guards {
		planGuards[index] = runtime.PlanGuard{
			Component: guard.component,
			Identity:  session.GuardPlanIdentity{RegistrationID: guard.ID, Scope: guard.Scope, Order: guard.Order},
			Guard: runtime.MountedToolGuard{ID: guard.ID, Order: guard.Order, InstanceID: guard.component.InstanceID, Guard: mountedToolGuard{
				callbackContext: guard.callbackContext, next: guard.Guard,
			}},
		}
	}
	planRestrictions := make([]runtime.PlanRestriction, len(restrictions))
	for index, restriction := range restrictions {
		rules, rulesErr := runtime.CanonicalizeRestrictionRules(restriction.Allowed, restriction.Denied)
		if rulesErr != nil {
			snapshot.Release()
			return nil, rulesErr
		}
		planRestrictions[index] = runtime.PlanRestriction{
			Component: restriction.component,
			Identity:  session.RestrictionPlanIdentity{RegistrationID: restriction.ID, Scope: restriction.Scope, RulesHash: rules.Hash},
			Allowed:   rules.Allowed, Denied: rules.Denied,
		}
	}
	return runtime.NewRunPlan(runtime.RunPlanSpec{Dispatch: snapshot.Dispatch(), Tools: planTools, Prompts: planPrompts, Guards: planGuards, Restrictions: planRestrictions})
}

type selectedComponent struct {
	component       extension.Component
	payload         componentPayload
	callbackContext func(context.Context) context.Context
}

func snapshotComponents(values []extension.MountedValue[componentPayload]) []selectedComponent {
	result := make([]selectedComponent, len(values))
	for index, value := range values {
		mounted := value
		result[index] = selectedComponent{
			component: mounted.Component(), payload: mounted.Value(), callbackContext: mounted.CallbackContext,
		}
	}
	return result
}

type selectedTool struct {
	ToolRegistration
	component       extension.Component
	callbackContext func(context.Context) context.Context
}

type selectedPrompt struct {
	PromptRegistration
	component       extension.Component
	callbackContext func(context.Context) context.Context
}

type selectedGuard struct {
	GuardRegistration
	component       extension.Component
	callbackContext func(context.Context) context.Context
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

func selectTools(components []selectedComponent, target extension.Scope, instances map[string]bool, selectTool planToolSelector) []selectedTool {
	var result []selectedTool
	for _, mounted := range components {
		for _, registration := range mounted.payload.tools {
			entry := selectedTool{ToolRegistration: registration, component: mounted.component, callbackContext: mounted.callbackContext}
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

func selectPrompts(components []selectedComponent, target extension.Scope, instances map[string]bool) []selectedPrompt {
	selected := make(map[string]selectedPrompt)
	for _, mounted := range components {
		for _, registration := range mounted.payload.prompts {
			entry := selectedPrompt{PromptRegistration: registration, component: mounted.component, callbackContext: mounted.callbackContext}
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

func selectGuards(components []selectedComponent, target extension.Scope, instances map[string]bool) []selectedGuard {
	var result []selectedGuard
	for _, mounted := range components {
		for _, registration := range mounted.payload.guards {
			entry := selectedGuard{GuardRegistration: registration, component: mounted.component, callbackContext: mounted.callbackContext}
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

func selectRestrictions(components []selectedComponent, target extension.Scope, instances map[string]bool) []selectedRestriction {
	var result []selectedRestriction
	for _, mounted := range components {
		for _, registration := range mounted.payload.restrictions {
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

var _ runtime.RunPlanProvider = (*Registry)(nil)
