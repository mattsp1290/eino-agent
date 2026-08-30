package composition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

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
	ID             string
	Order          int
	Scope          extension.Scope
	SourceIdentity ToolSourceIdentity
	Definition     tools.Definition
	schemaHash     string
	executorHash   string
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
	if err := registration.SourceIdentity.validate(); err != nil {
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
	registration.schemaHash, err = composedToolSchemaHash(registration)
	if err != nil {
		return fmt.Errorf("hash composed tool %q: %w", registration.Definition.Name, err)
	}
	registration.executorHash = composedToolExecutorHash(registration.SourceIdentity.executorHash, r.component.Artifact.Hash)
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

func NewRegistry(reporter extension.Reporter, customPoints ...extension.Point) (*Registry, error) {
	catalog := append(runtime.ExtensionPoints(), customPoints...)
	extensions, err := extension.NewRegistry[componentPayload](reporter, catalog...)
	if err != nil {
		return nil, err
	}
	return &Registry{extensions: extensions}, nil
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
	if request.SessionID == "" || request.Plan.Fingerprint() == "" {
		return nil, runtime.ErrExtensionPlanMismatch
	}
	persisted := request.Plan.Descriptor()
	instances := make(map[string]bool)
	toolIdentities := make(map[planToolIdentity]bool)
	for _, component := range persisted.Components {
		instances[component.InstanceID] = true
		for _, identity := range component.Tools {
			toolIdentities[planToolIdentity{InstanceID: component.InstanceID, Scope: identity.Scope, RegistrationID: identity.RegistrationID, ToolName: identity.Name}] = true
		}
	}
	return r.acquire(ctx, request.SessionID, instances, persistedToolSelector(toolIdentities))
}

func (r *Registry) acquire(ctx context.Context, sessionID session.ID, instances map[string]bool, selectTool planToolSelector) (*runtime.RunPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	target := extension.GlobalScope()
	if sessionID != "" {
		target = extension.SessionScope(string(sessionID))
	}
	snapshot, err := r.snapshotForPlan(target, instances)
	if err != nil {
		return nil, err
	}
	selection := newPlanSelection(target, selectTool, snapshot.Values())
	return runtime.NewRunPlan(runtime.RunPlanSpec{SessionID: sessionID, Dispatch: snapshot.Dispatch(), Components: selection.components()})
}

type planToolSelector func(extension.Component, ToolRegistration) bool

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
	return func(_ extension.Component, entry ToolRegistration) bool {
		if disabled[entry.Definition.Name] {
			return false
		}
		return toolConfig.Enabled == nil || enabled[entry.Definition.Name]
	}
}

func persistedToolSelector(identities map[planToolIdentity]bool) planToolSelector {
	return func(component extension.Component, entry ToolRegistration) bool {
		return identities[planToolIdentity{
			InstanceID: component.InstanceID, Scope: entry.Scope, RegistrationID: entry.ID, ToolName: entry.Definition.Name,
		}]
	}
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
