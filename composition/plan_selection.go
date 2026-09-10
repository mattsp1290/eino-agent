package composition

import (
	"context"
	"fmt"
	"sort"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/tools"
)

type selectedComponent struct {
	component       extension.Component
	payload         componentPayload
	callbackContext func(context.Context) context.Context
}

type promptOwner struct {
	instanceID, registrationID string
	scope                      extension.Scope
}

type planSelection struct {
	target        extension.Scope
	selectTool    planToolSelector
	componentsSet []selectedComponent
	promptWinners map[string]promptOwner
}

func (r *Registry) snapshotForPlan(target extension.Scope, instances map[string]bool) (*extension.Snapshot[componentPayload], error) {
	if instances == nil {
		return r.extensions.Snapshot(target)
	}
	ids := make([]string, 0, len(instances))
	for id := range instances {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return r.extensions.SnapshotInstances(target, ids)
}

func newPlanSelection(target extension.Scope, selectTool planToolSelector, values []extension.MountedValue[componentPayload]) planSelection {
	selection := planSelection{
		target:        target,
		selectTool:    selectTool,
		componentsSet: make([]selectedComponent, len(values)),
		promptWinners: make(map[string]promptOwner),
	}
	for index, value := range values {
		mounted := value
		selection.componentsSet[index] = selectedComponent{
			component: mounted.Component(), payload: mounted.Value(), callbackContext: mounted.CallbackContext,
		}
	}
	for _, mounted := range selection.componentsSet {
		for _, registration := range mounted.payload.prompts {
			if !extension.ScopeApplies(registration.Scope, target) {
				continue
			}
			current, exists := selection.promptWinners[registration.Name]
			if !exists || current.scope.Kind == extension.ScopeGlobal && registration.Scope.Kind == extension.ScopeSession {
				selection.promptWinners[registration.Name] = promptOwner{
					instanceID: mounted.component.InstanceID, registrationID: registration.ID, scope: registration.Scope,
				}
			}
		}
	}
	return selection
}

// toolSearchConfig returns the single active tool-search configuration
// across every selected component, or nil if none is registered. More than
// one active registration (even across different components) is rejected
// here, before runtime.NewRunPlan ever sees the spec.
func (s planSelection) toolSearchConfig() (*runtime.ToolSearchConfig, error) {
	var found *runtime.ToolSearchConfig
	for _, mounted := range s.componentsSet {
		for _, registration := range mounted.payload.toolSearch {
			if !extension.ScopeApplies(registration.Scope, s.target) {
				continue
			}
			if found != nil {
				return nil, fmt.Errorf("%w: more than one tool search registration is active", runtime.ErrExtensionPlanMismatch)
			}
			found = &runtime.ToolSearchConfig{Name: registration.Name, Description: registration.Description}
		}
	}
	return found, nil
}

func (s planSelection) components() []runtime.PlanComponent {
	result := make([]runtime.PlanComponent, 0, len(s.componentsSet))
	for _, mounted := range s.componentsSet {
		owned := runtime.PlanComponent{Component: mounted.component}
		for _, registration := range mounted.payload.tools {
			if !extension.ScopeApplies(registration.Scope, s.target) || s.selectTool != nil && !s.selectTool(mounted.component, registration) {
				continue
			}
			definition := mountToolDefinition(mounted.callbackContext, registration.Definition)
			owned.Tools = append(owned.Tools, runtime.PlanTool{
				Name: registration.Definition.Name, RegistrationID: registration.ID, Scope: registration.Scope,
				SchemaHash: registration.schemaHash, ExecutorHash: registration.executorHash, Order: registration.Order,
				Aliases: registration.Definition.Aliases, ArgumentAliases: registration.Definition.ArgumentAliases, Deferred: registration.Definition.Deferred,
				Resolve: func(ctx context.Context, scope runtime.ToolScopeContext) (runtime.Tool, error) {
					return tools.Materialize(ctx, definition, scope)
				},
			})
		}
		for _, registration := range mounted.payload.prompts {
			winner, ok := s.promptWinners[registration.Name]
			if !ok || winner != (promptOwner{instanceID: mounted.component.InstanceID, registrationID: registration.ID, scope: registration.Scope}) {
				continue
			}
			owned.Prompts = append(owned.Prompts, runtime.PlanPrompt{
				Name: registration.Name, RegistrationID: registration.ID, Scope: registration.Scope, Order: registration.Order,
				Provider: mountedPromptProvider{callbackContext: mounted.callbackContext, next: registration.Provider},
			})
		}
		for _, registration := range mounted.payload.guards {
			if extension.ScopeApplies(registration.Scope, s.target) {
				owned.Guards = append(owned.Guards, runtime.PlanGuard{
					RegistrationID: registration.ID, Scope: registration.Scope, Order: registration.Order,
					Guard: mountedToolGuard{callbackContext: mounted.callbackContext, next: registration.Guard},
				})
			}
		}
		for _, registration := range mounted.payload.restrictions {
			if extension.ScopeApplies(registration.Scope, s.target) {
				owned.Restrictions = append(owned.Restrictions, runtime.PlanRestriction{
					RegistrationID: registration.ID, Scope: registration.Scope,
					Allowed: registration.Allowed, Denied: registration.Denied,
				})
			}
		}
		if len(owned.Tools)+len(owned.Prompts)+len(owned.Guards)+len(owned.Restrictions) > 0 {
			result = append(result, owned)
		}
	}
	return result
}
