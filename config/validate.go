package config

import (
	"context"
	"errors"
	"fmt"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/obs"
)

// ValidationCode identifies a stable validation failure class.
type ValidationCode string

const (
	ValidationMissingDefaultAgent  ValidationCode = "missing_default_agent"
	ValidationUnknownAgent         ValidationCode = "unknown_agent"
	ValidationMissingModel         ValidationCode = "missing_model"
	ValidationUnknownModel         ValidationCode = "unknown_model"
	ValidationInvalidPermission    ValidationCode = "invalid_permission"
	ValidationInvalidObservability ValidationCode = "invalid_observability"
)

// ValidationError is returned when a config document cannot produce a safe
// run snapshot.
type ValidationError struct {
	Code    ValidationCode
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s %s: %s", e.Code, e.Field, e.Message)
}

// HasValidationCode reports whether err contains a ValidationError with code.
func HasValidationCode(err error, code ValidationCode) bool {
	var validationErr ValidationError
	if !errors.As(err, &validationErr) {
		return false
	}
	return validationErr.Code == code
}

// WithDefaults returns a normalized config without mutating the receiver.
func (c Config) WithDefaults() Config {
	next := c
	next.Agents = make(map[string]Agent, len(c.Agents))
	for name, agent := range c.Agents {
		agent.Options = cloneMap(agent.Options)
		if agent.Name == "" {
			agent.Name = name
		}
		next.Agents[name] = agent
	}
	next.Models = cloneSlice(c.Models)
	next.Tools = c.Tools.WithDefaults()
	next.Observability = c.Observability.WithDefaults()
	next.Metadata = cloneMap(c.Metadata)
	return next
}

// WithDefaults returns a tool config with explicit permission actions.
func (c ToolConfig) WithDefaults() ToolConfig {
	next := c.Clone()
	for i := range next.Permissions {
		if next.Permissions[i].Action == "" {
			next.Permissions[i].Action = PermissionActionAsk
		}
	}
	return next
}

// WithDefaults returns exporter-safe observability defaults.
func (c ObservabilityConfig) WithDefaults() ObservabilityConfig {
	next := c.Clone()
	defaultSummary := obs.DefaultSummaryPolicy()
	if next.Fields == nil {
		next.Fields = obs.DefaultFields()
	}
	if next.Correlation == nil {
		next.Correlation = obs.DefaultCorrelationFields()
	}
	if isZeroSummaryPolicy(next.Summary) {
		next.Summary = defaultSummary
		return next
	}
	if next.Summary.AllowedKinds == nil {
		next.Summary.AllowedKinds = defaultSummary.AllowedKinds
	}
	if next.Summary.ForbiddenInputs == nil {
		next.Summary.ForbiddenInputs = defaultSummary.ForbiddenInputs
	}
	return next
}

// Validate checks whether the config can produce snapshots for its agents. If
// catalog is nil, validation checks configured model selections only.
func (c Config) Validate(ctx context.Context, catalog model.Catalog) error {
	next := c.WithDefaults()
	if next.DefaultAgent == "" {
		return validationError(ValidationMissingDefaultAgent, "default_agent", "must name the default agent")
	}
	if _, ok := next.Agents[next.DefaultAgent]; !ok {
		return validationError(ValidationUnknownAgent, "default_agent", next.DefaultAgent)
	}
	for name, agent := range next.Agents {
		if err := validateAgentModel(ctx, agent, next.Models, catalog); err != nil {
			return fmt.Errorf("agent %q: %w", name, err)
		}
	}
	if err := validatePermissions(next.Tools); err != nil {
		return err
	}
	if err := validateObservability(next.Observability); err != nil {
		return err
	}
	return nil
}

// SnapshotForAgent validates the config and returns one immutable run snapshot.
func (c Config) SnapshotForAgent(ctx context.Context, agentName string, catalog model.Catalog) (Snapshot, error) {
	next := c.WithDefaults()
	if err := next.Validate(ctx, catalog); err != nil {
		return Snapshot{}, err
	}
	if agentName == "" {
		agentName = next.DefaultAgent
	}
	agent, ok := next.Agents[agentName]
	if !ok {
		return Snapshot{}, validationError(ValidationUnknownAgent, "agent", agentName)
	}
	return Snapshot{
		Agent:         agent,
		Model:         agent.Model,
		Tools:         next.Tools,
		Observability: next.Observability,
		Metadata:      next.Metadata,
	}.Clone(), nil
}

func validateAgentModel(ctx context.Context, agent Agent, selections []model.Selection, catalog model.Catalog) error {
	if agent.Model.ProviderID == "" || agent.Model.ModelID == "" {
		return validationError(ValidationMissingModel, "model", "must set provider and model")
	}
	if catalog != nil {
		if _, err := catalog.GetModel(ctx, agent.Model.ProviderID, agent.Model.ModelID); err != nil {
			return fmt.Errorf("%w: %w", validationError(ValidationUnknownModel, "model", formatSelection(agent.Model)), err)
		}
		return nil
	}
	if !selectionConfigured(agent.Model, selections) {
		return validationError(ValidationUnknownModel, "model", formatSelection(agent.Model))
	}
	return nil
}

func validatePermissions(tools ToolConfig) error {
	for _, rule := range tools.Permissions {
		switch rule.Action {
		case PermissionActionAsk, PermissionActionAllow, PermissionActionDeny:
		default:
			return validationError(ValidationInvalidPermission, "tools.permissions.action", rule.Action)
		}
	}
	return nil
}

func validateObservability(config ObservabilityConfig) error {
	if config.Summary.EnabledByDefault && config.Summary.MaxBytesDefault <= 0 {
		return validationError(ValidationInvalidObservability, "observability.summary.max_bytes_default", "must be positive when summaries are enabled")
	}
	for _, field := range config.Fields {
		switch field.Class {
		case obs.FieldAllowed, obs.FieldSummaryOnly, obs.FieldForbidden:
		default:
			return validationError(ValidationInvalidObservability, "observability.fields.class", string(field.Class))
		}
	}
	return nil
}

func selectionConfigured(selection model.Selection, selections []model.Selection) bool {
	for _, candidate := range selections {
		if candidate.ProviderID == selection.ProviderID &&
			candidate.ModelID == selection.ModelID &&
			candidate.Variant == selection.Variant {
			return true
		}
	}
	return false
}

func isZeroSummaryPolicy(policy obs.SummaryPolicy) bool {
	return !policy.EnabledByDefault &&
		policy.MaxBytesDefault == 0 &&
		len(policy.AllowedKinds) == 0 &&
		len(policy.ForbiddenInputs) == 0
}

func formatSelection(selection model.Selection) string {
	return string(selection.ProviderID) + "/" + string(selection.ModelID)
}

func validationError(code ValidationCode, field string, message string) ValidationError {
	return ValidationError{
		Code:    code,
		Field:   field,
		Message: message,
	}
}
