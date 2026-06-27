package config

import (
	"context"
	"testing"

	"github.com/mattsp1290/eino-agent/catalog"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/obs"
)

func TestValidateRequiresDefaultAgent(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.DefaultAgent = ""

	err := cfg.Validate(context.Background(), nil)
	if !HasValidationCode(err, ValidationMissingDefaultAgent) {
		t.Fatalf("Validate error = %v, want %s", err, ValidationMissingDefaultAgent)
	}
}

func TestValidateRejectsUnknownDefaultAgent(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.DefaultAgent = "missing"

	err := cfg.Validate(context.Background(), nil)
	if !HasValidationCode(err, ValidationUnknownAgent) {
		t.Fatalf("Validate error = %v, want %s", err, ValidationUnknownAgent)
	}
}

func TestSnapshotForAgentRejectsUnknownAgent(t *testing.T) {
	t.Parallel()

	cfg := validConfig()

	_, err := cfg.SnapshotForAgent(context.Background(), "missing", nil)
	if !HasValidationCode(err, ValidationUnknownAgent) {
		t.Fatalf("SnapshotForAgent error = %v, want %s", err, ValidationUnknownAgent)
	}
}

func TestValidateRejectsUnknownModel(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Agents["default"] = Agent{
		Name: "default",
		Model: model.Selection{
			ProviderID: "openai",
			ModelID:    "missing",
		},
	}

	err := cfg.Validate(context.Background(), nil)
	if !HasValidationCode(err, ValidationUnknownModel) {
		t.Fatalf("Validate error = %v, want %s", err, ValidationUnknownModel)
	}
}

func TestValidateUsesCatalogAsAuthoritativeModelSource(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Models = []model.Selection{{
		ProviderID: "openai",
		ModelID:    "not-in-catalog",
	}}
	cfg.Agents["default"] = Agent{
		Name: "default",
		Model: model.Selection{
			ProviderID: "openai",
			ModelID:    "not-in-catalog",
		},
	}

	err := cfg.Validate(context.Background(), staticCatalog())
	if !HasValidationCode(err, ValidationUnknownModel) {
		t.Fatalf("Validate error = %v, want %s", err, ValidationUnknownModel)
	}
}

func TestValidateDistinguishesModelVariant(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Agents["default"] = Agent{
		Name: "default",
		Model: model.Selection{
			ProviderID: "openai",
			ModelID:    "gpt-4.1",
			Variant:    "large-context",
		},
	}

	err := cfg.Validate(context.Background(), nil)
	if !HasValidationCode(err, ValidationUnknownModel) {
		t.Fatalf("Validate error = %v, want %s", err, ValidationUnknownModel)
	}
}

func TestToolPermissionDefaultsToAsk(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Tools.Permissions = []PermissionRule{{
		Permission: "shell",
		Pattern:    "go test ./...",
	}}

	snapshot, err := cfg.SnapshotForAgent(context.Background(), "", nil)
	if err != nil {
		t.Fatalf("SnapshotForAgent error = %v", err)
	}
	if got := snapshot.Tools.Permissions[0].Action; got != PermissionActionAsk {
		t.Fatalf("permission action = %q, want %q", got, PermissionActionAsk)
	}
	if cfg.Tools.Permissions[0].Action != "" {
		t.Fatalf("SnapshotForAgent mutated original permission action to %q", cfg.Tools.Permissions[0].Action)
	}
}

func TestObservabilityRedactionDefaultsAreSafe(t *testing.T) {
	t.Parallel()

	cfg := validConfig()

	snapshot, err := cfg.SnapshotForAgent(context.Background(), "", nil)
	if err != nil {
		t.Fatalf("SnapshotForAgent error = %v", err)
	}
	if snapshot.Observability.Summary.EnabledByDefault {
		t.Fatal("summary export should be disabled by default")
	}
	if snapshot.Observability.Summary.MaxBytesDefault != 0 {
		t.Fatalf("summary max bytes = %d, want 0", snapshot.Observability.Summary.MaxBytesDefault)
	}
	fields := fieldsByName(snapshot.Observability.Fields)
	for _, name := range []string{"raw_prompt", "raw_output", "raw_tool_payload", "reasoning", "api_keys"} {
		if fields[name].Class != obs.FieldForbidden {
			t.Fatalf("%s class = %q, want %q", name, fields[name].Class, obs.FieldForbidden)
		}
	}
}

func TestObservabilityPartialSummaryKeepsDefaultRedactionGuardrails(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Observability.Summary.EnabledByDefault = true
	cfg.Observability.Summary.MaxBytesDefault = 256

	snapshot, err := cfg.SnapshotForAgent(context.Background(), "", nil)
	if err != nil {
		t.Fatalf("SnapshotForAgent error = %v", err)
	}
	for _, name := range []string{"raw_prompt", "raw_output", "raw_tool_payload", "reasoning", "api_keys"} {
		if !contains(snapshot.Observability.Summary.ForbiddenInputs, name) {
			t.Fatalf("summary forbidden inputs missing %q: %#v", name, snapshot.Observability.Summary.ForbiddenInputs)
		}
	}
	if len(snapshot.Observability.Summary.AllowedKinds) == 0 {
		t.Fatal("summary allowed kinds should be defaulted")
	}
}

func TestValidateAcceptsCatalogModel(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Models = nil
	cat := staticCatalog()

	err := cfg.Validate(context.Background(), cat)
	if err != nil {
		t.Fatalf("Validate error = %v", err)
	}
}

func validConfig() Config {
	selection := model.Selection{
		ProviderID: "openai",
		ModelID:    "gpt-4.1",
	}
	return Config{
		DefaultAgent: "default",
		Agents: map[string]Agent{
			"default": {
				Name:  "default",
				Model: selection,
			},
		},
		Models: []model.Selection{selection},
	}
}

func staticCatalog() model.Catalog {
	return catalog.Static{
		Models: []model.Descriptor{{
			ID:         "gpt-4.1",
			ProviderID: "openai",
			Name:       "GPT-4.1",
		}},
	}
}

func fieldsByName(fields []obs.FieldPolicy) map[string]obs.FieldPolicy {
	result := make(map[string]obs.FieldPolicy, len(fields))
	for _, field := range fields {
		result[field.Name] = field
	}
	return result
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
