package datadogexample

import (
	"context"
	"errors"
	"strings"
	"testing"

	einoobs "github.com/mattsp1290/eino-obs"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/obs"
	"github.com/mattsp1290/eino-agent/runtime"
)

func TestNewObserverFromConfigDefaultsToNoNetwork(t *testing.T) {
	t.Parallel()

	observer, mode, err := NewObserverFromConfig(config.ObservabilityConfig{
		Service: "test-agent",
		Env:     "test",
		Version: "v1",
	}, mapLookup(nil))
	if err != nil {
		t.Fatalf("NewObserverFromConfig error = %v", err)
	}
	if mode != ModeNoNetwork {
		t.Fatalf("mode = %q, want %q", mode, ModeNoNetwork)
	}
	orchestrator := &runtime.StreamingOrchestrator{}
	AttachRuntimeObserver(orchestrator, observer)
	if orchestrator.Observer != observer {
		t.Fatal("observer was not attached to runtime")
	}

	ctx := context.Background()
	session := observer.StartSession(ctx, einoobs.SessionStart{Name: "test session"})
	session.End(einoobs.SessionEnd{})
	if err := observer.Flush(ctx); err != nil {
		t.Fatalf("Flush error = %v", err)
	}
	if got := len(observer.Snapshot().Observations); got == 0 {
		t.Fatal("no-network observer recorded no observations")
	}
}

func TestNewObserverFromConfigValidatesDatadogEnableFlag(t *testing.T) {
	t.Parallel()

	_, _, err := NewObserverFromConfig(config.ObservabilityConfig{}, mapLookup(map[string]string{
		EnvDatadogEnabled: "sometimes",
	}))
	if !errors.Is(err, ErrInvalidEnvironment) {
		t.Fatalf("error = %v, want ErrInvalidEnvironment", err)
	}
}

func TestNewObserverFromConfigRequiresAPIKeyWhenDatadogEnabled(t *testing.T) {
	_, _, err := NewObserverFromConfig(config.ObservabilityConfig{}, mapLookup(map[string]string{
		EnvDatadogEnabled: "true",
		EnvDDService:      "test-agent",
	}))
	if !errors.Is(err, ErrInvalidEnvironment) {
		t.Fatalf("error = %v, want ErrInvalidEnvironment", err)
	}
}

func TestNewObserverFromConfigIgnoresAmbientExporterEnvironment(t *testing.T) {
	t.Setenv(EnvDatadogEndpoint, "not a url")
	t.Setenv("EINO_OBS_EXPORT_TIMEOUT", "not a duration")

	observer, mode, err := NewObserverFromConfig(config.ObservabilityConfig{}, mapLookup(map[string]string{
		EnvDatadogEnabled: "true",
		EnvDDAPIKey:       "dummy-key",
		EnvDDService:      "test-agent",
		EnvDDSite:         "datadoghq.com",
	}))
	if err != nil {
		t.Fatalf("NewObserverFromConfig error = %v", err)
	}
	if mode != ModeDatadog {
		t.Fatalf("mode = %q, want %q", mode, ModeDatadog)
	}
	if observer == nil {
		t.Fatal("observer is nil")
	}
}

func TestSettingsFromConfigDoesNotReadDatadogSecretsWhenDisabled(t *testing.T) {
	t.Parallel()

	lookedUp := map[string]bool{}
	_, err := SettingsFromConfig(config.ObservabilityConfig{}, func(key string) string {
		lookedUp[key] = true
		if key == EnvDatadogEnabled {
			return "false"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("SettingsFromConfig error = %v", err)
	}
	for _, key := range []string{EnvDDAPIKey, EnvDDMLApp, EnvDDSite, EnvDatadogEndpoint} {
		if lookedUp[key] {
			t.Fatalf("disabled mode looked up %s", key)
		}
	}
}

func TestSettingsFromConfigRejectsInvalidSummaryConfig(t *testing.T) {
	t.Parallel()

	_, err := SettingsFromConfig(config.ObservabilityConfig{
		Summary: config.ObservabilityConfig{}.WithDefaults().Summary,
	}, mapLookup(nil))
	if err != nil {
		t.Fatalf("default summary config should be valid: %v", err)
	}
	_, err = SettingsFromConfig(config.ObservabilityConfig{
		Summary: obs.SummaryPolicy{EnabledByDefault: true, MaxBytesDefault: 0},
	}, mapLookup(nil))
	if !errors.Is(err, ErrInvalidEnvironment) {
		t.Fatalf("error = %v, want ErrInvalidEnvironment", err)
	}
}

func TestDatadogConstructionErrorRedactsAPIKey(t *testing.T) {
	t.Parallel()

	secret := "super-secret-token"
	_, _, err := NewObserverFromConfig(config.ObservabilityConfig{}, mapLookup(map[string]string{
		EnvDatadogEnabled: "true",
		EnvDDAPIKey:       secret,
		EnvDDSite:         "not-a-real-site",
	}))
	if !errors.Is(err, ErrInvalidEnvironment) {
		t.Fatalf("error = %v, want ErrInvalidEnvironment", err)
	}
	if err != nil && strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked API key: %v", err)
	}
}

func TestSettingsFromConfigUsesRuntimeObservabilityDefaults(t *testing.T) {
	t.Parallel()

	settings, err := SettingsFromConfig(config.ObservabilityConfig{
		Service: "runtime-service",
		Env:     "staging",
		Version: "abc123",
		Summary: config.ObservabilityConfig{}.WithDefaults().Summary,
	}, mapLookup(map[string]string{
		EnvDDService: "env-service",
		EnvDDMLApp:   "ml-app",
	}))
	if err != nil {
		t.Fatalf("SettingsFromConfig error = %v", err)
	}
	if settings.Service != "runtime-service" || settings.Env != "staging" || settings.Version != "abc123" {
		t.Fatalf("settings identity = %#v", settings)
	}
	if settings.MLApp != "runtime-service" {
		t.Fatalf("MLApp = %q, want runtime-service", settings.MLApp)
	}
	if settings.DatadogEnabled {
		t.Fatal("Datadog export should be disabled by default")
	}
}

func mapLookup(values map[string]string) LookupEnv {
	return func(key string) string {
		return values[key]
	}
}
