// Package datadogexample shows environment-driven eino-obs wiring for
// eino-agent runtimes. It is safe by default: without an explicit enable flag,
// observers use no-network recording and require no Datadog credentials.
package datadogexample

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	einoobs "github.com/mattsp1290/eino-obs"
	"github.com/mattsp1290/eino-obs/exporter/datadog"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/runtime"
)

const (
	EnvDatadogEnabled = "EINO_AGENT_DATADOG_ENABLED"
	EnvDDAPIKey       = "DD_API_KEY"
	EnvDDService      = "DD_SERVICE"
	EnvDDEnv          = "DD_ENV"
	EnvDDVersion      = "DD_VERSION"
	EnvDDMLApp        = "DD_LLMOBS_ML_APP"
	EnvDDSite         = "DD_SITE"
)

type Mode string

const (
	ModeNoNetwork Mode = "no-network"
	ModeDatadog   Mode = "datadog"
)

var ErrInvalidEnvironment = errors.New("invalid Datadog observability environment")

type LookupEnv func(string) string

// Settings is the no-secret runtime/exporter configuration used by this
// example. APIKey is intentionally not logged or rendered by docs/tests.
type Settings struct {
	Service              string
	Env                  string
	Version              string
	MLApp                string
	Site                 string
	APIKey               string
	DatadogEnabled       bool
	CaptureInputSummary  bool
	CaptureOutputSummary bool
	MaxSummaryBytes      int
}

// SettingsFromConfig combines runtime observability config with environment
// variables. The config snapshot owns service/env/version and redaction policy;
// environment variables own Datadog transport selection and credentials.
func SettingsFromConfig(observability config.ObservabilityConfig, lookup LookupEnv) (Settings, error) {
	if lookup == nil {
		lookup = os.Getenv
	}
	enabled, err := boolEnv(lookup(EnvDatadogEnabled))
	if err != nil {
		return Settings{}, fmt.Errorf("%w: %s must be true or false", ErrInvalidEnvironment, EnvDatadogEnabled)
	}
	service := first(observability.Service, lookup(EnvDDService), "eino-agent")
	env := first(observability.Env, lookup(EnvDDEnv), "local")
	version := first(observability.Version, lookup(EnvDDVersion))
	return Settings{
		Service:              service,
		Env:                  env,
		Version:              version,
		MLApp:                first(lookup(EnvDDMLApp), service),
		Site:                 first(lookup(EnvDDSite), "datadoghq.com"),
		APIKey:               lookup(EnvDDAPIKey),
		DatadogEnabled:       enabled,
		CaptureInputSummary:  observability.Summary.EnabledByDefault,
		CaptureOutputSummary: observability.Summary.EnabledByDefault,
		MaxSummaryBytes:      observability.Summary.MaxBytesDefault,
	}, nil
}

// NewObserverFromConfig returns an eino-obs observer and the selected export
// mode. Datadog export is opt-in through EINO_AGENT_DATADOG_ENABLED=true.
func NewObserverFromConfig(observability config.ObservabilityConfig, lookup LookupEnv) (*einoobs.Observer, Mode, error) {
	settings, err := SettingsFromConfig(observability, lookup)
	if err != nil {
		return nil, "", err
	}
	redaction := einoobs.RedactionOptions{
		CaptureInputSummary:  settings.CaptureInputSummary,
		CaptureOutputSummary: settings.CaptureOutputSummary,
		MaxSummaryBytes:      settings.MaxSummaryBytes,
	}
	observerConfig := einoobs.Config{
		Service:   settings.Service,
		Env:       settings.Env,
		Version:   settings.Version,
		Redaction: redaction,
	}
	if !settings.DatadogEnabled {
		return einoobs.New(observerConfig, einoobs.WithNoNetwork()), ModeNoNetwork, nil
	}
	exporter, err := datadog.New(datadog.Config{
		APIKey:  settings.APIKey,
		Site:    settings.Site,
		MLApp:   settings.MLApp,
		Service: settings.Service,
		Env:     settings.Env,
		Version: settings.Version,
	})
	if err != nil {
		return nil, "", fmt.Errorf("%w: create Datadog exporter: %v", ErrInvalidEnvironment, err)
	}
	observerConfig.Exporter = exporter
	return einoobs.New(observerConfig), ModeDatadog, nil
}

// AttachRuntimeObserver wires the configured observer into the runtime.
func AttachRuntimeObserver(orchestrator *runtime.StreamingOrchestrator, observer *einoobs.Observer) {
	if orchestrator == nil {
		return
	}
	orchestrator.Observer = observer
}

func boolEnv(raw string) (bool, error) {
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, err
	}
	return value, nil
}

func first(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
