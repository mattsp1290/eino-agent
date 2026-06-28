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
	"time"

	einoobs "github.com/mattsp1290/eino-obs"
	"github.com/mattsp1290/eino-obs/exporter/datadog"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/runtime"
)

const (
	EnvDatadogEnabled  = "EINO_AGENT_DATADOG_ENABLED"
	EnvDDAPIKey        = "DD_API_KEY"
	EnvDDService       = "DD_SERVICE"
	EnvDDEnv           = "DD_ENV"
	EnvDDVersion       = "DD_VERSION"
	EnvDDMLApp         = "DD_LLMOBS_ML_APP"
	EnvDDSite          = "DD_SITE"
	EnvDatadogEndpoint = "EINO_OBS_DATADOG_ENDPOINT"
)

type Mode string

const (
	ModeNoNetwork Mode = "no-network"
	ModeDatadog   Mode = "datadog"
)

var ErrInvalidEnvironment = errors.New("invalid Datadog observability environment")

type LookupEnv func(string) string

// Settings is the no-secret runtime/exporter configuration used by this
// example. Datadog API keys are read only while creating an enabled exporter
// and are intentionally not stored on this returned settings value.
type Settings struct {
	Service              string
	Env                  string
	Version              string
	MLApp                string
	Site                 string
	Endpoint             string
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
	if observability.Summary.EnabledByDefault && observability.Summary.MaxBytesDefault <= 0 {
		return Settings{}, fmt.Errorf("%w: observability summary max bytes must be positive when summaries are enabled", ErrInvalidEnvironment)
	}
	enabled, err := boolEnv(lookup(EnvDatadogEnabled))
	if err != nil {
		return Settings{}, fmt.Errorf("%w: %s must be true or false", ErrInvalidEnvironment, EnvDatadogEnabled)
	}
	service := first(observability.Service, lookup(EnvDDService), "eino-agent")
	env := first(observability.Env, lookup(EnvDDEnv), "local")
	version := first(observability.Version, lookup(EnvDDVersion))
	mlApp := service
	site := "datadoghq.com"
	endpoint := ""
	if enabled {
		mlApp = first(lookup(EnvDDMLApp), service)
		site = first(lookup(EnvDDSite), "datadoghq.com")
		endpoint = first(lookup(EnvDatadogEndpoint), endpointForSite(site))
	}
	return Settings{
		Service:              service,
		Env:                  env,
		Version:              version,
		MLApp:                mlApp,
		Site:                 site,
		Endpoint:             endpoint,
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
	apiKey := ""
	if lookup != nil {
		apiKey = lookup(EnvDDAPIKey)
	} else {
		apiKey = os.Getenv(EnvDDAPIKey)
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, "", fmt.Errorf("%w: missing %s", ErrInvalidEnvironment, EnvDDAPIKey)
	}
	exporter, err := datadog.New(datadog.Config{
		APIKey:                      apiKey,
		Site:                        settings.Site,
		Endpoint:                    settings.Endpoint,
		MLApp:                       settings.MLApp,
		Service:                     settings.Service,
		Env:                         settings.Env,
		Version:                     settings.Version,
		TimeoutOverride:             datadog.Duration(10 * time.Second),
		BatchSizeOverride:           datadog.Int(100),
		MaxPayloadBytesOverride:     datadog.Int(1024 * 1024),
		MaxRetriesOverride:          datadog.Int(3),
		RetryBaseDelayOverride:      datadog.Duration(200 * time.Millisecond),
		RetryMaxDelayOverride:       datadog.Duration(5 * time.Second),
		ValidateCredentialsOverride: datadog.Bool(false),
		DisableCompressionOverride:  datadog.Bool(false),
	})
	if err != nil {
		return nil, "", fmt.Errorf("%w: create Datadog exporter: %s", ErrInvalidEnvironment, scrubSecret(err.Error(), apiKey))
	}
	observerConfig.Exporter = exporter
	return einoobs.New(observerConfig), ModeDatadog, nil
}

func endpointForSite(site string) string {
	switch strings.TrimSpace(site) {
	case "datadoghq.com":
		return "https://api.datadoghq.com"
	case "us3.datadoghq.com":
		return "https://api.us3.datadoghq.com"
	case "us5.datadoghq.com":
		return "https://api.us5.datadoghq.com"
	case "datadoghq.eu":
		return "https://api.datadoghq.eu"
	case "ap1.datadoghq.com":
		return "https://api.ap1.datadoghq.com"
	case "ap2.datadoghq.com":
		return "https://api.ap2.datadoghq.com"
	case "ddog-gov.com":
		return "https://api.ddog-gov.com"
	default:
		return ""
	}
}

func scrubSecret(message, secret string) string {
	if secret == "" {
		return message
	}
	return strings.ReplaceAll(message, secret, "[redacted]")
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
