# Datadog Exporter Wiring

`eino-agent` records runtime observations through `github.com/mattsp1290/eino-obs`. Applications pass an `*einoobs.Observer` through `runtime.WithObserver`; applications decide whether that observer stays no-network or exports to Datadog.

The buildable example in `examples/datadog` is safe by default:

- no Datadog exporter is created unless `EINO_AGENT_DATADOG_ENABLED=true`;
- tests use the no-network recorder and never require live credentials;
- `DD_API_KEY` is read from the environment only when export is enabled;
- Datadog transport-only variables such as `DD_LLMOBS_ML_APP`, `DD_SITE`, and
  `EINO_OBS_DATADOG_ENDPOINT` are not read in no-network mode;
- docs and errors do not print token values.

## Runtime Wiring

```go
observer, mode, err := datadogexample.NewObserverFromConfig(snapshot.Observability, os.Getenv)
if err != nil {
	return err
}
orchestrator, err := runtime.NewStreamingOrchestrator(
    runtime.WithStore(store),
    runtime.WithModelResolver(resolver),
    runtime.WithIDGenerator(ids),
    runtime.WithObserver(observer),
)
if err != nil {
    return err
}
_ = mode // "no-network" unless Datadog export is explicitly enabled.
```

The observer will receive session, run, stream, retry, cancellation, interrupt, resume, and tool lifecycle observations emitted by `runtime.StreamingOrchestrator`.

## Local No-Network Mode

No environment is required:

```bash
go test ./examples/datadog
```

With `EINO_AGENT_DATADOG_ENABLED` unset or false, `eino-obs` stores observations in process. Use `observer.Snapshot()` in tests or local tools.

## Datadog Export Mode

Set credentials in the process environment, not in source files:

```bash
export EINO_AGENT_DATADOG_ENABLED=true
export DD_API_KEY='<from your secret manager>'
export DD_SERVICE=eino-agent
export DD_ENV=prod
export DD_VERSION="$(git rev-parse --short HEAD)"
export DD_LLMOBS_ML_APP=eino-agent
# Optional: export DD_SITE=datadoghq.com
```

Then create the observer through `NewObserverFromConfig`. This wrapper keeps the injected environment authoritative: it passes explicit exporter defaults so unrelated ambient `EINO_OBS_*` variables in a developer shell or CI job do not change construction. Customize exporter retry, timeout, or endpoint behavior in code if a host needs settings beyond this no-secret example.

Call `observer.Flush(ctx)` when you need pending observations delivered, and `observer.Shutdown(ctx)` during process shutdown.

## Redaction

`config.ObservabilityConfig` controls service identity and summary policy. Safe defaults keep raw prompts, model outputs, tool payloads, attachments, reasoning, headers, cookies, tokens, and API keys out of exported attributes. Bounded summaries are opt-in through the runtime config snapshot.
