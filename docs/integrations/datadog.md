# Datadog Exporter Wiring

`eino-agent` records runtime observations through `github.com/mattsp1290/eino-obs`. The runtime takes an `*einoobs.Observer` on `runtime.StreamingOrchestrator.Observer`; applications decide whether that observer stays no-network or exports to Datadog.

The buildable example in `examples/datadog` is safe by default:

- no Datadog exporter is created unless `EINO_AGENT_DATADOG_ENABLED=true`;
- tests use the no-network recorder and never require live credentials;
- `DD_API_KEY` is read from the environment only when export is enabled;
- docs and errors do not print token values.

## Runtime Wiring

```go
observer, mode, err := datadogexample.NewObserverFromConfig(snapshot.Observability, os.Getenv)
if err != nil {
	return err
}
datadogexample.AttachRuntimeObserver(orchestrator, observer)
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

Then create the observer through `NewObserverFromConfig`. The underlying exporter also honors the `eino-obs` Datadog environment variables such as `EINO_OBS_EXPORT_TIMEOUT`, `EINO_OBS_EXPORT_BATCH_SIZE`, and `EINO_OBS_EXPORT_MAX_RETRIES`.

Call `observer.Flush(ctx)` when you need pending observations delivered, and `observer.Shutdown(ctx)` during process shutdown.

## Redaction

`config.ObservabilityConfig` controls service identity and summary policy. Safe defaults keep raw prompts, model outputs, tool payloads, attachments, reasoning, headers, cookies, tokens, and API keys out of exported attributes. Bounded summaries are opt-in through the runtime config snapshot.
