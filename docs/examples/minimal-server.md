# Minimal Embedded AG-UI Server

This example shows the runtime embedded behind the repository HTTP/SSE adapter contracts. It uses local SQLite storage, admits one session, runs a deterministic streaming model, fans live runtime events through AG-UI SSE, persists durable lifecycle events, and replays history after reconnect.

Run it from the repository root:

```bash
go run ./examples/minimal-server -addr :8080 -db ./minimal-server.db
```

Start a run:

```bash
curl -sS -X POST http://localhost:8080/sessions/minimal/runs \
  -H 'Content-Type: application/json' \
  -d '{"message":"hello from curl"}'
```

Open or reconnect the AG-UI event stream, passing the returned `run_id`:

```bash
curl -N 'http://localhost:8080/sessions/minimal/events?run_id=<run-id>'
```

The SSE stream emits live AG-UI events such as `RUN_STARTED`, `TEXT_MESSAGE_CONTENT`, and `RUN_FINISHED`. Reopening the same stream later emits a `MESSAGES_SNAPSHOT` plus durable lifecycle events from the SQLite store.

Interrupt an active run:

```bash
curl -i -X POST 'http://localhost:8080/runs/<run-id>/interrupt?reason=user'
```

Resume an interrupted run boundary:

```bash
curl -i -X POST http://localhost:8080/runs/<run-id>/resume
```
