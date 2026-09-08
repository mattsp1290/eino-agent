# Minimal Embedded AG-UI Server

This credential-free example uses SQLite, a same-process watch service, a
composition.Registry-mounted native echo tool and a deterministic streaming
model. It demonstrates public admission, tool execution and observation.

```bash
go run ./examples/minimal-server -addr :8080 -db ./minimal-server.db
curl -N 'http://localhost:8080/sessions/minimal/events'
```

Attach before or during a run. In another terminal:

```bash
curl -sS -X POST http://localhost:8080/sessions/minimal/runs \
  -H 'Content-Type: application/json' -d '{"message":"hello from curl"}'
```

Submit only new user text. Runtime loads prior durable history. The script
emits a partial assistant response, invokes its local echo tool, and emits a
final response. Tools perform no network or filesystem operation.

The events route emits MESSAGES_SNAPSHOT and STATE_SNAPSHOT replacements,
preserving durable message IDs. The state frame includes run and tool status,
watermark, omitted-history flag and live availability. Tool arguments/results
and reasoning are excluded. Reconnect starts with a fresh bounded current
snapshot; run_id, after and Last-Event-ID are not watch resume tokens.
Intermediate transactions and live prefixes may coalesce.

```bash
curl -i -X POST 'http://localhost:8080/runs/<run-id>/interrupt?reason=user'
```

Closing the SSE connection leaves the run executing. Server.Close interrupts
and awaits owned handles, drains the mounted tool, closes observation, then
closes SQLite. The example explicitly permits public session access; embedding
applications must replace that policy with their authorization rules. Writers
must support response deadlines; this handler uses a one-second WriteTimeout.

The unreleased snapshot/watch schema requires explicit recreation of older
development databases. Open rejects old schemas without deleting them.
