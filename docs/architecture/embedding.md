# Embedding HTTP and AG-UI SSE

`eino-agent` provides library primitives. Applications own route paths, auth,
tenant/session lookup, request validation, and response policy.

The `transport` package contains small adapters for common HTTP glue:

- `SSEHandler` wires an application route to durable AG-UI replay plus live
  tailing. The application supplies auth, session extraction, replay cursor
  parsing, and optional completion handling for cursor persistence.
- `InterruptHandler` adapts an application interrupt endpoint to a runtime
  handle. Applications decide how handles are located and authorized.
- `ResumeHandler` adapts an application resume endpoint to a runtime resume
  call and returns the resumed run ID in a response header.
- `DecodeMessages` is a convenience JSON decoder, not a required wire format.

Durable replay comes from `session.Store` and AG-UI replay helpers. Live token
deltas come from a tail such as `stream.Tail`; they are not treated as durable
conversation facts. Disconnect cancellation is driven by `http.Request.Context`
so server shutdowns and client disconnects cancel the live subscription.

This package intentionally does not register routes, own cookies or bearer-token
policy, choose URL layouts, or prescribe product session identifiers. A server
should compose these adapters inside its existing router and authorization
middleware.

Hosts that support dynamically mounted capabilities should own one
`composition.Registry`, pass it with `runtime.WithRunPlanProvider`, and retain
each returned mount. During shutdown, call `Deactivate` to stop new admission,
then `Close` with a bounded context; close waits for already admitted frozen
plans and runs registered effects in reverse order. The complete example is
[`examples/native-extension`](../../examples/native-extension).


## Current session state over HTTP

Use transport.SessionWatchHandler with SessionWatchConfig for coherent bounded
observation. The host must supply Service, Auth, Session and a positive
WriteTimeout. Auth establishes identity and authorization for the exact session
resolved by Session; an explicit public-access function is appropriate only
when that is the host's intended policy. No runtime routes or authentication
policy are installed.

Each connection starts from a new current snapshot, ignoring EventCursor,
Last-Event-ID and historical replay parameters. The typed AG-UI adapter emits
MESSAGES_SNAPSHOT with durable IDs and bounded STATE_SNAPSHOT with Watermark,
Exists, OmittedOlderMessages, Runs, Tools and Live availability. Transient text
replaces a visible unfinalized message's overlay. Reconnect never restores an
unavailable prefix from private provider state.

Before committing stream headers the handler verifies
ResponseController.SetWriteDeadline support. Every physical write and flush
sets a finite deadline in the same goroutine, and Flush errors are observed.
Wrappers expose Unwrap and the underlying writer must honor deadlines.
Unsupported writers return a bounded configuration error before the first
frame. There is no background writer that can outlive a stuck request.
A connected nonreading client exits by its current operation's WriteTimeout,
including when the service or subscription closes. Deadline errors terminate
the handler.

Before streaming, invalid session input maps to 400, authorization failures
to 401/403, oversized snapshots to 413, service capacity/closure to 503, and
store failures to 502. Error bodies are content-free. After streaming begins,
observation termination emits a best-effort {ResyncRequired:true} state marker
when writing is still possible, then closes. Socket failure closes only this
subscription; it never interrupts an execution handle.

SSEHandler, agui.Replay and agui.Reconnect remain separate historical event
APIs. Their independently paged replay/live handoff does not establish the
transaction-consistent state-watch contract.
