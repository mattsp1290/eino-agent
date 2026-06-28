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
