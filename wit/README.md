# eino-agent extension contracts

`eino-agent:extensions@0.1.0` is the public Component Model contract for
eino-agent extensions. The Go interfaces remain the source of truth; these WIT
worlds define the bounded data that their Wasm-backed implementations may see.

Published packages are immutable. Compatible additions may be introduced in a
new package version, while a breaking change requires an `@0.2.0` package that
lives alongside `@0.1.0` support. Existing WIT files are never edited in place
after publication.

Phase A includes host wrappers and example components for the `tool` and
`permissions-policy` worlds. The `context-source`, `event-sink`, `hook`, and
`tool-middleware` worlds reserve the Phase B contracts; their wrappers and
examples follow the same versioned loading pattern.

All JSON strings and free-form text are subject to host-configured byte limits.
The host exposes only `eino-agent:host/log@0.1.0`; filesystem, sockets,
environment variables, process execution, clocks, random sources, credentials,
provider endpoints, complete configuration snapshots, and resolved model state
are not imported. Hosts may attach a `ModuleConfig.Observer`; guest log lines
are byte-bounded and carry the configured module name plus verified digest.
