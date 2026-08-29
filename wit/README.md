# eino-agent extension contracts

`eino-agent:extensions@0.1.0` is the current Component Model contract for
eino-agent extensions. The Go interfaces remain the source of truth; these WIT
worlds define the bounded data that their Wasm-backed implementations may see.

The project is pre-release and supports only this current schema. Regenerate
the checked-in Go bindings with `make wit` after changing the WIT contract, then
rebuild the checked-in guest components with `make wasm-fixtures`.

Host wrappers and checked-in example components cover the `tool`,
`permissions-policy`, `context-source`, `event-sink`, `hook`, and
`tool-middleware` worlds. Tool guests must export
`permission-pattern(input-json)` over final normalized input; context-source
messages can use only system or user text roles.

All JSON strings and free-form text are subject to host-configured byte limits.
The host exposes only `eino-agent:host/log@0.1.0`; filesystem, sockets,
environment variables, process execution, clocks, random sources, credentials,
provider endpoints, complete configuration snapshots, and resolved model state
are not imported. Hosts may attach a `ModuleConfig.Observer`; guest log lines
are byte-bounded and carry the configured module name plus verified digest.

## Native point adapters

`wasmext` maps `context-source` to the `runtime/context-assemble` transform,
`event-sink` to the contained `runtime/event-published` notification, `hook`
`before-run` and `after-run` to bounded `runtime/run-admitted` and
`runtime/run-settled` notifications, and
`tool-middleware` to `runtime/tool-prepare` plus
`runtime/tool-result-transform`. It does not expose native around execution or
a generic name-and-JSON event bus. The hook world has no turn callbacks; adding
new guest authority requires a separate security and design review.
