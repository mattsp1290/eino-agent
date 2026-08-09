# Phase A Wasm extension guests

These examples implement the generated Go exports for
`eino-agent:extensions@0.1.0`. Regenerate the bindings first with `make wit`,
then build the checked-in components with the pinned targets:

```sh
make wasm-fixtures
```

The build uses TinyGo's capability-free `wasm-unknown` reactor target, embeds
the selected WIT world with `wasm-tools`, and componentizes the result. The
fixtures import only the versioned host log interface; they do not import WASI.

The host loads only an explicitly named local file beneath `AllowedRoot` and
requires its SHA-256. For example:

```sh
shasum -a 256 tool.wasm
```

Pass that digest as `wasmext.ModuleConfig.ExpectedSHA256`. Prefer
`wasmext.NewLoader()` and `defer loader.Close(ctx)` so the embedding host, not
the orchestrator, owns module shutdown.
