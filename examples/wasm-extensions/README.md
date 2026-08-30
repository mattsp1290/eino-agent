# Wasm extension guests

These examples implement all six generated Go exports for
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
the orchestrator, owns module shutdown. Set `ModuleConfig.Observer` to route
the bounded `eino-agent:host/log@0.1.0` import through that observer's exporter
with verified module identity attached.

`context-source`, `event-sink`, `hook`, and `tool-middleware` are adapted into
the typed points documented in
[`docs/architecture/extension-points.md`](../../docs/architecture/extension-points.md).
The tool-middleware guest reaches prepare and protected result transformation
only; around tool execution remains native-only. No guest receives credentials,
clients, callbacks, raw event payloads, or authority to name arbitrary points.
The hook world contains run-scoped `before-run` and `after-run` exports only;
there is no separate Wasm turn lifecycle.

Register extension-backed guests directly while mounting their component; no
opaque loaded handle is exposed:

```go
loader := wasmext.NewLoader()
mount, err := registry.Mount(ctx, component, composition.InstallerFunc(
    func(ctx context.Context, registrar *composition.Registrar) error {
        return loader.RegisterHook(
            ctx,
            registrar.Extensions(),
            extension.Registration{ID: "lifecycle", Scope: extension.GlobalScope()},
            moduleConfig,
        )
    },
))
```

Keep `Loader` alive while its mounts are active. Failed preparation or commit
rolls the module back, `mount.Close` releases it, and `Loader.Close` remains an
idempotent host shutdown fallback.
