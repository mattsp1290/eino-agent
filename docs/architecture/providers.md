# Provider And Model Adapter Boundary

Date: 2026-06-27

The provider boundary keeps runtime orchestration independent from concrete SDKs
while still allowing Eino-compatible transports to be plugged in.

## Core Contracts

`model.Adapter` exposes provider metadata, model descriptors, and a `Build`
method that returns an immutable Eino `ToolCallingChatModel` for one runtime
selection. `model.AdapterResolver` chooses an adapter by provider ID and returns
the resolved provider, model descriptor, and client.

Provider request identity is represented by `model.Identity`, a model-layer
shape that intentionally does not import `runtime`, `session`, or the
`context` package. Runtime maps its richer turn identity into this provider
shape in `runtime.ProviderRequest`.

## Streaming Callbacks

Adapters that can expose normalized stream callbacks implement
`model.Streamer`. The callback shape reports:

- provider start;
- normalized message deltas;
- token and cost usage;
- normalized provider errors;
- terminal provider response.

Runtime can consume those callbacks for event sinks, AG-UI projection, and
observability without parsing provider-specific SDK payloads.

## Optional Bindings

Concrete provider packages may implement `model.OptionalAdapter` to report
availability at runtime. Optional packages should be registered by host code or
build-tagged packages. The core `model` and `runtime` packages must not import
provider SDKs directly, so a host can build the reusable runtime without
selecting OpenAI, Codex-style, local, or other provider transports.

## Immutability

Providers must treat Eino messages and tool info as immutable request values.
Tool binding uses Eino's `ToolCallingChatModel.WithTools`, which returns a
derived model instead of mutating the shared base model. This prevents one
session's tools from racing with another session's provider request.
