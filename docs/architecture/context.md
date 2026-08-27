# Runtime Context Boundaries

Date: 2026-06-27

This document defines the first implementation boundary for runtime context and
context sources. It complements `docs/architecture/runtime.md` by separating
request identity and bounded prompt material from concrete host filesystems,
HTTP transports, tracing SDKs, and provider adapters.

## Runtime Identity

Runtime identity is carried in ordinary Go `context.Context` values by
`runtime.WithContextIdentity` and read with `runtime.ContextIdentityFrom`.
Cancellation, deadlines, and host values remain owned by the caller's
`context.Context`; the runtime helper only attaches immutable metadata.

The propagated identity includes:

- session ID;
- run ID;
- agent ID;
- assistant message ID;
- tool call ID;
- provider ID;
- model ID;
- trace/span IDs and bounded attributes.

`runtime.TurnSnapshot.ContextIdentity` derives provider/model identity only from
the resolved model. Fresh runs validate that resolution against the requested
selection before history reads or admission; no downstream layer reconstructs
identity from configuration. This keeps context sources, tools, hooks, AG-UI
adapters, durable records, and observability aligned on the same identifiers.

## Context Source Kinds

The `context` package uses package name `agentcontext` to avoid shadowing the
standard library. It defines four initial context-source kinds:

- `system_prompt` for durable system instruction material;
- `project_instructions` for host or project instructions;
- `attachment` for user-visible attachment content or metadata;
- `reference` for future retrieval handles such as vector-search hits.

The source shape intentionally mirrors `config.ContextSource` without importing
the config package. Runtime or host loaders translate validated config
declarations into `agentcontext.Source` values at request time.

## Bounds

`agentcontext.Assembler` enforces item count, bytes per item, and total bytes
before returning material to runtime logic. Default bounds are nonzero so an
unbounded host loader cannot accidentally flood a provider request. Hosts may
set tighter source-specific limits when translating config declarations.

The assembler clones request and item metadata at package boundaries. Loader
mutations cannot alter retained identity, trace attributes, source options, or
assembled item metadata.

## Cancellation

Context-source loading uses the caller's `context.Context`. The assembler checks
`ctx.Err()` before starting and between loaders. Loaders must also honor
cancellation while performing host-specific reads, retrieval, or attachment
processing.

No runtime API wraps cancellation in a custom token. That keeps provider
adapters, tools, and host integrations compatible with standard Go cancellation
and future tracing middleware.

## Portable References

Context items may carry URIs for attachments or references, but runtime context
must not bake local-only paths into durable or cross-process state. The assembler
rejects absolute POSIX paths, Windows drive paths, and `file://` URIs. Portable
references should use host-defined schemes such as `project://`,
`attachment://`, or HTTPS URLs when the host explicitly allows them.

Filesystem resolution remains a host concern. Core runtime logic carries
portable references and bounded content; it does not infer a local checkout path
or process working directory.
