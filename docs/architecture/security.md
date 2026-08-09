# Security, Privacy, Cancellation, and Robustness Audit

Date: 2026-06-27

This audit records the runtime safety boundaries that must hold before
`eino-agent` is embedded in a host application. It focuses on negative behavior:
malformed inputs should return typed errors, secret-bearing data should not leave
its intended boundary, cancellation should settle durable state, and queues and
retries should be bounded.

## Scope

Covered surfaces:

- runtime admission, model streaming, tool-call persistence, interruption, and
  resume;
- AG-UI replay/live-tail and HTTP transport adapters;
- tool output encoding, retained output, and permission decisions;
- observability redaction defaults and exported observation metadata;
- provider credential handling at request resolution time.

Host applications remain responsible for route-level authorization, tenant
isolation, durable store encryption, provider-specific credential acquisition,
and any policy that intentionally allows plain reasoning storage.

## WebAssembly Guests

Wasm extensions are untrusted local components. `wasmext` confines loading to
an explicitly configured allowed root, resolves symlinks before containment
checks, rejects URL loads and non-regular files, enforces a pre-compilation size
limit, reads bytes once, and verifies the configured SHA-256 over those same
bytes. Wrong worlds and versions fail closed.

WASI is disabled. Guests receive no filesystem, network, environment, process,
clock, random, credential, endpoint, raw provider payload, complete
`config.Snapshot`, or resolved model capability. The sole v1 capability import
is a size-bounded log function whose module identity is host configured.
Cross-boundary JSON and text are checked against input/output bounds.

Execution uses instance-per-call semantics behind an internal engine boundary.
Timeout is active: cancellation increments Wasmtime's epoch so synchronous
guest code traps instead of merely observing a cooperative Go context. Close
stops new calls, interrupts in-flight calls, drains for a bounded interval, and
releases compiled state once. Errors expose only a stable class and configured
module name/hash prefix, never guest diagnostics or paths.

Evidence:

- `wasmext.TestSecureModuleLoadingRejectsHashSizeAndEscapingSymlink`
- `wasmext.TestModuleTimeoutActivelyInterruptsGuest`
- `wasmext.TestContractAndPayloadViolationsAreClassifiedAndBounded`
- `wasmext.TestToolWrapperRoundTripAndBoundedSnapshot`
- `internal/deps.TestCorePackagesDoNotDependOnWasmRuntimeOrBindings`

## Malformed Input

Runtime provider boundaries fail closed:

- Nil provider stream chunks are rejected as `malformed_provider_stream` and
  return a failed run without panicking.
- Malformed provider tool-call arguments are rejected as
  `malformed_provider_tool_call` before the tool call is persisted or executed.
- Empty provider tool-call arguments are normalized to `{}` for compatibility.
- Typed tool adapters return `ErrMalformedInput` for invalid tool JSON instead
  of calling executors with partial data.
- AG-UI client tool snapshots reject malformed inputs before materialization.
- Static permission rules report malformed glob patterns as operational policy
  errors.

Evidence:

- `runtime.TestStreamingOrchestratorFailsMalformedStreamWithoutPanic`
- `runtime.TestStreamingOrchestratorFailsMalformedToolArgumentsWithoutPanic`
- `runtime.TestStreamingOrchestratorNormalizesEmptyToolArguments`
- `tools.TestMalformedToolInputReturnsTypedError`
- `agui.TestClientToolSnapshotRejectsMalformedInput`
- `permissions.TestStaticPolicyMalformedPatternIsOperational`

## Redaction and Privacy

Default observability must not export raw prompt, model output, tool payload,
reasoning, compaction summary, attachment, path, URL, token, cookie, or API key
content.

Runtime observability emits IDs, stable classifications, bounded usage counts,
latencies, status, retryability, and cancellation flags. It does not attach raw
message content, provider error text, tool input/output JSON, permission
patterns, or compaction summaries. Tool observability metadata is allowlisted to
stable permission status only.

Live runtime events and model-facing tool output may contain content that the
host is authorized to deliver. Model-facing tool output is bounded by
`RetentionPolicy.MaxInlineBytes`. Redacted policies suppress raw content,
structured payloads, unsafe metadata, and attachment URLs. Attachments exposed
to the model are reduced to stable IDs and MIME types.

Evidence:

- `runtime.TestStreamingOrchestratorRecordsNoNetworkObservations`
- `runtime.TestStreamingOrchestratorRecordsRetryAndProviderError`
- `runtime.TestStreamingOrchestratorRecordsInterrupt`
- `runtime.TestObservabilitySinkRecordsCompactionWithoutPayloadLeak`
- `runtime.TestStreamingOrchestratorRecordsToolLifecycleWithoutPayloadLeak`
- `runtime.TestStreamingOrchestratorRecordsPermissionDeniedToolAsExpectedFailure`
- `runtime.TestStreamingOrchestratorRecordsOperationalToolFailure`
- `tools.TestEncodeModelOutputRedactsRawAndStructuredPayload`
- `tools.TestEncodeModelOutputSuppressesToolControlledFieldsWhenTruncated`
- `obs.TestDefaultFieldsForbidRawContent`
- `config.TestObservabilityRedactionDefaultsAreSafe`

## Reasoning Handling

Plain reasoning is a policy-gated AG-UI event family, but the current runtime
persists provider `ReasoningContent` when the provider emits it. Hosts and
provider adapters must suppress provider reasoning before it reaches the
orchestrator when durable reasoning storage is not allowed for that deployment.

Live provider deltas may contain plain reasoning for clients that are authorized
to see it, but observability defaults forbid both plain and encrypted reasoning
content. Encrypted reasoning is modeled as an omitted AG-UI event family and
must not be copied into observation attributes.

Evidence:

- `agui.TestRulesSafetyGates`
- `agui.TestRulesRedactionIsExplicit`
- `obs.TestDefaultFieldsForbidRawContent`
- `config.TestObservabilityPartialSummaryKeepsDefaultRedactionGuardrails`
- `runtime.TestStreamingOrchestratorFailsMalformedToolArgumentsWithoutPanic`

## Tool Output Bounds

Tool output is never allowed to grow unbounded in replayable model context.
Runtime and tool package encoders enforce UTF-8-safe truncation, structured
payload bounds, redaction, and external-retention signaling.

Evidence:

- `runtime.TestStreamingOrchestratorBoundsToolOutput`
- `tools.TestEncodeModelOutputTruncatesOversizedContent`
- `tools.TestEncodeModelOutputBoundsStructuredPayload`
- `tools/session.TestRetainedOutputIsBoundedAndSessionScoped`
- `tools/session.TestRetainedOutputHonorsAggregateSessionLimitAndZeroLimit`
- `tools/einotools.TestFileReadWrapperPreservesEinoToolsContract`

## Tokens and Credentials

Provider credentials are request-time runtime data, not catalog metadata.
`model.Runtime.Auth` is cloned before adapters receive it so caller mutations
cannot alter an in-flight resolution. Resolved provider/model descriptors expose
provider IDs, model IDs, capabilities, and safe options, not auth maps.

HTTP transport auth errors are reduced to generic `unauthorized` or `forbidden`
responses so application-specific auth failure text is not reflected to clients.
Observability policy marks tokens and API keys as forbidden fields.

Evidence:

- `model.TestAdapterResolverResolvesAdapterAndClonesRuntime`
- `transport.TestSSEHandlerRejectsUnauthorizedBeforeSessionExtraction`
- `obs.TestDefaultFieldsForbidRawContent`
- `config.TestObservabilityRedactionDefaultsAreSafe`

## Retry Bounds

Provider retries are bounded by `StreamingOrchestrator.Attempts`. Only
`model.Error` values with `Retryable` set are retried; cancellation is not
retried. Tool execution is not automatically rerun on resume unless a durable
pending call is safely claimed. Running calls found during resume are marked
interrupted rather than executed twice.

Evidence:

- `runtime.TestStreamingOrchestratorRetriesRetryableProviderErrors`
- `runtime.TestStreamingOrchestratorRecordsRetryAndProviderError`
- `runtime.TestStreamingOrchestratorResumeClaimsPendingToolOnce`
- `runtime.TestStreamingOrchestratorResumeDoesNotReexecuteRunningTool`
- `runtime.TestStreamingOrchestratorFailsWhenToolLoopExceedsLimit`

## Cancellation and Interruption

Cancellation is treated as an interrupted run/tool outcome, not as a generic
failure. Runtime interrupt cancels the active context, observes the interrupt,
settles durable run state, and emits a terminal run event. Tool cancellation is
encoded as interrupted and surfaced to observability as canceled/interrupted.

Transport cancellation propagates from disconnected SSE clients into live-tail
subscriptions. Workspace locks do not admit canceled waiters.

Evidence:

- `runtime.TestStreamingOrchestratorMarksCanceledRunsInterrupted`
- `runtime.TestStreamingOrchestratorMarksCanceledToolInterrupted`
- `runtime.TestStreamingOrchestratorHonorsCancellationDuringDeltaBackpressure`
- `runtime.TestStreamingOrchestratorRecordsCancellation`
- `runtime.TestStreamingOrchestratorRecordsInterrupt`
- `agui.TestReconnectCancelsTailOnDisconnect`
- `transport.TestSSEHandlerCancelsTailOnDisconnect`
- `stream.TestTailSubscriptionClosesOnContextCancellation`
- `internal/workspace.TestLockerDoReturnsWhenCanceledWhileWaiting`

## Queue Bounds and Backpressure

Runtime event queues are bounded and respect context cancellation while applying
backpressure to model streaming. Live-tail subscribers have bounded buffers;
slow subscribers are disconnected and must reconnect or replay. AG-UI reconnect
reports tail overflow explicitly.

Evidence:

- `runtime.TestStreamingOrchestratorBoundedQueueAppliesBackpressure`
- `runtime.TestStreamingOrchestratorHonorsCancellationDuringDeltaBackpressure`
- `stream.TestTailDisconnectsSlowSubscriberWhenQueueBounded`
- `agui.TestReconnectReportsTailOverflow`

## Remaining Host Responsibilities

Before production embedding, host applications should verify:

- the concrete provider adapters never put credentials into provider options,
  model options, errors, logs, or observation metadata;
- the durable store and backups meet the host's encryption and retention
  policy;
- route-level auth binds session IDs to tenants before calling transport
  handlers;
- any plain reasoning storage is provider-approved and explicitly enabled by
  host policy;
- external attachment stores enforce access control independently of model-facing
  attachment IDs.
