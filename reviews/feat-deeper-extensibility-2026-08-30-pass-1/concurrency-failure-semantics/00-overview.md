# Concurrency & Failure Semantics Review Overview

- Branch: `feat/deeper-extensibility`
- Base: `main`
- Reviewed HEAD: `78f4a2541a62e97d0c85cb2f8ac8d80c57c4b491`
- Date: 2026-08-30
- Reviewer display name: Concurrency & Failure Semantics
- Reviewer slug: `concurrency-failure-semantics`
- Role: Audit durable ordering, panic containment, queue behavior, races, and failure-state correctness.
- Diff statistics: 275 files changed, 29,629 insertions, 11,253 deletions
- Commits reviewed: 36
- Verdict: **REQUEST_CHANGES**

The branch establishes strong atomic state/event transitions, fenced execution, bounded best-effort fanout, deterministic extension-plan leases, and explicit panic handling at most host callback boundaries. The latest test repair is correct: `runtime/orchestrator_tool_test.go` now gives its three-event delivery assertion enough queue capacity instead of accidentally exercising the test helper's one-slot fallback, and `tools/einotools/einotools_test.go` supplies test-owned executable paths without weakening catalog or execution assertions. Repeated focused tests and the complete race-enabled suite pass. One Important failure-containment gap remains: `wasmext.module.call` executes the component invocation in a new goroutine without recovering panics there, so neither the caller nor the orchestrator's goroutine-local recovery can contain a panic from the component/host-log call path. That gap must be closed before approval.

Verification performed:

- `go test -count=10 ./runtime ./tools/einotools`
- `go test -race -count=3 ./runtime ./session/... ./store/sqlite ./tools/einotools`
- `go test -race ./...`
- `git diff --check main...HEAD`

All verification commands passed at the reviewed HEAD.
