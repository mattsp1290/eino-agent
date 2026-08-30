# Environment & Integration Contracts Review

- Branch: `feat/deeper-extensibility`
- Base: `main`
- Reviewed HEAD: `78f4a2541a62e97d0c85cb2f8ac8d80c57c4b491`
- Date: 2026-08-30
- Reviewer display name: Environment & Integration Contracts
- Reviewer slug: `environment-integration-contracts`
- Role: Audit host dependency assumptions, integration boundaries, validation, and long-term maintainability.
- Diff statistics: 275 files changed, 29,629 insertions, 11,253 deletions
- Commits reviewed: 36

## Summary

The latest repair correctly removes the accidental `rg` dependency from the Unix einotools tests by injecting catalog-owned executable paths, and it gives the asynchronous event transport enough test-local capacity to verify all three best-effort delivery attempts without conflating queue overflow with sink panic handling. The native test suite, repeated `PATH`-without-`rg` runs, `CGO_ENABLED=0` suite, vet, and non-Windows cross-compiles pass. However, the branch's new Windows-only einotools test does not compile because it redeclares an existing `err` variable with no new variable on the left side. This prevents the branch from satisfying its own unsupported-platform integration contract on Windows and requires a one-token assignment fix before approval.

## Verdict

**REQUEST_CHANGES**

## Finding Counts

- Critical: 0
- Important: 1
- Suggestions: 2

## Verification Evidence

- `go test ./runtime -run '^TestToolTransitionTransportPanicIsPostCommitBestEffort$' -count=50` — passed.
- `env PATH=/usr/bin:/bin "$(go env GOROOT)/bin/go" test ./tools/einotools -count=1` — passed with no `rg` available on `PATH`.
- `env PATH=/usr/bin:/bin "$(go env GOROOT)/bin/go" test ./runtime ./tools/einotools -count=5` — passed.
- `CGO_ENABLED=0 go test ./...` — passed.
- Darwin/amd64 and FreeBSD/amd64 `CGO_ENABLED=0` compile checks for `tools/einotools`, `runtime`, `transport`, and `wasmext` — passed.
- `go vet ./...` — passed.
- `git diff --check main...HEAD` — passed.
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -exec=/usr/bin/true ./tools/einotools` — failed to compile at `tools/einotools/einotools_windows_test.go:25` with `no new variables on left side of :=`.
