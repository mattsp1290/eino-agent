# Execution Handoff

## Work package order

### 1. Canonical required-around error composition

Files and symbols:

- `extension/dispatch.go:InvokeAround`
- `extension/extension_test.go`
- `runtime/orchestrator.go:executeToolOutcome`
- `runtime/extensions_test.go:TestToolExecutionPreservesExecutorAndCallbackErrors`

Prerequisites: none.

Result: generic and nested dispatch preserves delegated and callback failures with correct diagnostic ownership; runtime has no error side channel.

Gate:

```bash
go test ./extension -run 'Test(RequiredDelegation|Invoke|Interceptor)'
go test ./runtime -run 'TestToolExecutionPreservesExecutorAndCallbackErrors'
```

### 2. Single-owner run settlement

Files and symbols:

- `runtime/orchestrator.go:run`
- proposed `runtime/orchestrator.go:(*StreamingOrchestrator).settleRun`
- `runtime/interrupt.go:Resume`
- proposed `runtime/interrupt.go:terminalRunHandle`
- proposed `runtime/interrupt.go:(*StreamingOrchestrator).resumeRun`
- `runtime/interrupt.go:(*StreamingOrchestrator).executeResume`
- delete `runtime/interrupt.go:(*StreamingOrchestrator).finishResume`
- `runtime/extension_execution.go:newRunExecution`
- `session/types.go:Store.Execution`
- `store/storetest/contract.go`
- affected runtime resume, lifecycle, event-sink, and tool-execution tests

Prerequisites: package 1 should be committed in the same final change but has no lifecycle dependency. Do not edit `runtime/orchestrator.go` for both packages concurrently.

Result: one shared finalizer, complete work/lease/settlement error composition, exactly one outer settlement call per active run execution, and an explicit non-nil execution-store contract.

Gate:

```bash
go test ./runtime -run 'Test(StreamingOrchestratorResume|ExecuteResume|RunSettled|PendingResume|TerminalResume|RunExecution)'
go test -race ./runtime -run 'Test(StreamingOrchestratorResume|ExecuteResume|RunSettled|PendingResume|TerminalResume|RunExecution)'
go test ./store/...
```

### 3. Canonical model request and audit projection

Files and symbols:

- proposed `runtime/ledger.go:auditModelRequest`
- delete `runtime/ledger.go:AuditModelRequest`
- delete `runtime/ledger.go:validateAuditSafeMessage`
- delete `runtime/ledger.go:rejectCanonicalExtra`
- `runtime/model_stream.go:streamModel`
- `runtime/ledger_test.go`
- `runtime/provider.go:TurnSnapshot.ProviderRequest`
- `runtime/provider_test.go`

Prerequisites: package 2 should finish first to avoid overlapping runtime edits. The behavior is otherwise independent.

Result: one request clone supplies both provider dispatch and the ledger projection.

Gate:

```bash
go test ./model ./runtime -run 'Test(RequestClone|AuditModelRequest|ModelRequest|StreamingOrchestrator.*Model)'
```

### 4. Integration, cleanup, and delivery

Tasks:

- Run formatting only on changed Go files.
- Remove unused helpers and imports.
- Verify no compatibility aliases, flags, migration code, or dual paths were introduced.
- If recovery is required, revert the whole implementation commit, rerun `make check`, and push the revert. Do not create a compatibility path.
- Run all repository quality gates.
- Update and close Beads task `eino-agent-0ub` only after the quality gates pass.
- Run the required preflight checks before push.
- Rebase, push Beads data, push Git, and verify upstream status.

Gates:

```bash
gofmt -w <changed-go-files>
git diff --check
make check
git status --short
```

Delivery sequence:

```bash
git diff --check
git status --short
git add <only-implementation-files>
git add -f .agents/plans/runtime-boundary-simplification/*.md
git commit -m "simplify runtime ownership boundaries"
git pull --rebase
bd close eino-agent-0ub
bd dolt push
git push
git status --short --branch
```

If closing the Beads issue changes tracked repository files, include that state in the intended commit or make the required follow-up commit before pushing. Never leave Beads or Git changes unpushed.

## Integration and regression gates

- `go test ./...` must pass before the race suite.
- `go test -race ./...` must pass.
- `go vet ./...`, formatting checks, module-tidy checks, lint, and WIT regeneration checks must pass through `make check`.
- `git diff --check` must report no whitespace errors.
- The final worktree must be clean.
- The branch must report no commits ahead of or behind its configured upstream after push.
- Because no schema changes occur, rollback requires no data recovery or migration; use a whole-commit revert and the same integration gates.

## Stop/go conditions

- Stop if the generic extension policy cannot preserve both causes without exposing callback raw text; resolve that policy in `extension` before changing runtime.
- Stop if a resume path requires settlement before it returns its work result; identify the durable invariant and revise the outer boundary instead of reintroducing an inner settlement call.
- Stop if canonical cloning cannot retain a request field used by the provider; add a clone test and correct `model.Request.Clone` rather than adding a second runtime copy path.
- Go only when targeted tests pass for each package and no blocking decision remains.

## Definition of done

- All success criteria in `00-overview.md` have executable coverage or a structural search check.
- The two initial independent reviews and the adversarial review have completed, and every accepted correction is present in these plan files.
- The implementation contains one run settlement function, one required-around error policy, and one model request canonicalization entry point.
- No follow-up work remains untracked in Beads.
- `make check` and `git diff --check` pass.
- The implementation and plan are committed.
- `bd dolt push` and `git push` succeed.
- `git status --short --branch` is clean and up to date with origin.

## Deferred work

None. If implementation discovers an unrelated defect or broader cleanup, create a separate Beads issue and exclude it from this change.
