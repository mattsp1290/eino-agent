# Critical and Important Findings

## Critical

None.

## Important

### I1 — The Windows integration test does not compile

- Severity: Important
- Location: `tools/einotools/einotools_windows_test.go:25`
- Problem: `err` is introduced at line 18, then line 25 uses `_, err := MountStandard(...)`. The blank identifier does not count as a new variable, so the Windows-only file fails compilation with `no new variables on left side of :=`. Normal Unix quality gates cannot see this because the file is guarded by `//go:build windows`. As a result, the intended assertion that `MountStandard` preserves `catalog.ErrUnsupportedPlatform` is never executable on Windows, and the branch cannot build its test package for that supported compile target.
- Reproduction:

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -exec=/usr/bin/true ./tools/einotools
```

- Suggested fix:

```go
_, err = MountStandard(context.Background(), registry, component, Options{
	Scope: extension.GlobalScope(),
})
```

After the assignment fix, rerun the Windows compile command above as well as the native einotools tests with `rg` absent from `PATH`.
