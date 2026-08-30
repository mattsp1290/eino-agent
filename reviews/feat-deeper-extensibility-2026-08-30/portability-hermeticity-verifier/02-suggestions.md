# Suggestions

## S1 — Name the Windows gate after its intentionally narrow package scope

- Location: `Makefile:22-24`, `Makefile:87-88`
- Rationale: `windows-compile` successfully protects the new Windows-only einotools contract, but its broad name can be read as a whole-module portability guarantee. A whole-module Windows compile still encounters the pre-existing `internal/deps/deps.go:33` reference to the upstream Unix-only `shell.New`. Renaming the target makes the actual guarantee explicit and avoids misleading future maintainers while preserving the useful gate.
- Suggested snippet:

```make
.PHONY: einotools-windows-compile

check: fmt-check vet test race mod-tidy-check lint einotools-windows-compile wit-check

einotools-windows-compile:
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO_TEST) -exec=true ./tools/einotools
```

If whole-module Windows support becomes a project goal, separately split the `internal/deps` platform pins with build tags and expand the gate to `./...`.
