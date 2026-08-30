GO_FILES := $(shell find . -name '*.go' -not -path './.git/*')
GOIMPORTS_FILES := $(shell find . -name '*.go' -not -path './.git/*' -not -path './wasmext/gen/eino-agent/*')
GOLANGCI_LINT_TOOLCHAIN := go1.26.3
GO := GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) go
GOFMT := GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) gofmt
GO_TEST ?= go test
GUEST_GOTOOLCHAIN := go1.24.6
TINYGO ?= tinygo
WASM_TOOLS ?= wasm-tools
WASM_FIXTURES := examples/wasm-extensions/fixtures
WASM_BUILD := examples/wasm-extensions/.build

# Quality-gate tools live in a nested module (internal/tools) so their heavy
# dependency graph (golangci-lint pulls ~260 modules) never leaks into the
# consumer-facing eino-agent module graph. Build them into .bin and invoke
# from the repo root so they operate on this module's packages.
TOOLS_DIR := internal/tools
BIN_DIR := $(CURDIR)/.bin
GOIMPORTS := $(BIN_DIR)/goimports
GOLANGCI_LINT := $(BIN_DIR)/golangci-lint

.PHONY: check external-consumer-check fmt fmt-check lint mod-tidy-check race test tools vet wasm-fixtures windows-compile wit wit-check

check: fmt-check vet test race mod-tidy-check lint windows-compile wit-check external-consumer-check

wit:
	rm -rf wasmext/gen/eino-agent
	GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) go -C wasmext/gen generate .

wit-check:
	$(MAKE) wit
	git diff --exit-code -- wasmext/gen

wasm-fixtures: wit
	mkdir -p $(WASM_BUILD) $(WASM_FIXTURES)
	cd examples/wasm-extensions && GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) $(TINYGO) build -target=wasm-unknown -buildmode=c-shared -scheduler=none -panic=trap -o .build/tool.core.wasm ./tool
	$(WASM_TOOLS) component embed --world tool wit $(WASM_BUILD)/tool.core.wasm -o $(WASM_BUILD)/tool.embedded.wasm
	$(WASM_TOOLS) component new $(WASM_BUILD)/tool.embedded.wasm -o $(WASM_FIXTURES)/tool.wasm
	cd examples/wasm-extensions && GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) $(TINYGO) build -target=wasm-unknown -buildmode=c-shared -scheduler=none -panic=trap -o .build/permissions-policy.core.wasm ./permissions-policy
	$(WASM_TOOLS) component embed --world permissions-policy wit $(WASM_BUILD)/permissions-policy.core.wasm -o $(WASM_BUILD)/permissions-policy.embedded.wasm
	$(WASM_TOOLS) component new $(WASM_BUILD)/permissions-policy.embedded.wasm -o $(WASM_FIXTURES)/permissions-policy.wasm
	cd examples/wasm-extensions && GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) $(TINYGO) build -target=wasm-unknown -buildmode=c-shared -scheduler=none -panic=trap -o .build/context-source.core.wasm ./context-source
	$(WASM_TOOLS) component embed --world context-source wit $(WASM_BUILD)/context-source.core.wasm -o $(WASM_BUILD)/context-source.embedded.wasm
	$(WASM_TOOLS) component new $(WASM_BUILD)/context-source.embedded.wasm -o $(WASM_FIXTURES)/context-source.wasm
	cd examples/wasm-extensions && GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) $(TINYGO) build -target=wasm-unknown -buildmode=c-shared -scheduler=none -panic=trap -o .build/event-sink.core.wasm ./event-sink
	$(WASM_TOOLS) component embed --world event-sink wit $(WASM_BUILD)/event-sink.core.wasm -o $(WASM_BUILD)/event-sink.embedded.wasm
	$(WASM_TOOLS) component new $(WASM_BUILD)/event-sink.embedded.wasm -o $(WASM_FIXTURES)/event-sink.wasm
	cd examples/wasm-extensions && GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) $(TINYGO) build -target=wasm-unknown -buildmode=c-shared -scheduler=none -panic=trap -o .build/hook.core.wasm ./hook
	$(WASM_TOOLS) component embed --world hook wit $(WASM_BUILD)/hook.core.wasm -o $(WASM_BUILD)/hook.embedded.wasm
	$(WASM_TOOLS) component new $(WASM_BUILD)/hook.embedded.wasm -o $(WASM_FIXTURES)/hook.wasm
	cd examples/wasm-extensions && GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) $(TINYGO) build -target=wasm-unknown -buildmode=c-shared -scheduler=none -panic=trap -o .build/tool-middleware.core.wasm ./tool-middleware
	$(WASM_TOOLS) component embed --world tool-middleware wit $(WASM_BUILD)/tool-middleware.core.wasm -o $(WASM_BUILD)/tool-middleware.embedded.wasm
	$(WASM_TOOLS) component new $(WASM_BUILD)/tool-middleware.embedded.wasm -o $(WASM_FIXTURES)/tool-middleware.wasm
	$(WASM_TOOLS) validate $(WASM_FIXTURES)/tool.wasm
	$(WASM_TOOLS) validate $(WASM_FIXTURES)/permissions-policy.wasm
	$(WASM_TOOLS) validate $(WASM_FIXTURES)/context-source.wasm
	$(WASM_TOOLS) validate $(WASM_FIXTURES)/event-sink.wasm
	$(WASM_TOOLS) validate $(WASM_FIXTURES)/hook.wasm
	$(WASM_TOOLS) validate $(WASM_FIXTURES)/tool-middleware.wasm
	rm -rf $(WASM_BUILD)

tools: $(GOIMPORTS) $(GOLANGCI_LINT)

$(GOIMPORTS): $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum
	$(GO) -C $(TOOLS_DIR) build -o $(BIN_DIR)/goimports golang.org/x/tools/cmd/goimports

$(GOLANGCI_LINT): $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum
	$(GO) -C $(TOOLS_DIR) build -o $(BIN_DIR)/golangci-lint github.com/golangci/golangci-lint/v2/cmd/golangci-lint

fmt: $(GOIMPORTS)
	$(GOFMT) -w $(GO_FILES)
	$(GOIMPORTS) -w -local github.com/mattsp1290/eino-agent $(GOIMPORTS_FILES)

fmt-check: $(GOIMPORTS)
	@test -z "$$($(GOFMT) -l $(GO_FILES))"
	@test -z "$$($(GOIMPORTS) -l -local github.com/mattsp1290/eino-agent $(GOIMPORTS_FILES))"

vet:
	go vet ./...

test:
	$(GO_TEST) ./...

race:
	$(GO_TEST) -race ./...

windows-compile:
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO_TEST) -exec=true ./tools/einotools

mod-tidy-check:
	go mod tidy -diff
	GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) go -C wasmext/gen mod tidy -diff
	GOTOOLCHAIN=$(GUEST_GOTOOLCHAIN) go -C examples/wasm-extensions mod tidy -diff
	go -C $(TOOLS_DIR) mod tidy -diff

external-consumer-check:
	testdata/external-consumer/check.sh

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...
