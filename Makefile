GO_FILES := $(shell find . -name '*.go' -not -path './.git/*')
GOLANGCI_LINT_TOOLCHAIN := go1.26.3
GO := GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) go
GOFMT := GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) gofmt
GO_TEST ?= go test

# Quality-gate tools live in a nested module (internal/tools) so their heavy
# dependency graph (golangci-lint pulls ~260 modules) never leaks into the
# consumer-facing eino-agent module graph. Build them into .bin and invoke
# from the repo root so they operate on this module's packages.
TOOLS_DIR := internal/tools
BIN_DIR := $(CURDIR)/.bin
GOIMPORTS := $(BIN_DIR)/goimports
GOLANGCI_LINT := $(BIN_DIR)/golangci-lint

.PHONY: check fmt fmt-check lint mod-tidy-check race test vet tools

check: fmt-check vet test race mod-tidy-check lint

tools: $(GOIMPORTS) $(GOLANGCI_LINT)

$(GOIMPORTS): $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum
	$(GO) -C $(TOOLS_DIR) build -o $(BIN_DIR)/goimports golang.org/x/tools/cmd/goimports

$(GOLANGCI_LINT): $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum
	$(GO) -C $(TOOLS_DIR) build -o $(BIN_DIR)/golangci-lint github.com/golangci/golangci-lint/v2/cmd/golangci-lint

fmt: $(GOIMPORTS)
	$(GOFMT) -w $(GO_FILES)
	$(GOIMPORTS) -w -local github.com/mattsp1290/eino-agent $(GO_FILES)

fmt-check: $(GOIMPORTS)
	@test -z "$$($(GOFMT) -l $(GO_FILES))"
	@test -z "$$($(GOIMPORTS) -l -local github.com/mattsp1290/eino-agent $(GO_FILES))"

vet:
	go vet ./...

test:
	$(GO_TEST) ./...

race:
	$(GO_TEST) -race ./...

mod-tidy-check:
	go mod tidy -diff
	go -C $(TOOLS_DIR) mod tidy -diff

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...
