GO_FILES := $(shell find . -name '*.go' -not -path './.git/*')
GOLANGCI_LINT_TOOLCHAIN := go1.26.3
GO := GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) go
GOFMT := GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) gofmt
GOIMPORTS := $(GO) run golang.org/x/tools/cmd/goimports
GOLANGCI_LINT := $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint
GO_TEST ?= go test

.PHONY: check fmt fmt-check lint mod-tidy-check race test vet

check: fmt-check vet test race mod-tidy-check lint

fmt:
	$(GOFMT) -w $(GO_FILES)
	$(GOIMPORTS) -w -local github.com/mattsp1290/eino-agent $(GO_FILES)

fmt-check:
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

lint:
	$(GOLANGCI_LINT) run ./...
