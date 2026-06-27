GO_FILES := $(shell find . -name '*.go' -not -path './.git/*')
GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT_TOOLCHAIN := go1.26.3
GOIMPORTS_VERSION := v0.47.0
GOIMPORTS := GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) go run golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)
GO_TEST ?= go test

.PHONY: check fmt fmt-check lint mod-tidy-check race test vet

check: fmt-check vet test race mod-tidy-check lint

fmt:
	gofmt -w $(GO_FILES)
	$(GOIMPORTS) -w -local github.com/mattsp1290/eino-agent $(GO_FILES)

fmt-check:
	@test -z "$$(gofmt -l $(GO_FILES))"
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
	GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...
