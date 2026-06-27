//go:build tools

// Package tools pins quality-gate tools in the committed module graph.
package tools

import (
	_ "github.com/golangci/golangci-lint/v2/cmd/golangci-lint"
	_ "golang.org/x/tools/cmd/goimports"
)
