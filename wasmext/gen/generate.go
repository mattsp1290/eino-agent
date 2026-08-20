// Package gen anchors reproducible WIT binding generation. Generated packages
// live beneath this directory and are committed to the repository.
package gen

//go:generate go run go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0 generate --world tool --out . --package-root github.com/mattsp1290/eino-agent/wasmext/gen --versioned ../../wit
//go:generate go run go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0 generate --world permissions-policy --out . --package-root github.com/mattsp1290/eino-agent/wasmext/gen --versioned ../../wit
//go:generate go run go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0 generate --world context-source --out . --package-root github.com/mattsp1290/eino-agent/wasmext/gen --versioned ../../wit
//go:generate go run go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0 generate --world event-sink --out . --package-root github.com/mattsp1290/eino-agent/wasmext/gen --versioned ../../wit
//go:generate go run go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0 generate --world hook --out . --package-root github.com/mattsp1290/eino-agent/wasmext/gen --versioned ../../wit
//go:generate go run go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0 generate --world tool-middleware --out . --package-root github.com/mattsp1290/eino-agent/wasmext/gen --versioned ../../wit
