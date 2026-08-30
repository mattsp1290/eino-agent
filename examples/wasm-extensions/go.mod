module github.com/mattsp1290/eino-agent/examples/wasm-extensions

go 1.24.6

require (
	github.com/mattsp1290/eino-agent/wasmext/gen v0.1.0
	go.bytecodealliance.org/cm v0.3.0
)

replace github.com/mattsp1290/eino-agent/wasmext/gen => ../../wasmext/gen
