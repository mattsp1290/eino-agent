// Package wasmext adapts explicitly configured WebAssembly components to
// eino-agent's native Go extension seams.
//
// This leaf package is the only production package that imports the generated
// WIT bindings and Wasmtime. Wasmtime uses CGO; embedders must therefore use a
// supported CGO toolchain and target. The embedding host owns every loaded
// module's lifetime and must close its Loader or concrete wrapper. Phase A
// calls use an internal lifting/lowering layer over Wasmtime's official v47 C
// component API until equivalent calls and nested host linking are published
// by wasmtime-go.
package wasmext
