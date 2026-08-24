//go:build !cgo

package wasmext

import "errors"

const cgoEnabled = false

func newWasmtimeEngine(Limits) (engine, error) {
	return nil, errors.New("wasm extensions require cgo")
}
