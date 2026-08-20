// Package guestabi provides the canonical ABI allocator expected when TinyGo
// exports a WIT world. TinyGo's libc realloc has a two-argument signature,
// while the Component Model requires cabi_realloc's four-argument signature.
package guestabi

import "unsafe"

var (
	canonicalMemory [1 << 20]byte
	canonicalOffset uintptr
)

//go:wasmexport cabi_realloc
//export cabi_realloc
func cabiRealloc(oldPointer, oldSize, alignment, newSize uint32) uint32 {
	if newSize == 0 {
		return 0
	}
	align := uintptr(alignment)
	if align == 0 {
		align = 1
	}
	start := (canonicalOffset + align - 1) &^ (align - 1)
	end := start + uintptr(newSize)
	if end > uintptr(len(canonicalMemory)) {
		panic("canonical ABI fixture allocator exhausted")
	}
	pointer := uint32(uintptr(unsafe.Pointer(&canonicalMemory[start])))
	canonicalOffset = end
	if oldPointer != 0 && oldSize != 0 {
		count := uintptr(oldSize)
		if count > uintptr(newSize) {
			count = uintptr(newSize)
		}
		source := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(oldPointer))), count)
		copy(canonicalMemory[start:end], source)
	}
	return pointer
}
