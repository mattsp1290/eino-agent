//go:build cgo

package wasmext

/*
#include "wasmtime_abi.h"
*/
import "C"
import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	wasmtime "github.com/bytecodealliance/wasmtime-go/v47"
	einoobs "github.com/mattsp1290/eino-obs"
)

const (
	maxCInt              = 1<<31 - 1
	componentKindBool    = 0
	componentKindString  = 12
	componentKindList    = 13
	componentKindRecord  = 14
	componentKindEnum    = 17
	componentKindVariant = 16
	componentKindResult  = 19
)

type wasmHostLogState struct {
	component *wasmtimeComponent
}

var (
	hostLogSequence atomic.Uint64
	hostLogStates   sync.Map
)

//export wasmextHostLog
func wasmextHostLog(id C.uint64_t, levelData *C.char, levelSize C.size_t, messageData *C.char, messageSize C.size_t) {
	defer func() { _ = recover() }()
	stateValue, ok := hostLogStates.Load(uint64(id))
	if !ok {
		return
	}
	state := stateValue.(wasmHostLogState)
	if levelSize > 16 {
		levelSize = 16
	}
	if limit := C.size_t(state.component.guestLogLimit()); messageSize > limit {
		messageSize = limit
	}
	state.component.observeGuestLog(
		C.GoStringN(levelData, C.int(levelSize)),
		C.GoStringN(messageData, C.int(messageSize)),
	)
}

//export wasmextHostLogDrop
func wasmextHostLogDrop(id C.uint64_t) {
	hostLogStates.Delete(uint64(id))
}

// wasmtimePointerLayout mirrors the first field of the v47 Go wrapper handles.
// Keeping the conversion here avoids leaking Wasmtime handles into public APIs;
// the checked-in engine version and round-trip tests pin this ABI assumption.
type wasmtimePointerLayout struct{ pointer unsafe.Pointer }

func storeContext(store *wasmtime.Store) *C.wasmtime_context_t {
	storePointer := (*C.wasmtime_store_t)((*wasmtimePointerLayout)(unsafe.Pointer(store)).pointer)
	return C.wasmtime_store_context(storePointer)
}

func addHostLog(linker *wasmtime.ComponentLinker, component *wasmtimeComponent) error {
	id := hostLogSequence.Add(1)
	hostLogStates.Store(id, wasmHostLogState{component: component})
	errorPointer := C.wasmext_add_host_log(
		(*C.wasmtime_component_linker_t)((*wasmtimePointerLayout)(unsafe.Pointer(linker)).pointer),
		C.uint64_t(id),
	)
	runtime.KeepAlive(linker)
	if errorPointer != nil {
		hostLogStates.Delete(id)
		C.wasmtime_error_delete(errorPointer)
		return errors.New("host log import could not be linked")
	}
	return nil
}

func componentFunction(
	store *wasmtime.Store,
	instance *wasmtime.ComponentInstance,
	index *wasmtime.ComponentExportIndex,
) (C.wasmtime_component_func_t, error) {
	var function C.wasmtime_component_func_t
	found := C.wasmtime_component_instance_get_func(
		(*C.wasmtime_component_instance_t)(unsafe.Pointer(instance)),
		storeContext(store),
		(*C.wasmtime_component_export_index_t)((*wasmtimePointerLayout)(unsafe.Pointer(index)).pointer),
		&function,
	)
	runtime.KeepAlive(store)
	runtime.KeepAlive(instance)
	runtime.KeepAlive(index)
	if !bool(found) {
		return C.wasmtime_component_func_t{}, errors.New("component function unavailable")
	}
	return function, nil
}

func callComponentFunction(
	store *wasmtime.Store,
	function *C.wasmtime_component_func_t,
	arguments []C.wasmtime_component_val_t,
) (C.wasmtime_component_val_t, error) {
	var result C.wasmtime_component_val_t
	var argumentPointer *C.wasmtime_component_val_t
	if len(arguments) != 0 {
		argumentPointer = &arguments[0]
	}
	errorPointer := C.wasmtime_component_func_call(
		function,
		storeContext(store),
		argumentPointer,
		C.size_t(len(arguments)),
		&result,
		1,
	)
	runtime.KeepAlive(store)
	runtime.KeepAlive(arguments)
	if errorPointer != nil {
		C.wasmtime_error_delete(errorPointer)
		return C.wasmtime_component_val_t{}, errors.New("component call trapped")
	}
	return result, nil
}

func closeComponentValues(values []C.wasmtime_component_val_t) {
	for index := range values {
		C.wasmtime_component_val_delete(&values[index])
	}
}

func closeComponentValue(value *C.wasmtime_component_val_t) {
	C.wasmtime_component_val_delete(value)
}

func componentString(value *C.wasmtime_component_val_t, limit int64) (string, error) {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindString {
		return "", errors.New("component returned an unexpected value")
	}
	size := C.wasmext_string_size(value)
	if uint64(size) > uint64(limit) || uint64(size) > maxCInt {
		return "", errModuleTooLarge
	}
	return C.GoStringN(C.wasmext_string_data(value), C.int(size)), nil
}

func componentEnum(value *C.wasmtime_component_val_t) (string, error) {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindEnum {
		return "", errors.New("component returned an unexpected value")
	}
	return C.GoStringN(C.wasmext_enum_data(value), C.int(C.wasmext_enum_size(value))), nil
}

func componentBool(value *C.wasmtime_component_val_t) (bool, error) {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindBool {
		return false, errors.New("component returned an unexpected value")
	}
	return bool(C.wasmext_bool(value)), nil
}

func componentResult(value *C.wasmtime_component_val_t) (*C.wasmtime_component_val_t, error) {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindResult {
		return nil, errors.New("component returned an unexpected value")
	}
	if !bool(C.wasmext_result_ok(value)) {
		return nil, errors.New("guest returned a structured error")
	}
	return C.wasmext_result_value(value), nil
}

func componentRecordField(value *C.wasmtime_component_val_t, index, expected int) (*C.wasmtime_component_val_t, error) {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindRecord || int(C.wasmext_record_size(value)) != expected {
		return nil, errors.New("component returned an unexpected record")
	}
	return C.wasmext_record_value(value, C.size_t(index)), nil
}

func componentStringList(value *C.wasmtime_component_val_t, limit int64, maxItems int) ([]string, int64, error) {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindList {
		return nil, 0, errors.New("component returned an unexpected list")
	}
	count := int(C.wasmext_list_size(value))
	if count > maxItems {
		return nil, 0, errModuleTooLarge
	}
	items := make([]string, count)
	var total int64
	for index := range items {
		item, err := componentString(C.wasmext_list_value(value, C.size_t(index)), limit-total)
		if err != nil {
			return nil, 0, err
		}
		items[index] = item
		total += int64(len(item))
	}
	return items, total, nil
}

func setComponentString(value *C.wasmtime_component_val_t, text string) {
	var data *C.char
	if text != "" {
		data = (*C.char)(unsafe.Pointer(unsafe.StringData(text)))
	}
	C.wasmext_val_string(value, data, C.size_t(len(text)))
}

func setComponentBool(value *C.wasmtime_component_val_t, input bool) {
	C.wasmext_val_bool(value, C.bool(input))
}

func setComponentU32(value *C.wasmtime_component_val_t, input uint32) {
	C.wasmext_val_u32(value, C.uint32_t(input))
}

func setComponentS64(value *C.wasmtime_component_val_t, input int64) {
	C.wasmext_val_s64(value, C.int64_t(input))
}

func setComponentEnum(value *C.wasmtime_component_val_t, input string) {
	var data *C.char
	if input != "" {
		data = (*C.char)(unsafe.Pointer(unsafe.StringData(input)))
	}
	C.wasmext_val_enum(value, data, C.size_t(len(input)))
}

func newComponentRecord(fieldNames []string) C.wasmtime_component_val_t {
	var record C.wasmtime_component_val_t
	C.wasmext_val_record(&record, C.size_t(len(fieldNames)))
	for index, name := range fieldNames {
		nameBytes := []byte(name)
		C.wasmext_record_field(&record, C.size_t(index), (*C.char)(unsafe.Pointer(&nameBytes[0])), C.size_t(len(nameBytes)))
	}
	return record
}

func componentRecordFieldForSet(record *C.wasmtime_component_val_t, index int) *C.wasmtime_component_val_t {
	return C.wasmext_record_value(record, C.size_t(index))
}

func setComponentStringList(value *C.wasmtime_component_val_t, items []string) {
	C.wasmext_val_list(value, C.size_t(len(items)))
	for index, item := range items {
		setComponentString(C.wasmext_list_item(value, C.size_t(index)), item)
	}
}

func (c *wasmtimeComponent) invokeABI(_ context.Context, functionName string, arguments []C.wasmtime_component_val_t, decode func(*C.wasmtime_component_val_t) error) error {
	store := wasmtime.NewStore(c.engine)
	defer store.Close()
	store.SetEpochDeadline(1)
	store.Limiter(c.limits.MaxMemoryBytes, -1, -1, -1, -1)

	linker := wasmtime.NewComponentLinker(c.engine)
	defer linker.Close()
	if err := addHostLog(linker, c); err != nil {
		return err
	}
	instance, err := linker.Instantiate(store, c.component)
	if err != nil {
		return fmt.Errorf("component could not be instantiated: %w", err)
	}

	interfaceIndex := instance.GetExportIndex(store, nil, c.contract.exportName)
	if interfaceIndex == nil {
		return errors.New("component interface unavailable")
	}
	defer interfaceIndex.Close()
	functionIndex := instance.GetExportIndex(store, interfaceIndex, functionName)
	if functionIndex == nil {
		return errors.New("component function unavailable")
	}
	defer functionIndex.Close()
	function, err := componentFunction(store, instance, functionIndex)
	if err != nil {
		return err
	}

	defer closeComponentValues(arguments)
	result, err := callComponentFunction(store, &function, arguments)
	if err != nil {
		return err
	}
	defer closeComponentValue(&result)
	return decode(&result)
}

func (c *wasmtimeComponent) observeGuestLog(level, message string) {
	observer := c.contract.observer
	if observer == nil {
		return
	}
	limit := c.guestLogLimit()
	if len(message) > limit {
		message = message[:limit]
	}
	sequence := hostLogSequence.Add(1)
	observation := einoobs.Observation{
		ID:        fmt.Sprintf("wasm-log-%d", sequence),
		Kind:      "wasm.extension.log",
		Name:      "wasm.extension.log",
		Status:    "ok",
		Timestamp: time.Now(),
		Attributes: map[string]any{
			"wasm.module.name":   c.contract.identity.name,
			"wasm.module.sha256": c.contract.identity.hash,
			"log.level":          level,
			"log.message":        message,
		},
	}
	if exporter := observer.Exporter(); exporter != nil {
		_ = exporter.Export(context.Background(), []einoobs.Observation{observation})
	}
}

func (c *wasmtimeComponent) guestLogLimit() int {
	const hardLogLimit = 4096
	if c.limits.MaxOutputBytes < hardLogLimit {
		return int(c.limits.MaxOutputBytes)
	}
	return hardLogLimit
}
