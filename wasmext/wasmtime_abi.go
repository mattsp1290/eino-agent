package wasmext

/*
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct wasm_engine_t wasm_engine_t;
typedef struct wasmtime_store_t wasmtime_store_t;
typedef struct wasmtime_context_t wasmtime_context_t;
typedef struct wasmtime_error_t wasmtime_error_t;
typedef struct wasmtime_component_linker_t wasmtime_component_linker_t;
typedef struct wasmtime_component_linker_instance_t wasmtime_component_linker_instance_t;
typedef struct wasmtime_component_export_index_t wasmtime_component_export_index_t;

typedef struct {
  size_t size;
  char *data;
} wasm_name_t;

typedef struct wasmtime_component_val wasmtime_component_val_t;
typedef struct wasmtime_component_valrecord_entry wasmtime_component_valrecord_entry_t;

typedef struct {
  size_t size;
  wasmtime_component_val_t *data;
} wasmtime_component_vallist_t;

typedef struct {
  size_t size;
  wasmtime_component_valrecord_entry_t *data;
} wasmtime_component_valrecord_t;

typedef struct {
  size_t size;
  wasmtime_component_val_t *data;
} wasmtime_component_valtuple_t;

typedef struct {
  size_t size;
  wasm_name_t *data;
} wasmtime_component_valflags_t;

typedef struct {
  wasm_name_t discriminant;
  wasmtime_component_val_t *val;
} wasmtime_component_valvariant_t;

typedef struct {
  bool is_ok;
  wasmtime_component_val_t *val;
} wasmtime_component_valresult_t;

typedef union {
  bool boolean;
  int8_t s8;
  uint8_t u8;
  int16_t s16;
  uint16_t u16;
  int32_t s32;
  uint32_t u32;
  int64_t s64;
  uint64_t u64;
  float f32;
  double f64;
  uint32_t character;
  wasm_name_t string;
  wasmtime_component_vallist_t list;
  wasmtime_component_valrecord_t record;
  wasmtime_component_valtuple_t tuple;
  wasmtime_component_valvariant_t variant;
  wasm_name_t enumeration;
  wasmtime_component_val_t *option;
  wasmtime_component_valresult_t result;
  wasmtime_component_valflags_t flags;
  void *resource;
} wasmtime_component_valunion_t;

struct wasmtime_component_val {
  uint8_t kind;
  wasmtime_component_valunion_t of;
};

struct wasmtime_component_valrecord_entry {
  wasm_name_t name;
  wasmtime_component_val_t val;
};

typedef struct {
  struct {
    uint64_t store_id;
    uint32_t private1;
  } instance;
  uint32_t private2;
} wasmtime_component_func_t;

typedef struct {
  uint64_t store_id;
  uint32_t private1;
} wasmtime_component_instance_t;

_Static_assert(sizeof(wasmtime_component_val_t) == 32, "unexpected v47 component value layout");
_Static_assert(sizeof(wasmtime_component_func_t) == 24, "unexpected v47 component function layout");
_Static_assert(sizeof(wasmtime_component_instance_t) == 16, "unexpected v47 component instance layout");

typedef wasmtime_error_t *(*wasmtime_component_func_callback_t)(
    void *, wasmtime_context_t *, const void *, wasmtime_component_val_t *,
    size_t, wasmtime_component_val_t *, size_t);

extern wasmtime_context_t *wasmtime_store_context(wasmtime_store_t *);
extern wasmtime_component_linker_instance_t *wasmtime_component_linker_root(wasmtime_component_linker_t *);
extern wasmtime_error_t *wasmtime_component_linker_instance_add_instance(
    wasmtime_component_linker_instance_t *, const char *, size_t,
    wasmtime_component_linker_instance_t **);
extern wasmtime_error_t *wasmtime_component_linker_instance_add_func(
    wasmtime_component_linker_instance_t *, const char *, size_t,
    wasmtime_component_func_callback_t, void *, void (*)(void *));
extern void wasmtime_component_linker_instance_delete(wasmtime_component_linker_instance_t *);
extern bool wasmtime_component_instance_get_func(
    const wasmtime_component_instance_t *, wasmtime_context_t *,
    const wasmtime_component_export_index_t *, wasmtime_component_func_t *);
extern wasmtime_error_t *wasmtime_component_func_call(
    const wasmtime_component_func_t *, wasmtime_context_t *,
    const wasmtime_component_val_t *, size_t, wasmtime_component_val_t *, size_t);
extern void wasmtime_component_val_delete(wasmtime_component_val_t *);
extern void wasmtime_error_delete(wasmtime_error_t *);

extern void wasmextHostLog(uint64_t, char *, size_t, char *, size_t);
extern void wasmextHostLogDrop(uint64_t);

static wasmtime_error_t *wasmext_host_log(
    void *env, wasmtime_context_t *context, const void *type,
    wasmtime_component_val_t *args, size_t nargs,
    wasmtime_component_val_t *results, size_t nresults) {
  (void)context;
  (void)type;
  (void)results;
  (void)nresults;
  if (nargs == 2 && args[0].kind == 17 && args[1].kind == 12) {
    wasmextHostLog(*(uint64_t *)env,
      args[0].of.enumeration.data, args[0].of.enumeration.size,
      args[1].of.string.data, args[1].of.string.size);
  }
  return NULL;
}

static void wasmext_host_log_drop(void *env) {
  wasmextHostLogDrop(*(uint64_t *)env);
  free(env);
}

static void wasmext_name(wasm_name_t *out, const char *data, size_t size) {
  out->size = size;
  out->data = NULL;
  if (size != 0) {
    out->data = malloc(size);
    memcpy(out->data, data, size);
  }
}

static void wasmext_val_string(wasmtime_component_val_t *out, const char *data, size_t size) {
  memset(out, 0, sizeof(*out));
  out->kind = 12;
  wasmext_name(&out->of.string, data, size);
}

static void wasmext_val_bool(wasmtime_component_val_t *out, bool value) {
  memset(out, 0, sizeof(*out));
  out->kind = 0;
  out->of.boolean = value;
}

static void wasmext_val_u32(wasmtime_component_val_t *out, uint32_t value) {
  memset(out, 0, sizeof(*out));
  out->kind = 6;
  out->of.u32 = value;
}

static void wasmext_val_s64(wasmtime_component_val_t *out, int64_t value) {
  memset(out, 0, sizeof(*out));
  out->kind = 7;
  out->of.s64 = value;
}

static void wasmext_val_enum(wasmtime_component_val_t *out, const char *data, size_t size) {
  memset(out, 0, sizeof(*out));
  out->kind = 17;
  wasmext_name(&out->of.enumeration, data, size);
}

static void wasmext_val_record(wasmtime_component_val_t *out, size_t size) {
  memset(out, 0, sizeof(*out));
  out->kind = 14;
  out->of.record.size = size;
  out->of.record.data = calloc(size, sizeof(wasmtime_component_valrecord_entry_t));
}

static wasmtime_component_val_t *wasmext_record_field(
    wasmtime_component_val_t *record, size_t index, const char *name, size_t name_size) {
  wasmtime_component_valrecord_entry_t *field = &record->of.record.data[index];
  wasmext_name(&field->name, name, name_size);
  return &field->val;
}

static void wasmext_val_list(wasmtime_component_val_t *out, size_t size) {
  memset(out, 0, sizeof(*out));
  out->kind = 13;
  out->of.list.size = size;
  out->of.list.data = calloc(size, sizeof(wasmtime_component_val_t));
}

static wasmtime_component_val_t *wasmext_list_item(wasmtime_component_val_t *list, size_t index) {
  return &list->of.list.data[index];
}

static uint8_t wasmext_val_kind(const wasmtime_component_val_t *value) { return value->kind; }
static bool wasmext_result_ok(const wasmtime_component_val_t *value) { return value->of.result.is_ok; }
static wasmtime_component_val_t *wasmext_result_value(const wasmtime_component_val_t *value) { return value->of.result.val; }
static bool wasmext_bool(const wasmtime_component_val_t *value) { return value->of.boolean; }
static const char *wasmext_string_data(const wasmtime_component_val_t *value) { return value->of.string.data; }
static size_t wasmext_string_size(const wasmtime_component_val_t *value) { return value->of.string.size; }
static const char *wasmext_enum_data(const wasmtime_component_val_t *value) { return value->of.enumeration.data; }
static size_t wasmext_enum_size(const wasmtime_component_val_t *value) { return value->of.enumeration.size; }
static size_t wasmext_record_size(const wasmtime_component_val_t *value) { return value->of.record.size; }
static wasmtime_component_val_t *wasmext_record_value(const wasmtime_component_val_t *value, size_t index) { return &value->of.record.data[index].val; }
static size_t wasmext_list_size(const wasmtime_component_val_t *value) { return value->of.list.size; }
static wasmtime_component_val_t *wasmext_list_value(const wasmtime_component_val_t *value, size_t index) { return &value->of.list.data[index]; }
static const char *wasmext_variant_data(const wasmtime_component_val_t *value) { return value->of.variant.discriminant.data; }
static size_t wasmext_variant_size(const wasmtime_component_val_t *value) { return value->of.variant.discriminant.size; }
static wasmtime_component_val_t *wasmext_variant_value(const wasmtime_component_val_t *value) { return value->of.variant.val; }

static wasmtime_error_t *wasmext_add_host_log(
    wasmtime_component_linker_t *linker, uint64_t id) {
  wasmtime_component_linker_instance_t *root = wasmtime_component_linker_root(linker);
  wasmtime_component_linker_instance_t *logs = NULL;
  const char *interface_name = "eino-agent:host/log@0.1.0";
  wasmtime_error_t *error = wasmtime_component_linker_instance_add_instance(
      root, interface_name, strlen(interface_name), &logs);
  if (error == NULL) {
    uint64_t *env = malloc(sizeof(uint64_t));
    *env = id;
    error = wasmtime_component_linker_instance_add_func(
        logs, "log", 3, wasmext_host_log, env, wasmext_host_log_drop);
    if (error != NULL) {
      free(env);
    }
  }
  if (logs != NULL) {
    wasmtime_component_linker_instance_delete(logs);
  }
  wasmtime_component_linker_instance_delete(root);
  return error;
}
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
	"go.bytecodealliance.org/cm"

	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
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

func (c *wasmtimeComponent) callABI(_ context.Context, operation string, input, output any) error {
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
	functionName, err := operationFunction(operation)
	if err != nil {
		return err
	}
	functionIndex := instance.GetExportIndex(store, interfaceIndex, functionName)
	if functionIndex == nil {
		return errors.New("component function unavailable")
	}
	defer functionIndex.Close()
	function, err := componentFunction(store, instance, functionIndex)
	if err != nil {
		return err
	}

	arguments, err := componentArguments(operation, input)
	if err != nil {
		return err
	}
	defer closeComponentValues(arguments)
	result, err := callComponentFunction(store, &function, arguments)
	if err != nil {
		return err
	}
	defer closeComponentValue(&result)
	return decodeComponentResult(operation, &result, output, c.limits.MaxOutputBytes)
}

func operationFunction(operation string) (string, error) {
	switch operation {
	case "tool.metadata":
		return "metadata", nil
	case "tool.execute":
		return "execute", nil
	case "permissions-policy.decide":
		return "decide", nil
	case "context-source.load-context":
		return "load-context", nil
	case "event-sink.emit":
		return "emit", nil
	case "hook.before-run":
		return "before-run", nil
	case "hook.before-turn":
		return "before-turn", nil
	case "hook.after-turn":
		return "after-turn", nil
	case "hook.after-run":
		return "after-run", nil
	case "tool-middleware.before-tool-call":
		return "before-tool-call", nil
	case "tool-middleware.after-tool-call":
		return "after-tool-call", nil
	default:
		return "", errors.New("unsupported component operation")
	}
}

func componentArguments(operation string, input any) ([]C.wasmtime_component_val_t, error) {
	switch operation {
	case "tool.metadata":
		return nil, nil
	case "tool.execute":
		request, ok := input.(toolExecuteRequest)
		if !ok {
			return nil, errors.New("invalid tool execute input")
		}
		arguments := make([]C.wasmtime_component_val_t, 3)
		setComponentString(&arguments[0], request.ToolCallID)
		setComponentString(&arguments[1], request.InputJSON)
		arguments[2] = componentTurnMetadata(request.Turn)
		return arguments, nil
	case "permissions-policy.decide":
		request, ok := input.(wittypes.PermissionRequest)
		if !ok {
			return nil, errors.New("invalid permission input")
		}
		return []C.wasmtime_component_val_t{componentPermissionRequest(request)}, nil
	case "context-source.load-context", "hook.before-run", "hook.before-turn", "hook.after-turn", "hook.after-run":
		turn, ok := input.(wittypes.TurnMetadata)
		if !ok {
			return nil, errors.New("invalid turn metadata input")
		}
		return []C.wasmtime_component_val_t{componentTurnMetadata(turn)}, nil
	case "event-sink.emit":
		event, ok := input.(wittypes.BoundedEvent)
		if !ok {
			return nil, errors.New("invalid bounded event input")
		}
		return []C.wasmtime_component_val_t{componentBoundedEvent(event)}, nil
	case "tool-middleware.before-tool-call":
		request, ok := input.(toolMiddlewareBeforeRequest)
		if !ok {
			return nil, errors.New("invalid middleware input")
		}
		arguments := make([]C.wasmtime_component_val_t, 4)
		setComponentString(&arguments[0], request.ToolName)
		setComponentString(&arguments[1], request.ToolCallID)
		setComponentString(&arguments[2], request.InputJSON)
		arguments[3] = componentTurnMetadata(request.Turn)
		return arguments, nil
	case "tool-middleware.after-tool-call":
		request, ok := input.(toolMiddlewareAfterRequest)
		if !ok {
			return nil, errors.New("invalid middleware input")
		}
		arguments := make([]C.wasmtime_component_val_t, 5)
		setComponentString(&arguments[0], request.ToolName)
		setComponentString(&arguments[1], request.ToolCallID)
		setComponentString(&arguments[2], request.InputJSON)
		setComponentString(&arguments[3], request.OutputJSON)
		arguments[4] = componentTurnMetadata(request.Turn)
		return arguments, nil
	default:
		return nil, errors.New("unsupported component operation")
	}
}

func componentTurnMetadata(turn wittypes.TurnMetadata) C.wasmtime_component_val_t {
	record := newComponentRecord([]string{
		"run-id", "session-id", "epoch-id", "agent-name", "agent-mode",
		"provider-id", "model-id", "tool-names", "message-count", "role-counts",
		"has-system-prompt",
	})
	setComponentString(componentRecordFieldForSet(&record, 0), turn.RunID)
	setComponentString(componentRecordFieldForSet(&record, 1), turn.SessionID)
	setComponentString(componentRecordFieldForSet(&record, 2), turn.EpochID)
	setComponentString(componentRecordFieldForSet(&record, 3), turn.AgentName)
	setComponentString(componentRecordFieldForSet(&record, 4), turn.AgentMode)
	setComponentString(componentRecordFieldForSet(&record, 5), turn.ProviderID)
	setComponentString(componentRecordFieldForSet(&record, 6), turn.ModelID)
	setComponentStringList(componentRecordFieldForSet(&record, 7), turn.ToolNames.Slice())
	setComponentU32(componentRecordFieldForSet(&record, 8), turn.MessageCount)
	roleCounts := newComponentRecord([]string{"system", "user", "assistant", "tool"})
	setComponentU32(componentRecordFieldForSet(&roleCounts, 0), turn.RoleCounts.System)
	setComponentU32(componentRecordFieldForSet(&roleCounts, 1), turn.RoleCounts.User)
	setComponentU32(componentRecordFieldForSet(&roleCounts, 2), turn.RoleCounts.Assistant)
	setComponentU32(componentRecordFieldForSet(&roleCounts, 3), turn.RoleCounts.Tool)
	*componentRecordFieldForSet(&record, 9) = roleCounts
	setComponentBool(componentRecordFieldForSet(&record, 10), turn.HasSystemPrompt)
	return record
}

func componentPermissionRequest(request wittypes.PermissionRequest) C.wasmtime_component_val_t {
	record := newComponentRecord([]string{
		"tool-name", "tool-call-id", "permission", "arguments-summary", "session-id", "run-id",
	})
	setComponentString(componentRecordFieldForSet(&record, 0), request.ToolName)
	setComponentString(componentRecordFieldForSet(&record, 1), request.ToolCallID)
	setComponentString(componentRecordFieldForSet(&record, 2), request.Permission)
	setComponentString(componentRecordFieldForSet(&record, 3), request.ArgumentsSummary)
	setComponentString(componentRecordFieldForSet(&record, 4), request.SessionID)
	setComponentString(componentRecordFieldForSet(&record, 5), request.RunID)
	return record
}

func componentBoundedEvent(event wittypes.BoundedEvent) C.wasmtime_component_val_t {
	record := newComponentRecord([]string{"kind", "session-id", "run-id", "message-id", "tool-call-id", "epoch-id", "timestamp-unix-millis", "payload-summary"})
	setComponentString(componentRecordFieldForSet(&record, 0), event.Kind)
	setComponentString(componentRecordFieldForSet(&record, 1), event.SessionID)
	setComponentString(componentRecordFieldForSet(&record, 2), event.RunID)
	setComponentString(componentRecordFieldForSet(&record, 3), event.MessageID)
	setComponentString(componentRecordFieldForSet(&record, 4), event.ToolCallID)
	setComponentString(componentRecordFieldForSet(&record, 5), event.EpochID)
	setComponentS64(componentRecordFieldForSet(&record, 6), event.TimestampUnixMillis)
	setComponentString(componentRecordFieldForSet(&record, 7), event.PayloadSummary)
	return record
}

func decodeComponentResult(operation string, result *C.wasmtime_component_val_t, output any, limit int64) error {
	if operation == "tool-middleware.before-tool-call" || operation == "tool-middleware.after-tool-call" {
		replacement, ok := output.(*wittypes.Replacement)
		if !ok {
			return errors.New("invalid replacement output")
		}
		return decodeReplacement(result, replacement, limit)
	}
	payload, err := componentResult(result)
	if err != nil {
		return err
	}
	switch operation {
	case "tool.metadata":
		metadata, ok := output.(*wittypes.ToolMetadata)
		if !ok {
			return errors.New("invalid tool metadata output")
		}
		return decodeToolMetadata(payload, metadata, limit)
	case "tool.execute":
		text, err := componentString(payload, limit)
		if err != nil {
			return err
		}
		outputText, ok := output.(*string)
		if !ok {
			return errors.New("invalid tool execute output")
		}
		*outputText = text
		return nil
	case "permissions-policy.decide":
		decision, ok := output.(*wittypes.PermissionDecision)
		if !ok {
			return errors.New("invalid permission output")
		}
		return decodePermissionDecision(payload, decision, limit)
	case "context-source.load-context":
		messages, ok := output.(*[]wittypes.TextMessage)
		if !ok {
			return errors.New("invalid context output")
		}
		return decodeTextMessages(payload, messages, limit)
	case "event-sink.emit", "hook.before-run", "hook.before-turn", "hook.after-turn", "hook.after-run":
		return nil
	default:
		return errors.New("unsupported component operation")
	}
}

func decodeTextMessages(value *C.wasmtime_component_val_t, output *[]wittypes.TextMessage, limit int64) error {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindList {
		return errors.New("component returned an unexpected message list")
	}
	count := int(C.wasmext_list_size(value))
	if count > 4096 {
		return errModuleTooLarge
	}
	messages := make([]wittypes.TextMessage, 0, count)
	var total int64
	for index := 0; index < count; index++ {
		item := C.wasmext_list_value(value, C.size_t(index))
		roleValue, err := componentRecordField(item, 0, 2)
		if err != nil {
			return err
		}
		textValue, err := componentRecordField(item, 1, 2)
		if err != nil {
			return err
		}
		role, err := componentEnum(roleValue)
		if err != nil {
			return err
		}
		text, err := componentString(textValue, limit-total)
		if err != nil {
			return err
		}
		total += int64(len(text))
		message := wittypes.TextMessage{Text: text}
		switch role {
		case "system":
			message.Role = wittypes.TextRoleSystem
		case "user":
			message.Role = wittypes.TextRoleUser
		case "assistant":
			message.Role = wittypes.TextRoleAssistant
		default:
			return errors.New("component returned an invalid text role")
		}
		messages = append(messages, message)
	}
	*output = messages
	return nil
}

func decodeReplacement(value *C.wasmtime_component_val_t, output *wittypes.Replacement, limit int64) error {
	if value == nil || int(C.wasmext_val_kind(value)) != componentKindVariant {
		return errors.New("component returned an unexpected replacement")
	}
	name := C.GoStringN(C.wasmext_variant_data(value), C.int(C.wasmext_variant_size(value)))
	payload := C.wasmext_variant_value(value)
	switch name {
	case "unchanged":
		*output = wittypes.ReplacementUnchanged()
		return nil
	case "json":
		text, err := componentString(payload, limit)
		if err != nil {
			return err
		}
		*output = wittypes.ReplacementJSON(text)
		return nil
	case "error":
		guestErr, err := decodeStructuredError(payload, limit)
		if err != nil {
			return err
		}
		*output = wittypes.ReplacementError(guestErr)
		return nil
	default:
		return errors.New("component returned an invalid replacement case")
	}
}

func decodeStructuredError(value *C.wasmtime_component_val_t, limit int64) (wittypes.StructuredError, error) {
	codeValue, err := componentRecordField(value, 0, 3)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	messageValue, err := componentRecordField(value, 1, 3)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	retryableValue, err := componentRecordField(value, 2, 3)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	code, err := componentString(codeValue, limit)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	message, err := componentString(messageValue, limit-int64(len(code)))
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	retryable, err := componentBool(retryableValue)
	if err != nil {
		return wittypes.StructuredError{}, err
	}
	return wittypes.StructuredError{Code: code, Message: message, Retryable: retryable}, nil
}

func decodeToolMetadata(payload *C.wasmtime_component_val_t, metadata *wittypes.ToolMetadata, limit int64) error {
	fields := make([]*C.wasmtime_component_val_t, 5)
	for index := range fields {
		field, err := componentRecordField(payload, index, len(fields))
		if err != nil {
			return err
		}
		fields[index] = field
	}
	var err error
	if metadata.Name, err = componentString(fields[0], limit); err != nil {
		return err
	}
	remaining := limit - int64(len(metadata.Name))
	if metadata.Description, err = componentString(fields[1], remaining); err != nil {
		return err
	}
	remaining -= int64(len(metadata.Description))
	if metadata.ParametersJSONSchema, err = componentString(fields[2], remaining); err != nil {
		return err
	}
	if metadata.RetrySafe, err = componentBool(fields[3]); err != nil {
		return err
	}
	remaining -= int64(len(metadata.ParametersJSONSchema))
	permissions, _, err := componentStringList(fields[4], remaining, 1024)
	if err != nil {
		return err
	}
	metadata.RequiredPermissions = cm.ToList(permissions)
	return nil
}

func decodePermissionDecision(payload *C.wasmtime_component_val_t, decision *wittypes.PermissionDecision, limit int64) error {
	actionValue, err := componentRecordField(payload, 0, 2)
	if err != nil {
		return err
	}
	reasonValue, err := componentRecordField(payload, 1, 2)
	if err != nil {
		return err
	}
	action, err := componentEnum(actionValue)
	if err != nil {
		return err
	}
	switch action {
	case "allow":
		decision.Action = wittypes.PermissionActionAllow
	case "deny":
		decision.Action = wittypes.PermissionActionDeny
	case "ask":
		decision.Action = wittypes.PermissionActionAsk
	default:
		return errors.New("component returned an invalid permission action")
	}
	decision.Reason, err = componentString(reasonValue, limit)
	return err
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
