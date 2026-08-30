
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
