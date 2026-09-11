#ifndef MERIDIAN_STORAGE_H
#define MERIDIAN_STORAGE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct MeridianStorage MeridianStorage;

enum {
  MERIDIAN_STORAGE_OK = 0,
  MERIDIAN_STORAGE_INVALID_ARGUMENT = 1,
  MERIDIAN_STORAGE_IO_ERROR = 2,
  MERIDIAN_STORAGE_PANIC = 3,
};

int32_t meridian_storage_open(const char *data_dir, MeridianStorage **out_storage);
void meridian_storage_close(MeridianStorage *storage);
int32_t meridian_storage_put(MeridianStorage *storage, const uint8_t *key, size_t key_length,
                             const uint8_t *value, size_t value_length);
int32_t meridian_storage_delete(MeridianStorage *storage, const uint8_t *key, size_t key_length);
int32_t meridian_storage_get(MeridianStorage *storage, const uint8_t *key, size_t key_length,
                             bool *out_found, uint8_t **out_data, size_t *out_length,
                             size_t *out_capacity);
int32_t meridian_storage_flush(MeridianStorage *storage);
int32_t meridian_storage_compact(MeridianStorage *storage);
int32_t meridian_storage_free_buffer(uint8_t *data, size_t length, size_t capacity);

#endif
