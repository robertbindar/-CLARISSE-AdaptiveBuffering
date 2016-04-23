#pragma once

#include <stdint.h>
#include "uthash.h"
#include "buffering_types.h"

#define MAX_FILENAME_SIZE 256

typedef struct
{
  UT_hash_handle hh;
  cls_buf_handle_t handle;
  uint64_t file_offset;
} swap_entry_t;

typedef struct _buffer_swapper
{
  char dirname[MAX_FILENAME_SIZE];
  swap_entry_t *entries;
  uint64_t bufsize;
  pthread_mutex_t lock;
} buffer_swapper_t;

void swapper_init(buffer_swapper_t *sw, uint64_t bufsize);

void swapper_swapin(buffer_swapper_t *sw, cls_buf_t *buf);

void swapper_swapout(buffer_swapper_t *sw, cls_buf_t *buf);

void swapper_destroy(buffer_swapper_t *sw);

