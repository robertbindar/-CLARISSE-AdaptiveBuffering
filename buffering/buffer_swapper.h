#pragma once

#include <stdint.h>
#include "uthash.h"
#include "list.h"
#include "buffering_types.h"

#define MAX_FILENAME_SIZE 256

typedef struct
{
  UT_hash_handle hh;
  cls_buf_t *buf;
  uint64_t file_offset;
} swap_entry_t;

typedef struct
{
  dllist_link link;
  uint64_t offset;
} free_off_t;

typedef struct _buffer_swapper
{
  char dirname[MAX_FILENAME_SIZE];
  swap_entry_t *entries;
  uint64_t entries_count;
  uint64_t bufsize;
  dllist disk_free_offsets;
  pthread_mutex_t lock;
} buffer_swapper_t;

void swapper_init(buffer_swapper_t *sw, uint64_t bufsize);

void swapper_swapin(buffer_swapper_t *sw, cls_buf_t *buf);

void swapper_swapout(buffer_swapper_t *sw, cls_buf_t *buf);

void swapper_swapout_lockfree(buffer_swapper_t *sw, cls_buf_t *buf);

cls_buf_t *swapper_top(buffer_swapper_t *sw);

uint64_t swapper_getcount(buffer_swapper_t *sw);

uint8_t swapper_find(buffer_swapper_t *sw, cls_buf_t *buf);

void swapper_destroy(buffer_swapper_t *sw);

