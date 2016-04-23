/* vim: set ts=8 sts=2 et sw=2: */

#pragma once

#include <stdint.h>
#include "buffer_allocator.h"
#include "buffer_swapper.h"
#include "errors.h"
#include "buffering_types.h"

typedef struct _buffer_scheduler
{
  buffer_allocator_t allocator;
  buffer_swapper_t swapper;

  uint64_t capacity;

  uint64_t nr_swapout;

  uint64_t buffer_size;

  uint64_t nr_free_buffers;
  uint64_t nr_assigned_buffers;

  uint64_t max_free_buffers;
  uint64_t min_free_buffers;

  uint64_t max_pool_size;

  pthread_mutex_t lock;

  uint8_t resize_alloc;
  uint8_t shrink_alloc;
  pthread_cond_t cond_alloc;

  dllist mrucache;
} buffer_scheduler_t;

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size, uint64_t max_pool_size);
error_code sched_destroy(buffer_scheduler_t *bufsched);
error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t **buffer, cls_buf_handle_t bh);
error_code sched_free(buffer_scheduler_t *bufsched, cls_buf_t *buffer);

void sched_mark_updated(buffer_scheduler_t *bufsched, cls_buf_t *buf);

void sched_swapin(buffer_scheduler_t *bufsched, cls_buf_t *buf);

