/* vim: set ts=8 sts=2 et sw=2: */

#pragma once

#include <stdint.h>
#include "buffer_allocator.h"
#include "errors.h"

typedef struct _buffer_scheduler
{
  buffer_allocator_t allocator;

  uint64_t buffer_size;

  uint64_t nr_free_buffers;
  uint64_t nr_assigned_buffers;

  uint64_t max_free_buffers;
  uint64_t min_free_buffers;

  uint64_t max_pool_size;

  pthread_mutex_t lock;
} buffer_scheduler_t;

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size, uint64_t max_pool_size);
error_code sched_destroy(buffer_scheduler_t *bufsched);
error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t **buffer, cls_buf_handle_t bh);
error_code sched_free(buffer_scheduler_t *bufsched, cls_buf_t *buffer);

