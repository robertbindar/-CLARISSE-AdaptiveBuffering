/* vim: set ts=8 sts=2 et sw=2: */

#pragma once

#include <stdint.h>
#include "buffer_allocator.h"
#include "buffer_swapper.h"
#include "errors.h"
#include "buffering_types.h"
#include "worker.h"

typedef struct _buffer_scheduler
{
  allocator_t allocator_data;
  allocator_t allocator_md;

  buffer_swapper_t swapper;

  uint64_t buffer_size;

  uint64_t nr_free_buffers;
  uint64_t nr_assigned_buffers;

  uint64_t max_free_buffers;
  uint64_t min_free_buffers;

  uint64_t max_pool_size;
  uint64_t swapin_pool_limit;

  pthread_mutex_t lock;

  pthread_cond_t free_buffers_available;

  dllist mrucache;

  worker_t worker;
  task_queue_t task_queue;
} buffer_scheduler_t;

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size, uint64_t max_pool_size);
error_code sched_destroy(buffer_scheduler_t *bufsched);
error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t *buffer);
error_code sched_alloc_md(buffer_scheduler_t *bufsched, cls_buf_t **buffer, cls_buf_handle_t bh);
error_code sched_free(buffer_scheduler_t *bufsched, cls_buf_t *buffer);
error_code sched_free_unsafe(buffer_scheduler_t *bufsched, cls_buf_t *buffer);

void sched_mark_updated(buffer_scheduler_t *bufsched, cls_buf_t *buf);

uint8_t sched_mark_consumed(buffer_scheduler_t *bufsched, cls_buf_t *buf);

error_code sched_swapin(buffer_scheduler_t *bufsched, cls_buf_t *buf);

void copy_buf_handle(cls_buf_handle_t *dest, const cls_buf_handle_t *src);

error_code destroy_buffer(cls_buf_t *buff);

