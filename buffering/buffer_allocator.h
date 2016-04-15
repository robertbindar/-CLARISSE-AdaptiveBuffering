#pragma once

#include <stdint.h>
#include <pthread.h>
#include "errors.h"
#include "uthash.h"
#include "list.h"

typedef struct _cls_buf cls_buf_t;
typedef struct _cls_buf_handle  cls_buf_handle_t;

typedef struct
{
  // Tracks the buffers that are available to be fetched from the library
  dllist free_buffers;

  // Hashtable tracking buffers that are not currently available
  cls_buf_t *assigned_buffers;

  pthread_mutex_t lock;

  // All the allocated buffers have a fixed size established when calling
  // allocator_init
  uint64_t buffer_size;
} buffer_allocator_t;

error_code allocator_init(buffer_allocator_t *allocator, uint64_t buf_size);

// Assigns a free buffer to *data
// If there are no available buffers, it returns BUFALLOCATOR_FREEBUF_ERR
error_code allocator_get(buffer_allocator_t *allocator, cls_buf_t **buffer, cls_buf_handle_t bh);

// Inserts a free buffer into the pool. If "data" was previously assigned,
// it is moved from the assigned state to free.
// A common use-case is to fetch a buffer, use it, then put it back into the
// pool.
error_code allocator_put(buffer_allocator_t *allocator, cls_buf_t *buffer);

// Deletes "count" free buffers from the library. It might be used to free-up
// memory when the number of free buffers exceeds a specific threshold.
error_code allocator_shrink(buffer_allocator_t *allocator, uint64_t count);

// Allocates "count" free buffers. A common use-case is to increase the number
// of free buffers when it drops below an established threshold.
error_code allocator_new(buffer_allocator_t *allocator, uint64_t count);

error_code allocator_destroy(buffer_allocator_t *allocator);

void copy_buf_handle(cls_buf_handle_t *dest, cls_buf_handle_t *src);

error_code destroy_buffer(cls_buf_t *buff);

