/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_scheduler.h"
#include "buffering_types.h"
#include <stdio.h>

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size,
                      uint64_t max_pool_size)
{
  HANDLE_ERR(pthread_mutex_init(&bufsched->lock, NULL), BUFSCHEDULER_LOCK_ERR);

  bufsched->buffer_size = buffer_size;
  bufsched->max_pool_size = max_pool_size;
  bufsched->nr_assigned_buffers = 0;

  // min_free_buffers has to be bigger than 0
  bufsched->min_free_buffers = 1;
  bufsched->max_free_buffers = max_pool_size / 4 + 1;

  bufsched->capacity = 10;
  bufsched->nr_free_buffers = bufsched->capacity;

  allocator_init(&bufsched->allocator, buffer_size);

  allocator_new(&bufsched->allocator, bufsched->nr_free_buffers);

  return BUFFERING_SUCCESS;
}

error_code sched_destroy(buffer_scheduler_t *bufsched)
{
  HANDLE_ERR(pthread_mutex_destroy(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);

  allocator_destroy(&bufsched->allocator);

  return BUFFERING_SUCCESS;
}

static void stretch_allocator(buffer_scheduler_t *bufsched)
{
  if (bufsched->nr_free_buffers + bufsched->nr_assigned_buffers >= bufsched->max_pool_size) {
    // TODO: call swapper
    /*fprintf(stderr, "call the swapper \n");*/
  }

  uint64_t count = bufsched->max_pool_size - bufsched->nr_free_buffers - bufsched->nr_assigned_buffers;
  if (count > bufsched->capacity) {
    count = bufsched->capacity;
    bufsched->capacity = bufsched->capacity * 2;
  } else if (count > bufsched->max_free_buffers) {
    bufsched->capacity += bufsched->max_free_buffers - 1;
  } else {
    bufsched->capacity += count;
  }

  /*fprintf(stderr, "Stretch to %d\n", bufsched->capacity);*/

  allocator_new(&bufsched->allocator, count);
  bufsched->nr_free_buffers += count;
}

static void shrink_allocator(buffer_scheduler_t *bufsched)
{
  uint64_t count = bufsched->nr_free_buffers / 2;
  bufsched->capacity -= count;

  allocator_shrink(&bufsched->allocator, count);

  bufsched->nr_free_buffers -= count;
  /*fprintf(stderr, "Shrink to %d\n", bufsched->capacity);*/
}

error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t **buffer, cls_buf_handle_t bh)
{
  /*HANDLE_ERR(pthread_mutex_lock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);*/

  // If there are no more free buffers, allocate more. This means we could
  // insert some free buffers into the allocator or call the swapper to freeup
  // some memory if there's no available memory.
  if (bufsched->nr_free_buffers == bufsched->min_free_buffers) {
    stretch_allocator(bufsched);
  }
  bufsched->nr_assigned_buffers++;
  bufsched->nr_free_buffers--;

  allocator_get(&bufsched->allocator, buffer, bh);

  /*HANDLE_ERR(pthread_mutex_unlock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);*/

  return BUFFERING_SUCCESS;
}

error_code sched_free(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{
  /*HANDLE_ERR(pthread_mutex_lock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);*/

  allocator_put(&bufsched->allocator, buffer);

  bufsched->nr_assigned_buffers--;
  bufsched->nr_free_buffers++;

  // Release some memory, there are too many free buffers allocated
  if (bufsched->nr_free_buffers == bufsched->max_free_buffers) {
    shrink_allocator(bufsched);
  }

  /*HANDLE_ERR(pthread_mutex_unlock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);*/

  return BUFFERING_SUCCESS;
}

