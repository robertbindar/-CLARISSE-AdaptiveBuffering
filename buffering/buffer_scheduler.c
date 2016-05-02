/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_scheduler.h"
#include <pthread.h>
#include <stdio.h>

static inline void mrucache_put(buffer_scheduler_t *bufsched, cls_buf_t *buf);
static inline void mrucache_get(buffer_scheduler_t *bufsched, cls_buf_t **buf);
static inline void mrucache_remove(buffer_scheduler_t *bufsched, cls_buf_t *buf);

static void *stretch_allocator(void *arg)
{
  buffer_scheduler_t *bufsched = (buffer_scheduler_t *) arg;

  while (1) {
    // Wait for event
    pthread_mutex_lock(&bufsched->lock);
    while (!bufsched->async_shrink && !bufsched->async_expand &&
           !bufsched->async_swapout && !bufsched->async_cancel) {
      pthread_cond_wait(&bufsched->cond_alloc, &bufsched->lock);
    }

    if (bufsched->async_cancel) {
      pthread_mutex_unlock(&bufsched->lock);
      return NULL;
    }

    if (bufsched->async_shrink) {
      uint64_t count = bufsched->nr_free_buffers / 2;
      bufsched->async_shrink = 0;
      pthread_mutex_unlock(&bufsched->lock);

      uint64_t k = 0;
      cls_buf_t *buf = NULL;
      for (k = 0; k < count; ++k) {
        if (!(buf = swapper_top(&bufsched->swapper))) {
          break;
        }
        pthread_mutex_lock(&buf->lock_read);

        if (buf->is_swapped) {
          pthread_mutex_lock(&bufsched->lock);

          if (bufsched->nr_free_buffers == 0) {
            pthread_mutex_unlock(&bufsched->lock);
            pthread_mutex_unlock(&buf->lock_read);
            break;
          }
          allocator_get(&bufsched->allocator, buf);
          bufsched->nr_assigned_buffers++;
          bufsched->nr_free_buffers--;

          pthread_mutex_unlock(&bufsched->lock);

          swapper_swapin(&bufsched->swapper, buf);
        }
        pthread_mutex_unlock(&buf->lock_read);
      }
      count -= k;

      for (uint64_t i = 0; i < count; ++i) {
        pthread_mutex_lock(&bufsched->lock);
        bufsched->nr_free_buffers--;
        allocator_shrink(&bufsched->allocator, 1);
        pthread_mutex_unlock(&bufsched->lock);
      }
      bufsched->capacity -= count;
    } else if (bufsched->async_expand) {
      bufsched->async_expand = 0;

      uint64_t count = bufsched->max_pool_size - bufsched->nr_free_buffers - bufsched->nr_assigned_buffers;
      if (count > bufsched->capacity) {
        count = bufsched->capacity;
        bufsched->capacity = bufsched->capacity * 2;
      } else if (count > bufsched->max_free_buffers) {
        count = bufsched->max_free_buffers - 1;
        bufsched->capacity += count;
      } else {
        bufsched->capacity += count;
      }
      pthread_mutex_unlock(&bufsched->lock);

      for (uint64_t i = 0; i < count; ++i) {
        pthread_mutex_lock(&bufsched->lock);
        bufsched->nr_free_buffers++;
        allocator_new(&bufsched->allocator, 1);
        pthread_cond_broadcast(&bufsched->free_buffers_available);
        pthread_mutex_unlock(&bufsched->lock);
      }
    } else {
      bufsched->async_swapout--;

      pthread_mutex_unlock(&bufsched->lock);

      // Call the swapper to freeup some memory
      for (uint64_t i = 0; i < bufsched->nr_swapout; ++i) {
        cls_buf_t *buf = NULL;

        mrucache_get(bufsched, &buf);
        if (!buf) {
          break;
        }

        pthread_mutex_lock(&buf->lock_read);
        if (buf->freed_by_swapper) {
          pthread_mutex_unlock(&buf->lock_read);
          sched_free(bufsched, buf);
          continue;
        }
        pthread_mutex_unlock(&buf->lock_read);

        pthread_rwlock_wrlock(&buf->rwlock_swap);
        pthread_mutex_lock(&buf->lock_read);

        swapper_swapout_lockfree(&bufsched->swapper, buf);

        pthread_mutex_lock(&bufsched->lock);
        allocator_move_to_free(&bufsched->allocator, buf);
        bufsched->nr_free_buffers++;
        bufsched->nr_assigned_buffers--;
        pthread_cond_broadcast(&bufsched->free_buffers_available);
        pthread_mutex_unlock(&bufsched->lock);

        pthread_mutex_unlock(&buf->lock_read);
        pthread_rwlock_unlock(&buf->rwlock_swap);
      }
    }
  }

  return NULL;
}

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size,
                      uint64_t max_pool_size)
{
  pthread_mutex_init(&bufsched->lock, NULL);

  bufsched->buffer_size = buffer_size;
  bufsched->max_pool_size = max_pool_size;
  bufsched->nr_assigned_buffers = 0;

  // min_free_buffers has to be bigger than 0
  bufsched->min_free_buffers = 40;
  bufsched->max_free_buffers = max_pool_size / 4 + 1;

  bufsched->capacity = 50;
  bufsched->nr_free_buffers = bufsched->capacity;

  pthread_cond_init(&bufsched->cond_alloc, NULL);
  pthread_cond_init(&bufsched->free_buffers_available, NULL);
  bufsched->async_expand = 0;
  bufsched->async_shrink = 0;
  bufsched->async_swapout = 0;
  bufsched->async_cancel = 0;

  allocator_init(&bufsched->allocator, buffer_size);

  allocator_new(&bufsched->allocator, bufsched->nr_free_buffers);

  // dispatch async allocator
  pthread_create(&bufsched->worker_alloc_tid, NULL, stretch_allocator, bufsched);

  dllist_init(&bufsched->mrucache);
  bufsched->nr_swapout = 1;
  swapper_init(&bufsched->swapper, bufsched->buffer_size);

  return BUFFERING_SUCCESS;
}

error_code sched_destroy(buffer_scheduler_t *bufsched)
{

  // Signal the worker thread to terminate execution
  pthread_mutex_lock(&bufsched->lock);
  bufsched->async_cancel = 1;
  pthread_cond_signal(&bufsched->cond_alloc);
  pthread_mutex_unlock(&bufsched->lock);

  // Wait for the worker thread to terminate
  pthread_join(bufsched->worker_alloc_tid, NULL);

  pthread_cond_destroy(&bufsched->cond_alloc);

  pthread_mutex_destroy(&bufsched->lock);

  pthread_cond_destroy(&bufsched->free_buffers_available);

  allocator_destroy(&bufsched->allocator);

  swapper_destroy(&bufsched->swapper);

  return BUFFERING_SUCCESS;
}

error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{

  // If there are no more free buffers, allocate more. This means we could
  // insert some free buffers into the allocator or call the swapper to freeup
  // some memory if there's no available memory.
  pthread_mutex_lock(&bufsched->lock);
  if (bufsched->nr_free_buffers <= bufsched->min_free_buffers) {
    if (bufsched->nr_free_buffers + bufsched->nr_assigned_buffers >= bufsched->max_pool_size) {
      bufsched->async_swapout++;
    } else {
      bufsched->async_expand = 1;
    }
    pthread_cond_signal(&bufsched->cond_alloc);
  }

  // Block if there are no free buffers
  while (bufsched->nr_free_buffers == 0) {
    pthread_cond_wait(&bufsched->free_buffers_available, &bufsched->lock);
  }
  allocator_get(&bufsched->allocator, buffer);
  bufsched->nr_assigned_buffers++;
  bufsched->nr_free_buffers--;

  pthread_mutex_unlock(&bufsched->lock);

  return BUFFERING_SUCCESS;
}

error_code sched_alloc_md(buffer_scheduler_t *bufsched, cls_buf_t **buffer, cls_buf_handle_t bh)
{
  return allocator_get_md(&bufsched->allocator, buffer, bh);
}

error_code sched_free(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{
  pthread_mutex_lock(&bufsched->lock);

  allocator_put(&bufsched->allocator, buffer);

  bufsched->nr_assigned_buffers--;
  bufsched->nr_free_buffers++;
  pthread_cond_broadcast(&bufsched->free_buffers_available);

  // Release some memory, there are too many free buffers allocated
  // If there are buffers swapped on the disk, bring them in to speed up
  // upcoming consumers
  if (bufsched->nr_free_buffers == bufsched->max_free_buffers) {
    bufsched->async_shrink = 1;
    pthread_cond_signal(&bufsched->cond_alloc);
  }

  pthread_mutex_unlock(&bufsched->lock);

  return BUFFERING_SUCCESS;
}

void sched_mark_updated(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
  mrucache_put(bufsched, buf);
}

uint8_t sched_mark_consumed(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
  pthread_mutex_lock(&bufsched->lock);
  if (!buf->link_mru.next && !buf->link_mru.prev) {
    pthread_mutex_unlock(&bufsched->lock);
    return 0;
  }

  dllist_rem(&bufsched->mrucache, &buf->link_mru);
  pthread_mutex_unlock(&bufsched->lock);

  return 1;
}

void sched_swapin(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
  pthread_mutex_lock(&bufsched->lock);
  if (bufsched->nr_free_buffers == 0) {
    allocator_new(&bufsched->allocator, 1);
    bufsched->nr_free_buffers++;
  }

  allocator_get(&bufsched->allocator, buf);
  bufsched->nr_assigned_buffers++;
  bufsched->nr_free_buffers--;

  pthread_mutex_unlock(&bufsched->lock);

  swapper_swapin(&bufsched->swapper, buf);
}

static inline void mrucache_put(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
  pthread_mutex_lock(&bufsched->lock);
  dllist_iah(&bufsched->mrucache, &buf->link_mru);
  pthread_mutex_unlock(&bufsched->lock);
}

static inline void mrucache_get(buffer_scheduler_t *bufsched, cls_buf_t **buf)
{
  pthread_mutex_lock(&bufsched->lock);
  dllist_link *tmp = dllist_rem_head(&bufsched->mrucache);
  if (!tmp) {
    *buf = NULL;
  } else {
    *buf = DLLIST_ELEMENT(tmp, cls_buf_t, link_mru);
  }
  pthread_mutex_unlock(&bufsched->lock);
}

static inline void mrucache_remove(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
  pthread_mutex_lock(&bufsched->lock);
  dllist_rem(&bufsched->mrucache, &buf->link_mru);
  pthread_mutex_unlock(&bufsched->lock);
}

