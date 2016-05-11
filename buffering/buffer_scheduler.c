/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_scheduler.h"
#include <pthread.h>
#include <stdio.h>

static inline void mrucache_put(buffer_scheduler_t *bufsched, cls_buf_t *buf);
static inline void mrucache_get(buffer_scheduler_t *bufsched, cls_buf_t **buf);
static inline void mrucache_remove(buffer_scheduler_t *bufsched, cls_buf_t *buf);
static inline uint64_t swapin_buffers(buffer_scheduler_t *bufsched, uint64_t count);
static inline void expand_alloc(buffer_scheduler_t *bufsched, uint64_t count);
static inline void swapout_buffers(buffer_scheduler_t *bufsched);

void copy_buf_handle(cls_buf_handle_t *dest, cls_buf_handle_t *src)
{
  memset(dest, 0, sizeof(cls_buf_handle_t));
  dest->offset = src->offset;
  dest->global_descr = src->global_descr;
}

static void init_buf_fields(cls_buf_t *buffer)
{
  buffer->nr_coll_participants = 0;
  buffer->nr_consumers_finished = 0;
  buffer->ready = 0;
  buffer->is_swapped = 0;
  buffer->was_swapped_in = 0;
  buffer->freed_by_swapper = 0;
  buffer->data = NULL;
  buffer->link_mru.next = NULL;
  buffer->link_mru.prev = NULL;
}

static void init_buffer(cls_buf_t *buffer)
{
  init_buf_fields(buffer);

  pthread_mutex_init(&buffer->lock_read, NULL);
  pthread_rwlock_init(&buffer->rwlock_swap, NULL);
  pthread_cond_init(&buffer->buf_ready, NULL);
}

void destroy_buffer(cls_buf_t *buff)
{
  pthread_cond_destroy(&buff->buf_ready);
  pthread_mutex_destroy(&buff->lock_read);
  pthread_rwlock_destroy(&buff->rwlock_swap);
}

static void *stretch_allocator(void *arg)
{
  buffer_scheduler_t *bufsched = (buffer_scheduler_t *) arg;

  while (1) {
    // Wait for event
    pthread_mutex_lock(&bufsched->lock_worker);
    while (!bufsched->async_shrink && !bufsched->async_expand &&
           !bufsched->async_swapout && !bufsched->async_cancel) {
      pthread_cond_wait(&bufsched->cond_alloc, &bufsched->lock_worker);
    }

    if (bufsched->async_cancel) {
      pthread_mutex_unlock(&bufsched->lock_worker);
      return NULL;
    } else if (bufsched->async_shrink) {
      bufsched->async_shrink = 0;

      pthread_mutex_lock(&bufsched->lock);
      uint32_t count = allocator_shrink(&bufsched->allocator_data);
      bufsched->nr_free_buffers -= count;
      bufsched->capacity -= count;
      pthread_mutex_unlock(&bufsched->lock);

      /*uint64_t k = swapin_buffers(bufsched, count);*/

      /*count -= k;*/

    } else if (bufsched->async_expand) {
      bufsched->async_expand = 0;

      pthread_mutex_lock(&bufsched->lock);

      /*uint64_t count = bufsched->max_pool_size - bufsched->nr_free_buffers - bufsched->nr_assigned_buffers;*/

      /*if (count > bufsched->capacity / 4) {*/
        /*count = bufsched->capacity / 4;*/
      /*}*/
      /*if (count > bufsched->max_free_buffers - bufsched->nr_free_buffers) {*/
        /*count = bufsched->max_free_buffers - bufsched->nr_free_buffers;*/
      /*}*/

      uint32_t count = allocator_expand(&bufsched->allocator_data);
      bufsched->nr_free_buffers += count;
      pthread_cond_broadcast(&bufsched->free_buffers_available);
      pthread_mutex_unlock(&bufsched->lock);

    } else {
      bufsched->async_swapout--;

      // Call the swapper to freeup some memory
      /*swapout_buffers(bufsched);*/
      fprintf(stderr, "swapout buffers\n");
    }

    pthread_mutex_unlock(&bufsched->lock_worker);
  }

  return NULL;
}

/*static inline uint64_t swapin_buffers(buffer_scheduler_t *bufsched, uint64_t count)*/
/*{*/
  /*uint64_t k = 0;*/
  /*cls_buf_t *buf = NULL;*/
  /*for (k = 0; k < count; ++k) {*/
    /*if (!(buf = swapper_top(&bufsched->swapper))) {*/
      /*break;*/
    /*}*/

    /*pthread_mutex_lock(&buf->lock_read);*/

    /*if (buf->is_swapped && !buf->was_swapped_in) {*/
      /*pthread_mutex_lock(&bufsched->lock);*/

      /*if (bufsched->nr_free_buffers <= bufsched->min_free_buffers) {*/
        /*pthread_mutex_unlock(&bufsched->lock);*/
        /*pthread_mutex_unlock(&buf->lock_read);*/
        /*break;*/
      /*}*/
      /*allocator_get(&bufsched->allocator, buf);*/
      /*bufsched->nr_assigned_buffers++;*/
      /*bufsched->nr_free_buffers--;*/

      /*pthread_mutex_unlock(&bufsched->lock);*/

      /*swapper_swapin(&bufsched->swapper, buf);*/
      /*buf->was_swapped_in = 1;*/
    /*}*/
    /*pthread_mutex_unlock(&buf->lock_read);*/
  /*}*/

  /*return k;*/
/*}*/

/*static inline void expand_alloc(buffer_scheduler_t *bufsched, uint64_t count)*/
/*{*/
  /*for (uint64_t i = 0; i < count; ++i) {*/
    /*allocator_new(&bufsched->allocator, 1);*/

    /*pthread_mutex_lock(&bufsched->lock);*/
    /*bufsched->nr_free_buffers++;*/
    /*pthread_cond_broadcast(&bufsched->free_buffers_available);*/
    /*pthread_mutex_unlock(&bufsched->lock);*/
  /*}*/
/*}*/

/*static inline void swapout_buffers(buffer_scheduler_t *bufsched)*/
/*{*/
  /*for (uint64_t i = 0; i < bufsched->nr_swapout; ++i) {*/
    /*cls_buf_t *buf = NULL;*/

    /*mrucache_get(bufsched, &buf);*/
    /*if (!buf) {*/
      /*break;*/
    /*}*/

    /*pthread_rwlock_wrlock(&buf->rwlock_swap);*/
    /*pthread_mutex_lock(&buf->lock_read);*/
    /*if (buf->freed_by_swapper) {*/
      /*pthread_rwlock_unlock(&buf->rwlock_swap);*/
      /*pthread_mutex_unlock(&buf->lock_read);*/
      /*sched_free(bufsched, buf);*/
      /*continue;*/
    /*}*/

    /*swapper_swapout_lockfree(&bufsched->swapper, buf);*/

    /*allocator_move_to_free(&bufsched->allocator, buf);*/

    /*pthread_mutex_lock(&bufsched->lock);*/
    /*bufsched->nr_free_buffers++;*/
    /*bufsched->nr_assigned_buffers--;*/
    /*pthread_cond_broadcast(&bufsched->free_buffers_available);*/
    /*pthread_mutex_unlock(&bufsched->lock);*/

    /*pthread_mutex_unlock(&buf->lock_read);*/
    /*pthread_rwlock_unlock(&buf->rwlock_swap);*/
  /*}*/
/*}*/

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size,
                      uint64_t max_pool_size)
{
  pthread_mutex_init(&bufsched->lock, NULL);
  pthread_mutex_init(&bufsched->lock_worker, NULL);

  bufsched->buffer_size = buffer_size;
  bufsched->nr_assigned_buffers = 0;

  bufsched->min_free_buffers = 5 * max_pool_size / 100 + 1;
  bufsched->max_free_buffers = 50 * max_pool_size / 100 + 2;

  bufsched->max_pool_size = max_pool_size;

  bufsched->capacity = bufsched->min_free_buffers + 1;
  bufsched->nr_free_buffers = bufsched->capacity;

  pthread_cond_init(&bufsched->cond_alloc, NULL);
  pthread_cond_init(&bufsched->free_buffers_available, NULL);
  bufsched->async_expand = 0;
  bufsched->async_shrink = 0;
  bufsched->async_swapout = 0;
  bufsched->async_cancel = 0;

  allocator_init(&bufsched->allocator_md, sizeof(cls_buf_t), max_pool_size);
  allocator_init(&bufsched->allocator_data, buffer_size, bufsched->nr_free_buffers);

  allocator_expand(&bufsched->allocator_md);
  allocator_expand(&bufsched->allocator_data);

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
  pthread_mutex_lock(&bufsched->lock_worker);
  bufsched->async_cancel = 1;
  pthread_cond_signal(&bufsched->cond_alloc);
  pthread_mutex_unlock(&bufsched->lock_worker);

  // Wait for the worker thread to terminate
  pthread_join(bufsched->worker_alloc_tid, NULL);

  pthread_cond_destroy(&bufsched->cond_alloc);
  pthread_mutex_destroy(&bufsched->lock_worker);

  pthread_mutex_destroy(&bufsched->lock);

  pthread_cond_destroy(&bufsched->free_buffers_available);

  allocator_destroy(&bufsched->allocator_md);
  allocator_destroy(&bufsched->allocator_data);

  swapper_destroy(&bufsched->swapper);

  return BUFFERING_SUCCESS;
}

error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{

  // If there are no more free buffers, allocate more. This means we could
  // insert some free buffers into the allocator or call the swapper to freeup
  // some memory if there's no available memory.
  uint8_t swapout = 0, expand = 0;
  pthread_mutex_lock(&bufsched->lock);
  if (bufsched->nr_free_buffers <= bufsched->min_free_buffers) {
    if (bufsched->nr_free_buffers + bufsched->nr_assigned_buffers >= bufsched->max_pool_size) {
      swapout = 1;
    } else if (bufsched->nr_free_buffers == bufsched->min_free_buffers) {
      expand = 1;
    }
  }
  pthread_mutex_unlock(&bufsched->lock);

  if (swapout || expand ) {
    pthread_mutex_lock(&bufsched->lock_worker);
    bufsched->async_swapout += swapout;
    bufsched->async_expand = expand;
    pthread_cond_signal(&bufsched->cond_alloc);
    pthread_mutex_unlock(&bufsched->lock_worker);
  }

  pthread_mutex_lock(&bufsched->lock);
  // Block if there are no free buffers
  while (bufsched->nr_free_buffers == 0) {
    pthread_cond_wait(&bufsched->free_buffers_available, &bufsched->lock);
  }

  buffer->data = allocator_alloc(&bufsched->allocator_data);

  bufsched->nr_assigned_buffers++;
  bufsched->nr_free_buffers--;

  pthread_mutex_unlock(&bufsched->lock);

  return BUFFERING_SUCCESS;
}

error_code sched_alloc_md(buffer_scheduler_t *bufsched, cls_buf_t **buffer, cls_buf_handle_t bh)
{
  *buffer = (cls_buf_t*) allocator_alloc(&bufsched->allocator_md);
  init_buffer(*buffer);
  copy_buf_handle(&(*buffer)->handle, &bh);

  return BUFFERING_SUCCESS;
}

error_code sched_free(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{
  pthread_mutex_lock(&bufsched->lock);

  allocator_dealloc(&bufsched->allocator_data, (void*) buffer->data);

  destroy_buffer(buffer);
  allocator_dealloc(&bufsched->allocator_md, (void*) buffer);

  bufsched->nr_assigned_buffers--;
  bufsched->nr_free_buffers++;
  pthread_cond_broadcast(&bufsched->free_buffers_available);

  // Release some memory, there are too many free buffers allocated
  // If there are buffers swapped on the disk, bring them in to speed up
  // upcoming consumers
  uint8_t shrink = 0;
  if (bufsched->nr_free_buffers == bufsched->max_free_buffers) {
    shrink = 1;
  }
  pthread_mutex_unlock(&bufsched->lock);

  if (shrink) {
    pthread_mutex_lock(&bufsched->lock_worker);
    bufsched->async_shrink = 1;
    pthread_cond_signal(&bufsched->cond_alloc);
    pthread_mutex_unlock(&bufsched->lock_worker);
  }

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
  /*pthread_mutex_lock(&bufsched->lock);*/
  /*if (bufsched->nr_free_buffers == 0) {*/
    /*allocator_new(&bufsched->allocator, 1);*/
    /*bufsched->nr_free_buffers++;*/
  /*}*/

  /*allocator_get(&bufsched->allocator, buf);*/
  /*bufsched->nr_assigned_buffers++;*/
  /*bufsched->nr_free_buffers--;*/

  /*buf->was_swapped_in = 1;*/

  /*pthread_mutex_unlock(&bufsched->lock);*/

  /*swapper_swapin(&bufsched->swapper, buf);*/
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

