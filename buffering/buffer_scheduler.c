/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_scheduler.h"
#include <pthread.h>
#include <stdio.h>

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size,
                      uint64_t max_pool_size)
{
  pthread_mutex_init(&bufsched->lock, NULL);
  pthread_cond_init(&bufsched->free_buffers_available, NULL);

  bufsched->buffer_size = buffer_size;
  bufsched->nr_assigned_buffers = 0;

  // Default window limits for buffer allocation
  bufsched->min_free_buffers = 10 * max_pool_size / 100 + 1;
  bufsched->max_free_buffers = 60 * max_pool_size / 100 + 2;
  bufsched->max_pool_size = max_pool_size;
  bufsched->nr_free_buffers = bufsched->min_free_buffers + 1;


  // Init custom allocators for metadata and buffers memory
  allocator_init(&bufsched->allocator_md, sizeof(cls_buf_t), max_pool_size);
  allocator_init(&bufsched->allocator_data, buffer_size, bufsched->nr_free_buffers);

  // Grow allocators above the low window limit
  allocator_expand(&bufsched->allocator_md);
  allocator_expand(&bufsched->allocator_data);

  // Init task queue
  task_queue_init(&bufsched->task_queue, max_pool_size);

  // Dispatch worker
  dispatch_worker(&bufsched->worker, &bufsched->task_queue);

  dllist_init(&bufsched->mrucache);

  swapper_init(&bufsched->swapper, bufsched->buffer_size);

  return BUFFERING_SUCCESS;
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

void copy_buf_handle(cls_buf_handle_t *dest, cls_buf_handle_t *src)
{
  memset(dest, 0, sizeof(cls_buf_handle_t));
  dest->offset = src->offset;
  dest->global_descr = src->global_descr;
}

static void init_buffer(cls_buf_t *buffer)
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

static void swapout_buffer(void *arg)
{
  task_t *owner_task = (task_t *) arg;
  buffer_scheduler_t *bufsched = owner_task->bufsched;
  // TODO:

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
}

static void swapin_buffer(void *arg)
{
  task_t *owner_task = (task_t *) arg;
  buffer_scheduler_t *bufsched = owner_task->bufsched;

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
}

static void shrink_allocator(void *arg)
{
  task_t *owner_task = (task_t *) arg;
  buffer_scheduler_t *bufsched = owner_task->bufsched;

  // TODO: improve: see ::allocator_shrink()
  pthread_mutex_lock(&bufsched->lock);

  uint64_t nr_swapped = swapper_getcount(&bufsched->swapper);
  for (uint64_t i = 0; i < nr_swapped; ++i) {
    task_t *task = create_task(&bufsched->task_queue, (callback_t) swapin_buffer, TASK_DETACHED);
    task->bufsched = bufsched;

    submit_task(&bufsched->task_queue, task);
  }

  if (nr_swapped || bufsched->nr_free_buffers < bufsched->max_free_buffers) {
    goto cleanup;
  }

  uint32_t count = allocator_shrink(&bufsched->allocator_data);
  bufsched->nr_free_buffers -= count;

cleanup:
  pthread_mutex_unlock(&bufsched->lock);
}

static void expand_allocator(void *arg)
{
  task_t *owner_task = (task_t *) arg;
  buffer_scheduler_t *bufsched = owner_task->bufsched;

  pthread_mutex_lock(&bufsched->lock);
  if (bufsched->nr_free_buffers > bufsched->min_free_buffers) {
    goto cleanup;
  }
  pthread_mutex_unlock(&bufsched->lock);

  uint32_t count = allocator_expand(&bufsched->allocator_data);

  pthread_mutex_lock(&bufsched->lock);
  bufsched->nr_free_buffers += count;

cleanup:
  pthread_mutex_unlock(&bufsched->lock);
}

static void dealloc_buffer(void *arg)
{
  task_t *owner_task = (task_t *) arg;
  buffer_scheduler_t *bufsched = owner_task->bufsched;
  cls_buf_t *buffer = owner_task->buffer;

  // TODO: lock buffer
  allocator_dealloc(&bufsched->allocator_data, (void*) buffer->data);

  destroy_buffer(buffer);
  allocator_dealloc(&bufsched->allocator_md, (void*) buffer);

  pthread_mutex_lock(&bufsched->lock);
  bufsched->nr_assigned_buffers--;
  bufsched->nr_free_buffers++;
  pthread_cond_broadcast(&bufsched->free_buffers_available);

  // Release some memory, there are too many free buffers allocated
  // If there are buffers swapped on the disk, bring them in to speed up
  // upcoming consumers
  if (bufsched->nr_free_buffers >= bufsched->max_free_buffers) {
    task_t *task = create_task(&bufsched->task_queue, (callback_t) shrink_allocator, TASK_DETACHED);
    task->bufsched = bufsched;

    submit_task(&bufsched->task_queue, task);
  }
  pthread_mutex_unlock(&bufsched->lock);
}

error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{

  // If the lower window limit is reached, signal the worker to allocate more
  // free buffers. It will either alloc more free buffers or call the swapper to
  // free up some memory.
  pthread_mutex_lock(&bufsched->lock);
  if (bufsched->nr_free_buffers <= bufsched->min_free_buffers) {
    task_t *task;
    if (bufsched->nr_free_buffers + bufsched->nr_assigned_buffers >= bufsched->max_pool_size) {
      task = create_task(&bufsched->task_queue, (callback_t) swapout_buffer, TASK_DETACHED);
    } else {
      task = create_task(&bufsched->task_queue, (callback_t) expand_allocator, TASK_DETACHED);
    }
    task->bufsched = bufsched;

    submit_task(&bufsched->task_queue, task);
  }

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
  task_t *task = create_task(&bufsched->task_queue, (callback_t) dealloc_buffer, TASK_DETACHED);
  task->bufsched = bufsched;
  task->buffer = buffer;

  submit_task(&bufsched->task_queue, task);

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
  task_t *task = create_task(&bufsched->task_queue, (callback_t) swapin_buffer, TASK_OWN);
  task->bufsched = bufsched;
  task->buffer = buf;
  submit_task(&bufsched->task_queue, task);

  wait_task(task);
}

error_code sched_destroy(buffer_scheduler_t *bufsched)
{
  // Wait for the worker thread to terminate execution
  wait_worker_finished(&bufsched->worker);

  task_queue_destroy(&bufsched->task_queue);

  pthread_mutex_destroy(&bufsched->lock);

  pthread_cond_destroy(&bufsched->free_buffers_available);

  allocator_destroy(&bufsched->allocator_md);
  allocator_destroy(&bufsched->allocator_data);

  swapper_destroy(&bufsched->swapper);

  return BUFFERING_SUCCESS;
}

