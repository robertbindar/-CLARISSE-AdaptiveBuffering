/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_scheduler.h"
#include <pthread.h>
#include <stdio.h>

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size,
                      uint64_t max_pool_size)
{
  pthread_mutex_init(&bufsched->lock, NULL);

  bufsched->buffer_size = buffer_size;
  bufsched->nr_assigned_buffers = 0;
  bufsched->nr_free_buffers = max_pool_size;

  // Default window limits for buffer allocation
  bufsched->max_pool_size = max_pool_size + 1;

  // Init custom allocators for metadata and buffers memory
  allocator_init(&bufsched->allocator_md, sizeof(cls_buf_t), bufsched->max_pool_size);
  allocator_init(&bufsched->allocator_data, buffer_size, bufsched->max_pool_size);

  allocator_expand(&bufsched->allocator_md, bufsched->max_pool_size);
  allocator_expand(&bufsched->allocator_data, bufsched->max_pool_size);

  return BUFFERING_SUCCESS;
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
  buffer->state = BUF_ALLOCATED;
  buffer->data = NULL;
  buffer->link_mru.next = NULL;
  buffer->link_mru.prev = NULL;

  pthread_mutex_init(&buffer->lock, NULL);
  pthread_rwlock_init(&buffer->rwlock_swap, NULL);
  pthread_cond_init(&buffer->cond_state, NULL);
}

void destroy_buffer(cls_buf_t *buff)
{
  pthread_cond_destroy(&buff->cond_state);
  pthread_mutex_destroy(&buff->lock);
  pthread_rwlock_destroy(&buff->rwlock_swap);
}

error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{
  pthread_mutex_lock(&bufsched->lock);

  bufsched->nr_assigned_buffers++;
  bufsched->nr_free_buffers--;

  pthread_mutex_unlock(&bufsched->lock);

  buffer->data = allocator_alloc(&bufsched->allocator_data);

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
  allocator_dealloc(&bufsched->allocator_data, (void*) buffer->data);

  destroy_buffer(buffer);
  allocator_dealloc(&bufsched->allocator_md, (void*) buffer);

  pthread_mutex_lock(&bufsched->lock);
  bufsched->nr_assigned_buffers--;
  bufsched->nr_free_buffers++;
  pthread_mutex_unlock(&bufsched->lock);

  return BUFFERING_SUCCESS;
}

void sched_mark_updated(buffer_scheduler_t *bufsched, cls_buf_t *b)
{

}

uint8_t sched_mark_consumed(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
  return 1;
}

void sched_swapin(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
}

error_code sched_destroy(buffer_scheduler_t *bufsched)
{
  pthread_mutex_destroy(&bufsched->lock);

  allocator_destroy(&bufsched->allocator_md);
  allocator_destroy(&bufsched->allocator_data);

  return BUFFERING_SUCCESS;
}

