/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_scheduler.h"
#include "buffering_types.h"

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size,
                      uint64_t max_pool_size)
{
  HANDLE_ERR(pthread_mutex_init(&bufsched->lock, NULL), BUFSCHEDULER_LOCK_ERR);

  bufsched->buffer_size = buffer_size;
  bufsched->max_pool_size = max_pool_size;
  bufsched->nr_assigned_buffers = 0;

  allocator_init(&bufsched->allocator, buffer_size);

  bufsched->nr_free_buffers = 0;

  return BUFFERING_SUCCESS;
}

error_code sched_destroy(buffer_scheduler_t *bufsched)
{
  HANDLE_ERR(pthread_mutex_destroy(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);

  allocator_destroy(&bufsched->allocator);

  return BUFFERING_SUCCESS;
}

error_code sched_alloc(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{
  HANDLE_ERR(pthread_mutex_lock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);
  buffer->data = malloc(bufsched->buffer_size);
  bufsched->nr_assigned_buffers++;
  HANDLE_ERR(pthread_mutex_unlock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code sched_free(buffer_scheduler_t *bufsched, cls_buf_t *buffer)
{
  HANDLE_ERR(pthread_mutex_lock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);
  free(buffer->data);
  bufsched->nr_assigned_buffers--;
  HANDLE_ERR(pthread_mutex_unlock(&bufsched->lock), BUFSCHEDULER_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code sched_alloc_md(buffer_scheduler_t *bufsched, cls_buf_t **buffer, cls_buf_handle_t bh)
{
  return allocator_get_md(&bufsched->allocator, buffer, bh);
}

void sched_mark_updated(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{

}

uint8_t sched_mark_consumed(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{
  return 1;
}

void sched_swapin(buffer_scheduler_t *bufsched, cls_buf_t *buf)
{

}

