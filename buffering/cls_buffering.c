/* vim: set ts=8 sts=2 et sw=2: */

#include "cls_buffering.h"
#include <assert.h>

error_code cls_init_buffering(cls_buffering_t *bufservice, cls_size_t bsize,
                              cls_size_t max_elems)
{
  error_code rv = BUFFERING_SUCCESS;
  rv = sched_init(&bufservice->buf_sched);
  if (STATUS_FAILED(rv)) {
    return rv;
  }

  if (pthread_mutex_init(&bufservice->lock, NULL)) {
    return BUFSERVICE_LOCK_ERR;
  }

  bufservice->buffers = NULL;
  bufservice->buffer_size = bsize;

  // TODO: init fields

  return rv;
}

// TODO: init_buffer

// TODO: destroy_buffer

error_code cls_get(cls_buffering_t *bufservice, cls_buf_handle_t bh, cls_byte_t *data,
                   cls_size_t nr_consumers)
{
  HANDLE_ERR(!data, BUFFERING_INVALIDARGS);

  cls_buf_t *found = NULL;

  HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Check whether the requested buffer is allocated or not.
  HASH_FIND(hh, bufservice->buffers, &bh, sizeof(cls_buf_handle_t), found);
  if (!found) {
    init_buffer(&found, bh);

    // Get memory from the allocator
    sched_alloc(&bufservice->buf_sched, &found->data);

    // Add newly allocated buffer to hash
    HASH_ADD(hh, bufservice->buffers, handle, sizeof(cls_buf_handle_t), found);
  }

  HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
  while (!found->ready) {
    pthread_cond_wait(&found->buf_ready, &found->lock_read);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock_read), BUFSERVICE_LOCK_ERR);

  // Read data from buffer
  memcpy(data, found->data, bufservice->buffer_size);

  // The last consumer that finished reading will release the buffer
  HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
  found->nr_consumers_finished++;
  if (found->nr_consumers_finished == nr_consumers) {
    sched_release(&bufservice->buf_sched, found->data);
    destroy_buffer(found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock_read), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code cls_put(cls_buffering_t *bufservice, cls_buf_handle_t bh, cls_size_t offset,
                   const cls_byte_t *data, cls_size_t count)
{
  HANDLE_ERR(!data || offset + count >= bufservice->buffer_size, BUFFERING_INVALIDARGS);

  cls_buf_t *found = NULL;

  HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Check whether the requested buffer is allocated or not.
  HASH_FIND(hh, bufservice->buffers, &bh, sizeof(cls_buf_handle_t), found);
  if (!found) {
    init_buffer(&found, bh);

    // Get memory from the allocator
    sched_alloc(&bufservice->buf_sched, &found->data);

    // Add newly allocated buffer to hash
    HASH_ADD(hh, bufservice->buffers, handle, sizeof(cls_buf_handle_t), found);
  }

  HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Write the data to buffer
  HANDLE_ERR(pthread_mutex_lock(&found->lock_write), BUFSERVICE_LOCK_ERR);
  memcpy(found->data + offset, data, count);
  HANDLE_ERR(pthread_mutex_unlock(&found->lock_write), BUFSERVICE_LOCK_ERR);

  // Mark the buffer as being ready to be read
  HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
  found->ready++;
  HANDLE_ERR(pthread_mutex_unlock(&found->lock_read), BUFSERVICE_LOCK_ERR);

  // Notify any waiting consumer
  pthread_cond_broadcast(&found->buf_ready);

  return BUFFERING_SUCCESS;
}

error_code cls_put_all(cls_buffering_t *bufservice, cls_buf_handle_t bh,
                       cls_size_t offset, const cls_byte_t *data,
                       cls_size_t count, uint32_t nr_participants)
{
  HANDLE_ERR(!data || offset + count >= bufservice->buffer_size, BUFFERING_INVALIDARGS);

  cls_buf_t *found = NULL;

  HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Check whether the requested buffer is allocated or not.
  HASH_FIND(hh, bufservice->buffers, &bh, sizeof(cls_buf_handle_t), found);
  if (!found) {
    init_buffer(&found, bh);

    // Get memory from the allocator
    sched_alloc(&bufservice->buf_sched, &found->data);

    // Add newly allocated buffer to hash
    HASH_ADD(hh, bufservice->buffers, handle, sizeof(cls_buf_handle_t), found);
  }

  HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Write the data to buffer
  memcpy(found->data + offset, data, count);


  HANDLE_ERR(pthread_mutex_lock(&found->lock_write), BUFSERVICE_LOCK_ERR);
  found->nr_coll_participants++;
  if (found->nr_coll_participants == nr_participants) {
    // Mark the buffer as being ready to be read
    HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
    found->ready++;
    HANDLE_ERR(pthread_mutex_unlock(&found->lock_read), BUFSERVICE_LOCK_ERR);

    // Notify any waiting consumer
    pthread_cond_broadcast(&found->buf_ready);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock_write), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

