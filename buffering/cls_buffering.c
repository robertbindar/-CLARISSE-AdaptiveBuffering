/* vim: set ts=8 sts=2 et sw=2: */

#include "cls_buffering.h"
#include "buffer_scheduler.h"
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>

error_code cls_init_buffering(cls_buffering_t *bufservice, cls_size_t bsize,
                              cls_size_t max_elems)
{
  error_code rv = BUFFERING_SUCCESS;
  rv = sched_init(&bufservice->buf_sched, bsize, max_elems);
  if (STATUS_FAILED(rv)) {
    return rv;
  }

  HANDLE_ERR(pthread_mutex_init(&bufservice->lock, NULL), BUFSERVICE_LOCK_ERR);

  bufservice->buffers = NULL;
  bufservice->buffer_size = bsize;

  return BUFFERING_SUCCESS;
}

error_code cls_destroy_buffering(cls_buffering_t *bufservice)
{
  HANDLE_ERR(!bufservice, BUFFERING_INVALIDARGS);

  HANDLE_ERR(pthread_mutex_destroy(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  cls_buf_t *current, *tmp;
  HASH_ITER(hh, bufservice->buffers, current, tmp) {
    HASH_DELETE(hh, bufservice->buffers, current);
    destroy_buffer(current);
  }

  return sched_destroy(&bufservice->buf_sched);
}


error_code cls_get(cls_buffering_t *bufservice, cls_buf_handle_t buf_handle, cls_size_t offset,
                   cls_byte_t *data, cls_size_t count, cls_size_t nr_consumers)
{
  HANDLE_ERR(!data, BUFFERING_INVALIDARGS);

  cls_buf_t *found = NULL;

  cls_buf_handle_t bh;
  copy_buf_handle(&bh, &buf_handle);

  HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Check whether the requested buffer is allocated or not.
  HASH_FIND(hh, bufservice->buffers, &bh, sizeof(cls_buf_handle_t), found);
  if (!found) {
    // Request a buffer from the scheduler
    sched_alloc_md(&bufservice->buf_sched, &found, bh);

    // Add newly allocated buffer to hash
    HASH_ADD(hh, bufservice->buffers, handle, sizeof(cls_buf_handle_t), found);
  }

  HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  pthread_rwlock_rdlock(&found->rwlock_swap);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
  while (!found->ready) {
    pthread_cond_wait(&found->buf_ready, &found->lock_read);
  }

  if (found->is_swapped) {
    sched_swapin(&bufservice->buf_sched, found);
  }
  pthread_mutex_unlock(&found->lock_read);

  // Read data from buffer
  memcpy(data, found->data + offset, count);

  pthread_mutex_lock(&found->lock_read);
  found->nr_consumers_finished++;

  // The last consumer that finished reading will release the buffer
  if (found->nr_consumers_finished == nr_consumers) {
    HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);
    HASH_DEL(bufservice->buffers, found);
    HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

    if (sched_mark_consumed(&bufservice->buf_sched, found) || found->was_swapped_in) {
      pthread_mutex_unlock(&found->lock_read);
      pthread_rwlock_unlock(&found->rwlock_swap);

      sched_free(&bufservice->buf_sched, found);

      return BUFFERING_SUCCESS;
    }

    found->freed_by_swapper = 1;
  }

  // Unlock rwlock first, avoid race
  pthread_rwlock_unlock(&found->rwlock_swap);
  pthread_mutex_unlock(&found->lock_read);

  return BUFFERING_SUCCESS;
}

error_code cls_put(cls_buffering_t *bufservice, cls_buf_handle_t buf_handle, cls_size_t offset,
                   const cls_byte_t *data, cls_size_t count)
{
  HANDLE_ERR(!data || offset + count > bufservice->buffer_size, BUFFERING_INVALIDARGS);

  cls_buf_t *found = NULL;

  cls_buf_handle_t bh;
  copy_buf_handle(&bh, &buf_handle);

  HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Check whether the requested buffer is allocated or not.
  HASH_FIND(hh, bufservice->buffers, &bh, sizeof(cls_buf_handle_t), found);
  if (!found) {
    // Request a buffer from the scheduler
    sched_alloc_md(&bufservice->buf_sched, &found, bh);

    // Add newly allocated buffer to hash
    HASH_ADD(hh, bufservice->buffers, handle, sizeof(cls_buf_handle_t), found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
  if (!found->data) {
    sched_alloc(&bufservice->buf_sched, found);
  }

  // Write the data to buffer
  memcpy(found->data + offset, data, count);

  // Mark the buffer as being ready to be read
  found->ready++;

  // Notify any waiting consumer
  pthread_cond_broadcast(&found->buf_ready);

  // Tell the scheduler this buffer is written by all the participants.
  // Telling the consumers first about the buffer, might give them a chance to
  // read it before an eventual swapout might occur.
  sched_mark_updated(&bufservice->buf_sched, found);

  HANDLE_ERR(pthread_mutex_unlock(&found->lock_read), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code cls_put_all(cls_buffering_t *bufservice, cls_buf_handle_t buf_handle,
                       cls_size_t offset, const cls_byte_t *data,
                       cls_size_t count, uint32_t nr_participants)
{
  HANDLE_ERR(!data || offset + count > bufservice->buffer_size, BUFFERING_INVALIDARGS);

  cls_buf_t *found = NULL;

  cls_buf_handle_t bh;
  copy_buf_handle(&bh, &buf_handle);

  HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Check whether the requested buffer is allocated or not.
  HASH_FIND(hh, bufservice->buffers, &bh, sizeof(cls_buf_handle_t), found);
  if (!found) {
    // Request a buffer from the scheduler
    sched_alloc_md(&bufservice->buf_sched, &found, bh);

    // Add newly allocated buffer to hash
    HASH_ADD(hh, bufservice->buffers, handle, sizeof(cls_buf_handle_t), found);
  }

  HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
  if (!found->data) {
    sched_alloc(&bufservice->buf_sched, found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock_read), BUFSERVICE_LOCK_ERR);

  // Write the data to buffer
  memcpy(found->data + offset, data, count);

  HANDLE_ERR(pthread_mutex_lock(&found->lock_read), BUFSERVICE_LOCK_ERR);
  found->nr_coll_participants++;
  if (found->nr_coll_participants == nr_participants) {
    // Mark the buffer as being ready to be read
    found->ready++;

    // Notify any waiting consumer
    pthread_cond_broadcast(&found->buf_ready);

    sched_mark_updated(&bufservice->buf_sched, found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock_read), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

void print_buffers(cls_buffering_t *bufservice)
{
  cls_buf_t *current, *tmp;
  HASH_ITER(hh, bufservice->buffers, current, tmp) {
    fprintf(stderr, "{descr: %" PRIu32 ", off: %" PRIu64 "} ", current->handle.global_descr, current->handle.offset);
  }

  fprintf(stderr, "\n");
}

