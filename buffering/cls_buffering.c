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


error_code cls_get_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t offset,
                       cls_byte_t *data, const cls_size_t count, const cls_size_t nr_consumers)
{
  HANDLE_ERR(!data || nr_consumers <= 0, BUFFERING_INVALIDARGS);

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

  // We want the memcpy line to be fully parallel, but if the swapper is on,
  // it might try to swap out the buffer. This won't block the swapper too much
  // because consumers will be firstly notified when the buffer is transitioned
  // from BUF_ALLOCATED to BUF_UPDATED.
  // FIXME: implement rwlock, with high priority for readers
  pthread_rwlock_rdlock(&found->rwlock_swap);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  while (found->state == BUF_ALLOCATED) {
    pthread_cond_wait(&found->cond_state, &found->lock);
  }

  // If the buffer is swapped, the first consumer that arrives will schedule a
  // task to swap in the buffer. It will be either swapped in by this dispatched task
  // or by a voluntary task dispatched by the async allocator itself.
  while (found->state == BUF_SWAPPED_OUT || found->state == BUF_QUEUED_SWAPIN) {
    if (found->state == BUF_SWAPPED_OUT) {
      sched_swapin(&bufservice->buf_sched, found);
    }
    pthread_cond_wait(&found->cond_state, &found->lock);
  }
  pthread_mutex_unlock(&found->lock);

  // Read data from buffer
  memcpy(data, found->data + offset, count);

  pthread_mutex_lock(&found->lock);
  found->nr_consumers_finished++;

  // The last consumer that finished reading will release the buffer
  if (found->nr_consumers_finished == nr_consumers) {
    HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);
    HASH_DEL(bufservice->buffers, found);
    HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

    pthread_mutex_unlock(&found->lock);
    pthread_rwlock_unlock(&found->rwlock_swap);

    sched_free(&bufservice->buf_sched, found);

    return BUFFERING_SUCCESS;
  }

  // Unlock rwlock first, avoid race
  pthread_rwlock_unlock(&found->rwlock_swap);
  pthread_mutex_unlock(&found->lock);

  return BUFFERING_SUCCESS;
}

error_code cls_get(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t offset,
                   cls_byte_t *data, const cls_size_t count)
{
  //  return cls_get_all(bufservice, bh, offset, data, count, 1);

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

  // We want the memcpy line to be fully parallel, but if the swapper is on,
  // it might try to swap out the buffer. This won't block the swapper too much
  // because consumers will be firstly notified when the buffer is transitioned
  // from BUF_ALLOCATED to BUF_UPDATED.
  // FIXME: implement rwlock, with high priority for readers
  pthread_rwlock_rdlock(&found->rwlock_swap);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  while (found->state == BUF_ALLOCATED) {
    pthread_cond_wait(&found->cond_state, &found->lock);
  }

  // If the buffer is swapped, the first consumer that arrives will schedule a
  // task to swap in the buffer. It will be either swapped in by this dispatched task
  // or by a voluntary task dispatched by the async allocator itself.
  while (found->state == BUF_SWAPPED_OUT || found->state == BUF_QUEUED_SWAPIN) {
    if (found->state == BUF_SWAPPED_OUT) {
      sched_swapin(&bufservice->buf_sched, found);
    }
    pthread_cond_wait(&found->cond_state, &found->lock);
  }
  pthread_mutex_unlock(&found->lock);

  // Read data from buffer
  memcpy(data, found->data + offset, count);

  pthread_mutex_lock(&found->lock);

  // Unlock rwlock first, avoid race
  pthread_rwlock_unlock(&found->rwlock_swap);
  pthread_mutex_unlock(&found->lock);

  return BUFFERING_SUCCESS;

}

error_code cls_put(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t offset,
                   const cls_byte_t *data, const cls_size_t count)
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

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  if (!found->data) {
    sched_alloc(&bufservice->buf_sched, found);
  }

  // Write the data to buffer
  memcpy(found->data + offset, data, count);

  // Mark the buffer as being ready to be read
  found->state = BUF_UPDATED;

  // Notify any waiting consumer
  pthread_cond_broadcast(&found->cond_state);

  // Tell the scheduler this buffer is written by all the participants.
  // Telling the consumers first about the buffer, might give them a chance to
  // read it before an eventual swapout might occur.
  sched_mark_updated(&bufservice->buf_sched, found);

  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code cls_put_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle,
                       const cls_size_t offset, const cls_byte_t *data,
                       const cls_size_t count, const uint32_t nr_participants)
{
  HANDLE_ERR(!data || offset + count > bufservice->buffer_size || nr_participants <= 0,
             BUFFERING_INVALIDARGS);

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

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  if (!found->data) {
    sched_alloc(&bufservice->buf_sched, found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  // Write the data to buffer
  memcpy(found->data + offset, data, count);

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  found->nr_coll_participants++;
  if (found->nr_coll_participants == nr_participants) {
    // Mark the buffer as being ready to be read
    found->state = BUF_UPDATED;
    // Notify any waiting consumer
    pthread_cond_broadcast(&found->cond_state);

    sched_mark_updated(&bufservice->buf_sched, found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code cls_put_vector(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                          const cls_size_t *countv, const cls_size_t vector_size, const cls_byte_t *data)
{
  HANDLE_ERR(!data || vector_size <= 0 || !offsetv || !countv, BUFFERING_INVALIDARGS);

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

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  if (!found->data) {
    sched_alloc(&bufservice->buf_sched, found);
  }

  // Write the data to buffer
  cls_size_t i = 0;
  int cnt = 0;
  for (i = 0; i < vector_size; ++i) {
    memcpy(found->data + offsetv[i], data + cnt /*offsetv[i]*/, countv[i]);
    cnt += countv[i];
  }

  // Mark the buffer as being ready to be read
  found->state = BUF_UPDATED;

  // Notify any waiting consumer
  pthread_cond_broadcast(&found->cond_state);

  // Tell the scheduler this buffer is written by all the participants.
  // Telling the consumers first about the buffer, might give them a chance to
  // read it before an eventual swapout might occur.
  sched_mark_updated(&bufservice->buf_sched, found);

  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code cls_put_vector_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                              const cls_size_t *countv, const cls_size_t vector_size, const cls_byte_t *data,
                              const uint32_t nr_participants)
{
  HANDLE_ERR(!data || !offsetv || !countv || vector_size <= 0 || nr_participants <= 0,
             BUFFERING_INVALIDARGS);

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

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  if (!found->data) {
    sched_alloc(&bufservice->buf_sched, found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  // Write the data to buffer
  cls_size_t i = 0;
  int cnt = 0;
  for (i = 0; i < vector_size; ++i) {
    memcpy(found->data + offsetv[i], data + cnt/*offsetv[i]*/, countv[i]);
    cnt += countv[i];
  }

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  found->nr_coll_participants++;
  if (found->nr_coll_participants == nr_participants) {
    // Mark the buffer as being ready to be read
    found->state = BUF_UPDATED;
    // Notify any waiting consumer
    pthread_cond_broadcast(&found->cond_state);

    sched_mark_updated(&bufservice->buf_sched, found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code cls_get_vector_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                              const cls_size_t *countv, const cls_size_t vector_size, cls_byte_t *data,
                              const uint32_t nr_consumers)
{
  HANDLE_ERR(!data || !offsetv || !countv || vector_size <= 0 || nr_consumers <= 0,
             BUFFERING_INVALIDARGS);

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

  // We want the memcpy line to be fully parallel, but if the swapper is on,
  // it might try to swap out the buffer. This won't block the swapper too much
  // because consumers will be firstly notified when the buffer is transitioned
  // from BUF_ALLOCATED to BUF_UPDATED.
  // FIXME: implement rwlock, with high priority for readers
  pthread_rwlock_rdlock(&found->rwlock_swap);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  while (found->state == BUF_ALLOCATED) {
    pthread_cond_wait(&found->cond_state, &found->lock);
  }

  // If the buffer is swapped, the first consumer that arrives will schedule a
  // task to swap in the buffer. It will be either swapped in by this dispatched task
  // or by a voluntary task dispatched by the async allocator itself.
  while (found->state == BUF_SWAPPED_OUT || found->state == BUF_QUEUED_SWAPIN) {
    if (found->state == BUF_SWAPPED_OUT) {
      sched_swapin(&bufservice->buf_sched, found);
    }
    pthread_cond_wait(&found->cond_state, &found->lock);
  }
  pthread_mutex_unlock(&found->lock);

  // Read data from buffer
  cls_size_t i = 0;
  int cnt = 0;
  for (i = 0; i < vector_size; ++i) {
    memcpy(data + cnt /*offsetv[i]*/, found->data + offsetv[i], countv[i]);
    cnt += countv[i];
  }

  pthread_mutex_lock(&found->lock);
  found->nr_consumers_finished++;

  // The last consumer that finished reading will release the buffer
  if (found->nr_consumers_finished == nr_consumers) {
    HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);
    HASH_DEL(bufservice->buffers, found);
    HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

    pthread_mutex_unlock(&found->lock);

    sched_free(&bufservice->buf_sched, found);

    return BUFFERING_SUCCESS;
  }

  // Unlock rwlock first, avoid race
  pthread_rwlock_unlock(&found->rwlock_swap);
  pthread_mutex_unlock(&found->lock);

  return BUFFERING_SUCCESS;
}

//Same as cls_get_vector_all expect the result is returned as pointers 
error_code cls_get_vector_all2(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                              const cls_size_t *countv, const cls_size_t vector_size, cls_byte_t **result,
                              const uint32_t nr_consumers)
{
  HANDLE_ERR(!offsetv || !countv || vector_size <= 0 || nr_consumers <= 0,
             BUFFERING_INVALIDARGS);

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

  // We want the memcpy line to be fully parallel, but if the swapper is on,
  // it might try to swap out the buffer. This won't block the swapper too much
  // because consumers will be firstly notified when the buffer is transitioned
  // from BUF_ALLOCATED to BUF_UPDATED.
  // FIXME: implement rwlock, with high priority for readers
  pthread_rwlock_rdlock(&found->rwlock_swap);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  while (found->state == BUF_ALLOCATED) {
    pthread_cond_wait(&found->cond_state, &found->lock);
  }

  // If the buffer is swapped, the first consumer that arrives will schedule a
  // task to swap in the buffer. It will be either swapped in by this dispatched task
  // or by a voluntary task dispatched by the async allocator itself.
  while (found->state == BUF_SWAPPED_OUT || found->state == BUF_QUEUED_SWAPIN) {
    if (found->state == BUF_SWAPPED_OUT) {
      sched_swapin(&bufservice->buf_sched, found);
    }
    pthread_cond_wait(&found->cond_state, &found->lock);
  }
  pthread_mutex_unlock(&found->lock);

  cls_size_t i = 0;
  for (i = 0; i < vector_size; ++i) {
    result[i] = found->data + offsetv[i];
  }


  pthread_mutex_lock(&found->lock);
  found->nr_consumers_finished++;

  // The last consumer that finished reading will release the buffer
  if (found->nr_consumers_finished == nr_consumers) {
    HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);
    HASH_DEL(bufservice->buffers, found);
    HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

    pthread_mutex_unlock(&found->lock);

    sched_free(&bufservice->buf_sched, found);

    return BUFFERING_SUCCESS;
  }

  // Unlock rwlock first, avoid race
  pthread_rwlock_unlock(&found->rwlock_swap);
  pthread_mutex_unlock(&found->lock);

  return BUFFERING_SUCCESS;
}


error_code cls_get_vector(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                          const cls_size_t *countv, const cls_size_t vector_size, cls_byte_t *data)
{
  //  return cls_get_vector_all(bufservice, bh, offsetv, countv, vector_size, data, 1);
  HANDLE_ERR(!data || !offsetv || !countv || vector_size <= 0,
             BUFFERING_INVALIDARGS);

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

  // We want the memcpy line to be fully parallel, but if the swapper is on,
  // it might try to swap out the buffer. This won't block the swapper too much
  // because consumers will be firstly notified when the buffer is transitioned
  // from BUF_ALLOCATED to BUF_UPDATED.
  // FIXME: implement rwlock, with high priority for readers
  pthread_rwlock_rdlock(&found->rwlock_swap);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  while (found->state == BUF_ALLOCATED) {
    pthread_cond_wait(&found->cond_state, &found->lock);
  }

  // If the buffer is swapped, the first consumer that arrives will schedule a
  // task to swap in the buffer. It will be either swapped in by this dispatched task
  // or by a voluntary task dispatched by the async allocator itself.
  while (found->state == BUF_SWAPPED_OUT || found->state == BUF_QUEUED_SWAPIN) {
    if (found->state == BUF_SWAPPED_OUT) {
      sched_swapin(&bufservice->buf_sched, found);
    }
    pthread_cond_wait(&found->cond_state, &found->lock);
  }
  pthread_mutex_unlock(&found->lock);

  // Read data from buffer
  cls_size_t i = 0;
  int cnt = 0;
  for (i = 0; i < vector_size; ++i) {
    memcpy(data + cnt /*offsetv[i]*/, found->data + offsetv[i], countv[i]);
    cnt += countv[i];
  }

  pthread_mutex_lock(&found->lock);

  // Unlock rwlock first, avoid race
  pthread_rwlock_unlock(&found->rwlock_swap);
  pthread_mutex_unlock(&found->lock);

  return BUFFERING_SUCCESS;

}

error_code cls_get_vector2(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                          const cls_size_t *countv, const cls_size_t vector_size, cls_byte_t **result)
{
  //return cls_get_vector_all2(bufservice, bh, offsetv, countv, vector_size, result, 1);
  HANDLE_ERR(!offsetv || !countv || vector_size <= 0,
             BUFFERING_INVALIDARGS);

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

  // We want the memcpy line to be fully parallel, but if the swapper is on,
  // it might try to swap out the buffer. This won't block the swapper too much
  // because consumers will be firstly notified when the buffer is transitioned
  // from BUF_ALLOCATED to BUF_UPDATED.
  // FIXME: implement rwlock, with high priority for readers
  pthread_rwlock_rdlock(&found->rwlock_swap);

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  while (found->state == BUF_ALLOCATED) {
    pthread_cond_wait(&found->cond_state, &found->lock);
  }

  // If the buffer is swapped, the first consumer that arrives will schedule a
  // task to swap in the buffer. It will be either swapped in by this dispatched task
  // or by a voluntary task dispatched by the async allocator itself.
  while (found->state == BUF_SWAPPED_OUT || found->state == BUF_QUEUED_SWAPIN) {
    if (found->state == BUF_SWAPPED_OUT) {
      sched_swapin(&bufservice->buf_sched, found);
    }
    pthread_cond_wait(&found->cond_state, &found->lock);
  }
  pthread_mutex_unlock(&found->lock);

  cls_size_t i = 0;
  for (i = 0; i < vector_size; ++i) {
    result[i] = found->data + offsetv[i];
  }


  pthread_mutex_lock(&found->lock);
  // Unlock rwlock first, avoid race
  pthread_rwlock_unlock(&found->rwlock_swap);
  pthread_mutex_unlock(&found->lock);

  return BUFFERING_SUCCESS;

}

error_code cls_put_vector_noswap_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle,
                                     const cls_size_t *offsetv, const cls_size_t *countv,
                                     const cls_size_t vector_size, const cls_byte_t *data,
                                     const uint32_t nr_participants)
{
  HANDLE_ERR(!data || !offsetv || !countv || vector_size <= 0 || nr_participants <= 0,
             BUFFERING_INVALIDARGS);

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

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  if (!found->data) {
    sched_alloc(&bufservice->buf_sched, found);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  // Write the data to buffer
  cls_size_t i = 0;
  int cnt = 0;
  for (i = 0; i < vector_size; ++i) {
    memcpy(found->data + offsetv[i], data + cnt /*offsetv[i]*/, countv[i]);
    cnt += countv[i];
  }

  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  found->nr_coll_participants++;
  if (found->nr_coll_participants == nr_participants) {
    // Mark the buffer as being ready to be read
    found->state = BUF_UPDATED;
    // Notify any waiting consumer
    pthread_cond_broadcast(&found->cond_state);
  }
  HANDLE_ERR(pthread_mutex_unlock(&found->lock), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code cls_get_vector_noswap_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle,
                                     const cls_size_t *offsetv, const cls_size_t *countv,
                                     const cls_size_t vector_size, cls_byte_t **result,
                                     const uint32_t nr_consumers)
{
  HANDLE_ERR(!offsetv || !countv || vector_size <= 0 || nr_consumers <= 0,
             BUFFERING_INVALIDARGS);

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

  // Wait until the buffer is written by producers
  HANDLE_ERR(pthread_mutex_lock(&found->lock), BUFSERVICE_LOCK_ERR);
  while (found->state == BUF_ALLOCATED) {
    pthread_cond_wait(&found->cond_state, &found->lock);
  }
  pthread_mutex_unlock(&found->lock);

  cls_size_t i = 0;
  for (i = 0; i < vector_size; ++i) {
    result[i] = found->data + offsetv[i];
  }

  return BUFFERING_SUCCESS;
}

error_code cls_release_buf(cls_buffering_t *bufservice, cls_buf_handle_t buf_handle, uint32_t nr_participants)
{
  cls_buf_t *found = NULL;

  cls_buf_handle_t bh;
  copy_buf_handle(&bh, &buf_handle);

  HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  // Check whether the requested buffer is allocated or not.
  HASH_FIND(hh, bufservice->buffers, &bh, sizeof(cls_buf_handle_t), found);
  if (!found) {
    HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);
    return BUFFERING_INVALIDARGS;
  }

  HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

  pthread_mutex_lock(&found->lock);
  found->nr_consumers_finished++;

  // The last consumer that finished will release the buffer
  if (found->nr_consumers_finished == nr_participants) {
    HANDLE_ERR(pthread_mutex_lock(&bufservice->lock), BUFSERVICE_LOCK_ERR);
    HASH_DEL(bufservice->buffers, found);
    HANDLE_ERR(pthread_mutex_unlock(&bufservice->lock), BUFSERVICE_LOCK_ERR);

    pthread_mutex_unlock(&found->lock);
    sched_free_unsafe(&bufservice->buf_sched, found);

    return BUFFERING_SUCCESS;
  }

  pthread_mutex_unlock(&found->lock);

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

