/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_allocator.h"
#include "buffering_types.h"
#include <stdio.h>

error_code allocator_init(buffer_allocator_t *allocator, uint64_t buf_size)
{
  dllist_init(&allocator->free_buffers);
  allocator->assigned_buffers = NULL;
  allocator->buffer_size = buf_size;

  if (pthread_mutex_init(&allocator->lock, NULL)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  return BUFFERING_SUCCESS;
}

// uthash uses memcmp comparison on structs, we need member-wise copying to
// get rid of padding differences
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
}

static error_code init_buffer(cls_buf_t *buffer)
{
  init_buf_fields(buffer);

  HANDLE_ERR(pthread_mutex_init(&buffer->lock_write, NULL), BUFALLOCATOR_LOCK_ERR);
  HANDLE_ERR(pthread_mutex_init(&buffer->lock_read, NULL), BUFALLOCATOR_LOCK_ERR);

  HANDLE_ERR(pthread_cond_init(&buffer->buf_ready, NULL), BUFALLOCATOR_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

error_code destroy_buffer(cls_buf_t *buff)
{
  HANDLE_ERR(pthread_cond_destroy(&buff->buf_ready), BUFALLOCATOR_LOCK_ERR);

  HANDLE_ERR(pthread_mutex_destroy(&buff->lock_write), BUFALLOCATOR_LOCK_ERR);

  HANDLE_ERR(pthread_mutex_destroy(&buff->lock_read), BUFALLOCATOR_LOCK_ERR);

  free(buff->data);

  free(buff);

  return BUFFERING_SUCCESS;
}

error_code allocator_get(buffer_allocator_t *allocator, cls_buf_t **buffer, cls_buf_handle_t bh)
{
  if (pthread_mutex_lock(&allocator->lock)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  error_code err = BUFFERING_SUCCESS;

  if (dllist_is_empty(&allocator->free_buffers)) {
    *buffer = NULL;
    err = BUFALLOCATOR_FREEBUF_ERR;
    goto cleanup;
  }

  dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);
  cls_buf_t *b = DLLIST_ELEMENT(tmp, cls_buf_t, link_alloc);

  copy_buf_handle(&b->handle, &bh);
  init_buf_fields(b);
  *buffer = b;
  /*HASH_ADD(hh_alloc, allocator->assigned_buffers, handle, sizeof(cls_buf_handle_t), b);*/

cleanup:
  if(pthread_mutex_unlock(&allocator->lock)) {
    err = BUFALLOCATOR_LOCK_ERR;
  }

  return err;
}

error_code allocator_put(buffer_allocator_t *allocator, cls_buf_t *buffer)
{
  if (pthread_mutex_lock(&allocator->lock)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  error_code err = BUFFERING_SUCCESS;

  // If the buffer was assigned, remove it from the assigned table
  /*cls_buf_t *search;*/
  /*HASH_FIND(hh_alloc, allocator->assigned_buffers, &buffer->handle,*/
            /*sizeof(cls_buf_handle_t), search);*/
  /*if (search) {*/
    /*HASH_DELETE(hh_alloc, allocator->assigned_buffers, search);*/
  /*}*/

  dllist_iat(&allocator->free_buffers, &buffer->link_alloc);

  if(pthread_mutex_unlock(&allocator->lock)) {
    err = BUFALLOCATOR_LOCK_ERR;
  }

  return err;
}

error_code allocator_shrink(buffer_allocator_t *allocator, uint64_t count)
{
  if (pthread_mutex_lock(&allocator->lock)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  error_code err = BUFFERING_SUCCESS;

  while (count > 0) {
    dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);
    cls_buf_t *b = DLLIST_ELEMENT(tmp, cls_buf_t, link_alloc);
    destroy_buffer(b);
    --count;
  }

  if(pthread_mutex_unlock(&allocator->lock)) {
    err = BUFALLOCATOR_LOCK_ERR;
  }

  return err;
}

error_code allocator_new(buffer_allocator_t *allocator, uint64_t count)
{
  if (pthread_mutex_lock(&allocator->lock)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  error_code err = BUFFERING_SUCCESS;

  cls_buf_t *b = NULL;
  while (count > 0) {
    b = malloc(sizeof(cls_buf_t));
    b->data = malloc(allocator->buffer_size);
    if (!b || !b->data) {
      err = BUFALLOCATOR_BAD_ALLOC;
      goto cleanup;
    }

    init_buffer(b);
    dllist_iat(&allocator->free_buffers, &b->link_alloc);

    --count;
  }
cleanup:
  if(pthread_mutex_unlock(&allocator->lock)) {
    err = BUFALLOCATOR_LOCK_ERR;
  }

  return err;
}

error_code allocator_destroy(buffer_allocator_t *allocator)
{
  if (pthread_mutex_lock(&allocator->lock)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  error_code err = BUFFERING_SUCCESS;

  while (!dllist_is_empty(&allocator->free_buffers)) {
    dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);
    cls_buf_t *b = DLLIST_ELEMENT(tmp, cls_buf_t, link_alloc);
    destroy_buffer(b);
  }

  /*cls_buf_t *current, *tmp;*/
  /*HASH_ITER(hh_alloc, allocator->assigned_buffers, current, tmp) {*/
    /*HASH_DELETE(hh_alloc, allocator->assigned_buffers, current);*/
    /*destroy_buffer(current);*/
  /*}*/

  if(pthread_mutex_unlock(&allocator->lock)) {
    err = BUFALLOCATOR_LOCK_ERR;
  }

  return err;
}

