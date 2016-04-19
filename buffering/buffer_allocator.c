/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_allocator.h"
#include "buffering_types.h"
#include <stdio.h>

error_code allocator_init(buffer_allocator_t *allocator, uint64_t buf_size)
{
  dllist_init(&allocator->free_buffers);
  allocator->buffer_size = buf_size;

  pthread_cond_init(&allocator->free_buffer_available, NULL);

  pthread_mutex_init(&allocator->lock, NULL);

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

static void init_buffer(cls_buf_t *buffer)
{
  init_buf_fields(buffer);

  pthread_mutex_init(&buffer->lock_write, NULL);
  pthread_mutex_init(&buffer->lock_read, NULL);
  pthread_cond_init(&buffer->buf_ready, NULL);
}

void destroy_buffer(cls_buf_t *buff)
{
  pthread_cond_destroy(&buff->buf_ready);
  pthread_mutex_destroy(&buff->lock_write);
  pthread_mutex_destroy(&buff->lock_read);

  free(buff->data);
  free(buff);
}

error_code allocator_get(buffer_allocator_t *allocator, cls_buf_t **buffer, cls_buf_handle_t bh)
{
  pthread_mutex_lock(&allocator->lock);

  while (dllist_is_empty(&allocator->free_buffers)) {
    pthread_cond_wait(&allocator->free_buffer_available, &allocator->lock);
  }

  dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);

  pthread_mutex_unlock(&allocator->lock);

  cls_buf_t *b = DLLIST_ELEMENT(tmp, cls_buf_t, link_alloc);

  copy_buf_handle(&b->handle, &bh);
  init_buf_fields(b);
  *buffer = b;

  return BUFFERING_SUCCESS;
}

error_code allocator_put(buffer_allocator_t *allocator, cls_buf_t *buffer)
{
  pthread_mutex_lock(&allocator->lock);
  dllist_iat(&allocator->free_buffers, &buffer->link_alloc);
  pthread_mutex_unlock(&allocator->lock);

  pthread_cond_broadcast(&allocator->free_buffer_available);

  return BUFFERING_SUCCESS;
}

error_code allocator_shrink(buffer_allocator_t *allocator, uint64_t count)
{
  while (count > 0) {
    pthread_mutex_lock(&allocator->lock);
    dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);
    pthread_mutex_unlock(&allocator->lock);

    cls_buf_t *b = DLLIST_ELEMENT(tmp, cls_buf_t, link_alloc);
    destroy_buffer(b);
    --count;
  }

  return BUFFERING_SUCCESS;
}

error_code allocator_new(buffer_allocator_t *allocator, uint64_t count)
{
  cls_buf_t *b = NULL;
  while (count > 0) {
    b = malloc(sizeof(cls_buf_t));
    b->data = malloc(allocator->buffer_size);
    if (!b || !b->data) {
      return BUFALLOCATOR_BAD_ALLOC;
    }

    init_buffer(b);

    pthread_mutex_lock(&allocator->lock);
    dllist_iat(&allocator->free_buffers, &b->link_alloc);
    pthread_mutex_unlock(&allocator->lock);

    pthread_cond_broadcast(&allocator->free_buffer_available);

    --count;
  }

  return BUFFERING_SUCCESS;
}

error_code allocator_destroy(buffer_allocator_t *allocator)
{
  while (!dllist_is_empty(&allocator->free_buffers)) {
    dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);
    cls_buf_t *b = DLLIST_ELEMENT(tmp, cls_buf_t, link_alloc);
    destroy_buffer(b);
  }

  pthread_mutex_destroy(&allocator->lock);
  pthread_cond_destroy(&allocator->free_buffer_available);

  return BUFFERING_SUCCESS;
}

