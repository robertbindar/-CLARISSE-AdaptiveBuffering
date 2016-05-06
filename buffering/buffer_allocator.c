/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_allocator.h"
#include <stdio.h>

error_code allocator_init(buffer_allocator_t *allocator, uint64_t buf_size)
{
  dllist_init(&allocator->free_buffers);
  allocator->buffer_size = buf_size;

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

  pthread_mutex_init(&buffer->lock_write, NULL);
  pthread_mutex_init(&buffer->lock_read, NULL);
  pthread_rwlock_init(&buffer->rwlock_swap, NULL);
  pthread_cond_init(&buffer->buf_ready, NULL);
}

void destroy_buffer(cls_buf_t *buff)
{
  pthread_cond_destroy(&buff->buf_ready);
  pthread_mutex_destroy(&buff->lock_write);
  pthread_mutex_destroy(&buff->lock_read);
  pthread_rwlock_destroy(&buff->rwlock_swap);

  free(buff->data);
  free(buff);
}

static void release_buf_md(cls_buf_t *buff)
{
  pthread_cond_destroy(&buff->buf_ready);
  pthread_mutex_destroy(&buff->lock_write);
  pthread_mutex_destroy(&buff->lock_read);

  pthread_rwlock_destroy(&buff->rwlock_swap);

  free(buff);
}

error_code allocator_get(buffer_allocator_t *allocator, cls_buf_t *buffer)
{
  dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);

  mem_entry_t *b = DLLIST_ELEMENT(tmp, mem_entry_t, link);

  buffer->data = b->data;
  free(b);

  return BUFFERING_SUCCESS;
}

error_code allocator_get_md(buffer_allocator_t *allocator, cls_buf_t **buffer, cls_buf_handle_t bh)
{
  *buffer = malloc(sizeof(cls_buf_t));

  cls_buf_t *b = *buffer;
  copy_buf_handle(&b->handle, &bh);
  init_buffer(b);

  return BUFFERING_SUCCESS;
}

error_code allocator_put(buffer_allocator_t *allocator, cls_buf_t *buffer)
{
  mem_entry_t *m = malloc(sizeof(mem_entry_t));
  m->link.prev = m->link.next = NULL;
  m->data = buffer->data;
  buffer->data = NULL;

  release_buf_md(buffer);

  dllist_iat(&allocator->free_buffers, &m->link);

  return BUFFERING_SUCCESS;
}

error_code allocator_shrink(buffer_allocator_t *allocator, uint64_t count)
{
  while (count > 0) {
    dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);

    mem_entry_t *m = DLLIST_ELEMENT(tmp, mem_entry_t, link);
    free(m->data);
    free(m);
    --count;
  }

  return BUFFERING_SUCCESS;
}

error_code allocator_new(buffer_allocator_t *allocator, uint64_t count)
{
  mem_entry_t *m = NULL;
  while (count > 0) {
    m = malloc(sizeof(mem_entry_t));
    m->link.prev = m->link.next = NULL;
    m->data = malloc(allocator->buffer_size);
    if (!m || !m->data) {
      return BUFALLOCATOR_BAD_ALLOC;
    }

    dllist_iat(&allocator->free_buffers, &m->link);

    --count;
  }

  return BUFFERING_SUCCESS;
}

error_code allocator_move_to_free(buffer_allocator_t *allocator, cls_buf_t *buf)
{
  mem_entry_t *m = malloc(sizeof(mem_entry_t));;
  if (!m) {
    return BUFALLOCATOR_BAD_ALLOC;
  }

  m->link.prev = m->link.next = NULL;
  m->data = buf->data;
  buf->data = NULL;

  dllist_iat(&allocator->free_buffers, &m->link);

  return BUFFERING_SUCCESS;
}

error_code allocator_move(buffer_allocator_t *allocator, cls_buf_t *dest, cls_buf_t *src)
{
  dest->data = src->data;
  src->data = NULL;

  return BUFFERING_SUCCESS;
}

error_code allocator_destroy(buffer_allocator_t *allocator)
{
  while (!dllist_is_empty(&allocator->free_buffers)) {
    dllist_link *tmp = dllist_rem_head(&allocator->free_buffers);
    mem_entry_t *b = DLLIST_ELEMENT(tmp, mem_entry_t, link);
    free(b->data);
    free(b);
  }

  return BUFFERING_SUCCESS;
}

