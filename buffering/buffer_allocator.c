#include "buffer_allocator.h"

error_code allocator_init(buffer_allocator_t *allocator, uint64_t buf_size)
{
  dllist_init(&allocator->free_buffers);
  allocator->assigned_buffers = NULL;

  if (pthread_mutex_init(&allocator->lock, NULL)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  return BUFALLOCATOR_SUCCESS;
}

error_code allocator_get(buffer_allocator_t *allocator, char **data)
{
  if (pthread_mutex_lock(&allocator->lock)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  error_code err = BUFALLOCATOR_SUCCESS;

  if (dllist_is_empty(&allocator->free_buffers)) {
    *data = NULL;
    err = BUFALLOCATOR_FREEBUF_ERR;
    goto cleanup;
  }

  buffer_t *b = (buffer_t*) dllist_rem_head(&allocator->free_buffers);
  *data = b->data;
  HASH_ADD_PTR(allocator->assigned_buffers, data, b);

cleanup:
  if(pthread_mutex_unlock(&allocator->lock)) {
    err = BUFALLOCATOR_LOCK_ERR;
  }

  return err;
}

error_code allocator_put(buffer_allocator_t *allocator, char *data)
{
  if (pthread_mutex_lock(&allocator->lock)) {
    return BUFALLOCATOR_LOCK_ERR;
  }

  error_code err = BUFALLOCATOR_SUCCESS;

  // If the buffer was assigned, remove it from the assigned table
  buffer_t *search;
  HASH_FIND_PTR(allocator->assigned_buffers, &data, search);
  if (search) {
    HASH_DEL(allocator->assigned_buffers, search);
  } else {
    if ((search = malloc(sizeof(buffer_t))) == NULL) {
      err = BUFALLOCATOR_BAD_ALLOC;
      goto cleanup;
    }

    search->data = data;
  }

  dllist_iat(&allocator->free_buffers, &search->link);

cleanup:
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

  error_code err = BUFALLOCATOR_SUCCESS;

  while (count > 0) {
    buffer_t *b = (buffer_t*) dllist_rem_head(&allocator->free_buffers);
    free(b->data);
    free(b);
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

  error_code err = BUFALLOCATOR_SUCCESS;

  buffer_t *b = NULL;
  while (count > 0) {
    b = malloc(sizeof(buffer_t));
    b->data = malloc(allocator->buffer_size);
    if (!b || !b->data) {
      err = BUFALLOCATOR_BAD_ALLOC;
      goto cleanup;
    }

    dllist_iat(&allocator->free_buffers, &b->link);
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

  error_code err = BUFALLOCATOR_SUCCESS;

  while (!dllist_is_empty(&allocator->free_buffers)) {
    buffer_t *b = (buffer_t*) dllist_rem_head(&allocator->free_buffers);
    free(b->data);
    free(b);
  }

  buffer_t *current, *tmp;
  HASH_ITER(hh, allocator->assigned_buffers, current, tmp) {
    HASH_DEL(allocator->assigned_buffers, current);
    free(current->data);
    free(current);
  }

  if(pthread_mutex_unlock(&allocator->lock)) {
    err = BUFALLOCATOR_LOCK_ERR;
  }

  return err;
}

