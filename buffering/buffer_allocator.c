/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_allocator.h"

void allocator_init(allocator_t *allocator, uint32_t block_size, uint32_t nr_blocks)
{
  allocator->block_size = block_size;
  allocator->nr_blocks = nr_blocks;
  allocator->alloc_chunk = NULL;
  allocator->chunks_count = 0;
  dllist_init(&allocator->chunks);
  pthread_mutex_init(&allocator->lock, NULL);
}

void* allocator_alloc(allocator_t *allocator)
{
  pthread_mutex_lock(&allocator->lock);

  chunk_t *chunk = DLLIST_ELEMENT(allocator->alloc_chunk, chunk_t, link);
  if (!allocator->alloc_chunk || chunk_empty(chunk)) {
    dllist_link *l = allocator->chunks.head;
    for (; ; l = l->next) {
      if (!l) {
        chunk_t *c = malloc(sizeof(chunk_t));
        chunk_init(c, allocator->nr_blocks, allocator->block_size);
        allocator->alloc_chunk = &c->link;
        dllist_iat(&allocator->chunks, &c->link);
        allocator->chunks_count++;
        break;
      }

      chunk_t *tmp = DLLIST_ELEMENT(l, chunk_t, link);
      if (!chunk_empty(tmp)) {
        allocator->alloc_chunk = l;
        break;
      }
    }
  }

  chunk_t *tmp = DLLIST_ELEMENT(allocator->alloc_chunk, chunk_t, link);
  void *rv = chunk_alloc(tmp, allocator->block_size);

  pthread_mutex_unlock(&allocator->lock);

  return rv;
}

// TODO: improve, caching
void allocator_dealloc(allocator_t *allocator, void *p)
{
  pthread_mutex_lock(&allocator->lock);
  dllist_link *l = allocator->chunks.head;
  for (; l; l = l->next) {
    chunk_t *tmp = DLLIST_ELEMENT(l, chunk_t, link);
    if (chunk_exists(tmp, allocator->nr_blocks, allocator->block_size, p)) {
      chunk_dealloc(tmp, p, allocator->block_size);
      break;
    }
  }
  pthread_mutex_unlock(&allocator->lock);
}

uint32_t allocator_shrink(allocator_t *allocator)
{
  uint32_t count = 0;
  pthread_mutex_lock(&allocator->lock);
  dllist_link *l = allocator->chunks.tail;
  uint32_t nr_blocks = allocator->nr_blocks;
  for (; l && l->prev;) {
    chunk_t *tmp = DLLIST_ELEMENT(l, chunk_t, link);
    chunk_t *prev = DLLIST_ELEMENT(l->prev, chunk_t, link);
    if (chunk_get_count(tmp) != nr_blocks || chunk_get_count(prev) != nr_blocks) {
      break;
    }

    dllist_rem(&allocator->chunks, l);
    l = l->prev;

    chunk_destroy(tmp);
    free(tmp);
    count += nr_blocks;
    allocator->chunks_count--;
  }
  pthread_mutex_unlock(&allocator->lock);

  return count;
}

uint32_t allocator_expand(allocator_t *allocator)
{
  pthread_mutex_lock(&allocator->lock);

  chunk_t *c = malloc(sizeof(chunk_t));
  chunk_init(c, allocator->nr_blocks, allocator->block_size);

  chunk_t *tail = DLLIST_ELEMENT(allocator->chunks.tail, chunk_t, link);
  if (allocator->chunks.tail == NULL || chunk_empty(tail)) {
    allocator->alloc_chunk = &c->link;
  }

  dllist_iat(&allocator->chunks, &c->link);
  allocator->chunks_count++;

  pthread_mutex_unlock(&allocator->lock);

  return allocator->nr_blocks;
}

void allocator_destroy(allocator_t *allocator)
{
  while(!dllist_is_empty(&allocator->chunks)) {
    dllist_link *l = dllist_rem_head(&allocator->chunks);
    chunk_t *tmp = DLLIST_ELEMENT(l, chunk_t, link);
    chunk_destroy(tmp);
    free(tmp);
  }

  pthread_mutex_destroy(&allocator->lock);
}

