#pragma once

#include <stdint.h>
#include <pthread.h>
#include "chunk_pool.h"

typedef struct
{
  uint32_t block_size;
  uint32_t default_nr_blocks;

  dllist chunks;
  uint32_t chunks_count;
  dllist_link *alloc_chunk;

  pthread_mutex_t lock;
} allocator_t;

void allocator_init(allocator_t *allocator, uint32_t block_size, uint32_t nr_blocks);

void* allocator_alloc(allocator_t *allocator);

void allocator_dealloc(allocator_t *allocator, void *p);

uint32_t allocator_shrink(allocator_t *allocator);

void allocator_expand(allocator_t *allocator, uint32_t count);

void allocator_destroy(allocator_t *allocator);

