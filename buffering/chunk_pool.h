#pragma once

#include <stdint.h>
#include "list.h"

typedef struct _chunk_md
{
  uint32_t index;
  dllist_link link;
} chunk_md;

typedef struct _chunk
{
  char *p_data;
  chunk_md *p_md;
  dllist free_chunks;
  uint32_t count_free;
  uint32_t nr_blocks;

  dllist_link link;
} chunk_t;

void chunk_init(chunk_t *ch, uint32_t nr_blocks, uint32_t block_size);

void *chunk_alloc(chunk_t *ch, uint32_t block_size);

void chunk_dealloc(chunk_t *ch, void *p, uint32_t block_size);

void chunk_destroy(chunk_t *ch);

uint8_t chunk_empty(chunk_t *ch);

uint8_t chunk_exists(chunk_t *ch, uint32_t block_size, void *p);

uint32_t chunk_get_count(chunk_t *ch);

