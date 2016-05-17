#include "chunk_pool.h"
#include <stdio.h>

void chunk_init(chunk_t *ch, uint32_t nr_blocks, uint32_t block_size)
{
  ch->nr_blocks = nr_blocks;
  ch->p_data = malloc(nr_blocks * block_size * sizeof(char));
  ch->p_md = malloc(nr_blocks * sizeof(chunk_md));

  dllist_init(&ch->free_chunks);

  uint32_t i = 0;
  for (i = 0; i < nr_blocks; ++i) {
    ch->p_md[i].index = i;
    dllist_iat(&ch->free_chunks, &(ch->p_md[i].link));
  }
  ch->count_free = nr_blocks;
}

void *chunk_alloc(chunk_t *ch, uint32_t block_size)
{
  dllist_link *tmp = dllist_rem_head(&ch->free_chunks);
  chunk_md *m = DLLIST_ELEMENT(tmp, chunk_md, link);
  ch->count_free--;

  return ch->p_data + m->index * block_size;
}

void chunk_dealloc(chunk_t *ch, void *p, uint32_t block_size)
{
  uint32_t index = ((char*)p - ch->p_data) / block_size;

  chunk_md *m = &ch->p_md[index];
  dllist_iat(&ch->free_chunks, &m->link);
  ch->count_free++;
}

void chunk_destroy(chunk_t *ch)
{
  free(ch->p_data);
  free(ch->p_md);
}

uint8_t chunk_empty(chunk_t *ch)
{
  return ch->count_free == 0;
}

uint8_t chunk_exists(chunk_t *ch, uint32_t block_size, void *p)
{
  char *end = ((char*)ch->p_data + ch->nr_blocks * block_size);
  char *s = (char*) p;
  return s >= ch->p_data && s < end;
}

uint32_t chunk_get_count(chunk_t *ch)
{
  return ch->count_free;
}

