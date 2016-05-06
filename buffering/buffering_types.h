#pragma once

#include <stdint.h>
#include <pthread.h>
#include "uthash.h"
#include "list.h"

typedef char cls_byte_t;
typedef uint64_t cls_size_t;

typedef struct _cls_buf_handle
{
  cls_size_t offset;
  uint32_t global_descr;
} cls_buf_handle_t;

typedef struct _cls_buf
{
  cls_buf_handle_t handle;
  UT_hash_handle hh;

  cls_byte_t *data;

  pthread_mutex_t lock_write;
  uint32_t nr_coll_participants;

  pthread_mutex_t lock_read;
  uint8_t ready;
  pthread_cond_t buf_ready;

  uint32_t nr_consumers_finished;

  uint8_t is_swapped;
  uint8_t freed_by_swapper;
  uint8_t was_swapped_in;
  pthread_rwlock_t rwlock_swap;
  dllist_link link_mru;

} cls_buf_t;

