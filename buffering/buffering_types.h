#pragma once

#include <stdint.h>
#include <pthread.h>
#include "uthash.h"
#include "list.h"

typedef char cls_byte_t;
typedef uint64_t cls_size_t;

typedef enum
{
  BUF_ALLOCATED = 0,
  BUF_UPDATED,
  BUF_SWAPPED_OUT,
  BUF_QUEUED_SWAPIN,
  BUF_RELEASED
} buffer_state_t;

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

  uint32_t nr_consumers_finished;
  uint32_t nr_coll_participants;

  pthread_mutex_t lock;
  buffer_state_t state;
  pthread_cond_t cond_state;

  pthread_rwlock_t rwlock_swap;

  dllist_link link_mru;
} cls_buf_t;
