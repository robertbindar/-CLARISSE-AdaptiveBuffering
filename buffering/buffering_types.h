#pragma once
#include <stdint.h>
#include <pthread.h>
#include "uthash.h"
#include "list.h"
#include "buffer_scheduler.h"

typedef char cls_byte_t;
typedef uint64_t cls_size_t;

typedef struct
{
  cls_size_t offset;
  uint32_t global_descr;
} cls_buf_handle_t;

typedef struct
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
} cls_buf_t;

typedef struct
{
  buffer_scheduler_t buf_sched;

  cls_size_t buffer_size;

  pthread_mutex_t lock;
  cls_buf_t *buffers;

  cls_size_t buffers_count;
} cls_buffering_t;

