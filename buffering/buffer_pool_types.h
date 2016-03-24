#pragma once
#include <stdint.h>
#include <pthread.h>
#include "uthash.h"
#include "list.h"

typedef char cls_byte_t;
typedef uint64_t cls_size_t;

typedef enum
{
  UNALLOCATED = -1,
  READY,
  BLOCKED_READ,
  BLOCKED_WRITE,
  SWAPPED
} buffer_state_t;

typedef struct
{
  cls_size_t offset;
  uint32_t global_descr;
} cls_buf_handle_t;

typedef struct
{
  buffer_state_t state;

  cls_buf_handle_t handle;

  cls_byte_t *data;

  UT_hash_handle hh;

  pthread_mutex_t lock;

  uint8_t busy;
  pthread_cond_t buf_busy;

  uint32_t nr_consumers_waiting;
  uint32_t nr_coll_participants;
} cls_buf_t;

typedef struct
{
  cls_size_t buffer_size;

  cls_size_t buffers_count;

  cls_buf_t *buffers;

  pthread_mutex_t lock;
} cls_bufferpool_t;

