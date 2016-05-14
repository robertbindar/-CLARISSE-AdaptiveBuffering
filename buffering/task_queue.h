/* vim: set ts=8 sts=2 et sw=2: */

#pragma once

#include <stdint.h>
#include <pthread.h>
#include "list.h"
#include "buffer_allocator.h"
#include "buffering_types.h"

typedef struct _buffer_scheduler buffer_scheduler_t;

typedef enum
{
  TASK_DETACHED = 0,
  TASK_OWN = 1
} task_ownership;

typedef struct
{
  dllist queue;
  uint64_t task_count;
  pthread_cond_t queue_empty;
  pthread_mutex_t lock;
  allocator_t allocator;
} task_queue_t;

typedef void (*callback_t)(void*);

typedef struct
{
  uint8_t worker_done;

  callback_t cb;

  task_ownership own;

  dllist_link link;

  buffer_scheduler_t *bufsched;
  cls_buf_t *buffer;
} task_t;

void task_queue_init(task_queue_t *tq, uint32_t initial_size);

void submit_task(task_queue_t *tq, task_t *t);

task_t *get_task(task_queue_t *tq);

task_t *create_task(task_queue_t *tq, callback_t cb, task_ownership own);

void destroy_task(task_queue_t *tq, task_t *t);

void task_queue_destroy(task_queue_t *tq);

