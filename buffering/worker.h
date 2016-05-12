/* vim: set ts=8 sts=2 et sw=2: */

#pragma once

#include "task_queue.h"

typedef struct
{
  pthread_t tid;
  task_queue_t *queue;
} worker_t;

void dispatch_worker(worker_t *w, task_queue_t *q);

void wait_worker_finished(worker_t *w);

void *process_tasks(void *arg);

