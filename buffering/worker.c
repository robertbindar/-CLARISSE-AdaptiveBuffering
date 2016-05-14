/* vim: set ts=8 sts=2 et sw=2: */

#include "worker.h"

void dispatch_worker(worker_t *w, task_queue_t *q)
{
  w->queue = q;

  pthread_create(&w->tid, NULL, process_tasks, (void*) w);
}

void wait_worker_finished(worker_t *w)
{
  task_t *t = create_task(w->queue, NULL, TASK_DETACHED);
  t->worker_done = 1;
  submit_task(w->queue, t);

  pthread_join(w->tid, NULL);
}

void *process_tasks(void *arg)
{
  worker_t *w = (worker_t*) arg;

  while (1) {
    task_t *t = get_task(w->queue);

    if (t->worker_done) {
      destroy_task(w->queue, t);
      return NULL;
    }

    t->cb(t);

    destroy_task(w->queue, t);
  }
}

