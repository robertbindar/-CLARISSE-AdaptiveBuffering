/* vim: set ts=8 sts=2 et sw=2: */

#include "task_queue.h"
#include "buffer_scheduler.h"

void task_queue_init(task_queue_t *tq, uint32_t initial_size)
{
  pthread_mutex_init(&tq->lock, NULL);
  pthread_cond_init(&tq->queue_empty, NULL);
  tq->task_count = 0;
  allocator_init(&tq->allocator, sizeof(task_t), initial_size);
  dllist_init(&tq->queue);
}

void submit_task(task_queue_t *tq, task_t *t)
{
  pthread_mutex_lock(&tq->lock);
  dllist_iat(&tq->queue, &t->link);
  tq->task_count++;
  pthread_cond_broadcast(&tq->queue_empty);
  pthread_mutex_unlock(&tq->lock);
}

task_t *get_task(task_queue_t *tq)
{
  pthread_mutex_lock(&tq->lock);
  while (tq->task_count == 0) {
    pthread_cond_wait(&tq->queue_empty, &tq->lock);
  }

  tq->task_count--;
  dllist_link *tmp = dllist_rem_head(&tq->queue);
  task_t *t = DLLIST_ELEMENT(tmp, task_t, link);

  pthread_mutex_unlock(&tq->lock);

  return t;
}

task_t *create_task(task_queue_t *tq, callback_t cb, task_ownership own)
{
  task_t *t = (task_t*) allocator_alloc(&tq->allocator);
  t->cb = cb;
  t->worker_done = 0;
  t->own = own;
  t->buffer = NULL;
  t->bufsched = NULL;

  t->task_finished = 0;
  pthread_cond_init(&t->sync, NULL);
  pthread_mutex_init(&t->lock, NULL);

  return t;
}

void destroy_task(task_queue_t *tq, task_t *t)
{
  allocator_dealloc(&tq->allocator, (void*) t);
}

void wait_task(task_t *t)
{
  pthread_mutex_lock(&t->lock);
  while (!t->task_finished) {
    pthread_cond_wait(&t->sync, &t->lock);
  }
  pthread_mutex_unlock(&t->lock);

  destroy_task(&t->bufsched->task_queue, t);
}

void task_queue_destroy(task_queue_t *tq)
{
  pthread_mutex_destroy(&tq->lock);
  pthread_cond_destroy(&tq->queue_empty);
  allocator_destroy(&tq->allocator);
}

