/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_scheduler.h"

error_code sched_init(buffer_scheduler_t *bufsched, uint64_t buffer_size)
{
  bufsched->buffer_size = buffer_size;

  return BUFFERING_SUCCESS;
}
error_code sched_destroy(buffer_scheduler_t *bufsched)
{
  return BUFFERING_SUCCESS;
}
error_code sched_alloc(buffer_scheduler_t *bufsched, char **buffer)
{
  *buffer = malloc(bufsched->buffer_size * sizeof(char));

  return BUFFERING_SUCCESS;
}
error_code sched_free(buffer_scheduler_t *bufsched, char *buffer)
{
  free(buffer);

  return BUFFERING_SUCCESS;
}
