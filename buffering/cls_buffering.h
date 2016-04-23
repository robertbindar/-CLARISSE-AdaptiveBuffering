/* vim: set ts=8 sts=2 et sw=2: */

#pragma once

#include "buffering_types.h"
#include "buffer_scheduler.h"

typedef struct
{
  buffer_scheduler_t buf_sched;

  cls_size_t buffer_size;

  pthread_mutex_t lock;
  cls_buf_t *buffers;

  cls_size_t buffers_count;
} cls_buffering_t;

#include "errors.h"

#if defined (__cplusplus)
extern "C" {
#endif
error_code cls_init_buffering(cls_buffering_t *bufservice, cls_size_t bsize,
                              cls_size_t max_elems);
#if defined (__cplusplus)
}
#endif

#if defined (__cplusplus)
extern "C" {
#endif

error_code cls_get(cls_buffering_t *bufservice, cls_buf_handle_t bh, cls_size_t offset,
                   cls_byte_t *data, cls_size_t count, cls_size_t nr_consumers);

error_code cls_put(cls_buffering_t *bufservice, cls_buf_handle_t bh, cls_size_t offset,
                   const cls_byte_t *data, cls_size_t count);

error_code cls_put_all(cls_buffering_t *bufservice, cls_buf_handle_t bh,
                       cls_size_t offset, const cls_byte_t *data,
                       cls_size_t count, uint32_t participants);

error_code cls_destroy_buffering(cls_buffering_t *bufservice);

void print_buffers(cls_buffering_t *bufservice);

#if defined (__cplusplus)
}
#endif

