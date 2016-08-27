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

error_code cls_get(cls_buffering_t *bufservice, const cls_buf_handle_t bh, const cls_size_t offset,
                   cls_byte_t *data, const cls_size_t count);

error_code cls_get_all(cls_buffering_t *bufservice, const cls_buf_handle_t bh, const cls_size_t offset,
                       cls_byte_t *data, const cls_size_t count, const cls_size_t nr_consumers);

error_code cls_put(cls_buffering_t *bufservice, const cls_buf_handle_t bh, const cls_size_t offset,
                   const cls_byte_t *data, const cls_size_t count);

error_code cls_put_all(cls_buffering_t *bufservice, const cls_buf_handle_t bh,
                       const cls_size_t offset, const cls_byte_t *data,
                       const cls_size_t count, const uint32_t nr_participants);

error_code cls_put_vector(cls_buffering_t *bufservice, const cls_buf_handle_t bh, const cls_size_t *offsetv,
                          const cls_size_t *countv, const cls_size_t vector_size, const cls_byte_t *data);

error_code cls_put_vector_all(cls_buffering_t *bufservice, const cls_buf_handle_t bh, const cls_size_t *offsetv,
                              const cls_size_t *countv, const cls_size_t vector_size, const cls_byte_t *data,
                              const uint32_t nr_participants);

error_code cls_get_vector(cls_buffering_t *bufservice, const cls_buf_handle_t bh, const cls_size_t *offsetv,
                          const cls_size_t *countv, const cls_size_t vector_size, cls_byte_t *data);

error_code cls_get_vector_all(cls_buffering_t *bufservice, const cls_buf_handle_t bh, const cls_size_t *offsetv,
                              const cls_size_t *countv, const cls_size_t vector_size, cls_byte_t *data,
                              const uint32_t nr_participants);

error_code cls_get_vector_noswap_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                                     const cls_size_t *countv, const cls_size_t vector_size, cls_byte_t **result,
                                     const uint32_t nr_consumers);

error_code cls_put_vector_noswap_all(cls_buffering_t *bufservice, const cls_buf_handle_t buf_handle, const cls_size_t *offsetv,
                                     const cls_size_t *countv, const cls_size_t vector_size, const cls_byte_t *data,
                                     const uint32_t nr_participants);

error_code cls_release_buf(cls_buffering_t *bufservice, cls_buf_handle_t buf_handle, uint32_t nr_participants);

error_code cls_destroy_buffering(cls_buffering_t *bufservice);

void print_buffers(cls_buffering_t *bufservice);

#if defined (__cplusplus)
}
#endif

