#pragma once

#include "buffering_types.h"
#include "errors.h"

error_code cls_init_buffering(cls_bufferpool_t *bufpool, cls_size_t bsize,
                              cls_size_t max_elems, 


error_code cls_get(cls_buf_handle_t bh, cls_byte_t *data);

error_code cls_put(cls_buf_handle_t bh, cls_size_t offset, const cls_byte_t *data,
                   cls_size_t count);

error_code cls_put_all(cls_buf_handle_t bh, cls_size_t offset, const cls_byte_t *data,
                       cls_size_t count, uint32_t participants);

