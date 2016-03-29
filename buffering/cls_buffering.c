#include "cls_buffering.h"
#include <assert.h>

error_code cls_get(cls_buf_handle_t bh, cls_byte_t *data)
{

}

error_code cls_put(cls_buf_handle_t bh, cls_size_t offset, const cls_byte_t *data,
                   cls_size_t count)
{
  if (!data) {
    return BUFFERING_INVALIDARGS;
  }

}

error_code cls_put_all(cls_buf_handle_t bh, cls_size_t offset, const cls_byte_t *data,
                       cls_size_t count, uint32_t participants)
{

}

