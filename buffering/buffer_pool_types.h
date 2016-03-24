#pragma once
#include "stdint.h"

typedef char cls_byte_t;
typedef uint64_t cls_size_t;

typedef struct
{
  cls_size_t offset;
  uint32_t global_descr;
} cls_buf_handle_t;

typedef struct
{
  cls_buf_handle_t handle;
} cls_buf_t;


