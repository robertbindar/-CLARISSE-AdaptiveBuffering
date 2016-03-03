#pragma once
#include "stdint.h"

typedef struct
{
  uint64_t offset;
  uint32_t global_descr;
} cls_buf_handle_t;

typedef struct
{
  cls_buf_handle_t handle;
} cls_buf_t;
