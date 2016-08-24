#pragma once

#include "mpi.h"
#include "stdio.h"
#include "buffering_types.h"

void handle_err(int errcode, char *str);

typedef struct
{
  cls_buf_handle_t handle;
  uint64_t count;
  uint8_t quit;
} cls_op_put_t;

typedef struct
{
  cls_buf_handle_t handle;
  uint64_t count;
  uint8_t quit;
} cls_op_get_t;

typedef struct
{
  uint64_t offset;
  uint64_t count;
  uint8_t quit;
} cls_task_t;

