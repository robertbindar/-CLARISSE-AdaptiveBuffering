#pragma once

#include "mpi.h"
#include "stdio.h"
#include "buffering_types.h"

#define MAX_DATA 1024

void handle_err(int errcode, char *str);

typedef struct
{
  cls_buf_handle_t handle;
  uint64_t count;
  uint64_t offset;
  uint32_t nr_participants;
  uint32_t quit;
} cls_op_get_t;

typedef struct
{
  char data[MAX_DATA];
  cls_buf_handle_t handle;
  uint64_t count;
  uint64_t offset;
  uint32_t nr_participants;
  uint32_t quit;
} cls_op_put_t;

typedef struct
{
  char data[MAX_DATA];
  uint16_t status;
  double time;
} cls_get_result_t;

typedef struct
{
  uint32_t status;
  double time;
} cls_put_result_t;

