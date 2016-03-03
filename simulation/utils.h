#pragma once

#include "mpi.h"
#include "stdio.h"
#include "buffer_pool_types.h"

#define OP_SIZE 10
#define MAX_DATA 100

static void handle_err(int errcode, char *str)
{
  char msg[MPI_MAX_ERROR_STRING];
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int32_t namelen, resultlen, my_rank;

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Get_processor_name(processor_name,&namelen);
  MPI_Error_string(errcode, msg, &resultlen);
  fprintf(stderr,"Process %d on %s %s: %s\nStack trace:\n", my_rank, processor_name,str, msg);
  MPI_Abort(MPI_COMM_WORLD, 1);
}

typedef struct
{
  cls_buf_handle_t handle;
} cls_op_get_t;

typedef struct
{
  cls_buf_handle_t handle;
  uint64_t count;
  char data[MAX_DATA];
} cls_op_put_t;

typedef struct
{
  uint16_t status;
  char data[MAX_DATA];
} cls_get_result_t;

typedef struct
{
  uint16_t status;
} cls_put_result_t;

