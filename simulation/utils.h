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
  uint64_t nr_participants;
  uint64_t quit;
} cls_op_get_t;

typedef struct
{
  char data[MAX_DATA];
  cls_buf_handle_t handle;
  uint64_t count;
  uint64_t offset;
  uint64_t nr_participants;
  uint64_t quit;
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

static MPI_Datatype mpi_bufhandle_t;
static MPI_Datatype mpi_op_get_t;
static MPI_Datatype mpi_op_put_t;
static MPI_Datatype mpi_get_result_t;
static MPI_Datatype mpi_put_result_t;

static void build_types()
{
  MPI_Datatype oldtypes[6];
  int blockcounts[6];
  MPI_Aint offsets[6], extent;

  // cls_buf_handle_t
  offsets[0] = 0;
  oldtypes[0] = MPI_UINT64_T;
  blockcounts[0] = 1;
  MPI_Type_extent(MPI_UINT64_T, &extent);
  offsets[1] = extent;
  oldtypes[1] = MPI_UINT32_T;
  blockcounts[1] = 1;
  offsets[2] = sizeof(cls_buf_handle_t);
  oldtypes[2] = MPI_UB;
  blockcounts[2] = 1;
  MPI_Type_create_struct(3, blockcounts, offsets, oldtypes, &mpi_bufhandle_t);
  MPI_Type_commit(&mpi_bufhandle_t);

  // cls_op_get_t
  offsets[0] = 0;
  oldtypes[0] = mpi_bufhandle_t;
  blockcounts[0] = 1;
  MPI_Type_extent(mpi_bufhandle_t, &extent);
  offsets[1] = extent;
  oldtypes[1] = MPI_UINT64_T;
  blockcounts[1] = 4;
  offsets[2] = sizeof(cls_op_get_t);
  oldtypes[2] = MPI_UB;
  blockcounts[2] = 1;
  MPI_Type_create_struct(3, blockcounts, offsets, oldtypes, &mpi_op_get_t);
  MPI_Type_commit(&mpi_op_get_t);

  // cls_op_put_t
  offsets[0] = 0;
  oldtypes[0] = MPI_CHAR;
  blockcounts[0] = MAX_DATA;
  MPI_Type_extent(MPI_CHAR, &extent);
  offsets[1] = MAX_DATA * extent;
  oldtypes[1] = mpi_bufhandle_t;
  blockcounts[1] = 1;
  MPI_Type_extent(mpi_bufhandle_t, &extent);
  offsets[2] = offsets[1] + extent;
  oldtypes[2] = MPI_UINT64_T;
  blockcounts[2] = 4;
  offsets[3] = sizeof(cls_op_put_t);
  oldtypes[3] = MPI_UB;
  blockcounts[3] = 1;
  MPI_Type_create_struct(4, blockcounts, offsets, oldtypes, &mpi_op_put_t);
  MPI_Type_commit(&mpi_op_put_t);

  // cls_get_result_t
  offsets[0] = 0;
  oldtypes[0] = MPI_CHAR;
  blockcounts[0] = MAX_DATA;
  MPI_Type_extent(MPI_CHAR, &extent);
  offsets[1] = MAX_DATA * extent;
  oldtypes[1] = MPI_UINT16_T;
  blockcounts[1] = 1;
  MPI_Type_extent(MPI_UINT16_T, &extent);
  offsets[2] = offsets[1] + extent;
  oldtypes[2] = MPI_DOUBLE;
  blockcounts[2] = 2;
  offsets[3] = sizeof(cls_get_result_t);
  oldtypes[3] = MPI_UB;
  blockcounts[3] = 1;
  MPI_Type_create_struct(4, blockcounts, offsets, oldtypes, &mpi_get_result_t);
  MPI_Type_commit(&mpi_get_result_t);

  // cls_put_result_t
  offsets[0] = 0;
  oldtypes[0] = MPI_UINT32_T;
  blockcounts[0] = 1;
  MPI_Type_extent(MPI_UINT32_T, &extent);
  offsets[1] = extent;
  oldtypes[1] = MPI_DOUBLE;
  blockcounts[1] = 2;
  offsets[2] = sizeof(cls_put_result_t);
  oldtypes[2] = MPI_UB;
  blockcounts[2] = 1;
  MPI_Type_create_struct(3, blockcounts, offsets, oldtypes, &mpi_put_result_t);
  MPI_Type_commit(&mpi_put_result_t);
}

static void destroy_types()
{
  MPI_Type_free(&mpi_bufhandle_t);
  MPI_Type_free(&mpi_op_get_t);
  MPI_Type_free(&mpi_op_put_t);
  MPI_Type_free(&mpi_get_result_t);
  MPI_Type_free(&mpi_put_result_t);
}
