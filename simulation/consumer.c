#include "consumer.h"
#include "stdio.h"
#include "utils.h"
#include "string.h"
#include "stdlib.h"
#include "unistd.h"


void consumer(MPI_Comm intercomm_server)
{
  int32_t rank, nprocs, intercomm_server_rank, intercomm_server_nprocs, server_nprocs;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(intercomm_server, &intercomm_server_rank);
  MPI_Comm_size(intercomm_server, &intercomm_server_nprocs);
  MPI_Comm_remote_size(intercomm_server, &server_nprocs);

  cls_buf_handle_t handle;
  handle.global_descr = 0;
  handle.offset = 0;

  cls_op_get_t get;
  get.handle = handle;

  cls_get_result_t result;

  while (1) {
    MPI_Send(&get, sizeof(cls_op_get_t), MPI_CHAR, 0, 0, intercomm_server);

    MPI_Recv(&result, sizeof(cls_get_result_t), MPI_CHAR, 0, 0, intercomm_server,
             MPI_STATUS_IGNORE);

    sleep(2);
  }
}

