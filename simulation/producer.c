#include "producer.h"
#include "stdio.h"



void producer(MPI_Comm intercomm_server){
  char str[] = "hi";
  int rank, nprocs, intercomm_server_rank, intercomm_server_nprocs, server_nprocs, i;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(intercomm_server, &intercomm_server_rank);
  MPI_Comm_size(intercomm_server, &intercomm_server_nprocs);
  MPI_Comm_remote_size(intercomm_server, &server_nprocs);
  printf("Producer %d from %d with world rank=%d world nprocs=%d\n", intercomm_server_rank, intercomm_server_nprocs, rank, nprocs);
  for (i = 0; i < server_nprocs; i++)
      MPI_Send(str, 3, MPI_CHAR, i, 0, intercomm_server);
}

