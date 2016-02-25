#include "server.h"
#include "stdio.h"


void server(MPI_Comm intercomm_producer, MPI_Comm intercomm_consumer){
  char str[3];
  int rank, nprocs, server_rank, server_nprocs, producer_nprocs, consumer_nprocs, i, j;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(intercomm_producer, &server_rank);
  MPI_Comm_size(intercomm_producer, &server_nprocs);
  MPI_Comm_remote_size(intercomm_producer, &producer_nprocs);
  MPI_Comm_remote_size(intercomm_consumer, &consumer_nprocs);
  printf("Server %d from %d with world rank=%d world nprocs=%d\n", server_rank, server_nprocs, rank, nprocs);
  for (i = 0; i < producer_nprocs; i++) {
    MPI_Recv(str, 3, MPI_CHAR, i, 0, intercomm_producer, MPI_STATUS_IGNORE);
    printf("Server %d received from producer: %s\n", server_rank , str);
    fflush(stdout);
    for (j = 0; j < consumer_nprocs; j++)
      MPI_Send(str, 3, MPI_CHAR, j, 0, intercomm_consumer);
  }
}
