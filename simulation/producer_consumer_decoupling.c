#include "mpi.h"
#include "stdio.h"
#include "string.h"
#include "stdlib.h"
#include "sys/stat.h"
#include "sys/types.h"
#include "fcntl.h"
#include "unistd.h"
#include "consumer.h"
#include "producer.h"
#include "server.h"
#include "utils.h"

#define DEFAULT_PRODUCERS_NUMBER 1
#define DEFAULT_CONSUMERS_NUMBER 1
#define DEFAULT_SERVERS_NUMBER   1

int main(int argc, char **argv)
{
  int rank, nprocs, membership_key;
  MPI_Comm comm;

  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  int nr_producers = DEFAULT_PRODUCERS_NUMBER;
  int nr_consumers = DEFAULT_CONSUMERS_NUMBER;
  int nr_servers = DEFAULT_SERVERS_NUMBER;

  char *np = getenv("BUFFERING_NUMBER_OF_PRODUCERS");
  char *nc = getenv("BUFFERING_NUMBER_OF_CONSUMERS");
  char *ns = getenv("BUFFERING_NUMBER_OF_SERVERS");

  if (np && nc && ns) {
    sscanf(np, "%d", &nr_producers);
    sscanf(nc, "%d", &nr_consumers);
    sscanf(ns, "%d", &nr_servers);
  } else {
    printf("Warning: Default decoupling values will be used\n");
  }

  membership_key = ((rank / nr_producers) != 0) +
                   ((rank / (nr_producers + nr_servers)) != 0) +
                   ((rank / (nr_producers + nr_servers + nr_consumers)) != 0);

  // Build intra-communicator for local sub-group
  MPI_Comm_split(MPI_COMM_WORLD, membership_key, rank, &comm);

  switch (membership_key) {
  case 1: {
    MPI_Comm intercomm_producer, intercomm_consumer;
    int err;
    // local_intra, local group leader, peer comm, remote leader, tag, new intercomm
    err = MPI_Intercomm_create(comm, 0, MPI_COMM_WORLD, 0, 1, &intercomm_producer);
    if (err != MPI_SUCCESS)
      handle_err(err, "MPI_Intercomm_create in server\n");
    err = MPI_Intercomm_create(comm, 0, MPI_COMM_WORLD, nr_producers + nr_servers, 1, &intercomm_consumer);
    if (err != MPI_SUCCESS)
      handle_err(err, "MPI_Intercomm_create in server\n");

    server(intercomm_producer, intercomm_consumer);

    MPI_Comm_free(&intercomm_producer);
    MPI_Comm_free(&intercomm_consumer);
    break;
  }
  case 0: {
    MPI_Comm intercomm_server;
    int err;

    err = MPI_Intercomm_create(comm, 0, MPI_COMM_WORLD, nr_producers, 1, &intercomm_server);
    if (err != MPI_SUCCESS)
      handle_err(err, "MPI_Intercomm_create in proucer\n");

    producer(intercomm_server);

    MPI_Comm_free(&intercomm_server);
    break;
  }
  case 2: {
    MPI_Comm intercomm_server;
    int err;

    err = MPI_Intercomm_create(comm, 0, MPI_COMM_WORLD, nr_producers, 1, &intercomm_server);
    if (err != MPI_SUCCESS)
      handle_err(err, "MPI_Intercomm_create in proucer\n");

    consumer(intercomm_server);

    MPI_Comm_free(&intercomm_server);
    break;
  }
  }

  MPI_Comm_free(&comm);
  MPI_Finalize();

  return 0;
}

