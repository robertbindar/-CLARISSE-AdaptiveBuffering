#include "server.h"
#include "stdio.h"
#include "listener.h"
#include "stdlib.h"
#include "utils.h"

void server(MPI_Comm intercomm_producer, MPI_Comm intercomm_consumer)
{
  int32_t rank, nprocs, server_rank, server_nprocs;
  int32_t producer_nprocs, consumer_nprocs, i;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(intercomm_producer, &server_rank);
  MPI_Comm_size(intercomm_producer, &server_nprocs);
  MPI_Comm_remote_size(intercomm_producer, &producer_nprocs);
  MPI_Comm_remote_size(intercomm_consumer, &consumer_nprocs);


  listener_t *listeners = calloc(producer_nprocs + consumer_nprocs, sizeof(listener_t));
  uint32_t k = 0;
  for (i = 0; i < producer_nprocs; ++i, ++k) {
    listener_init(&listeners[k], i, producer_handler, intercomm_producer);
    dispatch_listener(&listeners[k]);
  }

  for (i = 0; i < consumer_nprocs; ++i, ++k) {
    listener_init(&listeners[k], i, consumer_handler, intercomm_consumer);
    dispatch_listener(&listeners[k]);
  }

  for (i = 0; i < k; ++i) {
    wait_listener(&listeners[i]);
  }

  free(listeners);
}

void *producer_handler(void *arg)
{
  listener_t *lst = (listener_t*) arg;

  cls_op_put_t op_put;
  while (1) {
    MPI_Recv(&op_put, sizeof(cls_op_put_t), MPI_CHAR, lst->rank, 0, lst->communicator,
             MPI_STATUS_IGNORE);

    // TODO: exec cls_put

    cls_put_result_t result;

    MPI_Send(&result, sizeof(cls_put_result_t), MPI_CHAR, lst->rank, 0,
             lst->communicator);
  }

  pthread_exit(NULL);
}

void *consumer_handler(void *arg)
{
  listener_t *lst = (listener_t*) arg;

  cls_op_get_t op_get;
  while (1) {
    MPI_Recv(&op_get, sizeof(cls_op_get_t), MPI_CHAR, lst->rank, 0, lst->communicator,
             MPI_STATUS_IGNORE);

    // TODO: exec cls_get

    cls_get_result_t result;

    MPI_Send(&result, sizeof(cls_get_result_t), MPI_CHAR, lst->rank, 0,
             lst->communicator);
  }

  pthread_exit(NULL);
}
